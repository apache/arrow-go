// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cdc implements content-defined chunking, which splits Parquet data
// pages at boundaries derived from the data itself, so identical content yields
// identical pages even when it is written at different offsets.
package cdc

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/debug"
)

// Chunk is a run of consecutive levels, and the value slots they cover, that
// belong in the same data page.
type Chunk struct {
	LevelOffset int64
	ValueOffset int64
	NumLevels   int64
	NumValues   int64
}

// Chunker splits a column's data into content-defined chunks.
type Chunker struct {
	minSize int64
	maxSize int64
	mask    uint64

	// The state below is column-global: it is never reset between calls, so
	// chunking can continue across successive writes.
	rollingHash uint64
	nthRun      int
	chunkSize   int64
	hasMatched  bool
}

// NewChunker validates opts and returns a Chunker that uses them.
func NewChunker(opts parquet.CDCOptions) (*Chunker, error) {
	mask, err := calculateMask(opts.MinChunkSize, opts.MaxChunkSize, opts.NormLevel)
	if err != nil {
		return nil, err
	}
	return &Chunker{
		minSize: opts.MinChunkSize,
		maxSize: opts.MaxChunkSize,
		mask:    mask,
	}, nil
}

// calculateMask derives the boundary mask from the chunk size bounds, aiming
// for an average chunk size of (minSize+maxSize)/2 after skipping minSize bytes.
func calculateMask(minSize, maxSize int64, normLevel int) (uint64, error) {
	if minSize < 0 {
		return 0, fmt.Errorf("%w: cdc: min chunk size must be non-negative, got %d", arrow.ErrInvalid, minSize)
	}
	if maxSize <= minSize {
		return 0, fmt.Errorf("%w: cdc: max chunk size must be greater than min chunk size, got min %d and max %d", arrow.ErrInvalid, minSize, maxSize)
	}
	// Written as minSize + half the span so that large bounds cannot overflow.
	avgSize := minSize + (maxSize-minSize)/2
	target := uint64((avgSize - minSize) / numGearTables)
	maskBits := max(bits.Len64(target)-1, 0)
	effectiveBits := maskBits - normLevel
	if effectiveBits < 1 || effectiveBits > 63 {
		return 0, fmt.Errorf("%w: cdc: the number of bits in the CDC mask must be between 1 and 63, got %d", arrow.ErrInvalid, effectiveBits)
	}
	return ^uint64(0) << (64 - effectiveBits), nil
}

// roll feeds the bytes of one value into the rolling hash. While the current
// chunk is still below the minimum size the hash is left untouched.
func (c *Chunker) roll(value []byte) {
	c.chunkSize += int64(len(value))
	if c.chunkSize < c.minSize {
		return
	}
	table := &gearTables[c.nthRun]
	hash, mask, matched := c.rollingHash, c.mask, c.hasMatched
	for _, b := range value {
		hash = hash<<1 + table[b]
		matched = matched || hash&mask == 0
	}
	c.rollingHash, c.hasMatched = hash, matched
}

// rollLevel feeds a definition or repetition level, as 2 little-endian bytes.
func (c *Chunker) rollLevel(level int16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(level))
	c.roll(buf[:])
}

// needNewChunk reports whether a chunk boundary should be placed before the
// value that was just rolled. A boundary needs the mask to match numGearTables
// times, each time with a different gear table, which approximates a normal
// distribution of chunk sizes. The maximum chunk size is a hard limit. The
// rolling hash is deliberately never reset.
func (c *Chunker) needNewChunk() bool {
	if c.hasMatched {
		c.hasMatched = false
		c.nthRun++
		if c.nthRun >= numGearTables {
			c.nthRun = 0
			c.chunkSize = 0
			return true
		}
	}
	if c.chunkSize >= c.maxSize {
		c.chunkSize = 0
		return true
	}
	return false
}

// valueRoller returns a function that rolls the value in slot i of values.
func (c *Chunker) valueRoller(values arrow.Array) (func(i int64), error) {
	switch arr := values.(type) {
	case *array.Null:
		return func(int64) {}, nil
	case *array.Boolean:
		return func(i int64) {
			if arr.Value(int(i)) {
				c.roll([]byte{1})
			} else {
				c.roll([]byte{0})
			}
		}, nil
	case *array.Dictionary:
		return c.valueRoller(arr.Indices())
	case array.ExtensionArray:
		return c.valueRoller(arr.Storage())
	case *array.String:
		return func(i int64) { c.rollString(arr.Value(int(i))) }, nil
	case *array.LargeString:
		return func(i int64) { c.rollString(arr.Value(int(i))) }, nil
	case *array.StringView:
		return func(i int64) { c.rollString(arr.Value(int(i))) }, nil
	case *array.Binary:
		return func(i int64) { c.roll(arr.Value(int(i))) }, nil
	case *array.LargeBinary:
		return func(i int64) { c.roll(arr.Value(int(i))) }, nil
	case *array.BinaryView:
		return func(i int64) { c.roll(arr.Value(int(i))) }, nil
	}

	fixed, ok := values.DataType().(arrow.FixedWidthDataType)
	if !ok || fixed.BitWidth()%8 != 0 {
		return nil, fmt.Errorf("cdc: unsupported type %s for content-defined chunking", values.DataType())
	}
	width := fixed.BitWidth() / 8
	var data []byte
	if buf := values.Data().Buffers()[1]; buf != nil {
		data = buf.Bytes()[values.Data().Offset()*width:]
	}
	return func(i int64) {
		start := int(i) * width
		c.roll(data[start : start+width])
	}, nil
}

func (c *Chunker) rollString(s string) {
	c.roll(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// Chunks splits the levels (and the value slots they cover) of one column write
// into content-defined chunks. The chunker state persists across calls, so a
// column written in several calls is split as if it were written in one.
//
// values holds the leaf values of the write: one slot per level for flat
// columns, and one slot per level with def >= levels.RepeatedAncestorDefLevel
// for repeated columns. A boundary at the very first level yields an empty first
// chunk, which closes the page left open by the previous call; no other chunk
// is empty.
func (c *Chunker) Chunks(levels file.LevelInfo, defLevels, repLevels []int16, values arrow.Array) ([]Chunk, error) {
	rollValue, err := c.valueRoller(values)
	if err != nil {
		return nil, err
	}

	numLevels := int64(values.Len())
	if levels.DefLevel > 0 {
		numLevels = int64(len(defLevels))
	}

	var (
		chunks          []Chunk
		prevOffset      int64
		prevValueOffset int64
		numValues       = numLevels
	)
	switch {
	case levels.RepLevel == 0 && levels.DefLevel == 0:
		for offset := int64(0); offset < numLevels; offset++ {
			rollValue(offset)
			if c.needNewChunk() {
				chunks = append(chunks, Chunk{prevOffset, prevOffset, offset - prevOffset, offset - prevOffset})
				prevOffset = offset
			}
		}
		prevValueOffset = prevOffset
	case levels.RepLevel == 0:
		for offset := int64(0); offset < numLevels; offset++ {
			def := defLevels[offset]
			c.rollLevel(def)
			if def == levels.DefLevel {
				rollValue(offset)
			}
			if c.needNewChunk() {
				chunks = append(chunks, Chunk{prevOffset, prevOffset, offset - prevOffset, offset - prevOffset})
				prevOffset = offset
			}
		}
		prevValueOffset = prevOffset
	default:
		var valueOffset int64
		for offset := int64(0); offset < numLevels; offset++ {
			def, rep := defLevels[offset], repLevels[offset]
			c.rollLevel(def)
			c.rollLevel(rep)
			if def == levels.DefLevel {
				rollValue(valueOffset)
			}
			// Cut only at record boundaries.
			if rep == 0 && c.needNewChunk() {
				chunks = append(chunks, Chunk{prevOffset, prevValueOffset, offset - prevOffset, valueOffset - prevValueOffset})
				prevOffset, prevValueOffset = offset, valueOffset
			}
			if def >= levels.RepeatedAncestorDefLevel {
				valueOffset++
			}
		}
		numValues = valueOffset
	}

	if prevOffset < numLevels {
		chunks = append(chunks, Chunk{prevOffset, prevValueOffset, numLevels - prevOffset, numValues - prevValueOffset})
	}
	debug.Assert(validateChunks(chunks, numLevels, numValues), "cdc: invalid chunks")
	return chunks, nil
}

// validateChunks checks that chunks are contiguous in levels and values, that
// only the first chunk is empty, and that they cover exactly numLevels levels
// and numValues value slots.
func validateChunks(chunks []Chunk, numLevels, numValues int64) bool {
	var levelOffset, valueOffset int64
	for i, ch := range chunks {
		if ch.LevelOffset != levelOffset || ch.ValueOffset != valueOffset ||
			ch.NumValues < 0 || ch.NumValues > ch.NumLevels || ch.NumLevels < 0 || (i > 0 && ch.NumLevels == 0) {
			return false
		}
		levelOffset += ch.NumLevels
		valueOffset += ch.NumValues
	}
	return levelOffset == numLevels && valueOffset == numValues
}
