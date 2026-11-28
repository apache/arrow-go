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

//go:generate go run gearhash_gen.go

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/endian"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
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

	// state is column-global: it is never reset between calls, so chunking can
	// continue across successive writes.
	state state
}

// state is the part of the chunker that changes while rolling. The chunking
// loops work on a local copy that is passed to the helpers by value, which lets
// the compiler keep it in registers: it must keep at most 4 fields and never
// have its address taken.
type state struct {
	hash    uint64
	nthRun  int
	size    int64
	matched bool
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
	target := uint64((maxSize - minSize) / 2 / numGearTables)
	maskBits := max(bits.Len64(target)-1, 0)
	effectiveBits := maskBits - normLevel
	if effectiveBits < 1 || effectiveBits > 63 {
		return 0, fmt.Errorf("%w: cdc: the number of bits in the CDC mask must be between 1 and 63, got %d", arrow.ErrInvalid, effectiveBits)
	}
	return ^uint64(0) << (64 - effectiveBits), nil
}

// roll feeds the bytes of one value into the rolling hash. While the current
// chunk is still below the minimum size the hash is left untouched.
func (c *Chunker) roll(s state, value []byte) state {
	s.size += int64(len(value))
	if s.size < c.minSize {
		return s
	}
	table, mask := &gearTables[s.nthRun], c.mask
	for _, b := range value {
		s.hash = s.hash<<1 + table[b]
		s.matched = s.matched || s.hash&mask == 0
	}
	return s
}

// rollLevel feeds a definition or repetition level, as 2 little-endian bytes.
func (c *Chunker) rollLevel(s state, level int16) state {
	s.size += 2
	if s.size < c.minSize {
		return s
	}
	table := &gearTables[s.nthRun]
	h1 := s.hash<<1 + table[byte(level)]
	s.hash = h1<<1 + table[byte(uint16(level)>>8)]
	s.matched = s.matched || h1&c.mask == 0 || s.hash&c.mask == 0
	return s
}

// needNewChunk reports whether a chunk boundary should be placed before the
// value that was just rolled. A boundary needs the mask to match numGearTables
// times, each time with a different gear table, which approximates a normal
// distribution of chunk sizes. The maximum chunk size is a hard limit. The
// rolling hash is deliberately never reset.
func (c *Chunker) needNewChunk(s state) (state, bool) {
	if s.matched {
		s.matched = false
		s.nthRun++
		if s.nthRun >= numGearTables {
			s.nthRun = 0
			s.size = 0
			return s, true
		}
	}
	if s.size >= c.maxSize {
		s.size = 0
		return s, true
	}
	return s, false
}

// chunkList collects the chunks of one Chunks call.
type chunkList struct {
	chunks          []Chunk
	prevOffset      int64
	prevValueOffset int64
}

// cut ends the current chunk before the given level and value slot. A cut at
// the very first level yields an empty chunk.
func (l *chunkList) cut(offset, valueOffset int64) {
	l.chunks = append(l.chunks, Chunk{l.prevOffset, l.prevValueOffset, offset - l.prevOffset, valueOffset - l.prevValueOffset})
	l.prevOffset, l.prevValueOffset = offset, valueOffset
}

// finish ends the last chunk, unless it would be empty, and returns the chunks.
func (l *chunkList) finish(numLevels, numValues int64) []Chunk {
	if l.prevOffset < numLevels {
		l.cut(numLevels, numValues)
	}
	return l.chunks
}

// valueSource gives access to the values of one Chunks call. Fixed-width values
// of 1, 2, 4 or 8 bytes are read as words, so that the chunking loops can roll
// them inline; every other type is rolled by the roll function.
type valueSource struct {
	data  []byte                       // raw value buffer, read by word when width > 0
	width int                          // byte width of a word value, or 0 when roll is used
	roll  func(s state, i int64) state // rolls the value in slot i
	// required, when set, chunks the n values of a column without levels in a
	// loop specialized for the value type.
	required func(n int64) []Chunk
}

// word returns the value in slot i. It is rolled least significant byte first,
// hashing its little-endian bytes: what Arrow C++ hashes on little-endian hosts.
func (v valueSource) word(i int64) uint64 {
	switch v.width {
	case 1:
		return uint64(v.data[i])
	case 2:
		return uint64(endian.Native.Uint16(v.data[i*2:]))
	case 4:
		return uint64(endian.Native.Uint32(v.data[i*4:]))
	}
	return endian.Native.Uint64(v.data[i*8:])
}

// newValueSource returns the accessor for the values of arr.
func (c *Chunker) newValueSource(arr arrow.Array) (valueSource, error) {
	switch arr := arr.(type) {
	case *array.Null:
		return valueSource{roll: func(s state, _ int64) state { return s }}, nil
	case *array.Boolean:
		return valueSource{roll: func(s state, i int64) state {
			if arr.Value(int(i)) {
				return c.roll(s, []byte{1})
			}
			return c.roll(s, []byte{0})
		}}, nil
	case *array.Dictionary:
		return c.newValueSource(arr.Indices())
	case array.ExtensionArray:
		return c.newValueSource(arr.Storage())
	case *array.String, *array.Binary:
		return newBinarySource[int32](c, arr.Data()), nil
	case *array.LargeString, *array.LargeBinary:
		return newBinarySource[int64](c, arr.Data()), nil
	// View arrays are matched by concrete type so that Value is inlined.
	case *array.StringView:
		return valueSource{roll: func(s state, i int64) state {
			v := arr.Value(int(i))
			return c.roll(s, unsafe.Slice(unsafe.StringData(v), len(v)))
		}}, nil
	case *array.BinaryView:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	case *array.FixedSizeBinary:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	}

	fixed, ok := arr.DataType().(arrow.FixedWidthDataType)
	if !ok {
		return valueSource{}, fmt.Errorf("%w: cdc: unsupported type %s for content-defined chunking", arrow.ErrNotImplemented, arr.DataType())
	}
	width := fixed.Bytes()
	var data []byte
	if buf := arr.Data().Buffers()[1]; buf != nil {
		data = buf.Bytes()[arr.Data().Offset()*width:]
	}
	if width == 1 || width == 2 || width == 4 || width == 8 {
		return valueSource{data: data, width: width}, nil
	}
	if !endian.IsBigEndian {
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, data[int(i)*width:][:width]) }}, nil
	}
	return valueSource{roll: func(s state, i int64) state {
		// Decimals are native-endian 64-bit words, least significant first;
		// hash their little-endian bytes.
		var le [32]byte
		value := data[int(i)*width:][:width]
		for w := 0; w < width; w += 8 {
			binary.LittleEndian.PutUint64(le[w:], endian.Native.Uint64(value[w:]))
		}
		return c.roll(s, le[:width])
	}}, nil
}

// newBinarySource returns the accessor for a binary-like array, which reads the
// values straight from its offsets and value buffer.
func newBinarySource[O int32 | int64](c *Chunker, data arrow.ArrayData) valueSource {
	var (
		offsets []O
		values  []byte
	)
	// An empty array may have no offsets buffer at all.
	if data.Len() > 0 {
		offsets = arrow.GetOffsets[O](data, 1)
		if buf := data.Buffers()[2]; buf != nil {
			values = buf.Bytes()
		}
	}
	return valueSource{
		roll:     func(s state, i int64) state { return c.roll(s, values[offsets[i]:offsets[i+1]]) },
		required: func(n int64) []Chunk { return requiredBinaryChunks(c, offsets, values, n) },
	}
}

// requiredBinaryChunks chunks the n values of a column without levels whose
// values are binary-like, given by their offsets into values.
func requiredBinaryChunks[O int32 | int64](c *Chunker, offsets []O, values []byte, n int64) []Chunk {
	var (
		l   chunkList
		cut bool
		s   = c.state
	)
	for i := int64(0); i < n; i++ {
		s = c.roll(s, values[offsets[i]:offsets[i+1]])
		if s, cut = c.needNewChunk(s); cut {
			l.cut(i, i)
		}
	}
	c.state = s
	return l.finish(n, n)
}

// requiredWordChunks chunks the n values of a column without levels whose
// values are read as words.
func (c *Chunker) requiredWordChunks(vals valueSource, n int64) []Chunk {
	var (
		l   chunkList
		cut bool
		s   = c.state
	)
	for i := int64(0); i < n; i++ {
		if s.size += int64(vals.width); s.size >= c.minSize {
			// Go does not unroll loops and will not inline a helper this
			// large, but a byte loop or a call per value is much slower. The
			// cases fall through from the widest word to the narrowest; the
			// same block is in levelChunks.
			v := vals.word(i)
			table, mask := &gearTables[s.nthRun], c.mask
			hash, matched := s.hash, s.matched
			switch vals.width {
			case 8:
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				fallthrough
			case 4:
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				fallthrough
			case 2:
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
				v >>= 8
				fallthrough
			case 1:
				hash = hash<<1 + table[byte(v)]
				matched = matched || hash&mask == 0
			}
			s.hash, s.matched = hash, matched
		}
		if s, cut = c.needNewChunk(s); cut {
			l.cut(i, i)
		}
	}
	c.state = s
	return l.finish(n, n)
}

// levelChunks chunks a column with levels, or one whose values have no
// specialized loop. It serves flat required, flat nullable and nested columns:
// flat columns have RepeatedAncestorDefLevel 0, which makes the value slot
// equal to the level offset.
func (c *Chunker) levelChunks(levels file.LevelInfo, defLevels, repLevels []int16, vals valueSource, numLevels int64) []Chunk {
	var (
		l           chunkList
		valueOffset int64
		cut         bool
		s           = c.state
	)
	for offset := int64(0); offset < numLevels; offset++ {
		def, rep := levels.DefLevel, int16(0)
		if levels.DefLevel > 0 {
			def = defLevels[offset]
			s = c.rollLevel(s, def)
		}
		if levels.RepLevel > 0 {
			rep = repLevels[offset]
			s = c.rollLevel(s, rep)
		}
		if def == levels.DefLevel {
			if vals.width == 0 {
				s = vals.roll(s, valueOffset)
			} else if s.size += int64(vals.width); s.size >= c.minSize {
				// The same block as in requiredWordChunks.
				v := vals.word(valueOffset)
				table, mask := &gearTables[s.nthRun], c.mask
				hash, matched := s.hash, s.matched
				switch vals.width {
				case 8:
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					fallthrough
				case 4:
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					fallthrough
				case 2:
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
					v >>= 8
					fallthrough
				case 1:
					hash = hash<<1 + table[byte(v)]
					matched = matched || hash&mask == 0
				}
				s.hash, s.matched = hash, matched
			}
		}
		// Cut only at record boundaries.
		if rep == 0 {
			if s, cut = c.needNewChunk(s); cut {
				l.cut(offset, valueOffset)
			}
		}
		if def >= levels.RepeatedAncestorDefLevel {
			valueOffset++
		}
	}
	c.state = s
	return l.finish(numLevels, valueOffset)
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
	vals, err := c.newValueSource(values)
	if err != nil {
		return nil, err
	}

	numLevels := int64(values.Len())
	if levels.DefLevel > 0 {
		numLevels = int64(len(defLevels))
	}
	// A flat column has a value slot per level, a nested one at most as many.
	if (levels.RepLevel == 0 && numLevels != int64(values.Len())) || numLevels < int64(values.Len()) ||
		(levels.RepLevel > 0 && int64(len(repLevels)) != numLevels) {
		return nil, fmt.Errorf("%w: cdc: got %d definition and %d repetition levels for %d values",
			arrow.ErrInvalid, len(defLevels), len(repLevels), values.Len())
	}

	var chunks []Chunk
	switch {
	case levels.DefLevel == 0 && vals.width > 0:
		chunks = c.requiredWordChunks(vals, numLevels)
	case levels.DefLevel == 0 && vals.required != nil:
		chunks = vals.required(numLevels)
	default:
		chunks = c.levelChunks(levels, defLevels, repLevels, vals, numLevels)
	}
	if err := validateChunks(chunks, numLevels, int64(values.Len())); err != nil {
		return nil, err
	}
	return chunks, nil
}

// validateChunks checks that chunks are contiguous in levels and values, that
// only the first one is empty, and that they cover exactly numLevels levels and
// numValues value slots.
func validateChunks(chunks []Chunk, numLevels, numValues int64) error {
	var levelOffset, valueOffset int64
	for i, ch := range chunks {
		if ch.LevelOffset != levelOffset || ch.ValueOffset != valueOffset ||
			ch.NumValues < 0 || ch.NumValues > ch.NumLevels || ch.NumLevels < 0 || (i > 0 && ch.NumLevels == 0) {
			return fmt.Errorf("%w: cdc: invalid chunk %d: %+v", arrow.ErrInvalid, i, ch)
		}
		levelOffset += ch.NumLevels
		valueOffset += ch.NumValues
	}
	if levelOffset != numLevels || valueOffset != numValues {
		return fmt.Errorf("%w: cdc: chunks cover %d levels and %d values, want %d and %d", arrow.ErrInvalid, levelOffset, valueOffset, numLevels, numValues)
	}
	return nil
}
