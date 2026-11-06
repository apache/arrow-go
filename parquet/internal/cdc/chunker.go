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

// state is the part of the chunker that changes while rolling. Chunks works on
// a local copy that is passed to the helpers by value, which lets the compiler
// keep it in registers: it must keep at most 4 fields and never have its
// address taken.
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

// valueSource gives access to the values of one Chunks call. Fixed-width values of
// 1, 2, 4 or 8 bytes are read as words, so that Chunks can roll them inline;
// every other type is rolled by the roll function.
type valueSource struct {
	data  []byte                       // raw value buffer, read by word when width > 0
	width int                          // byte width of a word value, or 0 when roll is used
	roll  func(s state, i int64) state // rolls the value in slot i
}

// word returns the value in slot i. Rolling it least significant byte first
// hashes its little-endian bytes, so chunk boundaries don't depend on the
// host's byte order.
func (v valueSource) word(i int64) uint64 {
	switch v.width {
	case 1:
		return uint64(v.data[i])
	case 2:
		return uint64(binary.NativeEndian.Uint16(v.data[i*2:]))
	case 4:
		return uint64(binary.NativeEndian.Uint32(v.data[i*4:]))
	}
	return binary.NativeEndian.Uint64(v.data[i*8:])
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
	// Binary-like arrays are matched by concrete type so that Value is inlined;
	// a case per interface instead is about 10% slower.
	case *array.String:
		return valueSource{roll: func(s state, i int64) state { return c.rollString(s, arr.Value(int(i))) }}, nil
	case *array.LargeString:
		return valueSource{roll: func(s state, i int64) state { return c.rollString(s, arr.Value(int(i))) }}, nil
	case *array.StringView:
		return valueSource{roll: func(s state, i int64) state { return c.rollString(s, arr.Value(int(i))) }}, nil
	case *array.FixedSizeBinary:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	case *array.Binary:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	case *array.LargeBinary:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	case *array.BinaryView:
		return valueSource{roll: func(s state, i int64) state { return c.roll(s, arr.Value(int(i))) }}, nil
	}

	fixed, ok := arr.DataType().(arrow.FixedWidthDataType)
	if !ok || fixed.BitWidth()%8 != 0 {
		return valueSource{}, fmt.Errorf("%w: cdc: unsupported type %s for content-defined chunking", arrow.ErrNotImplemented, arr.DataType())
	}
	width := fixed.BitWidth() / 8
	var data []byte
	if buf := arr.Data().Buffers()[1]; buf != nil {
		data = buf.Bytes()[arr.Data().Offset()*width:]
	}
	if width == 1 || width == 2 || width == 4 || width == 8 {
		return valueSource{data: data, width: width}, nil
	}
	return valueSource{roll: func(s state, i int64) state {
		// Decimals are native-endian 64-bit words, least significant first;
		// hash their little-endian bytes.
		var le [32]byte
		value := data[int(i)*width:][:width]
		for w := 0; w < width; w += 8 {
			binary.LittleEndian.PutUint64(le[w:], binary.NativeEndian.Uint64(value[w:]))
		}
		return c.roll(s, le[:width])
	}}, nil
}

func (c *Chunker) rollString(s state, v string) state {
	return c.roll(s, unsafe.Slice(unsafe.StringData(v), len(v)))
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
	if numLevels < int64(values.Len()) || (levels.RepLevel > 0 && int64(len(repLevels)) != numLevels) {
		return nil, fmt.Errorf("%w: cdc: got %d definition and %d repetition levels for %d values",
			arrow.ErrInvalid, len(defLevels), len(repLevels), values.Len())
	}

	var (
		chunks          []Chunk
		prevOffset      int64
		prevValueOffset int64
		valueOffset     int64
		cut             bool
		s               = c.state
	)
	// One loop serves flat required, flat nullable and nested columns, so that
	// the unrolled word roll below appears exactly once. Flat columns have
	// RepeatedAncestorDefLevel 0, which makes valueOffset equal to offset.
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
				// Go does not unroll loops and will not inline a helper this
				// large, but a byte loop or a call per value is much slower.
				// The cases fall through from the widest word to the narrowest.
				v, mask := vals.word(valueOffset), c.mask
				table := &gearTables[s.nthRun]
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
				chunks = append(chunks, Chunk{prevOffset, prevValueOffset, offset - prevOffset, valueOffset - prevValueOffset})
				prevOffset, prevValueOffset = offset, valueOffset
			}
		}
		if def >= levels.RepeatedAncestorDefLevel {
			valueOffset++
		}
	}
	c.state = s

	if prevOffset < numLevels {
		chunks = append(chunks, Chunk{prevOffset, prevValueOffset, numLevels - prevOffset, valueOffset - prevValueOffset})
	}
	assertValidChunks(chunks, numLevels, int64(values.Len()))
	return chunks, nil
}
