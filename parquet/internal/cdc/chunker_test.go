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

package cdc

import (
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGearTables(t *testing.T) {
	tables := gearTables()
	assert.Equal(t, uint64(0xf09f35a563783945), tables[0][0])
	assert.Equal(t, uint64(0x0dcc5b3bc5ae410a), tables[0][1])
	assert.Equal(t, uint64(0xecfcba92fe5691a3), tables[1][0])
	assert.Equal(t, uint64(0x15ed3fca3107bef5), tables[3][128])
	assert.Equal(t, uint64(0x8df9314914f0857e), tables[7][255])
	assert.Same(t, tables, gearTables())
}

func TestCalculateMask(t *testing.T) {
	const Ki, Mi = 1024, 1024 * 1024
	tests := []struct {
		name     string
		min, max int64
		norm     int
		mask     uint64
	}{
		{"256Ki-1Mi-norm0", 256 * Ki, 1 * Mi, 0, 0xFFFE000000000000},
		{"256Ki-1Mi-norm1", 256 * Ki, 1 * Mi, 1, 0xFFFC000000000000},
		{"256Ki-1Mi-norm2", 256 * Ki, 1 * Mi, 2, 0xFFF8000000000000},
		{"256Ki-1Mi-norm3", 256 * Ki, 1 * Mi, 3, 0xFFF0000000000000},
		{"256Ki-1Mi-norm-1", 256 * Ki, 1 * Mi, -1, 0xFFFF000000000000},
		{"0-32-norm0", 0, 32, 0, 0x8000000000000000},
		{"0-64-norm0", 0, 64, 0, 0xC000000000000000},
		{"0-16-norm-1", 0, 16, -1, 0x8000000000000000},
		{"128-384-norm-59", 128, 384, -59, 0xFFFFFFFFFFFFFFFE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mask, err := calculateMask(tt.min, tt.max, tt.norm)
			require.NoError(t, err)
			assert.Equal(t, tt.mask, mask)
		})
	}

	errTests := []struct {
		name     string
		min, max int64
		norm     int
	}{
		{"0-16-norm0", 0, 16, 0},
		{"0-32-norm1", 0, 32, 1},
		{"128-384-norm-60", 128, 384, -60},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := calculateMask(tt.min, tt.max, tt.norm)
			require.ErrorContains(t, err, "between 1 and 63")
		})
	}
}

func TestNewChunkerValidation(t *testing.T) {
	const Ki, Mi, Gi = 1024, 1024 * 1024, 1024 * 1024 * 1024
	valid := []parquet.CDCOptions{
		{MinChunkSize: 256 * Ki, MaxChunkSize: 1 * Mi, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 32, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 64, NormLevel: 0},
		{MinChunkSize: 1 * Mi, MaxChunkSize: 2 * Mi, NormLevel: 0},
		{MinChunkSize: 1 * Gi, MaxChunkSize: 2 * Gi, NormLevel: 0},
		{MinChunkSize: 1, MaxChunkSize: 65, NormLevel: 1},
		{MinChunkSize: 0, MaxChunkSize: 128, NormLevel: 2},
	}
	for _, opts := range valid {
		c, err := NewChunker(opts)
		require.NoError(t, err, "%+v", opts)
		assert.NotNil(t, c)
	}

	invalid := []parquet.CDCOptions{
		{MinChunkSize: 0, MaxChunkSize: -1, NormLevel: 0},
		{MinChunkSize: 1024, MaxChunkSize: 512, NormLevel: 0},
		{MinChunkSize: -1, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 16, NormLevel: 0},
		{MinChunkSize: -16, MaxChunkSize: -16, NormLevel: 0},
		{MinChunkSize: 16, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 32, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 48, NormLevel: 0},
		{MinChunkSize: 1, MaxChunkSize: -1, NormLevel: 1},
		{MinChunkSize: -1, MaxChunkSize: 1, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 1, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 32, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 33, NormLevel: 1},
		{MinChunkSize: 0, MaxChunkSize: 123, NormLevel: 2},
	}
	for _, opts := range invalid {
		_, err := NewChunker(opts)
		require.Error(t, err, "%+v", opts)
	}
}

const numTestValues = 1 << 16

var testOptions = parquet.CDCOptions{MinChunkSize: 1024, MaxChunkSize: 4096}

func splitmix(i uint64) uint64 {
	z := i + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func newTestChunker(t *testing.T, opts parquet.CDCOptions) *Chunker {
	t.Helper()
	c, err := NewChunker(opts)
	require.NoError(t, err)
	return c
}

type chunkInput struct {
	levels   file.LevelInfo
	def, rep []int16
	values   arrow.Array
}

func (in chunkInput) numLevels() int {
	if in.levels.DefLevel == 0 {
		return in.values.Len()
	}
	return len(in.def)
}

// slice returns the input restricted to levels [from, to).
func (in chunkInput) slice(from, to int) chunkInput {
	out := chunkInput{levels: in.levels}
	if in.levels.DefLevel == 0 {
		out.values = array.NewSlice(in.values, int64(from), int64(to))
		return out
	}
	out.def = in.def[from:to]
	if in.levels.RepLevel == 0 {
		out.values = array.NewSlice(in.values, int64(from), int64(to))
		return out
	}
	out.rep = in.rep[from:to]
	slot := func(n int) int64 {
		var count int64
		for _, d := range in.def[:n] {
			if d >= in.levels.RepeatedAncestorDefLevel {
				count++
			}
		}
		return count
	}
	out.values = array.NewSlice(in.values, slot(from), slot(to))
	return out
}

func (in chunkInput) chunks(t *testing.T, c *Chunker) []Chunk {
	t.Helper()
	chunks, err := c.Chunks(in.levels, in.def, in.rep, in.values)
	require.NoError(t, err)
	return chunks
}

// testInputs builds the required, nullable and list<int32> shapes. The caller
// must release every values array.
func testInputs(mem memory.Allocator) map[string]chunkInput {
	required := array.NewInt64Builder(mem)
	defer required.Release()
	nullable := array.NewInt64Builder(mem)
	defer nullable.Release()
	nullableDef := make([]int16, numTestValues)
	for i := 0; i < numTestValues; i++ {
		required.Append(int64(splitmix(uint64(i))))
		if i%10 == 0 {
			nullable.AppendNull()
		} else {
			nullable.Append(int64(splitmix(uint64(i))))
			nullableDef[i] = 1
		}
	}

	// Each record is null (def 0), empty (def 1) or a list of 1-5 elements, an
	// eighth of which are null (def 2).
	elems := array.NewInt32Builder(mem)
	defer elems.Release()
	var def, rep []int16
	for r := uint64(0); len(def) < numTestValues; r++ {
		h := splitmix(r + 1<<40)
		switch n := h % 8; n {
		case 0:
			def, rep = append(def, 0), append(rep, 0)
		case 1:
			def, rep = append(def, 1), append(rep, 0)
		default:
			for e := uint64(0); e < n-1; e++ {
				rep = append(rep, int16(min(e, 1)))
				if splitmix(h+e)%8 == 0 {
					def = append(def, 2)
					elems.AppendNull()
				} else {
					def = append(def, 3)
					elems.Append(int32(splitmix(h + e)))
				}
			}
		}
	}

	return map[string]chunkInput{
		"required": {values: required.NewArray()},
		"nullable": {levels: file.LevelInfo{DefLevel: 1}, def: nullableDef, values: nullable.NewArray()},
		"list": {
			levels: file.LevelInfo{DefLevel: 3, RepLevel: 1, RepeatedAncestorDefLevel: 2},
			def:    def, rep: rep, values: elems.NewArray(),
		},
	}
}

func TestChunksInvariants(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, in := range testInputs(mem) {
		t.Run(name, func(t *testing.T) {
			defer in.values.Release()
			chunks := in.chunks(t, newTestChunker(t, testOptions))
			require.Greater(t, len(chunks), 1)

			var levelSum, valueSum int64
			for i, c := range chunks {
				assert.Equal(t, levelSum, c.LevelOffset)
				assert.Equal(t, valueSum, c.ValueOffset)
				if i > 0 {
					assert.Positive(t, c.NumLevels)
				}
				if in.levels.RepLevel == 0 {
					assert.Equal(t, c.LevelOffset, c.ValueOffset)
					assert.Equal(t, c.NumLevels, c.NumValues)
				} else {
					var slots int64
					for _, d := range in.def[c.LevelOffset : c.LevelOffset+c.NumLevels] {
						if d >= in.levels.RepeatedAncestorDefLevel {
							slots++
						}
					}
					assert.Equal(t, slots, c.NumValues)
					if c.NumLevels > 0 {
						assert.Zero(t, in.rep[c.LevelOffset])
					}
				}
				levelSum += c.NumLevels
				valueSum += c.NumValues
			}
			assert.Equal(t, int64(in.numLevels()), levelSum)
			assert.Equal(t, int64(in.values.Len()), valueSum)
		})
	}
}

func TestChunksMaxSizeForcesCut(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	inputs := testInputs(mem)
	for _, in := range inputs {
		defer in.values.Release()
	}
	in := inputs["required"]
	chunks := in.chunks(t, newTestChunker(t, parquet.CDCOptions{MinChunkSize: 0, MaxChunkSize: 64}))
	require.Greater(t, len(chunks), 1)
	for _, c := range chunks[:len(chunks)-1] {
		assert.LessOrEqual(t, c.NumLevels*8, int64(64))
	}
}

func TestChunksDeterministic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, in := range testInputs(mem) {
		t.Run(name, func(t *testing.T) {
			defer in.values.Release()
			assert.Equal(t, in.chunks(t, newTestChunker(t, testOptions)), in.chunks(t, newTestChunker(t, testOptions)))
		})
	}
}

func TestChunksContinueAcrossCalls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// boundaries is the set of levels at which a chunk starts, apart from level 0.
	boundaries := func(chunks []Chunk, base int64) []int64 {
		var out []int64
		for i, c := range chunks {
			if i > 0 || (c.NumLevels == 0 && base > 0) {
				out = append(out, base+c.LevelOffset)
			}
		}
		return out
	}

	for name, in := range testInputs(mem) {
		t.Run(name, func(t *testing.T) {
			defer in.values.Release()
			whole := in.chunks(t, newTestChunker(t, testOptions))
			require.Greater(t, len(whole), 2)
			expected := boundaries(whole, 0)

			mid := int(whole[2].LevelOffset + whole[2].NumLevels/2)
			for in.levels.RepLevel > 0 && in.rep[mid] != 0 {
				mid++
			}
			for _, split := range []struct {
				name string
				at   int
			}{{"at boundary", int(whole[2].LevelOffset)}, {"inside chunk", mid}} {
				t.Run(split.name, func(t *testing.T) {
					c := newTestChunker(t, testOptions)
					first := in.slice(0, split.at)
					defer first.values.Release()
					second := in.slice(split.at, in.numLevels())
					defer second.values.Release()
					chunks1, chunks2 := first.chunks(t, c), second.chunks(t, c)
					if split.name == "at boundary" {
						require.Zero(t, chunks2[0].NumLevels)
					}
					got := slices.Compact(append(boundaries(chunks1, 0), boundaries(chunks2, int64(split.at))...))
					assert.Equal(t, expected, got)
				})
			}
		})
	}
}

func TestChunksTypeDispatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	chunk := func(arr arrow.Array) ([]Chunk, error) {
		return newTestChunker(t, testOptions).Chunks(file.LevelInfo{}, nil, nil, arr)
	}
	mustChunk := func(arr arrow.Array) []Chunk {
		chunks, err := chunk(arr)
		require.NoError(t, err)
		return chunks
	}

	t.Run("dictionary", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for i := 0; i < numTestValues; i++ {
			b.Append(int32(splitmix(uint64(i)) % 100))
		}
		indices := b.NewArray()
		defer indices.Release()
		db := array.NewInt32Builder(mem)
		defer db.Release()
		for i := 0; i < 100; i++ {
			db.Append(int32(i))
		}
		dict := db.NewArray()
		defer dict.Release()
		dictArr := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int32}, indices, dict)
		defer dictArr.Release()

		expected := mustChunk(indices)
		require.Greater(t, len(expected), 1)
		assert.Equal(t, expected, mustChunk(dictArr))
	})

	t.Run("extension", func(t *testing.T) {
		b := extensions.NewUUIDBuilder(mem)
		defer b.Release()
		for i := 0; i < numTestValues/4; i++ {
			var u [16]byte
			for j := range u {
				u[j] = byte(splitmix(uint64(i*16 + j)))
			}
			b.Append(u)
		}
		arr := b.NewArray()
		defer arr.Release()
		storage := arr.(array.ExtensionArray).Storage()

		expected := mustChunk(storage)
		require.Greater(t, len(expected), 1)
		assert.Equal(t, expected, mustChunk(arr))
	})

	t.Run("binary-like", func(t *testing.T) {
		values := make([]string, numTestValues/4)
		for i := range values {
			h := splitmix(uint64(i))
			values[i] = string(slices.Repeat([]byte{byte('a' + h%26)}, int(h>>32%24)))
		}
		sb, bb := array.NewStringBuilder(mem), array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		lsb, lbb := array.NewLargeStringBuilder(mem), array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
		svb, bvb := array.NewStringViewBuilder(mem), array.NewBinaryViewBuilder(mem)
		for _, v := range values {
			sb.Append(v)
			bb.Append([]byte(v))
			lsb.Append(v)
			lbb.Append([]byte(v))
			svb.Append(v)
			bvb.Append([]byte(v))
		}
		builders := []interface {
			NewArray() arrow.Array
			Release()
		}{sb, bb, lsb, lbb, svb, bvb}
		var expected []Chunk
		for i, b := range builders {
			arr := b.NewArray()
			chunks := mustChunk(arr)
			arr.Release()
			b.Release()
			if i == 0 {
				require.Greater(t, len(chunks), 1)
				expected = chunks
			}
			assert.Equal(t, expected, chunks, "builder %d", i)
		}
	})

	t.Run("fixed-width and null", func(t *testing.T) {
		types := []arrow.DataType{
			arrow.FixedWidthTypes.Boolean,
			arrow.FixedWidthTypes.Float16,
			&arrow.Decimal128Type{Precision: 10, Scale: 2},
			&arrow.TimestampType{Unit: arrow.Microsecond},
			&arrow.DurationType{Unit: arrow.Nanosecond},
			arrow.FixedWidthTypes.MonthDayNanoInterval,
			arrow.Null,
		}
		for _, dt := range types {
			t.Run(dt.String(), func(t *testing.T) {
				arr := array.MakeArrayOfNull(mem, dt, numTestValues)
				defer arr.Release()
				chunks := mustChunk(arr)
				var sum int64
				for _, c := range chunks {
					sum += c.NumLevels
				}
				assert.Equal(t, int64(numTestValues), sum)
			})
		}
	})

	t.Run("bool and float16 values", func(t *testing.T) {
		bb := array.NewBooleanBuilder(mem)
		defer bb.Release()
		fb := array.NewFloat16Builder(mem)
		defer fb.Release()
		db := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
		defer db.Release()
		for i := 0; i < numTestValues; i++ {
			h := splitmix(uint64(i))
			bb.Append(h&1 == 1)
			fb.Append(float16.New(float32(h % 1000)))
			db.Append(decimal128.FromU64(h))
		}
		for _, b := range []interface{ NewArray() arrow.Array }{bb, fb, db} {
			arr := b.NewArray()
			assert.Greater(t, len(mustChunk(arr)), 1, arr.DataType().String())
			arr.Release()
		}
	})

	t.Run("struct", func(t *testing.T) {
		arr := array.MakeArrayOfNull(mem, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), 4)
		defer arr.Release()
		_, err := chunk(arr)
		require.ErrorContains(t, err, "unsupported")
	})
}

func TestNeedNewChunkState(t *testing.T) {
	t.Run("match runs", func(t *testing.T) {
		c := newTestChunker(t, testOptions)
		c.minSize, c.maxSize, c.mask = 0, 1<<40, 0 // every hashed byte matches
		for run := 1; run < numGearTables; run++ {
			c.roll([]byte{1})
			require.True(t, c.hasMatched)
			require.False(t, c.needNewChunk())
			assert.Equal(t, run, c.nthRun)
			assert.False(t, c.hasMatched)
			assert.Equal(t, int64(run), c.chunkSize)
		}
		c.roll([]byte{1})
		hash := c.rollingHash
		require.True(t, c.needNewChunk())
		assert.Zero(t, c.nthRun)
		assert.Zero(t, c.chunkSize)
		assert.Equal(t, hash, c.rollingHash)
	})

	t.Run("no match leaves run", func(t *testing.T) {
		c := newTestChunker(t, testOptions)
		c.minSize, c.maxSize, c.mask, c.nthRun = 0, 1<<40, ^uint64(0), 3
		c.roll([]byte{1})
		require.False(t, c.hasMatched)
		assert.False(t, c.needNewChunk())
		assert.Equal(t, 3, c.nthRun)
	})

	t.Run("max size", func(t *testing.T) {
		c := newTestChunker(t, testOptions)
		c.minSize, c.maxSize, c.mask, c.nthRun = 0, 2, ^uint64(0), 3
		c.roll([]byte{1, 2})
		require.False(t, c.hasMatched)
		hash := c.rollingHash
		require.True(t, c.needNewChunk())
		assert.Equal(t, 3, c.nthRun)
		assert.Zero(t, c.chunkSize)
		assert.Equal(t, hash, c.rollingHash)
	})
}
