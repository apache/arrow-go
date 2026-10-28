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
	"crypto/fips140"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
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
	tables := &gearTables
	assert.Equal(t, uint64(0xf09f35a563783945), tables[0][0])
	assert.Equal(t, uint64(0x0dcc5b3bc5ae410a), tables[0][1])
	assert.Equal(t, uint64(0xecfcba92fe5691a3), tables[1][0])
	assert.Equal(t, uint64(0x15ed3fca3107bef5), tables[3][128])
	assert.Equal(t, uint64(0x8df9314914f0857e), tables[7][255])
}

// TestGearTablesMatchMD5 recomputes the generated tables. It uses crypto/md5,
// which is not available in FIPS 140-only mode; only this test needs it, the
// production code uses the checked-in tables.
func TestGearTablesMatchMD5(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("crypto/md5 is not allowed in FIPS 140-only mode")
	}
	var buf [128]byte
	for seed := range numGearTables {
		for n := range 256 {
			for i := range buf {
				if i < 64 {
					buf[i] = byte(seed)
				} else {
					buf[i] = byte(n)
				}
			}
			sum := md5.Sum(buf[:])
			require.Equal(t, binary.BigEndian.Uint64(sum[:8]), gearTables[seed][n], "seed %d byte %d", seed, n)
		}
	}
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

	return map[string]chunkInput{
		"required": {values: required.NewArray()},
		"nullable": {levels: file.LevelInfo{DefLevel: 1}, def: nullableDef, values: nullable.NewArray()},
		"list":     newListInput(mem, numTestValues),
	}
}

// newListInput builds a list<int32> column of at least numLevels levels. Each record is
// null (def 0), empty (def 1) or a list of 1-6 elements, an eighth of which are
// null (def 2).
func newListInput(mem memory.Allocator, numLevels int) chunkInput {
	elems := array.NewInt32Builder(mem)
	defer elems.Release()
	var def, rep []int16
	for r := uint64(0); len(def) < numLevels; r++ {
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
	return chunkInput{
		levels: file.LevelInfo{DefLevel: 3, RepLevel: 1, RepeatedAncestorDefLevel: 2},
		def:    def, rep: rep, values: elems.NewArray(),
	}
}

func TestChunksInvariants(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	inputs := testInputs(mem)
	for _, in := range inputs {
		defer in.values.Release()
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
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

	inputs := testInputs(mem)
	for _, in := range inputs {
		defer in.values.Release()
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
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

	inputs := testInputs(mem)
	for _, in := range inputs {
		defer in.values.Release()
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
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
		var s state
		var cut bool
		for run := 1; run < numGearTables; run++ {
			s = c.roll(s, []byte{1})
			require.True(t, s.matched)
			s, cut = c.needNewChunk(s)
			require.False(t, cut)
			assert.Equal(t, run, s.nthRun)
			assert.False(t, s.matched)
			assert.Equal(t, int64(run), s.size)
		}
		s = c.roll(s, []byte{1})
		hash := s.hash
		s, cut = c.needNewChunk(s)
		require.True(t, cut)
		assert.Zero(t, s.nthRun)
		assert.Zero(t, s.size)
		assert.Equal(t, hash, s.hash)
	})

	t.Run("no match leaves run", func(t *testing.T) {
		c := newTestChunker(t, testOptions)
		c.minSize, c.maxSize, c.mask = 0, 1<<40, ^uint64(0)
		s := c.roll(state{nthRun: 3}, []byte{1})
		require.False(t, s.matched)
		s, cut := c.needNewChunk(s)
		assert.False(t, cut)
		assert.Equal(t, 3, s.nthRun)
	})

	t.Run("max size", func(t *testing.T) {
		c := newTestChunker(t, testOptions)
		c.minSize, c.maxSize, c.mask = 0, 2, ^uint64(0)
		s := c.roll(state{nthRun: 3}, []byte{1, 2})
		require.False(t, s.matched)
		hash := s.hash
		s, cut := c.needNewChunk(s)
		require.True(t, cut)
		assert.Equal(t, 3, s.nthRun)
		assert.Zero(t, s.size)
		assert.Equal(t, hash, s.hash)
	})
}

// TestChunksWordsMatchBytes checks that the word path for 1, 2, 4 and 8 byte
// values chunks exactly like the byte path, by viewing the same buffers as
// fixed-size binary.
func TestChunksWordsMatchBytes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const numLevels = 1 << 13
	// newInts builds numLevels integers of the given width, every 10th one null
	// if nullable, as an integer array and as a fixed-size binary array that
	// shares its data buffer and validity bitmap.
	newInts := func(width int, nullable bool) (ints, fsb arrow.Array) {
		var b array.Builder
		var appendValue func(i uint64)
		switch width {
		case 1:
			ib := array.NewInt8Builder(mem)
			b, appendValue = ib, func(i uint64) { ib.Append(int8(splitmix(i))) }
		case 2:
			ib := array.NewInt16Builder(mem)
			b, appendValue = ib, func(i uint64) { ib.Append(int16(splitmix(i))) }
		case 4:
			ib := array.NewInt32Builder(mem)
			b, appendValue = ib, func(i uint64) { ib.Append(int32(splitmix(i))) }
		case 8:
			ib := array.NewInt64Builder(mem)
			b, appendValue = ib, func(i uint64) { ib.Append(int64(splitmix(i))) }
		}
		defer b.Release()
		for i := uint64(0); i < numLevels; i++ {
			if nullable && i%10 == 0 {
				b.AppendNull()
			} else {
				appendValue(i)
			}
		}
		ints = b.NewArray()
		data := ints.Data()
		fsbData := array.NewData(&arrow.FixedSizeBinaryType{ByteWidth: width}, data.Len(), data.Buffers(), nil, data.NullN(), data.Offset())
		defer fsbData.Release()
		return ints, array.NewFixedSizeBinaryData(fsbData)
	}

	optionSets := []parquet.CDCOptions{
		{MinChunkSize: 1024, MaxChunkSize: 4096},
		{MinChunkSize: 0, MaxChunkSize: 64},
		{MinChunkSize: 256, MaxChunkSize: 1024, NormLevel: 2},
	}
	for _, width := range []int{1, 2, 4, 8} {
		for _, shape := range []string{"required", "nullable", "nested"} {
			t.Run(fmt.Sprintf("%s/width %d", shape, width), func(t *testing.T) {
				ints, fsb := newInts(width, shape != "required")
				defer ints.Release()
				defer fsb.Release()

				var intIn, fsbIn chunkInput
				switch shape {
				case "required":
					intIn, fsbIn = chunkInput{values: ints}, chunkInput{values: fsb}
				case "nullable":
					def := make([]int16, numLevels)
					for i := range def {
						if ints.IsValid(i) {
							def[i] = 1
						}
					}
					levels := file.LevelInfo{DefLevel: 1}
					intIn, fsbIn = chunkInput{levels: levels, def: def, values: ints}, chunkInput{levels: levels, def: def, values: fsb}
				case "nested":
					// list<int> records that are null (def 0), empty (def 1) or
					// hold up to 3 elements, a null one having def 2. Null and empty
					// records have no value slot, so value and level offsets differ.
					var def, rep []int16
					for slot, r := 0, uint64(0); slot < numLevels; r++ {
						switch n := splitmix(r+1<<40) % 5; n {
						case 0, 1:
							def, rep = append(def, int16(n)), append(rep, 0)
						default:
							for e := uint64(0); e < n-1 && slot < numLevels; e++ {
								rep = append(rep, int16(min(e, 1)))
								if ints.IsNull(slot) {
									def = append(def, 2)
								} else {
									def = append(def, 3)
								}
								slot++
							}
						}
					}
					levels := file.LevelInfo{DefLevel: 3, RepLevel: 1, RepeatedAncestorDefLevel: 2}
					intIn, fsbIn = chunkInput{levels: levels, def: def, rep: rep, values: ints}, chunkInput{levels: levels, def: def, rep: rep, values: fsb}
				}

				for _, opts := range optionSets {
					for _, from := range []int{0, 3} {
						n := intIn.numLevels()
						intSlice, fsbSlice := intIn.slice(from, n), fsbIn.slice(from, n)
						want := fsbSlice.chunks(t, newTestChunker(t, opts))
						got := intSlice.chunks(t, newTestChunker(t, opts))
						assert.Equal(t, want, got, "options %+v offset %d", opts, from)
						intSlice.values.Release()
						fsbSlice.values.Release()
					}
				}
			})
		}
	}
}

// TestChunksStableBoundaries pins exact chunk boundaries, so that a change to
// the hashing cannot silently move them: files written by different versions
// must keep deduplicating against each other.
func TestChunksStableBoundaries(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const numLevels = 4096
	opts := parquet.CDCOptions{MinChunkSize: 256, MaxChunkSize: 1024}

	required := array.NewInt64Builder(mem)
	defer required.Release()
	nullable := array.NewInt32Builder(mem)
	defer nullable.Release()
	strs := array.NewStringBuilder(mem)
	defer strs.Release()
	nullableDef := make([]int16, numLevels)
	for i := uint64(0); i < numLevels; i++ {
		required.Append(int64(splitmix(i)))
		if i%7 == 0 {
			nullable.AppendNull()
		} else {
			nullable.Append(int32(splitmix(i)))
			nullableDef[i] = 1
		}
		strs.Append(strings.Repeat("x", int(splitmix(i)%12)) + strconv.FormatUint(splitmix(i)%1000, 10))
	}

	tests := []struct {
		name      string
		in        chunkInput
		numValues int64
		offsets   []int64
	}{
		{
			"required int64", chunkInput{values: required.NewArray()}, 4096,
			[]int64{
				0, 69, 127, 186, 242, 319, 386, 458, 541, 620, 721, 797, 882, 940, 1023, 1090,
				1163, 1235, 1314, 1385, 1448, 1515, 1582, 1639, 1726, 1790, 1861, 1922, 1987, 2042, 2106, 2168,
				2242, 2297, 2346, 2420, 2486, 2542, 2602, 2659, 2722, 2794, 2846, 2920, 2999, 3062, 3131, 3197,
				3258, 3314, 3381, 3476, 3543, 3606, 3698, 3784, 3844, 3917, 3989, 4052,
			},
		},
		{
			"nullable int32", chunkInput{levels: file.LevelInfo{DefLevel: 1}, def: nullableDef, values: nullable.NewArray()}, 4096,
			[]int64{
				0, 98, 193, 302, 364, 487, 602, 704, 824, 922, 1023, 1126, 1199, 1304, 1408, 1476,
				1579, 1659, 1727, 1815, 1918, 2005, 2105, 2230, 2325, 2420, 2492, 2567, 2665, 2795, 2893, 2981,
				3092, 3176, 3287, 3372, 3463, 3562, 3645, 3743, 3828, 3930, 4040,
			},
		},
		{
			"list int32", newListInput(mem, numLevels), 3738,
			[]int64{
				0, 77, 188, 287, 374, 467, 557, 656, 740, 815, 896, 982, 1075, 1156, 1239, 1335,
				1429, 1519, 1598, 1676, 1750, 1847, 1925, 2002, 2086, 2159, 2248, 2329, 2414, 2505, 2600, 2665,
				2764, 2852, 2938, 3012, 3095, 3184, 3258, 3350, 3421, 3511, 3620, 3743, 3844, 3940, 4032,
			},
		},
		{
			"required string", chunkInput{values: strs.NewArray()}, 4096,
			[]int64{
				0, 47, 105, 159, 235, 298, 373, 436, 493, 552, 618, 688, 737, 821, 882, 968,
				1027, 1076, 1128, 1196, 1239, 1283, 1345, 1414, 1486, 1547, 1620, 1699, 1765, 1833, 1896, 1962,
				2037, 2110, 2162, 2234, 2305, 2363, 2406, 2494, 2565, 2628, 2687, 2752, 2806, 2870, 2935, 3005,
				3089, 3153, 3201, 3262, 3348, 3410, 3459, 3513, 3569, 3640, 3708, 3782, 3837, 3884, 3937, 4024,
				4079,
			},
		},
	}
	for _, tt := range tests {
		defer tt.in.values.Release()
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := tt.in.chunks(t, newTestChunker(t, opts))
			var offsets []int64
			var numValues int64
			for _, c := range chunks {
				offsets = append(offsets, c.LevelOffset)
				numValues += c.NumValues
			}
			assert.Equal(t, tt.offsets, offsets)
			assert.Equal(t, tt.numValues, numValues)
		})
	}
}

func BenchmarkChunks(b *testing.B) {
	const numLevels = 1 << 20
	mem := memory.DefaultAllocator

	required := array.NewInt64Builder(mem)
	defer required.Release()
	nullable := array.NewInt64Builder(mem)
	defer nullable.Release()
	strs := array.NewStringBuilder(mem)
	defer strs.Release()
	nullableDef := make([]int16, numLevels)
	for i := uint64(0); i < numLevels; i++ {
		required.Append(int64(splitmix(i)))
		if i%10 == 0 {
			nullable.AppendNull()
		} else {
			nullable.Append(int64(splitmix(i)))
			nullableDef[i] = 1
		}
		strs.Append(strconv.FormatUint(splitmix(i), 16))
	}
	requiredArr, nullableArr, stringArr := required.NewArray(), nullable.NewArray(), strs.NewArray()
	defer requiredArr.Release()
	defer nullableArr.Release()
	defer stringArr.Release()
	list := newListInput(mem, numLevels)
	defer list.values.Release()

	benchmarks := []struct {
		name  string
		in    chunkInput
		bytes int64
	}{
		{"required/int64", chunkInput{values: requiredArr}, numLevels * 8},
		{"nullable/int64", chunkInput{levels: file.LevelInfo{DefLevel: 1}, def: nullableDef, values: nullableArr}, int64(nullableArr.Len()-nullableArr.NullN()) * 8},
		{"nested/list<int32>", list, int64(list.values.Len()-list.values.NullN()) * 4},
		{"required/string", chunkInput{values: stringArr}, int64(len(stringArr.(*array.String).ValueBytes()))},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			c, err := NewChunker(parquet.DefaultCDCOptions())
			require.NoError(b, err)
			b.SetBytes(bm.bytes)
			for b.Loop() {
				_, err := c.Chunks(bm.in.levels, bm.in.def, bm.in.rep, bm.in.values)
				require.NoError(b, err)
			}
		})
	}
}
