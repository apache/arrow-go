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
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/endian"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGearTables pins the first and last entry of every gear table to the values
// in Arrow C++'s chunker_internal_generated.h.
func TestGearTables(t *testing.T) {
	want := [numGearTables][2]uint64{
		{0xf09f35a563783945, 0x15a28844e64cb211},
		{0xecfcba92fe5691a3, 0xc281fb8acc6ec1d3},
		{0xdd16cb672ba6979c, 0xd0a34cfe027cbb7e},
		{0x39209fb3eb541043, 0x1cece97f30de48ad},
		{0x58bdf4338602e4fb, 0x4e17878356a55822},
		{0xb3c58c2483ad5ead, 0xaa815b37d052af59},
		{0x946ac62246e04460, 0x67ed6b9c724757ec},
		{0xc0272d42c19ff3ae, 0x8df9314914f0857e},
	}
	for seed, w := range want {
		assert.Equal(t, w, [2]uint64{gearTables[seed][0], gearTables[seed][255]}, "table %d", seed)
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
		{MinChunkSize: 256 * Ki, MaxChunkSize: 1 * Mi, NormLevel: 3},
		{MinChunkSize: 256 * Ki, MaxChunkSize: 1 * Mi, NormLevel: -1},
		{MinChunkSize: 0, MaxChunkSize: 16, NormLevel: -1},
		{MinChunkSize: 128, MaxChunkSize: 384, NormLevel: -59},
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
		{MinChunkSize: 0, MaxChunkSize: 32, NormLevel: 1},
		{MinChunkSize: 128, MaxChunkSize: 384, NormLevel: -60},
	}
	for _, opts := range invalid {
		_, err := NewChunker(opts)
		require.ErrorIs(t, err, arrow.ErrInvalid, "%+v", opts)
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

// newInt64Inputs builds required and nullable (every 10th value null) int64
// inputs of n levels. The caller must release their values.
func newInt64Inputs(mem memory.Allocator, n int) (required, nullable chunkInput) {
	rb, nb := array.NewInt64Builder(mem), array.NewInt64Builder(mem)
	defer rb.Release()
	defer nb.Release()
	def := make([]int16, n)
	for i := 0; i < n; i++ {
		rb.Append(int64(splitmix(uint64(i))))
		if i%10 == 0 {
			nb.AppendNull()
		} else {
			nb.Append(int64(splitmix(uint64(i))))
			def[i] = 1
		}
	}
	return chunkInput{values: rb.NewArray()}, chunkInput{levels: file.LevelInfo{DefLevel: 1}, def: def, values: nb.NewArray()}
}

// testInputs builds the required, nullable and list<int32> shapes on a checked
// allocator, and registers the leak check and the release of the values for the
// end of the test.
func testInputs(t *testing.T) map[string]chunkInput {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	required, nullable := newInt64Inputs(mem, numTestValues)
	inputs := map[string]chunkInput{"required": required, "nullable": nullable, "list": newListInput(mem, numTestValues)}
	for _, in := range inputs {
		t.Cleanup(in.values.Release)
	}
	return inputs
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
	for name, in := range testInputs(t) {
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
	chunks := testInputs(t)["required"].chunks(t, newTestChunker(t, parquet.CDCOptions{MinChunkSize: 0, MaxChunkSize: 64}))
	require.Greater(t, len(chunks), 1)
	for _, c := range chunks[:len(chunks)-1] {
		assert.LessOrEqual(t, c.NumLevels*8, int64(64))
	}
}

func TestChunksMissingLevels(t *testing.T) {
	inputs := testInputs(t)
	nullable, list := inputs["nullable"], inputs["list"]
	c := newTestChunker(t, testOptions)
	_, err := c.Chunks(nullable.levels, nil, nil, nullable.values)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	_, err = c.Chunks(list.levels, list.def, nil, list.values)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestChunksDeterministic(t *testing.T) {
	for name, in := range testInputs(t) {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in.chunks(t, newTestChunker(t, testOptions)), in.chunks(t, newTestChunker(t, testOptions)))
		})
	}
}

func TestChunksContinueAcrossCalls(t *testing.T) {
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

	for name, in := range testInputs(t) {
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
		var expected []Chunk
		for i, dt := range []arrow.DataType{
			arrow.BinaryTypes.String, arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeString,
			arrow.BinaryTypes.LargeBinary, arrow.BinaryTypes.StringView, arrow.BinaryTypes.BinaryView,
		} {
			b := array.NewBuilder(mem, dt)
			for _, v := range values {
				b.(interface{ AppendString(string) }).AppendString(v)
			}
			arr := b.NewArray()
			chunks := mustChunk(arr)
			arr.Release()
			b.Release()
			if i == 0 {
				require.Greater(t, len(chunks), 1)
				expected = chunks
			}
			assert.Equal(t, expected, chunks, dt.String())
		}
	})

	t.Run("fixed-width and null", func(t *testing.T) {
		// 16 random bytes per value cover every fixed-width type below.
		var raw []byte
		for i := range 2 * numTestValues {
			raw = binary.LittleEndian.AppendUint64(raw, splitmix(uint64(i)))
		}
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
				nulls := array.MakeArrayOfNull(mem, dt, numTestValues)
				defer nulls.Release()
				arrays := []arrow.Array{nulls}
				if dt.ID() != arrow.NULL {
					data := array.NewData(dt, numTestValues, []*memory.Buffer{nil, memory.NewBufferBytes(raw)}, nil, 0, 0)
					defer data.Release()
					values := array.MakeFromData(data)
					defer values.Release()
					arrays = append(arrays, values)
				}
				for _, arr := range arrays {
					chunks := mustChunk(arr)
					var sum int64
					for _, c := range chunks {
						sum += c.NumLevels
					}
					assert.Equal(t, int64(numTestValues), sum)
					if arr.NullN() == 0 {
						assert.Greater(t, len(chunks), 1)
					}
				}
			})
		}
	})

	t.Run("struct", func(t *testing.T) {
		arr := array.MakeArrayOfNull(mem, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), 4)
		defer arr.Release()
		_, err := chunk(arr)
		require.ErrorIs(t, err, arrow.ErrNotImplemented)
	})
}

// TestChunksWordsMatchBytes checks that fixed-width values, read by word for 1,
// 2, 4 and 8 bytes, chunk exactly like the fixed-size binary values of their
// little-endian bytes, on any host.
func TestChunksWordsMatchBytes(t *testing.T) {
	const numLevels = 1 << 13
	intTypes := map[int]arrow.DataType{
		1: arrow.PrimitiveTypes.Int8, 2: arrow.PrimitiveTypes.Int16, 4: arrow.PrimitiveTypes.Int32, 8: arrow.PrimitiveTypes.Int64,
		16: &arrow.Decimal128Type{Precision: 38}, 32: &arrow.Decimal256Type{Precision: 76},
	}

	optionSets := []parquet.CDCOptions{
		{MinChunkSize: 1024, MaxChunkSize: 4096},
		{MinChunkSize: 0, MaxChunkSize: 64},
		{MinChunkSize: 256, MaxChunkSize: 1024, NormLevel: 2},
	}
	for _, width := range []int{1, 2, 4, 8, 16, 32} {
		for _, shape := range []string{"required", "nullable", "nested"} {
			t.Run(fmt.Sprintf("%s/width %d", shape, width), func(t *testing.T) {
				// numLevels integers of the given width, every 10th one null unless
				// required, viewed as an integer array and as a fixed-size binary
				// array of their little-endian bytes, sharing the validity buffer.
				var raw []byte
				for i := 0; len(raw) < numLevels*width; i++ {
					raw = binary.LittleEndian.AppendUint64(raw, splitmix(uint64(i)))
				}
				raw = raw[:numLevels*width]
				validity, nulls := make([]byte, bitutil.BytesForBits(numLevels)), 0
				for i := range numLevels {
					valid := shape == "required" || i%10 != 0
					bitutil.SetBitTo(validity, i, valid)
					if !valid {
						nulls++
					}
				}
				// Decimals are native-endian 64-bit words, least significant first.
				native, lane := raw, min(width, 8)
				if endian.IsBigEndian {
					native = slices.Clone(raw)
					for i := 0; i < len(native); i += lane {
						slices.Reverse(native[i : i+lane])
					}
				}
				newView := func(dt arrow.DataType, values []byte) arrow.Array {
					buffers := []*memory.Buffer{memory.NewBufferBytes(validity), memory.NewBufferBytes(values)}
					data := array.NewData(dt, numLevels, buffers, nil, nulls, 0)
					defer data.Release()
					return array.MakeFromData(data)
				}
				ints, fsb := newView(intTypes[width], native), newView(&arrow.FixedSizeBinaryType{ByteWidth: width}, raw)
				defer ints.Release()
				defer fsb.Release()

				intIn := chunkInput{values: ints}
				switch shape {
				case "nullable":
					intIn.levels, intIn.def = file.LevelInfo{DefLevel: 1}, make([]int16, numLevels)
					for i := range intIn.def {
						if ints.IsValid(i) {
							intIn.def[i] = 1
						}
					}
				case "nested":
					// list<int> records that are null (def 0), empty (def 1) or
					// hold up to 3 elements, a null one having def 2. Null and empty
					// records have no value slot, so value and level offsets differ.
					intIn.levels = file.LevelInfo{DefLevel: 3, RepLevel: 1, RepeatedAncestorDefLevel: 2}
					for slot, r := 0, uint64(0); slot < numLevels; r++ {
						switch n := splitmix(r+1<<40) % 5; n {
						case 0, 1:
							intIn.def, intIn.rep = append(intIn.def, int16(n)), append(intIn.rep, 0)
						default:
							for e := uint64(0); e < n-1 && slot < numLevels; e++ {
								intIn.rep = append(intIn.rep, int16(min(e, 1)))
								if ints.IsNull(slot) {
									intIn.def = append(intIn.def, 2)
								} else {
									intIn.def = append(intIn.def, 3)
								}
								slot++
							}
						}
					}
				}
				fsbIn := intIn
				fsbIn.values = fsb

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

	required, nullable := newInt64Inputs(mem, numLevels)
	defer required.values.Release()
	defer nullable.values.Release()
	strs := array.NewStringBuilder(mem)
	defer strs.Release()
	for i := uint64(0); i < numLevels; i++ {
		strs.Append(strconv.FormatUint(splitmix(i), 16))
	}
	stringArr := strs.NewArray()
	defer stringArr.Release()
	list := newListInput(mem, numLevels)
	defer list.values.Release()

	benchmarks := []struct {
		name  string
		in    chunkInput
		bytes int64
	}{
		{"required/int64", required, numLevels * 8},
		{"nullable/int64", nullable, int64(nullable.values.Len()-nullable.values.NullN()) * 8},
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
