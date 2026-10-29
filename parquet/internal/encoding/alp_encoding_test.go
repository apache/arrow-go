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

package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alpTestUnpackBits unpacks with readers of its own, since the decoder's are
// tied to a page.
func alpTestUnpackBits(packed []byte, out []uint64, bitWidth int) error {
	reader := bytes.NewReader(nil)
	return alpUnpackBits(utils.NewBitReader(reader), reader, packed, out, bitWidth)
}

func TestALPPackUnpackBits(t *testing.T) {
	tests := []struct {
		name     string
		values   []uint64
		bitWidth int
	}{
		{"1-bit values", []uint64{0, 1, 0, 1, 1, 0, 1, 0}, 1},
		{"3-bit values", []uint64{0, 1, 2, 3, 4, 5, 6, 7}, 3},
		{"4-bit values", []uint64{0, 1, 5, 10, 15, 3, 7, 12}, 4},
		{"8-bit values", []uint64{0, 128, 255, 1, 42, 100, 200, 50}, 8},
		{"16-bit values", []uint64{0, 1000, 50000, 65535, 32768, 1, 256, 2048}, 16},
		{"32-bit values", []uint64{0, math.MaxUint32, 12345, 67890}, 32},
		{"48-bit values", []uint64{0, 1 << 47, 12345678901234, 1}, 48},
		{"64-bit values", []uint64{0, math.MaxUint64, 12345, 67890}, 64},
		{"zero bit width", []uint64{0, 0, 0, 0}, 0},
		{"single value", []uint64{42}, 6},
		// A full vector exercises the vectorized unpack path, which handles
		// whole groups of 32 values and leaves the remainder to the scalar one.
		{"full vector", func() []uint64 {
			v := make([]uint64, alpDefaultVectorSize)
			for i := range v {
				v[i] = uint64(i % 256)
			}
			return v
		}(), 8},
		{"partial group", func() []uint64 {
			v := make([]uint64, 100)
			for i := range v {
				v[i] = uint64(i * 1000)
			}
			return v
		}(), 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := alpAppendPackedBits(nil, tt.values, tt.bitWidth)
			assert.Len(t, packed, alpPackedSize(len(tt.values), tt.bitWidth))

			unpacked := make([]uint64, len(tt.values))
			require.NoError(t, alpTestUnpackBits(packed, unpacked, tt.bitWidth))
			assert.Equal(t, tt.values, unpacked, "pack/unpack round-trip failed")
		})
	}
}

// TestALPPackBitsKnownOutput pins the byte layout, so a change of bit packer
// cannot silently break pages written by an earlier one.
func TestALPPackBitsKnownOutput(t *testing.T) {
	packed := alpAppendPackedBits(nil, []uint64{0, 1, 2, 3}, 2)
	require.Len(t, packed, 1)
	assert.Equal(t, byte(0xE4), packed[0])
}

// TestALPUnpackBitsShortInput requires truncated packed data to return an error.
// The two lengths cover the scalar path for a partial group of 32 values and the
// vectorized path for a whole group.
func TestALPUnpackBitsShortInput(t *testing.T) {
	for _, count := range []int{8, 64} {
		out := make([]uint64, count)
		assert.Error(t, alpTestUnpackBits([]byte{0xFF}, out, 8), "count=%d", count)
	}
}

func newFloat32Column() *schema.Column {
	return schema.NewColumn(schema.NewFloat32Node("float32", parquet.Repetitions.Required, -1), 0, 0)
}

func newFloat64Column() *schema.Column {
	return schema.NewColumn(schema.NewFloat64Node("float64", parquet.Repetitions.Required, -1), 0, 0)
}

func alpRoundTrip[T alpFloat](t *testing.T, values []T) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[T](format.Encoding_ALP, nil, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	out := alpDecodePage[T](t, buf, len(values))
	for i, v := range values {
		switch want := any(v).(type) {
		case float32:
			assert.Equal(t, math.Float32bits(want), math.Float32bits(float32(out[i])),
				"index %d: got %v, want %v", i, out[i], v)
		case float64:
			assert.Equal(t, math.Float64bits(want), math.Float64bits(float64(out[i])),
				"index %d: got %v, want %v", i, out[i], v)
		}
	}
}

func testALPFloatRoundTrip[T alpFloat](t *testing.T) {
	t.Helper()
	negZero := alpNegZero[T]()
	inf, ninf := T(math.Inf(1)), T(math.Inf(-1))
	exceptions := []T{1.23, 4.56, inf, 7.89, negZero, 10.11, ninf, 12.13, 14.15, 16.17}
	allExceptions := []T{inf, ninf, negZero, inf, ninf, negZero, inf, ninf}
	tests := []struct {
		name   string
		counts []int
		value  func(int) T
	}{
		{"Monetary", []int{2000}, func(i int) T { return T(i) * 0.01 }},
		{"Integer", []int{1500}, func(i int) T { return T(i) }},
		{"Exceptions", []int{len(exceptions)}, func(i int) T { return exceptions[i] }},
		{"AllExceptions", []int{len(allExceptions)}, func(i int) T { return allExceptions[i] }},
		{"SingleValue/Positive", []int{1}, func(int) T { return 42 }},
		{"SingleValue/Zero", []int{1}, func(int) T { return 0 }},
		{"SingleValue/Negative", []int{1}, func(int) T { return -1.23 }},
		{"PartialVector", []int{1, 2, 3, 7, 100, 500, 1023}, func(i int) T { return T(i) * 0.1 }},
		{"ExactVectorSize", []int{1024}, func(i int) T { return T(i) * 0.01 }},
		{"MultipleVectors", []int{1025, 2048, 3000, 5000, 10000}, func(i int) T { return T(i) * 0.01 }},
		{"ConstantValue", []int{1500}, func(int) T { return 3.14 }},
		{"ZeroValues", []int{1024}, func(int) T { return 0 }},
		{"NegativeValues", []int{1500}, func(i int) T { return -T(i) * 0.01 }},
		{"Scientific", []int{2000}, func(i int) T { return T(i)*0.001 + 273.15 }}, // Kelvin temperatures
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, count := range tt.counts {
				t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
					values := make([]T, count)
					for i := range values {
						values[i] = tt.value(i)
					}
					alpRoundTrip(t, values)
				})
			}
		})
	}
}

func TestALPFloatRoundTrip(t *testing.T) {
	t.Run("Float32", testALPFloatRoundTrip[float32])
	t.Run("Float64", testALPFloatRoundTrip[float64])
}

func TestALPFloat32ProgressiveDecoding(t *testing.T) {
	values := make([]float32, 3000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}

	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	chunkSize := 100
	totalRead := 0
	for totalRead < len(values) {
		remaining := len(values) - totalRead
		toRead := chunkSize
		if toRead > remaining {
			toRead = remaining
		}
		out := make([]float32, toRead)
		n, err := dec.Decode(out)
		require.NoError(t, err)
		assert.Equal(t, toRead, n)

		for i := 0; i < n; i++ {
			assert.Equal(t, math.Float32bits(values[totalRead+i]), math.Float32bits(out[i]),
				"index %d: got %v, want %v", totalRead+i, out[i], values[totalRead+i])
		}
		totalRead += n
	}
	assert.Equal(t, len(values), totalRead)
}

func TestALPFloat64ProgressiveDecoding(t *testing.T) {
	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	col := newFloat64Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float64](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float64](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	chunkSize := 100
	totalRead := 0
	for totalRead < len(values) {
		remaining := len(values) - totalRead
		toRead := chunkSize
		if toRead > remaining {
			toRead = remaining
		}
		out := make([]float64, toRead)
		n, err := dec.Decode(out)
		require.NoError(t, err)
		assert.Equal(t, toRead, n)

		for i := 0; i < n; i++ {
			assert.Equal(t, math.Float64bits(values[totalRead+i]), math.Float64bits(out[i]),
				"index %d: got %v, want %v", totalRead+i, out[i], values[totalRead+i])
		}
		totalRead += n
	}
	assert.Equal(t, len(values), totalRead)
}

func TestALPFloat32Discard(t *testing.T) {
	values := make([]float32, 3000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}

	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	discarded, err := dec.Discard(1500)
	require.NoError(t, err)
	assert.Equal(t, 1500, discarded)

	out := make([]float32, 1500)
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, 1500, n)

	for i := 0; i < n; i++ {
		assert.Equal(t, math.Float32bits(values[1500+i]), math.Float32bits(out[i]),
			"index %d: got %v, want %v", 1500+i, out[i], values[1500+i])
	}
}

func TestALPFloat64Discard(t *testing.T) {
	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	col := newFloat64Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float64](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float64](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	discarded, err := dec.Discard(1500)
	require.NoError(t, err)
	assert.Equal(t, 1500, discarded)

	out := make([]float64, 1500)
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, 1500, n)

	for i := 0; i < n; i++ {
		assert.Equal(t, math.Float64bits(values[1500+i]), math.Float64bits(out[i]),
			"index %d: got %v, want %v", 1500+i, out[i], values[1500+i])
	}
}

func TestALPDiscardMoreThanAvailable(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0}

	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	discarded, err := dec.Discard(100)
	require.NoError(t, err)
	assert.Equal(t, 3, discarded, "should only discard available values")
}

func TestALPFloat32MultiplePages(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dec := newALPDecoder[float32](format.Encoding_ALP, col)

	for page := 0; page < 5; page++ {
		values := make([]float32, 500)
		for i := range values {
			values[i] = float32(page*500+i) * 0.01
		}

		enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
		defer enc.Release()
		enc.Put(values)
		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()

		err = dec.SetData(len(values), buf.Bytes())
		require.NoError(t, err)

		out := make([]float32, len(values))
		n, err := dec.Decode(out)
		require.NoError(t, err)
		assert.Equal(t, len(values), n)

		for i, v := range values {
			assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
				"page %d, index %d: mismatch", page, i)
		}
	}
}

func TestALPFloat64MultiplePages(t *testing.T) {
	col := newFloat64Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dec := newALPDecoder[float64](format.Encoding_ALP, col)

	for page := 0; page < 5; page++ {
		values := make([]float64, 500)
		for i := range values {
			values[i] = float64(page*500+i) * 0.01
		}

		enc := newALPEncoder[float64](format.Encoding_ALP, col, mem)
		defer enc.Release()
		enc.Put(values)
		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()

		err = dec.SetData(len(values), buf.Bytes())
		require.NoError(t, err)

		out := make([]float64, len(values))
		n, err := dec.Decode(out)
		require.NoError(t, err)
		assert.Equal(t, len(values), n)

		for i, v := range values {
			assert.Equal(t, math.Float64bits(v), math.Float64bits(out[i]),
				"page %d, index %d: mismatch", page, i)
		}
	}
}

func TestALPEncoderReset(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()

	values1 := []float32{1.23, 4.56, 7.89}
	enc.Put(values1)
	buf1, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf1.Release()

	enc.Reset()
	values2 := []float32{10.11, 12.13}
	enc.Put(values2)
	buf2, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf2.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values1), buf1.Bytes())
	require.NoError(t, err)
	out1 := make([]float32, len(values1))
	n, err := dec.Decode(out1)
	require.NoError(t, err)
	assert.Equal(t, len(values1), n)
	for i, v := range values1 {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out1[i]))
	}

	err = dec.SetData(len(values2), buf2.Bytes())
	require.NoError(t, err)
	out2 := make([]float32, len(values2))
	n, err = dec.Decode(out2)
	require.NoError(t, err)
	assert.Equal(t, len(values2), n)
	for i, v := range values2 {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out2[i]))
	}
}

func TestALPEncoderPutSpaced(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := []float32{1.0, 2.0, 0.0, 4.0, 0.0, 6.0, 7.0, 0.0}
	validBits := []byte{0b01101011} // bits 0,1,3,5,6 set

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.PutSpaced(values, validBits, 0)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	expected := []float32{1.0, 2.0, 4.0, 6.0, 7.0}

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(expected), buf.Bytes())
	require.NoError(t, err)

	out := make([]float32, len(expected))
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(expected), n)

	for i, v := range expected {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
			"index %d: got %v, want %v", i, out[i], v)
	}
}

func TestALPHeaderFormat(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i)
	}

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), alpHeaderSize)

	assert.Equal(t, byte(alpCompressionMode), data[0], "compression mode")
	assert.Equal(t, byte(alpIntegerEncodingFOR), data[1], "integer encoding")
	assert.Equal(t, byte(alpDefaultLogVectorSize), data[2], "log vector size")
	assert.Equal(t, uint32(100), binary.LittleEndian.Uint32(data[3:]), "element count")
}

func TestALPHeaderFormatMultipleVectors(t *testing.T) {
	col := newFloat64Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	enc := newALPEncoder[float64](format.Encoding_ALP, col, mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), alpHeaderSize)
	assert.Equal(t, uint32(3000), binary.LittleEndian.Uint32(data[3:]))

	numVectors := 3
	offsetArraySize := numVectors * 4
	require.GreaterOrEqual(t, len(data), alpHeaderSize+offsetArraySize)

	// Offsets count from the start of the array, so the first vector begins
	// where the array ends and each later one begins after its predecessor.
	firstOffset := binary.LittleEndian.Uint32(data[alpHeaderSize:])
	assert.Equal(t, uint32(offsetArraySize), firstOffset)

	for i := 1; i < numVectors; i++ {
		offset := binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
		prevOffset := binary.LittleEndian.Uint32(data[alpHeaderSize+(i-1)*4:])
		assert.Greater(t, offset, prevOffset,
			"offset[%d]=%d should be greater than offset[%d]=%d", i, offset, i-1, prevOffset)
	}
}

// alpTestPage returns a page whose header is valid, for tests that corrupt one
// field at a time.
func alpTestPage(numElements uint32) []byte {
	data := make([]byte, 100)
	data[0] = alpCompressionMode
	data[1] = alpIntegerEncodingFOR
	data[2] = alpDefaultLogVectorSize
	binary.LittleEndian.PutUint32(data[3:], numElements)
	return data
}

func TestALPDecoderInvalidData(t *testing.T) {
	col := newFloat32Column()
	dec := newALPDecoder[float32](format.Encoding_ALP, col)

	err := dec.SetData(10, []byte{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestALPDecoderInvalidHeaderFields(t *testing.T) {
	tests := []struct {
		name   string
		offset int
		value  byte
		errMsg string
	}{
		{"compression mode", 0, 99, "compression mode"},
		{"integer encoding", 1, 99, "integer encoding"},
		{"log vector size too large", 2, alpMaxLogVectorSize + 1, "log vector size"},
		{"log vector size too small", 2, alpMinLogVectorSize - 1, "log vector size"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newFloat32Column()
			dec := newALPDecoder[float32](format.Encoding_ALP, col)

			data := alpTestPage(1)
			data[tt.offset] = tt.value

			err := dec.SetData(1, data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestALPDecoderTruncatedOffsetArray(t *testing.T) {
	col := newFloat32Column()
	dec := newALPDecoder[float32](format.Encoding_ALP, col)

	data := alpTestPage(1024)[:alpHeaderSize+2] // not enough for a 4-byte offset

	err := dec.SetData(1024, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

// TestALPFloat64WidestPacking covers the widest packing a page can carry. The
// values spread over most of what an int64 holds, so their differences need all
// 64 bits and the difference between the two ends overflows a signed subtraction.
func TestALPFloat64WidestPacking(t *testing.T) {
	// Every value is an odd multiple of 2^53, which a float64 holds exactly.
	// The two ends of the run differ by 1023 * 2^54.
	values := make([]float64, alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(int64(i-len(values)/2)*(1<<54) + (1 << 53))
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float64](format.Encoding_ALP, newFloat64Column(), mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	// One offset follows the header, and it holds the size of the offset array.
	page := buf.Bytes()
	vectorPos := alpHeaderSize + int(binary.LittleEndian.Uint32(page[alpHeaderSize:]))
	assert.Equal(t, byte(64), page[vectorPos+alpVectorHeaderSize[float64]()-1], "bit width")
	assert.Zero(t, binary.LittleEndian.Uint16(page[vectorPos+2:]), "exception count")

	alpRoundTrip(t, values)
}

// alpEncodeTestPage encodes values into a single-vector page and returns the
// byte offset of that vector.
func alpEncodeTestPage(t *testing.T, values []float32) (page []byte, vectorPos int) {
	t.Helper()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float32](format.Encoding_ALP, newFloat32Column(), mem)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	page = append([]byte(nil), buf.Bytes()...)
	// One offset follows the header, and it holds the size of the offset array.
	vectorPos = alpHeaderSize + int(binary.LittleEndian.Uint32(page[alpHeaderSize:]))
	return page, vectorPos
}

// TestALPDecoderCorruptVector covers the checks a vector header gets. Each case
// writes one field a writer could not have produced, which the decoder has to
// report rather than read past the page or scale by a table it has no entry for.
func TestALPDecoderCorruptVector(t *testing.T) {
	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i) * 1.25
	}

	tests := []struct {
		name    string
		corrupt func(page []byte, vectorPos int) []byte
		errMsg  string
	}{
		{
			name: "exponent past the power table",
			corrupt: func(page []byte, vectorPos int) []byte {
				page[vectorPos] = byte(alpNumExponents[float32]())
				return page
			},
			errMsg: "invalid ALP exponent",
		},
		{
			name: "factor above the exponent",
			corrupt: func(page []byte, vectorPos int) []byte {
				page[vectorPos+1] = page[vectorPos] + 1
				return page
			},
			errMsg: "invalid ALP exponent",
		},
		{
			name: "more exceptions than values",
			corrupt: func(page []byte, vectorPos int) []byte {
				binary.LittleEndian.PutUint16(page[vectorPos+2:], uint16(len(values)+1))
				return page
			},
			errMsg: "exceptions in",
		},
		{
			name: "bit width past the integer width",
			corrupt: func(page []byte, vectorPos int) []byte {
				page[vectorPos+alpVectorHeaderSize[float32]()-1] = 33
				return page
			},
			errMsg: "invalid ALP bit width",
		},
		{
			name: "vector longer than the page",
			corrupt: func(page []byte, vectorPos int) []byte {
				return page[:len(page)-1]
			},
			errMsg: "past the end of the page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, vectorPos := alpEncodeTestPage(t, values)
			page = tt.corrupt(page, vectorPos)

			dec := newALPDecoder[float32](format.Encoding_ALP, newFloat32Column())
			require.NoError(t, dec.SetData(len(values), page))

			_, err := dec.Decode(make([]float32, len(values)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestALPDecoderExceptionPositionOutsideVector covers the one check that needs a
// vector with an exception in it: the position that patches the value back in
// has to land inside the vector.
func TestALPDecoderExceptionPositionOutsideVector(t *testing.T) {
	values := []float32{1.25, 2.5, float32(math.Inf(1)), 3.75}
	page, vectorPos := alpEncodeTestPage(t, values)

	// The positions follow the packed deltas, which the vector's own bit width
	// sizes.
	headerSize := alpVectorHeaderSize[float32]()
	bitWidth := int(page[vectorPos+headerSize-1])
	positionPos := vectorPos + headerSize + alpPackedSize(len(values), bitWidth)
	require.Equal(t, 1, int(binary.LittleEndian.Uint16(page[vectorPos+2:])), "one exception")
	binary.LittleEndian.PutUint16(page[positionPos:], uint16(len(values)))

	dec := newALPDecoder[float32](format.Encoding_ALP, newFloat32Column())
	require.NoError(t, dec.SetData(len(values), page))

	_, err := dec.Decode(make([]float32, len(values)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exception position")
}

func TestALPPresetCacheBuilds(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()

	values := make([]float32, 9000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}
	enc.Put(values)

	assert.NotNil(t, enc.cachedPresets, "preset cache should be built after 8 vectors")
	assert.True(t, len(enc.cachedPresets) > 0, "should have at least one preset")
	assert.True(t, len(enc.cachedPresets) <= alpMaxPresetCombinations, "should not exceed max presets")

	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	out := make([]float32, len(values))
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(values), n)

	for i, v := range values {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
			"index %d: got %v, want %v", i, out[i], v)
	}
}

func testALPEstimatedDataEncodedSize[T alpFloat](t *testing.T) {
	t.Helper()
	tests := []struct {
		name          string
		count         int
		value         func(int) T
		allExceptions bool
	}{
		{"Empty", 0, func(int) T { return 0 }, false},
		{"BufferedMonetary", 100, func(i int) T { return T(i) * 0.01 }, false},
		{"BufferedDecimals", 100, func(i int) T { return T(i) / 4 }, false},
		{"IntegerVector", alpDefaultVectorSize, func(i int) T { return T(i) }, false},
		{"ConstantVector", alpDefaultVectorSize, func(int) T { return 3.14 }, false},
		{"MonetaryVectors", 2 * alpDefaultVectorSize, func(i int) T { return T(i) * 0.01 }, false},
		{"DecimalsWithBufferedTail", alpDefaultVectorSize + 100, func(i int) T { return T(i) / 4 }, false},
		{"AllExceptionsVector", alpDefaultVectorSize, func(int) T { return T(math.Inf(1)) }, true},
		{"AllExceptionsBuffered", 100, func(int) T { return T(math.Inf(1)) }, true},
		{"AllExceptionsWithBufferedTail", alpDefaultVectorSize + 100, func(int) T { return T(math.Inf(1)) }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			enc := newALPEncoder[T](format.Encoding_ALP, nil, mem)
			defer enc.Release()
			assert.Zero(t, enc.EstimatedDataEncodedSize())

			values := make([]T, tt.count)
			for i := range values {
				values[i] = tt.value(i)
			}
			enc.Put(values)
			estimate := enc.EstimatedDataEncodedSize()
			buf, err := enc.FlushValues()
			require.NoError(t, err)
			defer buf.Release()

			const pageHeaderSize = 7
			require.GreaterOrEqual(t, buf.Len(), pageHeaderSize)
			payloadSize := int64(buf.Len() - pageHeaderSize)
			pending := tt.count % alpDefaultVectorSize
			switch {
			case pending == 0:
				assert.Equal(t, payloadSize, estimate, "completed vectors have an exact size")
			case tt.allExceptions:
				assert.Equal(t, payloadSize, estimate,
					"a pending vector is estimated as if every value were an exception")
			default:
				assert.GreaterOrEqual(t, estimate, payloadSize,
					"a pending vector must not be underestimated")
			}
			assert.Zero(t, enc.EstimatedDataEncodedSize(), "flushing starts a new page")
		})
	}
}

func TestALPEstimatedDataEncodedSize(t *testing.T) {
	t.Run("Float32", func(t *testing.T) {
		testALPEstimatedDataEncodedSize[float32](t)
	})
	t.Run("Float64", func(t *testing.T) {
		testALPEstimatedDataEncodedSize[float64](t)
	})
}

func TestALPFloat32IncrementalPut(t *testing.T) {
	col := newFloat32Column()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newALPEncoder[float32](format.Encoding_ALP, col, mem)
	defer enc.Release()
	allValues := make([]float32, 2500)
	for i := range allValues {
		allValues[i] = float32(i) * 0.01
	}

	offset := 0
	chunks := []int{100, 200, 500, 724, 976} // total = 2500
	for _, chunk := range chunks {
		enc.Put(allValues[offset : offset+chunk])
		offset += chunk
	}

	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newALPDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(allValues), buf.Bytes())
	require.NoError(t, err)

	out := make([]float32, len(allValues))
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(allValues), n)

	for i, v := range allValues {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
			"index %d: got %v, want %v", i, out[i], v)
	}
}

func TestALPFloat32MixedDataPatterns(t *testing.T) {
	var values []float32

	for i := 0; i < 500; i++ {
		values = append(values, float32(i)*0.01)
	}
	for i := 0; i < 500; i++ {
		values = append(values, float32(i))
	}
	for i := 0; i < 500; i++ {
		values = append(values, float32(i)*0.001+100.0)
	}
	values = append(values, float32(math.Inf(1)))
	values = append(values, float32(math.Inf(-1)))
	values = append(values, alpNegZero[float32]())
	for i := 0; i < 497; i++ {
		values = append(values, float32(i)*0.1)
	}

	alpRoundTrip(t, values)
}

func TestALPFloat64MixedDataPatterns(t *testing.T) {
	var values []float64

	for i := 0; i < 500; i++ {
		values = append(values, float64(i)*0.01)
	}
	for i := 0; i < 500; i++ {
		values = append(values, float64(i))
	}
	for i := 0; i < 500; i++ {
		values = append(values, float64(i)*0.001+100.0)
	}
	values = append(values, math.Inf(1))
	values = append(values, math.Inf(-1))
	values = append(values, alpNegZero[float64]())
	for i := 0; i < 497; i++ {
		values = append(values, float64(i)*0.1)
	}

	alpRoundTrip(t, values)
}

func TestALPDecoderPanicsForNonFloatType(t *testing.T) {
	col := schema.NewColumn(schema.NewInt32Node("int32", parquet.Repetitions.Required, -1), 0, 0)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	assert.Panics(t, func() {
		NewDecoder(parquet.Types.Int32, parquet.Encodings.ALP, col, mem)
	}, "ALP should panic for non-float decoder")
}

// alpDecodePage decodes a whole page, so that a test can check what an encoder
// wrote without going through a file.
func alpDecodePage[T alpFloat](t *testing.T, page Buffer, numValues int) []T {
	t.Helper()
	dec := newALPDecoder[T](format.Encoding_ALP, nil)
	require.NoError(t, dec.SetData(numValues, page.Bytes()))

	out := make([]T, numValues)
	n, err := dec.Decode(out)
	require.NoError(t, err)
	require.Equal(t, numValues, n)
	return out
}

// A column writes a new page every time its buffered size passes the page size,
// and calls FlushValues for each one without resetting the encoder in between.
// A page therefore has to hold its own values and no others.
func TestALPEncoderFlushValuesStartsANewPage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float64](format.Encoding_ALP, nil, mem)
	defer enc.Release()

	first := make([]float64, 2*alpDefaultVectorSize)
	for i := range first {
		first[i] = float64(i) / 100
	}
	enc.Put(first)
	page, err := enc.FlushValues()
	require.NoError(t, err)
	defer page.Release()
	got := alpDecodePage[float64](t, page, len(first))
	assert.Equal(t, first, got)

	second := []float64{9.5, 8.25, 7.125}
	enc.Put(second)
	fresh := newALPEncoder[float64](format.Encoding_ALP, nil, mem)
	defer fresh.Release()
	fresh.Put(second)
	assert.Equal(t, fresh.EstimatedDataEncodedSize(), enc.EstimatedDataEncodedSize(),
		"the flushed page no longer counts towards the next one")

	page, err = enc.FlushValues()
	require.NoError(t, err)
	defer page.Release()
	got = alpDecodePage[float64](t, page, len(second))
	assert.Equal(t, second, got)
}

// The shortlist describes the column, so it outlives a page. A second page of
// the same values encodes to the same bytes as the first.
func TestALPEncoderKeepsPresetsAcrossPages(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float64](format.Encoding_ALP, nil, mem)
	defer enc.Release()

	values := make([]float64, 10*alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(i%997) / 100
	}

	enc.Put(values)
	first, err := enc.FlushValues()
	require.NoError(t, err)
	defer first.Release()
	require.NotNil(t, enc.cachedPresets, "ten vectors is past the sampling threshold")
	presets := slices.Clone(enc.cachedPresets)

	enc.Put(values)
	second, err := enc.FlushValues()
	require.NoError(t, err)
	defer second.Release()

	assert.Equal(t, presets, enc.cachedPresets)
	assert.Equal(t, first.Bytes(), second.Bytes())
}

// Reset returns the encoder to its initial state, shortlist included.
func TestALPEncoderResetClearsPresets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float64](format.Encoding_ALP, nil, mem)
	defer enc.Release()

	values := make([]float64, 10*alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(i) / 100
	}
	enc.Put(values)
	require.NotNil(t, enc.cachedPresets)

	enc.Reset()
	assert.Nil(t, enc.cachedPresets)
	assert.Empty(t, enc.presetCounts)
	assert.Zero(t, enc.vectorsProcessed)
	assert.Zero(t, enc.EstimatedDataEncodedSize())
}

// Every vector size the format allows must round-trip, including partial last
// vectors and the largest size, whose exception positions reach 32767.
func TestALPEncoderVectorSizes(t *testing.T) {
	for _, size := range []int{8, 32, 1024, 4096, 1 << 15} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			values := make([]float64, 2*size+size/2+3)
			for i := range values {
				values[i] = float64(i%1000) / 100
			}
			// Exceptions at both ends of every vector.
			for i := 0; i < len(values); i += size {
				values[i] = math.Pi
				values[min(i+size, len(values))-1] = math.NaN()
			}

			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			enc := newALPEncoder[float64](format.Encoding_ALP, nil, mem)
			defer enc.Release()
			require.NoError(t, enc.SetVectorSize(size))
			enc.Put(values)
			page, err := enc.FlushValues()
			require.NoError(t, err)
			defer page.Release()

			data := page.Bytes()
			assert.Equal(t, byte(bits.TrailingZeros(uint(size))), data[2], "log vector size")
			numVectors := (len(values) + size - 1) / size
			assert.EqualValues(t, numVectors*4, binary.LittleEndian.Uint32(data[alpHeaderSize:]),
				"the first vector starts after %d offsets", numVectors)

			got := alpDecodePage[float64](t, page, len(values))
			for i, v := range values {
				require.Equal(t, math.Float64bits(v), math.Float64bits(got[i]), "index %d", i)
			}
		})
	}
}

func TestALPEncoderSetVectorSizeRejects(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	enc := newALPEncoder[float32](format.Encoding_ALP, nil, mem)
	defer enc.Release()
	for _, size := range []int{0, 4, 1000, 1 << 16} {
		assert.Error(t, enc.SetVectorSize(size), "size %d", size)
	}

	enc.Put([]float32{1.5})
	assert.Error(t, enc.SetVectorSize(32), "values written at 1024 cannot move to 32")

	page, err := enc.FlushValues()
	require.NoError(t, err)
	page.Release()
	require.NoError(t, enc.SetVectorSize(32), "a flushed page leaves nothing buffered")
}

// A FLOAT value scaled to just under 2^31 can round up to 2^31, which a vector
// cannot hold: readers rebuild its integers in int32 and would flip the sign.
func TestALPFloat32RoundingPastInt32IsAnException(t *testing.T) {
	s := newALPScaling[float32](9, 6)
	_, ok := s.tryEncode(2147483.5)
	assert.False(t, ok, "2147483.5 at (9,6) rounds to 2^31")

	alpRoundTrip(t, []float32{-24.875, 2147483.5, 12})
	alpRoundTrip(t, []float32{-24.875, -2147483.5, 12})
}

// Page fields near the int32 limits must be rejected, not overflow int on
// 32-bit platforms.
func TestALPDecoderHugeFields(t *testing.T) {
	dec := newALPDecoder[float32](format.Encoding_ALP, newFloat32Column())
	assert.Error(t, dec.SetData(1, alpTestPage(math.MaxInt32)),
		"the vector count must not overflow")

	page, _ := alpEncodeTestPage(t, []float32{1.5, 2.5})
	binary.LittleEndian.PutUint32(page[alpHeaderSize:], math.MaxInt32-4)
	require.NoError(t, dec.SetData(2, page))
	_, err := dec.Decode(make([]float32, 2))
	assert.ErrorContains(t, err, "past the end of the page")
}
