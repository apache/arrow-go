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

func TestAlpPackUnpackBits(t *testing.T) {
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

// TestAlpPackBitsKnownOutput pins the byte layout, so a change of bit packer
// cannot silently break pages written by an earlier one.
func TestAlpPackBitsKnownOutput(t *testing.T) {
	packed := alpAppendPackedBits(nil, []uint64{0, 1, 2, 3}, 2)
	require.Len(t, packed, 1)
	assert.Equal(t, byte(0xE4), packed[0])
}

// TestAlpUnpackBitsShortInput covers a truncated page: unpacking has to report
// an error rather than hand back whatever it managed to read. The two lengths
// take the two paths, scalar for a partial group of 32 and vectorized for a
// whole one.
func TestAlpUnpackBitsShortInput(t *testing.T) {
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

func alpFloat32RoundTrip(t *testing.T, values []float32) {
	t.Helper()
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	out := make([]float32, len(values))
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(values), n)

	for i, v := range values {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
			"index %d: got %v (bits %08x), want %v (bits %08x)",
			i, out[i], math.Float32bits(out[i]), v, math.Float32bits(v))
	}
}

func alpFloat64RoundTrip(t *testing.T, values []float64) {
	t.Helper()
	col := newFloat64Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float64](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float64](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	out := make([]float64, len(values))
	n, err := dec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(values), n)

	for i, v := range values {
		assert.Equal(t, math.Float64bits(v), math.Float64bits(out[i]),
			"index %d: got %v (bits %016x), want %v (bits %016x)",
			i, out[i], math.Float64bits(out[i]), v, math.Float64bits(v))
	}
}

func TestAlpFloat32MonetaryRoundTrip(t *testing.T) {
	values := make([]float32, 2000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64MonetaryRoundTrip(t *testing.T) {
	values := make([]float64, 2000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32IntegerRoundTrip(t *testing.T) {
	values := make([]float32, 1500)
	for i := range values {
		values[i] = float32(i)
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64IntegerRoundTrip(t *testing.T) {
	values := make([]float64, 1500)
	for i := range values {
		values[i] = float64(i)
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32ExceptionsRoundTrip(t *testing.T) {
	negZero := alpNegZero[float32]()
	inf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))

	values := []float32{
		1.23, 4.56, inf, 7.89, negZero, 10.11,
		ninf, 12.13, 14.15, 16.17,
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ExceptionsRoundTrip(t *testing.T) {
	negZero := alpNegZero[float64]()
	inf := math.Inf(1)
	ninf := math.Inf(-1)

	values := []float64{
		1.23, 4.56, inf, 7.89, negZero, 10.11,
		ninf, 12.13, 14.15, 16.17,
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32AllExceptionsRoundTrip(t *testing.T) {
	negZero := alpNegZero[float32]()
	inf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))

	values := []float32{inf, ninf, negZero, inf, ninf, negZero, inf, ninf}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64AllExceptionsRoundTrip(t *testing.T) {
	negZero := alpNegZero[float64]()
	inf := math.Inf(1)
	ninf := math.Inf(-1)

	values := []float64{inf, ninf, negZero, inf, ninf, negZero, inf, ninf}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32SingleValueRoundTrip(t *testing.T) {
	alpFloat32RoundTrip(t, []float32{42.0})
	alpFloat32RoundTrip(t, []float32{0.0})
	alpFloat32RoundTrip(t, []float32{-1.23})
}

func TestAlpFloat64SingleValueRoundTrip(t *testing.T) {
	alpFloat64RoundTrip(t, []float64{42.0})
	alpFloat64RoundTrip(t, []float64{0.0})
	alpFloat64RoundTrip(t, []float64{-1.23})
}

func TestAlpFloat32PartialVectorRoundTrip(t *testing.T) {
	for _, count := range []int{1, 2, 3, 7, 100, 500, 1023} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			values := make([]float32, count)
			for i := range values {
				values[i] = float32(i) * 0.1
			}
			alpFloat32RoundTrip(t, values)
		})
	}
}

func TestAlpFloat64PartialVectorRoundTrip(t *testing.T) {
	for _, count := range []int{1, 2, 3, 7, 100, 500, 1023} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			values := make([]float64, count)
			for i := range values {
				values[i] = float64(i) * 0.1
			}
			alpFloat64RoundTrip(t, values)
		})
	}
}

func TestAlpFloat32ExactVectorSizeRoundTrip(t *testing.T) {
	values := make([]float32, 1024)
	for i := range values {
		values[i] = float32(i) * 0.01
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ExactVectorSizeRoundTrip(t *testing.T) {
	values := make([]float64, 1024)
	for i := range values {
		values[i] = float64(i) * 0.01
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32MultipleVectorsRoundTrip(t *testing.T) {
	for _, count := range []int{1025, 2048, 3000, 5000, 10000} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			values := make([]float32, count)
			for i := range values {
				values[i] = float32(i) * 0.01
			}
			alpFloat32RoundTrip(t, values)
		})
	}
}

func TestAlpFloat64MultipleVectorsRoundTrip(t *testing.T) {
	for _, count := range []int{1025, 2048, 3000, 5000, 10000} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			values := make([]float64, count)
			for i := range values {
				values[i] = float64(i) * 0.01
			}
			alpFloat64RoundTrip(t, values)
		})
	}
}

func TestAlpFloat32ConstantValueRoundTrip(t *testing.T) {
	values := make([]float32, 1500)
	for i := range values {
		values[i] = 3.14
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ConstantValueRoundTrip(t *testing.T) {
	values := make([]float64, 1500)
	for i := range values {
		values[i] = 3.14
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32ZeroValuesRoundTrip(t *testing.T) {
	values := make([]float32, 1024)
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ZeroValuesRoundTrip(t *testing.T) {
	values := make([]float64, 1024)
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32NegativeValuesRoundTrip(t *testing.T) {
	values := make([]float32, 1500)
	for i := range values {
		values[i] = -float32(i) * 0.01
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64NegativeValuesRoundTrip(t *testing.T) {
	values := make([]float64, 1500)
	for i := range values {
		values[i] = -float64(i) * 0.01
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32ScientificRoundTrip(t *testing.T) {
	values := make([]float32, 2000)
	for i := range values {
		values[i] = float32(i)*0.001 + 273.15 // Kelvin temperatures
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ScientificRoundTrip(t *testing.T) {
	values := make([]float64, 2000)
	for i := range values {
		values[i] = float64(i)*0.001 + 273.15
	}
	alpFloat64RoundTrip(t, values)
}
func TestAlpFloat32ProgressiveDecoding(t *testing.T) {
	values := make([]float32, 3000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}

	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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

func TestAlpFloat64ProgressiveDecoding(t *testing.T) {
	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	col := newFloat64Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float64](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float64](format.Encoding_ALP, col)
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
func TestAlpFloat32Discard(t *testing.T) {
	values := make([]float32, 3000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}

	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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

func TestAlpFloat64Discard(t *testing.T) {
	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	col := newFloat64Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float64](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float64](format.Encoding_ALP, col)
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

func TestAlpDiscardMoreThanAvailable(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0}

	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	discarded, err := dec.Discard(100)
	require.NoError(t, err)
	assert.Equal(t, 3, discarded, "should only discard available values")
}
func TestAlpFloat32MultiplePages(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)

	for page := 0; page < 5; page++ {
		values := make([]float32, 500)
		for i := range values {
			values[i] = float32(page*500+i) * 0.01
		}

		enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
		enc.Put(values)
		buf, err := enc.FlushValues()
		require.NoError(t, err)

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

		buf.Release()
	}
}

func TestAlpFloat64MultiplePages(t *testing.T) {
	col := newFloat64Column()
	mem := memory.DefaultAllocator

	dec := newAlpDecoder[float64](format.Encoding_ALP, col)

	for page := 0; page < 5; page++ {
		values := make([]float64, 500)
		for i := range values {
			values[i] = float64(page*500+i) * 0.01
		}

		enc := newAlpEncoder[float64](format.Encoding_ALP, col, mem)
		enc.Put(values)
		buf, err := enc.FlushValues()
		require.NoError(t, err)

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

		buf.Release()
	}
}
func TestAlpEncoderReset(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)

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

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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

func TestAlpEncoderType(t *testing.T) {
	col32 := newFloat32Column()
	col64 := newFloat64Column()
	mem := memory.DefaultAllocator

	enc32 := newAlpEncoder[float32](format.Encoding_ALP, col32, mem)
	assert.Equal(t, parquet.Types.Float, enc32.Type())

	enc64 := newAlpEncoder[float64](format.Encoding_ALP, col64, mem)
	assert.Equal(t, parquet.Types.Double, enc64.Type())
}

func TestAlpDecoderType(t *testing.T) {
	col32 := newFloat32Column()
	col64 := newFloat64Column()

	dec32 := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col32)}
	assert.Equal(t, parquet.Types.Float, dec32.Type())

	dec64 := &alpDecoder[float64]{decoder: newDecoderBase(format.Encoding_ALP, col64)}
	assert.Equal(t, parquet.Types.Double, dec64.Type())
}

func TestAlpEncoderPutSpaced(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	values := []float32{1.0, 2.0, 0.0, 4.0, 0.0, 6.0, 7.0, 0.0}
	validBits := []byte{0b01101011} // bits 0,1,3,5,6 set

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
	enc.PutSpaced(values, validBits, 0)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	expected := []float32{1.0, 2.0, 4.0, 6.0, 7.0}

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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
func TestAlpHeaderFormat(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i)
	}

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
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

func TestAlpHeaderFormatMultipleVectors(t *testing.T) {
	col := newFloat64Column()
	mem := memory.DefaultAllocator

	values := make([]float64, 3000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	enc := newAlpEncoder[float64](format.Encoding_ALP, col, mem)
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

func TestAlpDecoderInvalidData(t *testing.T) {
	col := newFloat32Column()
	dec := newAlpDecoder[float32](format.Encoding_ALP, col)

	err := dec.SetData(10, []byte{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestAlpDecoderInvalidHeaderFields(t *testing.T) {
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
			dec := newAlpDecoder[float32](format.Encoding_ALP, col)

			data := alpTestPage(1)
			data[tt.offset] = tt.value

			err := dec.SetData(1, data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestAlpDecoderTruncatedOffsetArray(t *testing.T) {
	col := newFloat32Column()
	dec := newAlpDecoder[float32](format.Encoding_ALP, col)

	data := alpTestPage(1024)[:alpHeaderSize+2] // not enough for a 4-byte offset

	err := dec.SetData(1024, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

// alpEncodeTestPage encodes values into a one vector page, and returns the page
// along with where that vector starts in it.
// TestAlpFloat64WidestPacking covers the widest packing a page can carry. The
// values spread over most of what an int64 holds, so their differences need all
// 64 bits and the difference between the two ends overflows a signed subtraction.
func TestAlpFloat64WidestPacking(t *testing.T) {
	// Every value is an odd multiple of 2^53, which a float64 holds exactly.
	// The two ends of the run differ by 1023 * 2^54.
	values := make([]float64, alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(int64(i-len(values)/2)*(1<<54) + (1 << 53))
	}

	enc := newAlpEncoder[float64](format.Encoding_ALP, newFloat64Column(), memory.DefaultAllocator)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	// One offset follows the header, and it holds the size of the offset array.
	page := buf.Bytes()
	vectorPos := alpHeaderSize + int(binary.LittleEndian.Uint32(page[alpHeaderSize:]))
	assert.Equal(t, byte(64), page[vectorPos+alpVectorHeaderSize[float64]()-1], "bit width")
	assert.Zero(t, binary.LittleEndian.Uint16(page[vectorPos+2:]), "exception count")

	alpFloat64RoundTrip(t, values)
}

func alpEncodeTestPage(t *testing.T, values []float32) (page []byte, vectorPos int) {
	t.Helper()

	enc := newAlpEncoder[float32](format.Encoding_ALP, newFloat32Column(), memory.DefaultAllocator)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	t.Cleanup(buf.Release)

	page = append([]byte(nil), buf.Bytes()...)
	// One offset follows the header, and it holds the size of the offset array.
	vectorPos = alpHeaderSize + int(binary.LittleEndian.Uint32(page[alpHeaderSize:]))
	return page, vectorPos
}

// TestAlpDecoderCorruptVector covers the checks a vector header gets. Each case
// writes one field a writer could not have produced, which the decoder has to
// report rather than read past the page or scale by a table it has no entry for.
func TestAlpDecoderCorruptVector(t *testing.T) {
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

			dec := newAlpDecoder[float32](format.Encoding_ALP, newFloat32Column())
			require.NoError(t, dec.SetData(len(values), page))

			_, err := dec.Decode(make([]float32, len(values)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestAlpDecoderExceptionPositionOutsideVector covers the one check that needs a
// vector with an exception in it: the position that patches the value back in
// has to land inside the vector.
func TestAlpDecoderExceptionPositionOutsideVector(t *testing.T) {
	values := []float32{1.25, 2.5, float32(math.Inf(1)), 3.75}
	page, vectorPos := alpEncodeTestPage(t, values)

	// The positions follow the packed deltas, which the vector's own bit width
	// sizes.
	headerSize := alpVectorHeaderSize[float32]()
	bitWidth := int(page[vectorPos+headerSize-1])
	positionPos := vectorPos + headerSize + alpPackedSize(len(values), bitWidth)
	require.Equal(t, 1, int(binary.LittleEndian.Uint16(page[vectorPos+2:])), "one exception")
	binary.LittleEndian.PutUint16(page[positionPos:], uint16(len(values)))

	dec := newAlpDecoder[float32](format.Encoding_ALP, newFloat32Column())
	require.NoError(t, dec.SetData(len(values), page))

	_, err := dec.Decode(make([]float32, len(values)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exception position")
}

func TestAlpPresetCacheBuilds(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)

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

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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
func TestAlpEstimatedDataEncodedSize(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)

	assert.Equal(t, int64(0), enc.EstimatedDataEncodedSize())

	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i) * 0.01
	}
	enc.Put(values)
	size := enc.EstimatedDataEncodedSize()
	assert.True(t, size > 0, "estimated size should be positive after Put")
}
func TestAlpFloat32IncrementalPut(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	enc := newAlpEncoder[float32](format.Encoding_ALP, col, mem)
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

	dec := newAlpDecoder[float32](format.Encoding_ALP, col)
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
func TestAlpFloat32MixedDataPatterns(t *testing.T) {
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

	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64MixedDataPatterns(t *testing.T) {
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

	alpFloat64RoundTrip(t, values)
}
func TestAlpFloat32ViaPublicAPI(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	values := make([]float32, 2000)
	for i := range values {
		values[i] = float32(i) * 0.01
	}

	enc := NewEncoder(parquet.Types.Float, parquet.Encodings.ALP, false, col, mem)
	assert.Equal(t, parquet.Encodings.ALP, enc.Encoding())
	assert.Equal(t, parquet.Types.Float, enc.Type())

	enc.(Float32Encoder).Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := NewDecoder(parquet.Types.Float, parquet.Encodings.ALP, col, mem)
	assert.Equal(t, parquet.Encodings.ALP, dec.Encoding())
	assert.Equal(t, parquet.Types.Float, dec.Type())

	dec.SetData(len(values), buf.Bytes())
	out := make([]float32, len(values))
	n, err := dec.(Float32Decoder).Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(values), n)

	for i, v := range values {
		assert.Equal(t, math.Float32bits(v), math.Float32bits(out[i]),
			"index %d: mismatch", i)
	}
}

func TestAlpFloat64ViaPublicAPI(t *testing.T) {
	col := newFloat64Column()
	mem := memory.DefaultAllocator

	values := make([]float64, 2000)
	for i := range values {
		values[i] = float64(i) * 0.01
	}

	enc := NewEncoder(parquet.Types.Double, parquet.Encodings.ALP, false, col, mem)
	assert.Equal(t, parquet.Encodings.ALP, enc.Encoding())
	assert.Equal(t, parquet.Types.Double, enc.Type())

	enc.(Float64Encoder).Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	dec := NewDecoder(parquet.Types.Double, parquet.Encodings.ALP, col, mem)
	assert.Equal(t, parquet.Encodings.ALP, dec.Encoding())
	assert.Equal(t, parquet.Types.Double, dec.Type())

	dec.SetData(len(values), buf.Bytes())
	out := make([]float64, len(values))
	n, err := dec.(Float64Decoder).Decode(out)
	require.NoError(t, err)
	assert.Equal(t, len(values), n)

	for i, v := range values {
		assert.Equal(t, math.Float64bits(v), math.Float64bits(out[i]),
			"index %d: mismatch", i)
	}
}

func TestAlpPanicsForNonFloatTypes(t *testing.T) {
	col := schema.NewColumn(schema.NewInt32Node("int32", parquet.Repetitions.Required, -1), 0, 0)
	mem := memory.DefaultAllocator

	assert.Panics(t, func() {
		NewEncoder(parquet.Types.Int32, parquet.Encodings.ALP, false, col, mem)
	}, "ALP should panic for non-float encoder")

	assert.Panics(t, func() {
		NewDecoder(parquet.Types.Int32, parquet.Encodings.ALP, col, mem)
	}, "ALP should panic for non-float decoder")
}

// alpDecodePage decodes a whole page, so that a test can check what an encoder
// wrote without going through a file.
func alpDecodePage[T alpFloat](t *testing.T, page Buffer, numValues int) []T {
	t.Helper()
	dec := newAlpDecoder[T](format.Encoding_ALP, nil)
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
func TestAlpEncoderFlushValuesStartsANewPage(t *testing.T) {
	enc := newAlpEncoder[float64](format.Encoding_ALP, nil, memory.DefaultAllocator)

	first := make([]float64, 2*alpDefaultVectorSize)
	for i := range first {
		first[i] = float64(i) / 100
	}
	enc.Put(first)
	page, err := enc.FlushValues()
	require.NoError(t, err)
	got := alpDecodePage[float64](t, page, len(first))
	page.Release()
	assert.Equal(t, first, got)

	second := []float64{9.5, 8.25, 7.125}
	enc.Put(second)
	assert.EqualValues(t, len(second)*8, enc.EstimatedDataEncodedSize(),
		"the flushed page still counts towards the next one")

	page, err = enc.FlushValues()
	require.NoError(t, err)
	got = alpDecodePage[float64](t, page, len(second))
	page.Release()
	assert.Equal(t, second, got)
}

// The shortlist describes the column, so it outlives a page. A second page of
// the same values encodes to the same bytes as the first.
func TestAlpEncoderKeepsPresetsAcrossPages(t *testing.T) {
	enc := newAlpEncoder[float64](format.Encoding_ALP, nil, memory.DefaultAllocator)

	values := make([]float64, 10*alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(i%997) / 100
	}

	enc.Put(values)
	first, err := enc.FlushValues()
	require.NoError(t, err)
	require.NotNil(t, enc.cachedPresets, "ten vectors is past the sampling threshold")
	presets := slices.Clone(enc.cachedPresets)

	enc.Put(values)
	second, err := enc.FlushValues()
	require.NoError(t, err)

	assert.Equal(t, presets, enc.cachedPresets)
	assert.Equal(t, first.Bytes(), second.Bytes())
	first.Release()
	second.Release()
}

// Reset returns the encoder to its initial state, shortlist included.
func TestAlpEncoderResetClearsPresets(t *testing.T) {
	enc := newAlpEncoder[float64](format.Encoding_ALP, nil, memory.DefaultAllocator)

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
