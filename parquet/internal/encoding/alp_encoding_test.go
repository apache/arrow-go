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
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlpFastRoundFloat32(t *testing.T) {
	tests := []struct {
		name     string
		input    float32
		expected int32
	}{
		{"positive integer", 5.0, 5},
		{"positive round up", 2.7, 3},
		{"positive round down", 2.3, 2},
		{"positive half", 2.5, 2}, // round-to-even via magic trick
		{"negative integer", -5.0, -5},
		{"negative round up", -2.3, -2},
		{"negative round down", -2.7, -3},
		{"zero", 0.0, 0},
		{"small positive", 0.1, 0},
		{"large integer", 12345.0, 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := alpFastRoundFloat32(tt.input)
			assert.Equal(t, tt.expected, result, "alpFastRoundFloat32(%v) = %d, want %d", tt.input, result, tt.expected)
		})
	}
}

func TestAlpFastRoundFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected int64
	}{
		{"positive integer", 5.0, 5},
		{"positive round up", 2.7, 3},
		{"positive round down", 2.3, 2},
		{"negative integer", -5.0, -5},
		{"negative round up", -2.3, -2},
		{"negative round down", -2.7, -3},
		{"zero", 0.0, 0},
		{"small positive", 0.1, 0},
		{"large integer", 123456789.0, 123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := alpFastRoundFloat64(tt.input)
			assert.Equal(t, tt.expected, result, "alpFastRoundFloat64(%v) = %d, want %d", tt.input, result, tt.expected)
		})
	}
}

func TestAlpIsBasicFloatException(t *testing.T) {
	assert.True(t, alpIsBasicFloatException(float32(math.NaN())), "NaN should be exception")
	assert.True(t, alpIsBasicFloatException(float32(math.Inf(1))), "+Inf should be exception")
	assert.True(t, alpIsBasicFloatException(float32(math.Inf(-1))), "-Inf should be exception")
	negZero := math.Float32frombits(alpNegZeroFloat32Bits)
	assert.True(t, alpIsBasicFloatException(negZero), "-0.0 should be exception")
	assert.False(t, alpIsBasicFloatException(0.0), "+0.0 should not be exception")
	assert.False(t, alpIsBasicFloatException(1.0), "1.0 should not be exception")
	assert.False(t, alpIsBasicFloatException(-1.0), "-1.0 should not be exception")
}

func TestAlpIsBasicDoubleException(t *testing.T) {
	assert.True(t, alpIsBasicDoubleException(math.NaN()), "NaN should be exception")
	assert.True(t, alpIsBasicDoubleException(math.Inf(1)), "+Inf should be exception")
	assert.True(t, alpIsBasicDoubleException(math.Inf(-1)), "-Inf should be exception")
	negZero := math.Float64frombits(alpNegZeroFloat64Bits)
	assert.True(t, alpIsBasicDoubleException(negZero), "-0.0 should be exception")
	assert.False(t, alpIsBasicDoubleException(0.0), "+0.0 should not be exception")
	assert.False(t, alpIsBasicDoubleException(1.0), "1.0 should not be exception")
	assert.False(t, alpIsBasicDoubleException(-1.0), "-1.0 should not be exception")
}

func TestAlpIsFloatException(t *testing.T) {
	assert.True(t, alpIsFloatException(float32(math.NaN()), 2, 0))
	assert.True(t, alpIsFloatException(float32(math.Inf(1)), 2, 0))
	negZero := math.Float32frombits(alpNegZeroFloat32Bits)
	assert.True(t, alpIsFloatException(negZero, 2, 0))

	assert.False(t, alpIsFloatException(1.23, 2, 0), "1.23 with exp=2 should encode to 123")
	assert.False(t, alpIsFloatException(0.0, 2, 0), "0.0 should not be exception")
	assert.False(t, alpIsFloatException(42.0, 0, 0), "42.0 with exp=0 should encode to 42")

	assert.True(t, alpIsFloatException(float32(1e30), 10, 0),
		"1e30 * 10^10 overflows int32")
}

func TestAlpIsDoubleException(t *testing.T) {
	assert.True(t, alpIsDoubleException(math.NaN(), 2, 0))
	assert.True(t, alpIsDoubleException(math.Inf(1), 2, 0))
	negZero := math.Float64frombits(alpNegZeroFloat64Bits)
	assert.True(t, alpIsDoubleException(negZero, 2, 0))

	assert.False(t, alpIsDoubleException(1.23, 2, 0), "1.23 with exp=2 should encode to 123")
	assert.False(t, alpIsDoubleException(0.0, 2, 0), "0.0 should not be exception")
	assert.False(t, alpIsDoubleException(42.0, 0, 0), "42.0 with exp=0 should encode to 42")
}

func TestAlpEncodeDecodeFloat32(t *testing.T) {
	tests := []struct {
		name     string
		value    float32
		exponent int
		factor   int
		encoded  int32
	}{
		{"integer no scaling", 42.0, 0, 0, 42},
		{"one decimal", 1.5, 1, 0, 15},
		{"two decimals", 1.23, 2, 0, 123},
		{"negative", -1.23, 2, 0, -123},
		{"zero", 0.0, 5, 0, 0},
		{"with factor", 1230.0, 5, 2, 1230000}, // 1230 * 10^5 / 10^2 = 1230 * 1000
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := alpEncodeFloat32(tt.value, tt.exponent, tt.factor)
			assert.Equal(t, tt.encoded, encoded, "encode(%v, exp=%d, fac=%d)", tt.value, tt.exponent, tt.factor)

			decoded := alpDecodeFloat32(encoded, tt.exponent, tt.factor)
			assert.Equal(t, math.Float32bits(tt.value), math.Float32bits(decoded),
				"decode(%d, exp=%d, fac=%d) = %v, want %v", encoded, tt.exponent, tt.factor, decoded, tt.value)
		})
	}
}

func TestAlpEncodeDecodeFloat64(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		exponent int
		factor   int
		encoded  int64
	}{
		{"integer no scaling", 42.0, 0, 0, 42},
		{"one decimal", 1.5, 1, 0, 15},
		{"two decimals", 1.23, 2, 0, 123},
		{"negative", -1.23, 2, 0, -123},
		{"zero", 0.0, 5, 0, 0},
		{"with factor", 1230.0, 5, 2, 1230000}, // 1230 * 10^5 / 10^2 = 1230 * 1000
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := alpEncodeFloat64(tt.value, tt.exponent, tt.factor)
			assert.Equal(t, tt.encoded, encoded, "encode(%v, exp=%d, fac=%d)", tt.value, tt.exponent, tt.factor)

			decoded := alpDecodeFloat64(encoded, tt.exponent, tt.factor)
			assert.Equal(t, math.Float64bits(tt.value), math.Float64bits(decoded),
				"decode(%d, exp=%d, fac=%d) = %v, want %v", encoded, tt.exponent, tt.factor, decoded, tt.value)
		})
	}
}

func TestAlpMultiplier(t *testing.T) {
	assert.Equal(t, float32(1.0), alpFloatMultiplier(0, 0))
	assert.Equal(t, float32(100.0), alpFloatMultiplier(2, 0))
	assert.Equal(t, float32(10.0), alpFloatMultiplier(2, 1))   // 100/10 = 10
	assert.Equal(t, float32(1.0), alpFloatMultiplier(2, 2))    // 100/100 = 1
	assert.Equal(t, float32(1e10), alpFloatMultiplier(10, 0))

	assert.Equal(t, float64(1.0), alpDoubleMultiplier(0, 0))
	assert.Equal(t, float64(100.0), alpDoubleMultiplier(2, 0))
	assert.Equal(t, float64(10.0), alpDoubleMultiplier(2, 1))
	assert.Equal(t, float64(1e18), alpDoubleMultiplier(18, 0))
}
func TestAlpBitWidth(t *testing.T) {
	assert.Equal(t, 0, alpBitWidth32(0))
	assert.Equal(t, 1, alpBitWidth32(1))
	assert.Equal(t, 2, alpBitWidth32(2))
	assert.Equal(t, 2, alpBitWidth32(3))
	assert.Equal(t, 8, alpBitWidth32(255))
	assert.Equal(t, 9, alpBitWidth32(256))
	assert.Equal(t, 32, alpBitWidth32(math.MaxUint32))

	assert.Equal(t, 0, alpBitWidth64(0))
	assert.Equal(t, 1, alpBitWidth64(1))
	assert.Equal(t, 8, alpBitWidth64(255))
	assert.Equal(t, 64, alpBitWidth64(math.MaxUint64))
}

func TestAlpPackUnpackBits32(t *testing.T) {
	tests := []struct {
		name     string
		values   []uint32
		bitWidth int
	}{
		{"1-bit values", []uint32{0, 1, 0, 1, 1, 0, 1, 0}, 1},
		{"4-bit values", []uint32{0, 1, 5, 10, 15, 3, 7, 12}, 4},
		{"8-bit values", []uint32{0, 128, 255, 1, 42, 100, 200, 50}, 8},
		{"16-bit values", []uint32{0, 1000, 50000, 65535, 32768, 1, 256, 2048}, 16},
		{"32-bit values", []uint32{0, math.MaxUint32, 12345, 67890}, 32},
		{"zero bit width", []uint32{0, 0, 0, 0}, 0},
		{"single value", []uint32{42}, 6},
		{"mixed bit widths", []uint32{0, 1, 2, 3, 4, 5, 6, 7}, 3},
		{"large count", func() []uint32 {
			v := make([]uint32, 1024)
			for i := range v {
				v[i] = uint32(i % 256)
			}
			return v
		}(), 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := alpPackBits32(tt.values, len(tt.values), tt.bitWidth)
			unpacked := alpUnpackBits32(packed, len(tt.values), tt.bitWidth)
			assert.Equal(t, tt.values, unpacked, "pack/unpack round-trip failed")
		})
	}
}

func TestAlpPackUnpackBits64(t *testing.T) {
	tests := []struct {
		name     string
		values   []uint64
		bitWidth int
	}{
		{"1-bit values", []uint64{0, 1, 0, 1, 1, 0, 1, 0}, 1},
		{"8-bit values", []uint64{0, 128, 255, 1, 42, 100, 200, 50}, 8},
		{"32-bit values", []uint64{0, math.MaxUint32, 12345, 67890}, 32},
		{"48-bit values", []uint64{0, 1 << 47, 12345678901234, 1}, 48},
		{"64-bit values", []uint64{0, math.MaxUint64, 12345, 67890}, 64},
		{"zero bit width", []uint64{0, 0, 0, 0}, 0},
		{"single value", []uint64{42}, 6},
		{"large count", func() []uint64 {
			v := make([]uint64, 1024)
			for i := range v {
				v[i] = uint64(i * 1000)
			}
			return v
		}(), 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := alpPackBits64(tt.values, len(tt.values), tt.bitWidth)
			unpacked := alpUnpackBits64(packed, len(tt.values), tt.bitWidth)
			assert.Equal(t, tt.values, unpacked, "pack/unpack round-trip failed")
		})
	}
}

func TestAlpPackBits32KnownOutput(t *testing.T) {
	values := []uint32{0, 1, 2, 3}
	packed := alpPackBits32(values, 4, 2)
	require.Len(t, packed, 1)
	assert.Equal(t, byte(0xE4), packed[0])
}

func TestAlpPackBits64KnownOutput(t *testing.T) {
	values := []uint64{0, 1, 2, 3}
	packed := alpPackBits64(values, 4, 2)
	require.Len(t, packed, 1)
	assert.Equal(t, byte(0xE4), packed[0])
}
func TestAlpFindBestFloat32Params(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89, 10.11, 12.13}
	params := alpFindBestFloat32Params(values, 0, len(values))
	assert.Equal(t, 0, params.numExceptions, "clean monetary data should have 0 exceptions")
	assert.Equal(t, 2, params.exponent)
	assert.Equal(t, 0, params.factor)
}

func TestAlpFindBestFloat64Params(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89, 10.11, 12.13}
	params := alpFindBestFloat64Params(values, 0, len(values))
	assert.Equal(t, 0, params.numExceptions, "clean monetary data should have 0 exceptions")
	assert.Equal(t, 2, params.exponent)
	assert.Equal(t, 0, params.factor)
}

func TestAlpFindBestParamsWithExceptions(t *testing.T) {
	values := []float32{
		1.23, 4.56, float32(math.Inf(1)), 7.89,
		math.Float32frombits(alpNegZeroFloat32Bits), 10.11,
	}
	params := alpFindBestFloat32Params(values, 0, len(values))
	assert.Equal(t, 2, params.numExceptions)
}

func TestAlpFindBestFloat32ParamsWithPresets(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89}
	presets := [][2]int{{2, 0}, {3, 0}, {1, 0}}
	params := alpFindBestFloat32ParamsWithPresets(values, 0, len(values), presets)
	assert.Equal(t, 0, params.numExceptions)
	assert.Equal(t, 2, params.exponent)
}

func TestAlpFindBestFloat64ParamsWithPresets(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89}
	presets := [][2]int{{2, 0}, {3, 0}, {1, 0}}
	params := alpFindBestFloat64ParamsWithPresets(values, 0, len(values), presets)
	assert.Equal(t, 0, params.numExceptions)
	assert.Equal(t, 2, params.exponent)
}

func TestAlpFindBestParamsIntegerData(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0, 100.0, 200.0}
	params := alpFindBestFloat32Params(values, 0, len(values))
	assert.Equal(t, 0, params.numExceptions)
	assert.Equal(t, 0, params.exponent)
	assert.Equal(t, 0, params.factor)
}

func TestAlpFindBestParamsAllExceptions(t *testing.T) {
	inf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))
	negZero := math.Float32frombits(alpNegZeroFloat32Bits)
	values := []float32{inf, ninf, negZero, inf}
	params := alpFindBestFloat32Params(values, 0, len(values))
	assert.Equal(t, 4, params.numExceptions, "all values are exceptions")
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float64]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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
	negZero := math.Float32frombits(alpNegZeroFloat32Bits)
	inf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))

	values := []float32{
		1.23, 4.56, inf, 7.89, negZero, 10.11,
		ninf, 12.13, 14.15, 16.17,
	}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64ExceptionsRoundTrip(t *testing.T) {
	negZero := math.Float64frombits(alpNegZeroFloat64Bits)
	inf := math.Inf(1)
	ninf := math.Inf(-1)

	values := []float64{
		1.23, 4.56, inf, 7.89, negZero, 10.11,
		ninf, 12.13, 14.15, 16.17,
	}
	alpFloat64RoundTrip(t, values)
}

func TestAlpFloat32AllExceptionsRoundTrip(t *testing.T) {
	negZero := math.Float32frombits(alpNegZeroFloat32Bits)
	inf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))

	values := []float32{inf, ninf, negZero, inf, ninf, negZero, inf, ninf}
	alpFloat32RoundTrip(t, values)
}

func TestAlpFloat64AllExceptionsRoundTrip(t *testing.T) {
	negZero := math.Float64frombits(alpNegZeroFloat64Bits)
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float64]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float64]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
	err = dec.SetData(len(values), buf.Bytes())
	require.NoError(t, err)

	discarded, err := dec.Discard(100)
	require.NoError(t, err)
	assert.Equal(t, 3, discarded, "should only discard available values")
}
func TestAlpFloat32MultiplePages(t *testing.T) {
	col := newFloat32Column()
	mem := memory.DefaultAllocator

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

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

	dec := &alpDecoder[float64]{decoder: newDecoderBase(format.Encoding_ALP, col)}

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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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
	require.True(t, len(data) >= alpHeaderSize, "encoded data should be at least header size")

	assert.Equal(t, byte(alpVersion), data[0], "version")
	assert.Equal(t, byte(alpCompressionMode), data[1], "compression mode")
	assert.Equal(t, byte(alpIntegerEncodingFOR), data[2], "integer encoding")
	assert.Equal(t, byte(alpDefaultLogVector), data[3], "log vector size")

	numElements := binary.LittleEndian.Uint32(data[4:8])
	assert.Equal(t, uint32(100), numElements, "element count")
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
	require.True(t, len(data) >= alpHeaderSize)

	numElements := binary.LittleEndian.Uint32(data[4:8])
	assert.Equal(t, uint32(3000), numElements)

	numVectors := (3000 + 1023) / 1024
	assert.Equal(t, 3, numVectors)

	offsetArraySize := numVectors * 4
	require.True(t, len(data) >= alpHeaderSize+offsetArraySize)

	firstOffset := binary.LittleEndian.Uint32(data[alpHeaderSize:])
	assert.Equal(t, uint32(offsetArraySize), firstOffset,
		"first vector offset should equal offset array size")

	for i := 1; i < numVectors; i++ {
		offset := binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
		prevOffset := binary.LittleEndian.Uint32(data[alpHeaderSize+(i-1)*4:])
		assert.True(t, offset > prevOffset,
			"offset[%d]=%d should be greater than offset[%d]=%d", i, offset, i-1, prevOffset)
	}
}
func TestAlpDecoderInvalidData(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	err := dec.SetData(10, []byte{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestAlpDecoderInvalidVersion(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	data := make([]byte, 100)
	data[0] = 99 // invalid version
	data[1] = 0
	data[2] = 0
	data[3] = 10
	binary.LittleEndian.PutUint32(data[4:], 1)

	err := dec.SetData(1, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "version")
}

func TestAlpDecoderInvalidLogVectorSize(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	data := make([]byte, 100)
	data[0] = alpVersion
	data[1] = alpCompressionMode
	data[2] = alpIntegerEncodingFOR
	data[3] = 20 // invalid: > alpMaxLogVectorSize
	binary.LittleEndian.PutUint32(data[4:], 1)

	err := dec.SetData(1, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "log vector size")
}

func TestAlpDecoderInvalidCompressionMode(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	data := make([]byte, 100)
	data[0] = alpVersion
	data[1] = 99 // invalid compression mode
	data[2] = alpIntegerEncodingFOR
	data[3] = alpDefaultLogVector
	binary.LittleEndian.PutUint32(data[4:], 1)

	err := dec.SetData(1, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "compression mode")
}

func TestAlpDecoderInvalidIntegerEncoding(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	data := make([]byte, 100)
	data[0] = alpVersion
	data[1] = alpCompressionMode
	data[2] = 99 // invalid integer encoding
	data[3] = alpDefaultLogVector
	binary.LittleEndian.PutUint32(data[4:], 1)

	err := dec.SetData(1, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "integer encoding")
}

func TestAlpDecoderTruncatedOffsetArray(t *testing.T) {
	col := newFloat32Column()
	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}

	data := make([]byte, alpHeaderSize+2) // not enough for 4-byte offset
	data[0] = alpVersion
	data[1] = alpCompressionMode
	data[2] = alpIntegerEncodingFOR
	data[3] = alpDefaultLogVector
	binary.LittleEndian.PutUint32(data[4:], 1024)

	err := dec.SetData(1024, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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

	dec := &alpDecoder[float32]{decoder: newDecoderBase(format.Encoding_ALP, col)}
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
	values = append(values, math.Float32frombits(alpNegZeroFloat32Bits))
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
	values = append(values, math.Float64frombits(alpNegZeroFloat64Bits))
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

