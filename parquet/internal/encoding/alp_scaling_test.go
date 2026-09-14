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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlpFastRoundFloat32(t *testing.T) {
	tests := []struct {
		name     string
		input    float32
		expected int64
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
			result := alpFastRound(tt.input)
			assert.Equal(t, tt.expected, result, "alpFastRound(%v) = %d, want %d", tt.input, result, tt.expected)
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
			result := alpFastRound(tt.input)
			assert.Equal(t, tt.expected, result, "alpFastRound(%v) = %d, want %d", tt.input, result, tt.expected)
		})
	}
}

// alpNegZero returns negative zero, which ALP has to store as an exception
// because the round trip would drop the sign.
func alpNegZero[T alpFloat]() T {
	return T(math.Copysign(0, -1))
}

func alpIsException[T alpFloat](t *testing.T, v T, exponent, factor int) bool {
	t.Helper()
	_, ok := alpTryEncode(v, exponent, factor)
	return !ok
}

func TestAlpTryEncodeFloat32Exceptions(t *testing.T) {
	assert.True(t, alpIsException(t, float32(math.NaN()), 2, 0), "NaN has no integer form")
	assert.True(t, alpIsException(t, float32(math.Inf(1)), 2, 0), "+Inf has no integer form")
	assert.True(t, alpIsException(t, float32(math.Inf(-1)), 2, 0), "-Inf has no integer form")
	assert.True(t, alpIsException(t, alpNegZero[float32](), 2, 0), "-0.0 would come back as +0.0")
	assert.True(t, alpIsException(t, float32(1e30), 10, 0), "1e30 * 10^10 overflows int32")

	assert.False(t, alpIsException(t, float32(1.23), 2, 0), "1.23 at 10^2 encodes to 123")
	assert.False(t, alpIsException(t, float32(0.0), 2, 0))
	assert.False(t, alpIsException(t, float32(-1.0), 2, 0))
	assert.False(t, alpIsException(t, float32(42.0), 0, 0), "42.0 is already an integer")
}

func TestAlpTryEncodeFloat64Exceptions(t *testing.T) {
	assert.True(t, alpIsException(t, math.NaN(), 2, 0), "NaN has no integer form")
	assert.True(t, alpIsException(t, math.Inf(1), 2, 0), "+Inf has no integer form")
	assert.True(t, alpIsException(t, math.Inf(-1), 2, 0), "-Inf has no integer form")
	assert.True(t, alpIsException(t, alpNegZero[float64](), 2, 0), "-0.0 would come back as +0.0")
	assert.True(t, alpIsException(t, 1e300, 18, 0), "1e300 * 10^18 overflows int64")

	assert.False(t, alpIsException(t, 1.23, 2, 0), "1.23 at 10^2 encodes to 123")
	assert.False(t, alpIsException(t, 0.0, 2, 0))
	assert.False(t, alpIsException(t, -1.0, 2, 0))
	assert.False(t, alpIsException(t, 42.0, 0, 0), "42.0 is already an integer")
}

func TestAlpTryEncodeReturnsTheEncodedValue(t *testing.T) {
	e, ok := alpTryEncode(1.23, 2, 0)
	require.True(t, ok)
	assert.EqualValues(t, 123, e, "1.23 at 10^2 encodes to 123")

	e, ok = alpTryEncode(1.5, 3, 2)
	require.True(t, ok)
	assert.EqualValues(t, 15, e, "10^3 * 10^-2 scales by ten")
}

func TestAlpEncodeDecodeFloat32(t *testing.T) {
	tests := []struct {
		name     string
		value    float32
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
			encoded := alpEncode(tt.value, tt.exponent, tt.factor)
			assert.Equal(t, tt.encoded, encoded, "encode(%v, exp=%d, fac=%d)", tt.value, tt.exponent, tt.factor)

			decoded := alpDecode[float32](encoded, tt.exponent, tt.factor)
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
			encoded := alpEncode(tt.value, tt.exponent, tt.factor)
			assert.Equal(t, tt.encoded, encoded, "encode(%v, exp=%d, fac=%d)", tt.value, tt.exponent, tt.factor)

			decoded := alpDecode[float64](encoded, tt.exponent, tt.factor)
			assert.Equal(t, math.Float64bits(tt.value), math.Float64bits(decoded),
				"decode(%d, exp=%d, fac=%d) = %v, want %v", encoded, tt.exponent, tt.factor, decoded, tt.value)
		})
	}
}

func TestAlpPowerTables(t *testing.T) {
	assert.Equal(t, float32(1.0), alpPow10[float32](0))
	assert.Equal(t, float32(100.0), alpPow10[float32](2))
	assert.Equal(t, float32(1e10), alpPow10[float32](alpNumExponents[float32]()-1))
	assert.Equal(t, float32(1.0), alpNegPow10[float32](0))
	assert.Equal(t, float32(1e-10), alpNegPow10[float32](alpNumExponents[float32]()-1))

	assert.Equal(t, float64(1.0), alpPow10[float64](0))
	assert.Equal(t, float64(1e18), alpPow10[float64](alpNumExponents[float64]()-1))
	assert.Equal(t, float64(1e-18), alpNegPow10[float64](alpNumExponents[float64]()-1))
}

// TestAlpScalingMultipliesByANegativePower guards the one arithmetic choice that
// two implementations can disagree on. Dividing by 10^exponent and multiplying by
// the tabulated 10^-exponent differ by a unit in the last place, so a page written
// here would decode elsewhere to a value one bit away from the original.
func TestAlpScalingMultipliesByANegativePower(t *testing.T) {
	const value = float32(1016.10999)
	const exponent, factor = 6, 0

	encoded := alpEncode(value, exponent, factor)
	assert.Equal(t, value, alpDecode[float32](encoded, exponent, factor))
	assert.NotEqual(t, value, float32(encoded)/(alpPow10[float32](exponent)/alpPow10[float32](factor)),
		"dividing has stopped losing the last bit, so this test no longer guards anything")
}
