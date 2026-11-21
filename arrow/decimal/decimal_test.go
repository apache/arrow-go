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

package decimal_test

import (
	"fmt"
	"math"
	"math/big"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ulps64(actual, expected float64) int64 {
	ulp := math.Nextafter(actual, math.Inf(1)) - actual
	return int64(math.Abs((expected - actual) / ulp))
}

func ulps32(actual, expected float32) int64 {
	ulp := math.Nextafter32(actual, float32(math.Inf(1))) - actual
	return int64(math.Abs(float64((expected - actual) / ulp)))
}

func assertFloat32Approx(t *testing.T, x, y float32) bool {
	const maxulps int64 = 4
	ulps := ulps32(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func assertFloat64Approx(t *testing.T, x, y float64) bool {
	const maxulps int64 = 4
	ulps := ulps64(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func TestDecimalToReal(t *testing.T) {
	tests := []struct {
		decimalVal string
		scale      int32
		exp        float64
	}{
		{"0", 0, 0},
		{"0", 10, 0.0},
		{"0", -10, 0.0},
		{"1", 0, 1.0},
		{"12345", 0, 12345.0},
		{"12345", 1, 1234.5},
		{"536870912", 0, math.Pow(2, 29)},
	}

	t.Run("float32", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float32, scale int32) {
			n, err := decimal.Decimal32FromString(str, 9, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n.ToFloat32(scale), "Decimal Val: %s, Scale: %d", str, scale)

			n64, err := decimal.Decimal64FromString(str, 18, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n64.ToFloat32(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, float32(tt.exp), tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, float32(-tt.exp), tt.scale)
				}
			})
		}

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimaltoFloat := func(str string, v float32, scale int32) {
				n, err := decimal.Decimal32FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat32Approx(t, v, n.ToFloat32(scale))
			}

			checkApproxDecimal64toFloat := func(str string, v float32, scale int32) {
				n, err := decimal.Decimal64FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat32Approx(t, v, n.ToFloat32(scale))
			}

			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-9); scale <= 9; scale++ {
				checkApproxDecimaltoFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimaltoFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}

			for scale := int32(-18); scale <= 18; scale++ {
				checkApproxDecimal64toFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimal64toFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}
		})
	})

	t.Run("float64", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float64, scale int32) {
			n, err := decimal.Decimal32FromString(str, 9, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)

			assert.Equalf(t, big.NewFloat(v).SetPrec(32), n.ToBigFloat(scale),
				"Decimal Val: %s, Scale: %d", str, scale)

			n64, err := decimal.Decimal64FromString(str, 18, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n64.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)
			assert.Equalf(t, big.NewFloat(v).SetPrec(64), n64.ToBigFloat(scale),
				"Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, tt.exp, tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, -tt.exp, tt.scale)
				}
			})
		}

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimaltoFloat := func(str string, v float64, scale int32) {
				n, err := decimal.Decimal32FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat64Approx(t, v, n.ToFloat64(scale))

				assert.Equalf(t, big.NewFloat(v).SetPrec(32), n.ToBigFloat(scale),
					"Decimal Val: %s, Scale: %d", str, scale)
			}

			checkApproxDecimal64toFloat := func(str string, v float64, scale int32) {
				n, err := decimal.Decimal64FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat64Approx(t, v, n.ToFloat64(scale))

				bf, _ := n.ToBigFloat(scale).Float64()
				assertFloat64Approx(t, v, bf)
			}

			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-19); scale <= 19; scale++ {
				checkApproxDecimaltoFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimaltoFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
			}

			for scale := int32(-19); scale <= 19; scale++ {
				checkApproxDecimal64toFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimal64toFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
			}
		})
	})

	assert.Zero(t, decimal.Decimal32(0).ToBigFloat(math.MinInt32).Sign())
	assert.Zero(t, decimal.Decimal64(0).ToBigFloat(math.MinInt32).Sign())
	// 10^646456993 overflows big.Float, but 10^-646456993 doesn't.
	assert.Equal(t, 1, decimal.Decimal32(1).ToBigFloat(646456993).Sign())
	assert.Equal(t, 1, decimal.Decimal64(1).ToBigFloat(646456993).Sign())
	// 10^28 is not exactly representable with 64 bits of precision.
	want := new(big.Float).SetPrec(64).SetRat(new(big.Rat).SetFrac(big.NewInt(3), new(big.Int).Exp(big.NewInt(10), big.NewInt(28), nil)))
	assert.Zero(t, want.Cmp(decimal.Decimal64(3).ToBigFloat(28)))
}

func TestDecimalFromFloat64ExactScaling(t *testing.T) {
	tests := []struct {
		val              float64
		precision, scale int32
		expected         string
	}{
		{1, 24, 23, "100000000000000000000000"},
		{-1, 24, 23, "-100000000000000000000000"},
		{12345678, 26, 18, "12345678000000000000000000"},
		{-12345678, 26, 18, "-12345678000000000000000000"},
		{0.125, 24, 23, "12500000000000000000000"},
		{0.5, 38, 38, "50000000000000000000000000000000000000"},
		{1844674407370955.5, 20, 4, "18446744073709555000"},
		{1.25, 2, 1, "12"},
		{1.75, 2, 1, "18"},
		{math.Nextafter(1.25, 0), 2, 1, "12"},
		{math.Nextafter(1.25, 2), 2, 1, "13"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%g/%d/%d", tt.val, tt.precision, tt.scale), func(t *testing.T) {
			n128, err := decimal.Decimal128FromFloat(tt.val, tt.precision, tt.scale)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, n128.BigInt().String())

			n256, err := decimal.Decimal256FromFloat(tt.val, tt.precision, tt.scale)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, n256.BigInt().String())
		})
	}

	n, err := decimal.Decimal256FromFloat(0.5, 76, 76)
	require.NoError(t, err)
	assert.Equal(t, "5000000000000000000000000000000000000000000000000000000000000000000000000000", n.BigInt().String())

	_, err = decimal.Decimal128FromFloat(1, 23, 23)
	require.Error(t, err)
	_, err = decimal.Decimal256FromFloat(1, 23, 23)
	require.Error(t, err)
}

func TestDecimalFromFloat(t *testing.T) {
	tests := []struct {
		val              float64
		precision, scale int32
		expected         string
	}{
		{0, 1, 0, "0"},
		{-0, 1, 0, "0"},
		{0, 9, 4, "0.0000"},
		{math.Copysign(0.0, -1), 9, 4, "0.0000"},
		{123, 7, 4, "123.0000"},
		{-123, 7, 4, "-123.0000"},
		{456.78, 7, 4, "456.7800"},
		{-456.78, 7, 4, "-456.7800"},
		{456.784, 5, 2, "456.78"},
		{-456.784, 5, 2, "-456.78"},
		{456.786, 5, 2, "456.79"},
		{-456.786, 5, 2, "-456.79"},
		{999.99, 5, 2, "999.99"},
		{-999.99, 5, 2, "-999.99"},
		{123, 9, 0, "123"},
		{-123, 9, 0, "-123"},
		{123.4, 9, 0, "123"},
		{-123.4, 9, 0, "-123"},
		{123.6, 9, 0, "124"},
		{-123.6, 9, 0, "-124"},
	}

	t.Run("float64", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal.Decimal32FromFloat(tt.val, tt.precision, tt.scale)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, fmt.Sprintf("%."+strconv.Itoa(int(tt.scale))+"f", n.ToFloat64(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float64 range
			for scale := int32(-308); scale <= 308; scale++ {
				val := math.Pow10(int(scale))
				n, err := decimal.Decimal64FromFloat(val, 1, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 1, n)
			}

			for scale := int32(-307); scale <= 306; scale++ {
				val := 123 * math.Pow10(int(scale))
				n, err := decimal.Decimal64FromFloat(val, 2, -scale-1)
				require.NoError(t, err)
				assert.EqualValues(t, 12, n)
				n, err = decimal.Decimal64FromFloat(val, 3, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 123, n)
				n, err = decimal.Decimal64FromFloat(val, 4, -scale+1)
				require.NoError(t, err)
				assert.EqualValues(t, 1230, n)
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal.Decimal32FromFloat(float32(tt.val), tt.precision, tt.scale)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, fmt.Sprintf("%."+strconv.Itoa(int(tt.scale))+"f", n.ToFloat32(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float32 range
			for scale := int32(-38); scale <= 38; scale++ {
				val := float32(math.Pow10(int(scale)))
				n, err := decimal.Decimal64FromFloat(val, 1, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 1, n)
			}

			for scale := int32(-37); scale <= 36; scale++ {
				val := 123 * float32(math.Pow10(int(scale)))
				n, err := decimal.Decimal64FromFloat(val, 2, -scale-1)
				require.NoError(t, err)
				assert.EqualValues(t, 12, n)
				n, err = decimal.Decimal64FromFloat(val, 3, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 123, n)
				n, err = decimal.Decimal64FromFloat(val, 4, -scale+1)
				require.NoError(t, err)
				assert.EqualValues(t, 1230, n)
			}
		})
	})
}

func TestFromString(t *testing.T) {
	tests := []struct {
		s             string
		expected      int64
		expectedScale int32
	}{
		{"12.3", 123, 1},
		{"0.00123", 123, 5},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"1.23e+3", 1230, 0},
		{"-1.23E+3", -1230, 0},
		{"1.23e+5", 123000, 0},
		{"1.2345E+7", 12345000, 0},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"0000000", 0, 0},
		{"000.0000", 0, 4},
		{".0000", 0, 5},
		{"1e1", 10, 0},
		{"+234.567", 234567, 3},
		{"1e-8", 1, 8},
		{"2112.33", 211233, 2},
		{"-2112.33", -211233, 2},
		{"12E2", 12, -2},
		{"0", 0, 0},
		{"0.24", 0, 0},
		{"0.5", 1, 0},
		{"-0.5", -1, 0},
		{"0.51", 1, 0},
		{"-0.51", -1, 0},
		{"0.0051", 1, 2},
		{"-0.0051", -1, 2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := decimal.Decimal32FromString(tt.s, 8, tt.expectedScale)
			require.NoError(t, err)

			ex := decimal.Decimal32(tt.expected)
			assert.Equal(t, ex, n)

			n64, err := decimal.Decimal64FromString(tt.s, 8, tt.expectedScale)
			require.NoError(t, err)

			ex64 := decimal.Decimal64(tt.expected)
			assert.Equal(t, ex64, n64)
		})
	}

	for _, s := range []string{"abc", "Inf", "-inf"} {
		_, err := decimal.Decimal32FromString(s, 8, 2)
		assert.Error(t, err, s)
		_, err = decimal.Decimal64FromString(s, 8, 2)
		assert.Error(t, err, s)
	}

	for _, s := range []string{"2147483648", "4294967295"} {
		_, err := decimal.Decimal32FromString(s, 9, 0)
		assert.Error(t, err, s)
	}
	n32, err := decimal.Decimal32FromString("4294967295", 9, -1)
	require.NoError(t, err)
	assert.Equal(t, decimal.Decimal32(429496730), n32)
	_, err = decimal.Decimal32FromString("4294967296", 9, -1)
	assert.ErrorContains(t, err, "bitlen too large for decimal32")
	_, err = decimal.Decimal64FromString("9223372036854775808", 18, 0)
	assert.Error(t, err)
	_, err = decimal.Decimal64FromString("18446744073709551616", 18, -1)
	assert.ErrorContains(t, err, "bitlen too large for decimal64")

	for _, tc := range []struct {
		s     string
		scale int32
		want  decimal.Decimal64
	}{
		{"9223372036854775808", -1, 922337203685477581},
		{"18446744073709551615", -2, 184467440737095516},
		{"10000000000000000000", -19, 1},
		{"18000000000000000000", -20, 0},
		{"1", math.MinInt32, 0},
	} {
		n64, err := decimal.Decimal64FromString(tc.s, 18, tc.scale)
		require.NoError(t, err, tc.s)
		assert.Equal(t, tc.want, n64, tc.s)
	}

	for _, tc := range []struct {
		s           string
		prec, scale int32
		want        int64
		wantErr     bool
	}{
		{"1e-18", 9, 19, 10, false},
		{"1e-18", 9, 20, 100, false},
		{"-1e-76", 1, 76, -1, false},
		{"3000000000", 9, -2, 30000000, false},
		{"1e-18", 1, 20, 0, true},
		{"1", 9, 100000000, 0, true},
		{"1", 9, math.MaxInt32, 0, true},
	} {
		n32, err := decimal.Decimal32FromString(tc.s, tc.prec, tc.scale)
		n64, err64 := decimal.Decimal64FromString(tc.s, tc.prec, tc.scale)
		if tc.wantErr {
			assert.Error(t, err, tc.s)
			assert.Error(t, err64, tc.s)
			continue
		}
		require.NoError(t, err, tc.s)
		require.NoError(t, err64, tc.s)
		assert.Equal(t, decimal.Decimal32(tc.want), n32, tc.s)
		assert.Equal(t, decimal.Decimal64(tc.want), n64, tc.s)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	tiny32, err := decimal.Decimal32FromString("-1e-20000000", 9, 0)
	require.NoError(t, err)
	tiny64, err := decimal.Decimal64FromString("-1e-20000000", 18, 0)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	assert.Equal(t, decimal.Decimal32(0), tiny32)
	assert.Equal(t, decimal.Decimal64(0), tiny64)
	// Rounding these values directly would allocate about 33 MB.
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}

func TestCmp(t *testing.T) {
	for _, tc := range []struct {
		n    decimal.Decimal32
		rhs  decimal.Decimal32
		want int
	}{
		{decimal.Decimal32(2), decimal.Decimal32(1), 1},
		{decimal.Decimal32(-1), decimal.Decimal32(-2), 1},
		{decimal.Decimal32(2), decimal.Decimal32(3), -1},
		{decimal.Decimal32(-3), decimal.Decimal32(-2), -1},
		{decimal.Decimal32(2), decimal.Decimal32(2), 0},
		{decimal.Decimal32(-2), decimal.Decimal32(-2), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}

	for _, tc := range []struct {
		n    decimal.Decimal64
		rhs  decimal.Decimal64
		want int
	}{
		{decimal.Decimal64(2), decimal.Decimal64(1), 1},
		{decimal.Decimal64(-1), decimal.Decimal64(-2), 1},
		{decimal.Decimal64(2), decimal.Decimal64(3), -1},
		{decimal.Decimal64(-3), decimal.Decimal64(-2), -1},
		{decimal.Decimal64(2), decimal.Decimal64(2), 0},
		{decimal.Decimal64(-2), decimal.Decimal64(-2), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestDecimalRescale(t *testing.T) {
	tests := []struct {
		orig, exp          int32
		oldScale, newScale int32
	}{
		{111, 11100, 0, 2},
		{11100, 111, 2, 0},
		{500000, 5, 6, 1},
		{5, 500000, 1, 6},
		{-111, -11100, 0, 2},
		{-11100, -111, 2, 0},
		{555, 555, 2, 2},
	}

	for _, tt := range tests {
		t.Run("decimal32", func(t *testing.T) {
			out, err := decimal.Decimal32(tt.orig).Rescale(tt.oldScale, tt.newScale)
			require.NoError(t, err)
			assert.Equal(t, decimal.Decimal32(tt.exp), out)
		})
		t.Run("decimal64", func(t *testing.T) {
			out, err := decimal.Decimal64(tt.orig).Rescale(tt.oldScale, tt.newScale)
			require.NoError(t, err)
			assert.Equal(t, decimal.Decimal64(tt.exp), out)
		})
	}

	_, err := decimal.Decimal32(555555).Rescale(6, 1)
	assert.Error(t, err)
	_, err = decimal.Decimal64(555555).Rescale(6, 1)
	assert.Error(t, err)

	_, err = decimal.Decimal32(555555).Rescale(0, 5)
	assert.ErrorContains(t, err, "rescale data loss")
	// 555555 at scale 5 is 55,555,500,000, which fits comfortably in
	// Decimal64's range, so the rescale is lossless. (The 32-bit path wrongly
	// reported data loss by truncating the value to uint32 before multiplying.)
	out64, err := decimal.Decimal64(555555).Rescale(0, 5)
	assert.NoError(t, err)
	assert.Equal(t, decimal.Decimal64(55_555_500_000), out64)
	// A value that genuinely overflows Decimal64 still reports data loss.
	_, err = decimal.Decimal64(5_000_000_000_000_000_000).Rescale(0, 1)
	assert.ErrorContains(t, err, "rescale data loss")

	for _, tc := range []struct {
		bits               int
		n                  int64
		oldScale, newScale int32
		want               int64
		wantErr            bool
	}{
		{64, 1, 0, 10, 10_000_000_000, false},
		{64, -1, 0, 18, -1_000_000_000_000_000_000, false},
		{64, 10_000_000_000, 10, 0, 1, false},
		{64, -1_000_000_000_000_000_000, 18, 0, -1, false},
		{32, 2, 0, 9, 2_000_000_000, false},
		{32, -3, 0, 9, 0, true},
		{64, -9, 0, 18, -9_000_000_000_000_000_000, false},
		{64, 10, 0, 18, 0, true},
		{32, 214748364, 0, 1, 2147483640, false},
		{32, 214748365, 0, 1, 0, true},
		{64, -922337203685477580, 0, 1, -9223372036854775800, false},
		{64, -922337203685477581, 0, 1, 0, true},
		{32, 1, 0, 10, 0, true},
		{64, -1, 19, 0, 0, true},
		{32, 1410065408, 10, 0, 0, true},
		{32, 0, 32, 0, 0, false},
		{64, 0, math.MinInt32, math.MaxInt32, 0, false},
		{32, math.MinInt32, 5, 5, math.MinInt32, false},
		{32, math.MinInt32, 1, 0, 0, true},
		{64, math.MinInt64, 0, 1, 0, true},
		{32, 10, math.MinInt32, math.MaxInt32, 0, true},
		{64, 10, math.MaxInt32, math.MinInt32, 0, true},
		{32, 1, -1, math.MaxInt32, 0, true},
		{32, 111, math.MaxInt32 - 2, math.MaxInt32, 11100, false},
		{64, 11100, math.MinInt32 + 2, math.MinInt32, 111, false},
		{64, -555, 2, 1, 0, true},
	} {
		t.Run(fmt.Sprintf("decimal%d_%d_%d_%d", tc.bits, tc.n, tc.oldScale, tc.newScale), func(t *testing.T) {
			var got int64
			var err error
			if tc.bits == 32 {
				var out decimal.Decimal32
				out, err = decimal.Decimal32(tc.n).Rescale(tc.oldScale, tc.newScale)
				got = int64(out)
			} else {
				var out decimal.Decimal64
				out, err = decimal.Decimal64(tc.n).Rescale(tc.oldScale, tc.newScale)
				got = int64(out)
			}
			if tc.wantErr {
				assert.EqualError(t, err, "rescale data loss")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDecimalIncreaseScale(t *testing.T) {
	assert.Equal(t, decimal.Decimal32(1234), decimal.Decimal32(1234).IncreaseScaleBy(0))
	assert.Equal(t, decimal.Decimal32(1234000), decimal.Decimal32(1234).IncreaseScaleBy(3))
	assert.Equal(t, decimal.Decimal32(-1234000), decimal.Decimal32(-1234).IncreaseScaleBy(3))

	assert.Equal(t, decimal.Decimal64(1234), decimal.Decimal64(1234).IncreaseScaleBy(0))
	assert.Equal(t, decimal.Decimal64(1234000), decimal.Decimal64(1234).IncreaseScaleBy(3))
	assert.Equal(t, decimal.Decimal64(-1234000), decimal.Decimal64(-1234).IncreaseScaleBy(3))
}

func TestDecimalReduceScale(t *testing.T) {
	tests := []struct {
		value    int32
		scale    int32
		round    bool
		expected int32
	}{
		{123456, 0, false, 123456},
		{123456, 1, false, 12345},
		{123456, 1, true, 12346},
		{123451, 1, true, 12345},
		{123789, 2, true, 1238},
		{123749, 2, true, 1237},
		{123750, 2, true, 1238},
		{5, 1, true, 1},
		{0, 1, true, 0},
	}

	for _, tt := range tests {
		assert.Equal(t, decimal.Decimal32(tt.expected),
			decimal.Decimal32(tt.value).ReduceScaleBy(tt.scale, tt.round), "decimal32")
		assert.Equal(t, decimal.Decimal32(tt.expected).Negate(),
			decimal.Decimal32(tt.value).Negate().ReduceScaleBy(tt.scale, tt.round), "decimal32")
		assert.Equal(t, decimal.Decimal64(tt.expected),
			decimal.Decimal64(tt.value).ReduceScaleBy(tt.scale, tt.round), "decimal64")
		assert.Equal(t, decimal.Decimal64(tt.expected).Negate(),
			decimal.Decimal64(tt.value).Negate().ReduceScaleBy(tt.scale, tt.round), "decimal64")
	}

	assert.Equal(t, decimal.Decimal64(1), decimal.Decimal64(10_000_000_000).ReduceScaleBy(10, false))
}

func TestDecimalBasics(t *testing.T) {
	tests := []struct {
		lhs, rhs int32
	}{
		{100, 3},
		{200, 3},
		{20100, 301},
		{-20100, 301},
		{20100, -301},
		{-20100, -301},
	}

	for _, tt := range tests {
		assert.EqualValues(t, tt.lhs+tt.rhs,
			decimal.Decimal32(tt.lhs).Add(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs+tt.rhs,
			decimal.Decimal64(tt.lhs).Add(decimal.Decimal64(tt.rhs)))

		assert.EqualValues(t, tt.lhs-tt.rhs,
			decimal.Decimal32(tt.lhs).Sub(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs-tt.rhs,
			decimal.Decimal64(tt.lhs).Sub(decimal.Decimal64(tt.rhs)))

		assert.EqualValues(t, tt.lhs*tt.rhs,
			decimal.Decimal32(tt.lhs).Mul(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs*tt.rhs,
			decimal.Decimal64(tt.lhs).Mul(decimal.Decimal64(tt.rhs)))

		expdiv, expmod := tt.lhs/tt.rhs, tt.lhs%tt.rhs
		div, mod := decimal.Decimal32(tt.lhs).Div(decimal.Decimal32(tt.rhs))
		assert.EqualValues(t, expdiv, div)
		assert.EqualValues(t, expmod, mod)

		div64, mod64 := decimal.Decimal64(tt.lhs).Div(decimal.Decimal64(tt.rhs))
		assert.EqualValues(t, expdiv, div64)
		assert.EqualValues(t, expmod, mod64)
	}
}

func TestFromStringRoundsExactTies(t *testing.T) {
	// These halfway values are not exact in binary, so rounding a binary
	// approximation sent some of them down. Ties round away from zero.
	tests := []struct {
		s        string
		scale    int32
		expected int64
	}{
		{"0.35", 1, 4},
		{"0.15", 1, 2},
		{"2.675", 2, 268},
		{"1.005", 2, 101},
		{"-0.35", 1, -4},
		{"-1.005", 2, -101},
		{"19", -1, 2},
		{"15", -1, 2},
		{"14", -1, 1},
		{"-15", -1, -2},
		{"-19", -1, -2},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.scale), func(t *testing.T) {
			n32, err := decimal.Decimal32FromString(tt.s, 9, tt.scale)
			require.NoError(t, err)
			assert.EqualValues(t, tt.expected, n32)

			n64, err := decimal.Decimal64FromString(tt.s, 18, tt.scale)
			require.NoError(t, err)
			assert.EqualValues(t, tt.expected, n64)
		})
	}
}

func TestFromStringLongFraction(t *testing.T) {
	// Fractions longer than 1024 digits must still round from the first
	// dropped digit instead of being rejected as too large.
	tests := []struct {
		s        string
		scale    int32
		expected int64
	}{
		{"0." + strings.Repeat("7", 1000), 2, 78},
		{"0." + strings.Repeat("7", 1030), 2, 78},
		{"0." + strings.Repeat("7", 5000), 2, 78},
		{"-0." + strings.Repeat("7", 5000), 2, -78},
		{"0.125" + strings.Repeat("0", 2000), 2, 13},
		{"0.124" + strings.Repeat("9", 2000), 2, 12},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d_%d", i, len(tt.s)), func(t *testing.T) {
			n32, err := decimal.Decimal32FromString(tt.s, 9, tt.scale)
			require.NoError(t, err)
			assert.EqualValues(t, tt.expected, n32)

			n64, err := decimal.Decimal64FromString(tt.s, 18, tt.scale)
			require.NoError(t, err)
			assert.EqualValues(t, tt.expected, n64)
		})
	}
}

func TestMaxScale(t *testing.T) {
	assert.Equal(t, 9, decimal.MaxScale[decimal.Decimal32]())
	assert.Equal(t, 18, decimal.MaxScale[decimal.Decimal64]())
	assert.Equal(t, 38, decimal.MaxScale[decimal.Decimal128]())
	assert.Equal(t, 76, decimal.MaxScale[decimal.Decimal256]())
}

func TestDecimal256FromI64AndDecimal128(t *testing.T) {
	ints := []int64{
		0, 1, -1, 2, -2, math.MaxInt32, math.MinInt32, 1 << 32, -(1 << 32),
		math.MaxInt64, math.MinInt64, math.MinInt64 + 1,
	}
	x := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return x
	}
	for range 1000 {
		ints = append(ints, int64(next())>>(next()%63))
	}

	for _, v := range ints {
		ext := uint64(0)
		if v < 0 {
			ext = math.MaxUint64
		}
		n := decimal.Decimal256FromI64(v)
		if want := [4]uint64{uint64(v), ext, ext, ext}; n.Array() != want {
			t.Fatalf("Decimal256FromI64(%d).Array() = %x, want %x", v, n.Array(), want)
		}
		if n.BigInt().Cmp(big.NewInt(v)) != 0 {
			t.Fatalf("Decimal256FromI64(%d) = %v", v, n.BigInt())
		}

		lo := next()
		for _, d := range []decimal128.Num{decimal128.New(v, lo), decimal128.New(v, 0), decimal128.New(v, math.MaxUint64), decimal128.FromI64(v)} {
			n := decimal.Decimal256FromDecimal128(d)
			if want := [4]uint64{d.LowBits(), uint64(d.HighBits()), ext, ext}; n.Array() != want {
				t.Fatalf("Decimal256FromDecimal128(%v).Array() = %x, want %x", d, n.Array(), want)
			}
			if n.BigInt().Cmp(d.BigInt()) != 0 {
				t.Fatalf("Decimal256FromDecimal128(%v) = %v", d, n.BigInt())
			}
		}
	}
}

func TestDecimal256FromBigInt(t *testing.T) {
	one := big.NewInt(1)
	pow2 := func(shift uint) *big.Int {
		return new(big.Int).Lsh(one, shift)
	}
	maxPos := new(big.Int).Sub(pow2(255), one)
	minNeg := new(big.Int).Neg(maxPos)

	distinct := new(big.Int).SetBits([]big.Word{0x1111})
	distinct.Lsh(distinct, 64).Or(distinct, new(big.Int).SetUint64(0x2222333344445555))
	distinct.Lsh(distinct, 64).Or(distinct, new(big.Int).SetUint64(0x6666777788889999))
	distinct.Lsh(distinct, 64).Or(distinct, new(big.Int).SetUint64(0xaaaabbbbccccdddd))

	exactCases := []struct {
		name string
		val  *big.Int
		want [4]uint64
	}{
		{"zero", big.NewInt(0), [4]uint64{0, 0, 0, 0}},
		{"pos_1", big.NewInt(1), [4]uint64{1, 0, 0, 0}},
		{"neg_1", big.NewInt(-1), [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}},
		{"pos_2word_pow64", pow2(64), [4]uint64{0, 1, 0, 0}},
		{"neg_2word_pow64", new(big.Int).Neg(pow2(64)), [4]uint64{0, math.MaxUint64, math.MaxUint64, math.MaxUint64}},
		{"pos_3word_pow128", pow2(128), [4]uint64{0, 0, 1, 0}},
		{"neg_3word_pow128", new(big.Int).Neg(pow2(128)), [4]uint64{0, 0, math.MaxUint64, math.MaxUint64}},
		{"pos_4word_pow192", pow2(192), [4]uint64{0, 0, 0, 1}},
		{"neg_4word_pow192", new(big.Int).Neg(pow2(192)), [4]uint64{0, 0, 0, math.MaxUint64}},
		{"pos_distinct", distinct, [4]uint64{0xaaaabbbbccccdddd, 0x6666777788889999, 0x2222333344445555, 0x1111}},
		{"neg_distinct", new(big.Int).Neg(distinct), [4]uint64{^uint64(0xaaaabbbbccccdddd) + 1, ^uint64(0x6666777788889999), ^uint64(0x2222333344445555), ^uint64(0x1111)}},
		{"max_pos_2_255_minus_1", maxPos, [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxInt64}},
		{"min_neg_minus_2_255_plus_1", minNeg, [4]uint64{1, 0, 0, 1 << 63}},
	}
	for _, tc := range exactCases {
		t.Run(tc.name, func(t *testing.T) {
			got := decimal.Decimal256FromBigInt(tc.val)
			assert.Equal(t, tc.want, got.Array())
			assert.Equal(t, 0, got.BigInt().Cmp(tc.val), "round-trip %s: got %v, want %v", tc.name, got.BigInt(), tc.val)
		})
	}

	// Carry-propagation and multi-word round-trip coverage across 1..255 bits.
	x := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return x
	}
	var buf [32]byte
	for bitlen := uint(1); bitlen <= 255; bitlen++ {
		for _, sign := range []int{1, -1} {
			for i := 0; i < 4; i++ {
				w := next()
				for j := 0; j < 8; j++ {
					buf[i*8+j] = byte(w >> (8 * j))
				}
			}
			v := new(big.Int).SetBytes(buf[:])
			v.Rsh(v, 256-bitlen)
			if sign < 0 {
				v.Neg(v)
			}
			got := decimal.Decimal256FromBigInt(v)
			if got.BigInt().Cmp(v) != 0 {
				t.Fatalf("Decimal256FromBigInt(bitlen=%d, sign=%d): got %v, want %v", bitlen, sign, got.BigInt(), v)
			}
		}
	}

	const overflowMsg = "arrow/decimal256: cannot represent value larger than 256bits"
	for _, tc := range []struct {
		name string
		val  *big.Int
	}{
		{"pos_2_255", pow2(255)},
		{"neg_2_255", new(big.Int).Neg(pow2(255))},
		{"pos_2_256", pow2(256)},
		{"neg_2_256", new(big.Int).Neg(pow2(256))},
		{"pos_2_300", pow2(300)},
		{"neg_2_300", new(big.Int).Neg(pow2(300))},
	} {
		t.Run("overflow_"+tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, overflowMsg, func() {
				_ = decimal.Decimal256FromBigInt(tc.val)
			})
		})
	}
}
