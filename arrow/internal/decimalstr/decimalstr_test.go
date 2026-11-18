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

package decimalstr_test

import (
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/internal/decimalstr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScaleAndRound(t *testing.T) {
	tests := []struct {
		s        string
		scale    int64
		expected string
	}{
		{"0.35", 1, "4"},
		{"0.15", 1, "2"},
		{"0.25", 1, "3"},
		{"2.675", 2, "268"},
		{"2.675", 1, "27"},
		{"-0.35", 1, "-4"},
		{"-0.15", 1, "-2"},
		{"0.349999", 1, "3"},
		{"0.5", 0, "1"},
		{"-0.5", 0, "-1"},
		{"0.49", 0, "0"},
		{"-0.4", 0, "0"},
		{"0.05", 0, "0"},
		{"5e-1", 0, "1"},
		{"35e-2", 1, "4"},
		{"0.035E+1", 1, "4"},
		{"+12.3", 1, "123"},
		{"12.", 2, "1200"},
		{".5", 0, "1"},
		{"1.23e+3", 0, "1230"},
		{"0000.000", 4, "0"},
		{"-0", 0, "0"},
		{"1e-50", 2, "0"},
		{"12345678901234567890.5", 0, "12345678901234567891"},
		{"1.00000000000000000000000000000000000005", 37, "10000000000000000000000000000000000001"},
		{"19", -1, "2"},
		{"15", -1, "2"},
		{"14", -1, "1"},
		{"-15", -1, "-2"},
		{"-19", -1, "-2"},
		{"10", -1, "1"},
		{"5", -1, "1"},
		{"4", -1, "0"},
		{"50", -2, "1"},
		{"25", -2, "0"},
		{"1e38", -38, "1"},
	}
	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			got, ok := decimalstr.ScaleAndRound(tt.s, tt.scale)
			require.True(t, ok)
			assert.Equal(t, tt.expected, got.String())
		})
	}
}

func TestScaleAndRoundRejectsNonDecimal(t *testing.T) {
	for _, s := range []string{"", "+", ".", "e5", "1e", "1e+", "1p3", "inf", "-Inf", "NaN", "0x10", "1_000", "1/2", "1.2.3", " 1", "1e99999999999999999999"} {
		t.Run(s, func(t *testing.T) {
			_, ok := decimalstr.ScaleAndRound(s, 2)
			assert.False(t, ok)
		})
	}
}

func TestScaleAndRoundBoundsDigits(t *testing.T) {
	_, ok := decimalstr.ScaleAndRound("1e5000", 0)
	assert.False(t, ok)
	_, ok = decimalstr.ScaleAndRound("1", 5000)
	assert.False(t, ok)
}

func TestScaleAndRoundLongFraction(t *testing.T) {
	// Only the first dropped digit decides the rounding, so the number of
	// dropped fraction digits is not bounded.
	tests := []struct {
		s        string
		scale    int64
		expected string
	}{
		{"0." + strings.Repeat("7", 1000), 2, "78"},
		{"0." + strings.Repeat("7", 1030), 2, "78"},
		{"-0." + strings.Repeat("7", 5000), 2, "-78"},
		{"0.12" + strings.Repeat("4", 2000), 2, "12"},
		{"0.125" + strings.Repeat("0", 2000), 2, "13"},
		{"0.124" + strings.Repeat("9", 2000), 2, "12"},
		{"1." + strings.Repeat("9", 3000), 0, "2"},
		{"12" + strings.Repeat("0", 1100) + "e-1100", -1, "1"},
		{"0." + strings.Repeat("0", 1500) + "5", 2, "0"},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got, ok := decimalstr.ScaleAndRound(tt.s, tt.scale)
			require.True(t, ok)
			assert.Equal(t, tt.expected, got.String())
		})
	}
}

func TestRoundScaledFallsBackToFloat(t *testing.T) {
	scaled := new(big.Float).SetFloat64(2.5)
	// Not a decimal literal, so the float is rounded instead.
	assert.Equal(t, "3", decimalstr.RoundScaled(scaled, "0x1.4p1", 0).String())
	assert.Equal(t, "-3", decimalstr.RoundScaled(new(big.Float).Neg(scaled), "-0x1.4p1", 0).String())
	assert.Equal(t, "0", decimalstr.RoundScaled(new(big.Float).SetFloat64(0.2), "0.2", 0).String())
}

func TestParsedAtLeastTwoPower(t *testing.T) {
	mustParse := func(s string) decimalstr.Parsed {
		t.Helper()
		p, ok := decimalstr.Parse(s)
		require.True(t, ok, s)
		return p
	}
	assert.False(t, mustParse("19").AtLeastTwoPower(0, 127))
	assert.False(t, mustParse("1e38").AtLeastTwoPower(0, 127))
	assert.True(t, mustParse("1e40").AtLeastTwoPower(0, 127))
	assert.False(t, mustParse("1.5e38").AtLeastTwoPower(0, 127))
	assert.True(t, mustParse("1e1000000000").AtLeastTwoPower(0, 128))
	assert.False(t, mustParse("-1e-20000000").AtLeastTwoPower(0, 127))
	assert.False(t, mustParse("4294967295").AtLeastTwoPower(0, 32))
	assert.True(t, mustParse("4294967296").AtLeastTwoPower(0, 32))
	assert.True(t, mustParse("18446744073709551616").AtLeastTwoPower(0, 64))
	assert.False(t, mustParse("9223372036854775808").AtLeastTwoPower(0, 64))
}

func TestScaleAndRoundTinyExponentDoesNotAllocate(t *testing.T) {
	got, ok := decimalstr.ScaleAndRound("-1e-20000000", 0)
	require.True(t, ok)
	assert.Equal(t, "0", got.String())
}
