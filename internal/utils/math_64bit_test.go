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

//go:build amd64 || arm64 || mips64 || ppc64 || s390x

package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMul_64bit(t *testing.T) {
	// These constants are smaller in magnitude than the exact square root.
	assert.Greater(t, sqrtMinInt, int(math.Sqrt(math.MinInt64)))
	assert.Less(t, sqrtMaxInt, int(math.Sqrt(math.MaxInt64)))

	for _, tt := range []struct {
		a, b, expected int
		ok             bool
	}{
		{ok: true, a: math.MinInt32, b: 0, expected: 0},
		{ok: true, a: math.MinInt32, b: 1, expected: math.MinInt32},
		{ok: true, a: math.MinInt32, b: -1, expected: -math.MinInt32},

		{ok: true, a: math.MaxInt32, b: 0, expected: 0},
		{ok: true, a: math.MaxInt32, b: 1, expected: math.MaxInt32},
		{ok: true, a: math.MaxInt32, b: -1, expected: -math.MaxInt32},

		{ok: true, a: math.MinInt32, b: math.MinInt32, expected: 4611686018427387904},
		{ok: true, a: math.MaxInt32, b: math.MaxInt32, expected: 4611686014132420609},
		{ok: true, a: math.MinInt32, b: math.MaxInt32, expected: -4611686016279904256},
		{ok: true, a: math.MaxInt32, b: math.MinInt32, expected: -4611686016279904256},

		{ok: true, a: math.MinInt64, b: 0, expected: 0},
		{ok: true, a: math.MinInt64, b: 1, expected: math.MinInt64},
		{ok: false, a: math.MinInt64, b: -1, expected: math.MinInt64},
		{ok: false, a: math.MinInt64, b: -2, expected: 0},
		{ok: false, a: math.MinInt64, b: 2, expected: 0},

		{ok: true, a: math.MaxInt64, b: 0, expected: 0},
		{ok: true, a: math.MaxInt64, b: 1, expected: math.MaxInt64},
		{ok: true, a: math.MaxInt64, b: -1, expected: -math.MaxInt64},
		{ok: false, a: math.MaxInt64, b: -2, expected: 2},
		{ok: false, a: math.MaxInt64, b: 2, expected: -2},
	} {
		actual, ok := Mul(tt.a, tt.b)
		assert.Equal(t, tt.ok, ok, "(%v * %v)", tt.a, tt.b)
		require.Equal(t, tt.expected, actual, "(%v * %v)", tt.a, tt.b)
	}
}
