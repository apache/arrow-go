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

//go:build go1.24

// Benchmarks use [testing.B.Loop], introduced in Go 1.24.

package utils

import (
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func name[T int | int64](i T) string {
	if i == math.MaxInt {
		return "MaxInt"
	}
	if i == math.MaxInt32 {
		return "MaxInt32"
	}
	if i == math.MaxInt16 {
		return "MaxInt16"
	}
	return fmt.Sprint(i)
}

func BenchmarkAdd(b *testing.B) {
	b.Run("int", func(b *testing.B) {
		for _, bb := range [][]int{
			{8192, 8192},
			{math.MaxInt16, 1},
			{math.MaxInt16, 5},
			{math.MaxInt16, math.MaxInt16},
			{math.MaxInt32, 1},
			{math.MaxInt32, 5},
			{math.MaxInt32, math.MaxInt32},
			{math.MaxInt, math.MaxInt},
		} {
			x, y := bb[0], bb[1]

			b.Run(fmt.Sprintf("%s + %s", name(x), name(y)), func(b *testing.B) {
				for b.Loop() {
					Add(x, y)
				}
			})
		}
	})
}

func TestAdd(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		for _, tt := range []struct {
			a, b, expected int32
			ok             bool
		}{
			{ok: true, a: math.MinInt16, b: math.MaxInt16, expected: -1},
			{ok: true, a: math.MinInt16, b: math.MinInt16, expected: -65536},
			{ok: true, a: math.MaxInt16, b: math.MaxInt16, expected: 65534},

			{ok: true, a: math.MinInt32, b: math.MaxInt32, expected: -1},
			{ok: true, a: math.MinInt32, b: 0, expected: math.MinInt32},
			{ok: false, a: math.MinInt32, b: -1, expected: math.MaxInt32},

			{ok: true, a: math.MaxInt32, b: math.MinInt32, expected: -1},
			{ok: true, a: math.MaxInt32, b: 0, expected: math.MaxInt32},
			{ok: false, a: math.MaxInt32, b: 1, expected: math.MinInt32},
		} {
			actual, ok := Add(tt.a, tt.b)
			assert.Equal(t, tt.ok, ok, "(%v + %v)", tt.a, tt.b)
			require.Equal(t, tt.expected, actual, "(%v + %v)", tt.a, tt.b)
		}
	})

	t.Run("int64", func(t *testing.T) {
		for _, tt := range []struct {
			a, b, expected int64
			ok             bool
		}{
			{ok: true, a: math.MinInt32, b: math.MaxInt32, expected: -1},
			{ok: true, a: math.MinInt32, b: math.MinInt32, expected: -4294967296},
			{ok: true, a: math.MaxInt32, b: math.MaxInt32, expected: 4294967294},

			{ok: true, a: math.MinInt64, b: math.MaxInt64, expected: -1},
			{ok: true, a: math.MinInt64, b: 0, expected: math.MinInt64},
			{ok: false, a: math.MinInt64, b: -1, expected: math.MaxInt64},

			{ok: true, a: math.MaxInt64, b: math.MinInt64, expected: -1},
			{ok: true, a: math.MaxInt64, b: 0, expected: math.MaxInt64},
			{ok: false, a: math.MaxInt64, b: 1, expected: math.MinInt64},
		} {
			actual, ok := Add(tt.a, tt.b)
			assert.Equal(t, tt.ok, ok, "(%v + %v)", tt.a, tt.b)
			require.Equal(t, tt.expected, actual, "(%v + %v)", tt.a, tt.b)
		}
	})
}

func BenchmarkMul(b *testing.B) {
	b.Run("int", func(b *testing.B) {
		for _, bb := range [][]int{
			{8192, 8192},
			{math.MaxInt16, 1},
			{math.MaxInt16, 5},
			{math.MaxInt16, math.MaxInt16},
			{math.MaxInt32, 1},
			{math.MaxInt32, 5},
			{math.MaxInt32, math.MaxInt32},
			{math.MaxInt, math.MaxInt},
		} {
			x, y := bb[0], bb[1]

			b.Run(fmt.Sprintf("%s × %s", name(x), name(y)), func(b *testing.B) {
				for b.Loop() {
					Mul(x, y)
				}
			})
		}
	})
}

// See [TestMul_32bit] and [TestMul_64bit] tests.
func TestMul(t *testing.T) {
	require.Truef(t,
		bits.UintSize == 32 || bits.UintSize == 64,
		"Expected a 32-bit or 64-bit system, got %d-bit", bits.UintSize)
}

func BenchmarkMul64(b *testing.B) {
	b.Run("int64", func(b *testing.B) {
		for _, bb := range [][]int64{
			{8192, 8192},
			{math.MaxInt16, 1},
			{math.MaxInt16, 5},
			{math.MaxInt16, math.MaxInt16},
			{math.MaxInt32, 1},
			{math.MaxInt32, 5},
			{math.MaxInt32, math.MaxInt32},
			{math.MaxInt64, math.MaxInt64},
		} {
			x, y := bb[0], bb[1]

			b.Run(fmt.Sprintf("%s × %s", name(x), name(y)), func(b *testing.B) {
				for b.Loop() {
					Mul64(x, y)
				}
			})
		}
	})
}

func TestMul64(t *testing.T) {
	for _, tt := range []struct {
		a, b, expected int64
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
		actual, ok := Mul64(tt.a, tt.b)
		assert.Equal(t, tt.ok, ok, "(%v * %v)", tt.a, tt.b)
		require.Equal(t, tt.expected, actual, "(%v * %v)", tt.a, tt.b)
	}
}
