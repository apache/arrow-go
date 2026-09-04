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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package kernels

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

var narrowComparisonBenchmarkSink byte

func BenchmarkNarrowComparisons(b *testing.B) {
	b.Run("int8", func(b *testing.B) {
		benchmarkNarrowComparison[int8](b)
	})
	b.Run("uint8", func(b *testing.B) {
		benchmarkNarrowComparison[uint8](b)
	})
	b.Run("int16", func(b *testing.B) {
		benchmarkNarrowComparison[int16](b)
	})
	b.Run("uint16", func(b *testing.B) {
		benchmarkNarrowComparison[uint16](b)
	})
}

func benchmarkNarrowComparison[T arrow.NumericType](b *testing.B) {
	patterns := []struct {
		name string
		pick func(int) bool
	}{
		{"alternating", func(i int) bool { return i%2 == 0 }},
		{"random25", func(i int) bool { return (uint32(i)*1664525+1013904223)%100 < 25 }},
		{"random50", func(i int) bool { return (uint32(i)*1664525+1013904223)%100 < 50 }},
		{"random75", func(i int) bool { return (uint32(i)*1664525+1013904223)%100 < 75 }},
		{"clustered50", func(i int) bool { return (i/64)%2 == 0 }},
		{"all", func(int) bool { return true }},
		{"none", func(int) bool { return false }},
	}

	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		for _, pattern := range patterns {
			left, right := makeNarrowComparisonData[T](size, pattern.pick)
			leftBytes := arrow.GetBytes(left)
			rightBytes := arrow.GetBytes(right)
			scalarBytes := arrow.GetBytes([]T{100})

			for _, shape := range []string{"array_array", "array_scalar", "scalar_array"} {
				b.Run(fmt.Sprintf("%s/%d/%s", shape, size, pattern.name), func(b *testing.B) {
					cmp := genCompareKernel[T](CmpGT)
					out := make([]byte, bitutil.BytesForBits(int64(size)))
					b.SetBytes(int64(size) * int64(unsafe.Sizeof(T(0))))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch shape {
						case "array_array":
							cmp.funcAA(leftBytes, rightBytes, out, 0)
						case "array_scalar":
							cmp.funcAS(leftBytes, scalarBytes, out, 0)
						case "scalar_array":
							cmp.funcSA(scalarBytes, rightBytes, out, 0)
						}
						narrowComparisonBenchmarkSink ^= out[0]
					}
				})
			}
		}
	}
}

func makeNarrowComparisonData[T arrow.NumericType](size int, pick func(int) bool) ([]T, []T) {
	left := make([]T, size)
	right := make([]T, size)
	for i := range left {
		if pick(i) {
			left[i] = 100
			right[i] = 50
		} else {
			left[i] = 50
			right[i] = 100
		}
	}
	return left, right
}
