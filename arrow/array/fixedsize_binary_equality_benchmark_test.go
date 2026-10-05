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

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

var fixedSizeBinaryEqualityResult bool

func BenchmarkFixedSizeBinaryEquality(b *testing.B) {
	const length = 64 * 1024
	for _, width := range []int{8, 32, 128} {
		for _, size := range []int{4, 8, 64, 1024, length} {
			for _, pattern := range []struct {
				name  string
				valid func(int) []bool
			}{
				{name: "all_valid"},
				{name: "materialized_all_valid", valid: func(n int) []bool {
					valid := make([]bool, n)
					for i := range valid {
						valid[i] = true
					}
					return valid
				}},
				{name: "one_percent_null", valid: func(n int) []bool {
					valid := make([]bool, n)
					for i := range valid {
						valid[i] = i%100 != 0
					}
					return valid
				}},
				{name: "alternating_null", valid: func(n int) []bool {
					valid := make([]bool, n)
					for i := range valid {
						valid[i] = i%2 == 0
					}
					return valid
				}},
				{name: "clustered_null", valid: func(n int) []bool {
					valid := make([]bool, n)
					for i := range valid {
						valid[i] = i >= n/2
					}
					return valid
				}},
			} {
				b.Run(fmt.Sprintf("width_%d/length_%d/%s", width, size, pattern.name), func(b *testing.B) {
					leftValues := make([]byte, size*width)
					for i := range leftValues {
						leftValues[i] = byte(i*31 + i/width*17)
					}
					rightValues := append([]byte(nil), leftValues...)
					var valid []bool
					if pattern.valid != nil {
						valid = pattern.valid(size)
					}
					nulls := 0
					for i, isValid := range valid {
						if !isValid {
							nulls++
							rightValues[i*width]++
						}
					}

					left := makeFixedSizeBinaryEqualityArray(width, leftValues, valid, size, 0, nulls)
					right := makeFixedSizeBinaryEqualityArray(width, rightValues, valid, size, 0, nulls)
					b.Cleanup(left.Release)
					b.Cleanup(right.Release)

					b.ReportAllocs()
					b.SetBytes(int64(size * width))
					b.ResetTimer()
					for b.Loop() {
						fixedSizeBinaryEqualityResult = array.Equal(left, right)
					}
				})
			}
		}
	}
}


func BenchmarkFixedSizeBinaryEqualityMismatch(b *testing.B) {
	const size = 64 * 1024
	for _, width := range []int{8, 32, 128} {
		for _, mismatch := range []struct {
			name string
			row  int
		}{
			{name: "first", row: 0},
			{name: "last", row: size - 1},
		} {
			b.Run(fmt.Sprintf("width_%d/%s", width, mismatch.name), func(b *testing.B) {
				leftValues := make([]byte, size*width)
				for i := range leftValues {
					leftValues[i] = byte(i*31 + i/width*17)
				}
				rightValues := append([]byte(nil), leftValues...)
				rightValues[mismatch.row*width]++

				left := makeFixedSizeBinaryEqualityArray(width, leftValues, nil, size, 0, 0)
				right := makeFixedSizeBinaryEqualityArray(width, rightValues, nil, size, 0, 0)
				b.Cleanup(left.Release)
				b.Cleanup(right.Release)

				b.ReportAllocs()
				b.SetBytes(int64(size * width))
				b.ResetTimer()
				for b.Loop() {
					fixedSizeBinaryEqualityResult = array.Equal(left, right)
				}
			})
		}
	}
}
