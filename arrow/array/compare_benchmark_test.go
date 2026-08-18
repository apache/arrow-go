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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type fixedWidthArrayFactory func([]int64, []bool) arrow.Array

func BenchmarkEqualFixedWidth(b *testing.B) {
	types := []struct {
		name    string
		factory fixedWidthArrayFactory
	}{
		{"int32", makeInt32Array},
		{"int64", makeInt64Array},
	}
	nullPatterns := []struct {
		name  string
		valid func(int) []bool
	}{
		{"all-valid", func(int) []bool { return nil }},
		{"one-percent-null", makeValidityEvery(100)},
		{"alternating-null", makeValidityEvery(2)},
		{"clustered-null", makeClusteredValidity},
	}

	for _, typ := range types {
		b.Run(typ.name, func(b *testing.B) {
			for _, length := range []int{1024, 65536} {
				b.Run(fmt.Sprintf("len-%d", length), func(b *testing.B) {
					for _, pattern := range nullPatterns {
						b.Run(pattern.name, func(b *testing.B) {
							values := makeBenchmarkValues(length)
							valid := pattern.valid(length)
							rightValues := append([]int64(nil), values...)
							for idx, isValid := range valid {
								if !isValid {
									rightValues[idx]++
								}
							}

							left := typ.factory(values, valid)
							defer left.Release()
							right := typ.factory(rightValues, valid)
							defer right.Release()

							b.ReportAllocs()
							b.ResetTimer()
							for b.Loop() {
								if !array.Equal(left, right) {
									b.Fatal("expected arrays to be equal")
								}
							}
						})
					}
				})
			}

			for _, mismatch := range []struct {
				name string
				pos  int
			}{
				{"mismatch-first", 0},
				{"mismatch-middle", 65536 / 2},
				{"mismatch-last", 65536 - 1},
			} {
				b.Run(mismatch.name, func(b *testing.B) {
					values := makeBenchmarkValues(65536)
					rightValues := append([]int64(nil), values...)
					rightValues[mismatch.pos]++

					left := typ.factory(values, nil)
					defer left.Release()
					right := typ.factory(rightValues, nil)
					defer right.Release()

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if array.Equal(left, right) {
							b.Fatal("expected arrays to differ")
						}
					}
				})
			}
		})
	}
}

func makeBenchmarkValues(length int) []int64 {
	values := make([]int64, length)
	for idx := range values {
		values[idx] = int64(idx*17 + idx%11)
	}
	return values
}

func makeValidityEvery(n int) func(int) []bool {
	return func(length int) []bool {
		valid := make([]bool, length)
		for idx := range valid {
			valid[idx] = idx%n != 0
		}
		return valid
	}
}

func makeClusteredValidity(length int) []bool {
	valid := make([]bool, length)
	for idx := length / 2; idx < length; idx++ {
		valid[idx] = true
	}
	return valid
}

func makeInt32Array(values []int64, valid []bool) arrow.Array {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()

	converted := make([]int32, len(values))
	for idx, value := range values {
		converted[idx] = int32(value)
	}
	builder.AppendValues(converted, valid)
	return builder.NewInt32Array()
}

func makeInt64Array(values []int64, valid []bool) arrow.Array {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.AppendValues(values, valid)
	return builder.NewInt64Array()
}
