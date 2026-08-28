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

var benchmarkUnionEqualResult bool

func BenchmarkUnionEqual(b *testing.B) {
	const rows = 65536

	for _, mode := range []string{"sparse", "dense"} {
		for _, pattern := range []string{"one-type", "runs-64", "alternating"} {
			for _, mismatch := range []bool{false, true} {
				for _, comparison := range []struct {
					name string
					fn   func(arrow.Array, arrow.Array) bool
				}{
					{name: "equal", fn: array.Equal},
					{name: "approx", fn: func(left, right arrow.Array) bool {
						return array.ApproxEqual(left, right)
					}},
				} {
					name := fmt.Sprintf("%s/%s/%s/%s", mode, pattern, comparison.name, unionEqualBenchmarkMismatchName(mismatch))
					b.Run(name, func(b *testing.B) {
						left := makeUnionEqualBenchmarkArray(b, mode, pattern, rows, false)
						right := makeUnionEqualBenchmarkArray(b, mode, pattern, rows, mismatch)
						defer left.Release()
						defer right.Release()

						b.ReportAllocs()
						b.SetBytes(int64(rows))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							benchmarkUnionEqualResult = comparison.fn(left, right)
						}
					})
				}
			}
		}
	}
}

func unionEqualBenchmarkMismatchName(mismatch bool) string {
	if mismatch {
		return "mismatch-last"
	}
	return "equal"
}

func makeUnionEqualBenchmarkArray(b *testing.B, mode, pattern string, rows int, mismatch bool) arrow.Array {
	b.Helper()

	typeIDs := make([]int8, rows)
	offsets := make([]int32, rows)
	sparseInts := make([]int32, rows)
	sparseStrings := make([]string, rows)
	denseInts := make([]int32, 0, rows)
	denseStrings := make([]string, 0, rows)
	childOffsets := [2]int32{}

	for i := 0; i < rows; i++ {
		childID := unionEqualBenchmarkChildID(pattern, i)
		typeIDs[i] = int8(childID)

		if mode == "sparse" {
			sparseInts[i] = int32(i)
			sparseStrings[i] = fmt.Sprintf("value-%d", i)
			if mismatch && i == rows-1 {
				if childID == 0 {
					sparseInts[i]++
				} else {
					sparseStrings[i] = "different"
				}
			}
			continue
		}

		offsets[i] = childOffsets[childID]
		if childID == 0 {
			value := int32(i)
			if mismatch && i == rows-1 {
				value++
			}
			denseInts = append(denseInts, value)
		} else {
			value := fmt.Sprintf("value-%d", i)
			if mismatch && i == rows-1 {
				value = "different"
			}
			denseStrings = append(denseStrings, value)
		}
		childOffsets[childID]++
	}

	typeIDsArray := makeUnionEqualBenchmarkInt8Array(b, typeIDs)
	defer typeIDsArray.Release()
	if mode == "sparse" {
		intArray := makeUnionEqualBenchmarkInt32Array(b, sparseInts)
		defer intArray.Release()
		stringArray := makeUnionEqualBenchmarkStringArray(b, sparseStrings)
		defer stringArray.Release()

		result, err := array.NewSparseUnionFromArrays(typeIDsArray, []arrow.Array{intArray, stringArray})
		if err != nil {
			b.Fatal(err)
		}
		return result
	}

	offsetsArray := makeUnionEqualBenchmarkInt32Array(b, offsets)
	defer offsetsArray.Release()
	intArray := makeUnionEqualBenchmarkInt32Array(b, denseInts)
	defer intArray.Release()
	stringArray := makeUnionEqualBenchmarkStringArray(b, denseStrings)
	defer stringArray.Release()

	result, err := array.NewDenseUnionFromArrays(typeIDsArray, offsetsArray, []arrow.Array{intArray, stringArray})
	if err != nil {
		b.Fatal(err)
	}
	return result
}

func unionEqualBenchmarkChildID(pattern string, index int) int {
	switch pattern {
	case "one-type":
		return 0
	case "runs-64":
		return (index / 64) % 2
	case "alternating":
		return index % 2
	default:
		panic("unsupported union equality benchmark pattern")
	}
}

func makeUnionEqualBenchmarkInt8Array(b *testing.B, values []int8) arrow.Array {
	b.Helper()
	builder := array.NewInt8Builder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	result := builder.NewInt8Array()
	builder.Release()
	return result
}

func makeUnionEqualBenchmarkInt32Array(b *testing.B, values []int32) arrow.Array {
	b.Helper()
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	result := builder.NewInt32Array()
	builder.Release()
	return result
}

func makeUnionEqualBenchmarkStringArray(b *testing.B, values []string) arrow.Array {
	b.Helper()
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	result := builder.NewStringArray()
	builder.Release()
	return result
}
