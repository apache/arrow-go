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

var benchmarkListEqualResult bool

func BenchmarkListEqual(b *testing.B) {
	const rows = 65536
	tests := []struct {
		listType string
		child    arrow.DataType
		listSize int
		validity string
	}{
		{"list", arrow.PrimitiveTypes.Int32, 1, "all-valid"},
		{"list", arrow.PrimitiveTypes.Int32, 4, "all-valid"},
		{"list", arrow.PrimitiveTypes.Int32, 16, "all-valid"},
		{"list", arrow.PrimitiveTypes.Int32, 64, "all-valid"},
		{"list", arrow.BinaryTypes.String, 16, "all-valid"},
		{"large-list", arrow.PrimitiveTypes.Int32, 16, "all-valid"},
		{"fixed-size-list", arrow.PrimitiveTypes.Int32, 16, "all-valid"},
		{"list", arrow.PrimitiveTypes.Int32, 16, "10pct-null"},
		{"list", arrow.PrimitiveTypes.Int32, 16, "clustered-10pct-null"},
		{"list", arrow.PrimitiveTypes.Int32, 16, "alternating-null"},
	}

	for _, tc := range tests {
		name := fmt.Sprintf("%s/%s/size=%d/%s", tc.listType, tc.child.Name(), tc.listSize, tc.validity)
		b.Run(name, func(b *testing.B) {
			left := makeListEqualBenchmarkArray(tc.listType, tc.child, rows, tc.listSize, tc.validity)
			right := makeListEqualBenchmarkArray(tc.listType, tc.child, rows, tc.listSize, tc.validity)
			defer left.Release()
			defer right.Release()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkListEqualResult = array.Equal(left, right)
			}
		})
	}
}

func makeListEqualBenchmarkArray(listType string, childType arrow.DataType, rows, listSize int, validity string) arrow.Array {
	var (
		child        array.Builder
		appendParent func(bool)
		newArray     func() arrow.Array
		release      func()
	)

	switch listType {
	case "list":
		bldr := array.NewListBuilder(memory.DefaultAllocator, childType)
		child = bldr.ValueBuilder()
		appendParent = bldr.Append
		newArray = bldr.NewArray
		release = bldr.Release
	case "large-list":
		bldr := array.NewLargeListBuilder(memory.DefaultAllocator, childType)
		child = bldr.ValueBuilder()
		appendParent = bldr.Append
		newArray = bldr.NewArray
		release = bldr.Release
	case "fixed-size-list":
		bldr := array.NewFixedSizeListBuilder(memory.DefaultAllocator, int32(listSize), childType)
		child = bldr.ValueBuilder()
		appendParent = bldr.Append
		newArray = bldr.NewArray
		release = bldr.Release
	default:
		panic("unsupported list type")
	}
	defer release()

	for i := 0; i < rows; i++ {
		valid := listBenchmarkValueIsValid(i, rows, validity)
		appendParent(valid)
		for j := 0; j < listSize; j++ {
			switch child := child.(type) {
			case *array.Int32Builder:
				child.Append(int32(j))
			case *array.StringBuilder:
				child.Append([]string{"alpha", "bravo", "charlie", "delta"}[j%4])
			default:
				panic("unsupported child type")
			}
		}
	}
	return newArray()
}

func listBenchmarkValueIsValid(index, length int, pattern string) bool {
	switch pattern {
	case "all-valid":
		return true
	case "10pct-null":
		return index%10 != 0
	case "clustered-10pct-null":
		return index < length*45/100 || index >= length*55/100
	case "alternating-null":
		return index%2 == 0
	default:
		panic("unsupported validity pattern")
	}
}
