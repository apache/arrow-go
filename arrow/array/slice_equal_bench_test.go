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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkSliceEqualFullRange(b *testing.B) {
	tests := []struct {
		name     string
		newArray func() arrow.Array
	}{
		{name: "int64_64", newArray: func() arrow.Array {
			return makeSliceEqualInt64Array(64)
		}},
		{name: "string_64", newArray: func() arrow.Array {
			return makeSliceEqualStringArray(64)
		}},
	}

	for _, test := range tests {
		arr := test.newArray()
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !array.SliceEqual(arr, 0, int64(arr.Len()), arr, 0, int64(arr.Len())) {
					b.Fatal("array should equal itself")
				}
			}
		})
		arr.Release()
	}
}

func BenchmarkChunkedEqualFullChunks(b *testing.B) {
	tests := []struct {
		name        string
		numChunks   int
		chunkLength int
	}{
		{name: "64chunks_1024values", numChunks: 64, chunkLength: 1024},
		{name: "1024chunks_64values", numChunks: 1024, chunkLength: 64},
	}

	for _, test := range tests {
		left, right := makeSliceEqualChunkedArrays(test.numChunks, test.chunkLength)
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !array.ChunkedEqual(left, right) {
					b.Fatal("chunked arrays should be equal")
				}
			}
		})
		left.Release()
		right.Release()
	}
}

func makeSliceEqualInt64Array(length int) arrow.Array {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	values := make([]int64, length)
	for i := range values {
		values[i] = int64(i)
	}
	builder.AppendValues(values, nil)
	return builder.NewInt64Array()
}

func makeSliceEqualStringArray(length int) arrow.Array {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

	values := make([]string, length)
	for i := range values {
		values[i] = "value"
	}
	builder.AppendValues(values, nil)
	return builder.NewStringArray()
}

func makeSliceEqualChunkedArrays(numChunks, chunkLength int) (*arrow.Chunked, *arrow.Chunked) {
	chunks := make([]arrow.Array, numChunks)
	values := make([]int64, chunkLength)
	for i := 0; i < numChunks; i++ {
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		for j := range values {
			values[j] = int64(i*chunkLength + j)
		}
		builder.AppendValues(values, nil)
		chunks[i] = builder.NewInt64Array()
		builder.Release()
	}

	left := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	right := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return left, right
}
