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

var benchmarkNewChunkedSliceSink int

func BenchmarkNewChunkedSlice(b *testing.B) {
	const rowsPerChunk = 64

	for _, numChunks := range []int{64, 1024, 8192} {
		for _, withNulls := range []bool{false, true} {
			for _, partial := range []bool{false, true} {
				name := fmt.Sprintf("chunks=%d/nulls=%t/partial=%t", numChunks, withNulls, partial)
				b.Run(name, func(b *testing.B) {
					input := makeChunkedSliceBenchmarkInput(numChunks, rowsPerChunk, withNulls)
					defer input.Release()

					var start, end int64
					if partial {
						start = 1
						end = int64(input.Len() - 1)
					} else {
						end = int64(input.Len())
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result := array.NewChunkedSlice(input, start, end)
						benchmarkNewChunkedSliceSink = result.Len()
						result.Release()
					}
				})
			}
		}
	}
}

func makeChunkedSliceBenchmarkInput(numChunks, rowsPerChunk int, withNulls bool) *arrow.Chunked {
	chunks := make([]arrow.Array, numChunks)
	values := make([]int64, rowsPerChunk)
	validity := make([]bool, rowsPerChunk)
	for i := range values {
		values[i] = int64(i)
		validity[i] = i%10 != 0
	}

	for i := range chunks {
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		if withNulls {
			builder.AppendValues(values, validity)
		} else {
			builder.AppendValues(values, nil)
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return result
}
