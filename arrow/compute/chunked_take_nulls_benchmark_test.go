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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkTakeChunkedBinaryNulls(b *testing.B) {
	for _, typ := range chunkedTakeBinaryTypes {
		for _, numChunks := range []int{8, 64} {
			for _, nullPattern := range []string{"none", "sparse", "alternating", "all"} {
				for _, order := range []string{"sequential", "random"} {
					name := fmt.Sprintf("%s/chunks=%d/nulls=%s/%s", typ, numChunks, nullPattern, order)
					b.Run(name, func(b *testing.B) {
						mem := memory.DefaultAllocator
						ctx := compute.WithAllocator(context.Background(), mem)
						const rowsPerChunk = 4096
						chunks := make([]arrow.Array, numChunks)
						for i := range chunks {
							bldr := array.NewBinaryBuilder(mem, typ.(arrow.BinaryDataType))
							bldr.Reserve(rowsPerChunk)
							bldr.ReserveData(rowsPerChunk * 8)
							for j := 0; j < rowsPerChunk; j++ {
								isNull := nullPattern == "all" ||
									(nullPattern == "sparse" && j%100 == 0) ||
									(nullPattern == "alternating" && j%2 == 0)
								if isNull {
									bldr.AppendNull()
								} else {
									bldr.Append([]byte("value123"))
								}
							}
							chunks[i] = bldr.NewArray()
							bldr.Release()
						}
						values := arrow.NewChunked(typ, chunks)
						defer values.Release()
						for _, chunk := range chunks {
							chunk.Release()
						}

						totalRows := numChunks * rowsPerChunk
						indicesBldr := array.NewInt64Builder(mem)
						indicesBldr.Reserve(totalRows / 10)
						for i := 0; i < totalRows/10; i++ {
							idx := int64(i)
							if order == "random" {
								idx = (idx*1103515245 + 12345) % int64(totalRows)
							}
							indicesBldr.Append(idx)
						}
						indices := indicesBldr.NewArray()
						indicesBldr.Release()
						defer indices.Release()
						valuesDatum := &compute.ChunkedDatum{Value: values}
						indicesDatum := &compute.ArrayDatum{Value: indices.Data()}
						opts := *compute.DefaultTakeOptions()

						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							result, err := compute.Take(ctx, opts, valuesDatum, indicesDatum)
							if err != nil {
								b.Fatal(err)
							}
							result.Release()
						}
					})
				}
			}
		}
	}
}
