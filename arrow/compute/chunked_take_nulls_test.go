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
	"github.com/stretchr/testify/require"
)

func TestChunkedBinaryTakeNullPatterns(t *testing.T) {
	for _, typ := range chunkedTakeBinaryTypes {
		for _, nullPattern := range []string{"none", "sparse", "alternating", "all", "mixed_chunks"} {
			for _, prefix := range []int{1, 3, 7, 8, 15} {
				t.Run(fmt.Sprintf("%s/%s/offset=%d", typ, nullPattern, prefix), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer mem.AssertSize(t, 0)
					ctx := compute.WithAllocator(context.Background(), mem)
					const rowsPerChunk = 9
					logicalValues := make([][]byte, 2*rowsPerChunk)
					logicalValid := make([]bool, len(logicalValues))
					chunks := make([]arrow.Array, 0, 5)
					empty := newTestBinaryArray(mem, typ, nil, nil)
					defer empty.Release()
					for chunkIndex := 0; chunkIndex < 2; chunkIndex++ {
						values := make([][]byte, prefix+rowsPerChunk+1)
						valid := make([]bool, len(values))
						for i := range values {
							values[i] = []byte("padding")
							valid[i] = true
						}
						for i := 0; i < rowsPerChunk; i++ {
							globalIndex := chunkIndex*rowsPerChunk + i
							value := []byte(fmt.Sprintf("row-%d", globalIndex))
							if i == 0 || i == rowsPerChunk-1 {
								value = nil
							}
							isNull := nullPattern == "all" ||
								(nullPattern == "sparse" && i == 1) ||
								(nullPattern == "alternating" && i%2 == 0) ||
								(nullPattern == "mixed_chunks" && chunkIndex == 1 && i%2 == 0)
							values[prefix+i] = value
							valid[prefix+i] = !isNull
							logicalValues[globalIndex] = value
							logicalValid[globalIndex] = !isNull
						}
						full := newTestBinaryArray(mem, typ, values, valid)
						if nullPattern == "mixed_chunks" && chunkIndex == 0 {
							buffers := append([]*memory.Buffer(nil), full.Data().Buffers()...)
							buffers[0] = nil
							data := array.NewData(typ, full.Len(), buffers, nil, 0, 0)
							full.Release()
							full = array.MakeFromData(data)
							data.Release()
						}
						sliced := array.NewSlice(full, int64(prefix), int64(prefix+rowsPerChunk))
						full.Release()
						chunks = append(chunks, empty, sliced)
					}
					chunks = append(chunks, empty)
					values := arrow.NewChunked(typ, chunks)
					defer values.Release()
					for _, chunk := range chunks {
						if chunk != empty {
							chunk.Release()
						}
					}

					for _, indexType := range chunkedTakeIndexTypes {
						t.Run(indexType.String(), func(t *testing.T) {
							selected := []int{17, 0, 9, 8, 10, 9, 1, 16, 3, 17}
							indicesValues := make([]string, len(selected)+2)
							indicesValid := make([]bool, len(indicesValues))
							for i := range indicesValues {
								indicesValues[i] = "99"
								indicesValid[i] = true
							}
							wantValues := make([][]byte, len(selected))
							wantValid := make([]bool, len(selected))
							for i, idx := range selected {
								if i == 4 {
									indicesValid[i+1] = false
									continue
								}
								indicesValues[i+1] = fmt.Sprint(idx)
								wantValues[i] = logicalValues[idx]
								wantValid[i] = logicalValid[idx]
							}
							indicesFull := makeChunkedTakeIndexArray(t, mem, indexType, indicesValues, indicesValid)
							defer indicesFull.Release()
							indices := array.NewSlice(indicesFull, 1, int64(1+len(selected)))
							defer indices.Release()
							result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
								&compute.ChunkedDatum{Value: values}, &compute.ArrayDatum{Value: indices.Data()})
							require.NoError(t, err)
							defer result.Release()
							expectedArray := newTestBinaryArray(mem, typ, wantValues, wantValid)
							defer expectedArray.Release()
							expected := arrow.NewChunked(typ, []arrow.Array{expectedArray})
							defer expected.Release()
							actual := result.(*compute.ChunkedDatum).Value
							require.True(t, array.ChunkedEqual(expected, actual))
							require.Equal(t, expected.NullN(), actual.NullN())
						})
					}
				})
			}
		}
	}
}
