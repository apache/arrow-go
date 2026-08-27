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

func TestChunkedBinaryTake(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(context.Background(), mem)
	for _, typ := range []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.LargeBinary,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			chunk0Full := newTestBinaryArray(mem, typ,
				[][]byte{[]byte("prefix"), []byte("hello"), []byte("world"), []byte("suffix")},
				[]bool{true, true, true, true})
			chunk0 := array.NewSlice(chunk0Full, 1, 3)
			chunk1 := newTestBinaryArray(mem, typ,
				[][]byte{[]byte("unused"), []byte("foo"), []byte("bar"), []byte("baz")},
				[]bool{false, true, true, true})
			empty := newTestBinaryArray(mem, typ, nil, nil)
			values := arrow.NewChunked(typ, []arrow.Array{empty, chunk0, empty, chunk1})
			defer values.Release()
			empty.Release()
			chunk0.Release()
			chunk0Full.Release()
			chunk1.Release()

			indices := newTestInt64Array(mem, []int64{4, 0, 2, 0, 5, 1}, []bool{true, true, true, false, true, true})
			defer indices.Release()
			result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
				&compute.ChunkedDatum{Value: values}, &compute.ArrayDatum{Value: indices.Data()})
			require.NoError(t, err)
			actual := result.(*compute.ChunkedDatum).Value
			expectedArray := newTestBinaryArray(mem, typ,
				[][]byte{[]byte("bar"), []byte("hello"), nil, nil, []byte("baz"), []byte("world")},
				[]bool{true, true, false, false, true, true})
			expected := arrow.NewChunked(typ, []arrow.Array{expectedArray})
			require.True(t, array.ChunkedEqual(expected, actual))
			result.Release()

			invalid := newTestInt64Array(mem, []int64{6}, nil)
			_, err = compute.Take(ctx, *compute.DefaultTakeOptions(),
				&compute.ChunkedDatum{Value: values}, &compute.ArrayDatum{Value: invalid.Data()})
			require.ErrorIs(t, err, arrow.ErrIndex)
			invalid.Release()

			indices0 := newTestInt64Array(mem, []int64{4, 0, 2}, nil)
			indices1 := newTestInt64Array(mem, []int64{0, 5, 1}, []bool{false, true, true})
			chunkedIndices := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{indices0, indices1})
			defer chunkedIndices.Release()
			indices0.Release()
			indices1.Release()

			result, err = compute.Take(ctx, *compute.DefaultTakeOptions(),
				&compute.ChunkedDatum{Value: values}, &compute.ChunkedDatum{Value: chunkedIndices})
			require.NoError(t, err)
			actual = result.(*compute.ChunkedDatum).Value
			require.True(t, array.ChunkedEqual(expected, actual))
			result.Release()
			expected.Release()
			expectedArray.Release()
		})
	}
	mem.AssertSize(t, 0)
}

func newTestBinaryArray(mem memory.Allocator, typ arrow.DataType, values [][]byte, valid []bool) arrow.Array {
	bldr := array.NewBinaryBuilder(mem, typ.(arrow.BinaryDataType))
	bldr.Reserve(len(values))
	for i, value := range values {
		if len(valid) != 0 && !valid[i] {
			bldr.AppendNull()
		} else {
			bldr.Append(value)
		}
	}
	result := bldr.NewArray()
	bldr.Release()
	return result
}

func newTestInt64Array(mem memory.Allocator, values []int64, valid []bool) arrow.Array {
	bldr := array.NewInt64Builder(mem)
	bldr.Reserve(len(values))
	for i, value := range values {
		if len(valid) != 0 && !valid[i] {
			bldr.AppendNull()
		} else {
			bldr.Append(value)
		}
	}
	result := bldr.NewArray()
	bldr.Release()
	return result
}

func BenchmarkTakeChunkedBinary(b *testing.B) {
	for _, typ := range []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.LargeBinary,
	} {
		for _, numChunks := range []int{8, 64} {
			for _, selectivity := range []int{1, 10, 50, 100} {
				name := fmt.Sprintf("%s/chunks=%d/selectivity=%d%%", typ, numChunks, selectivity)
				b.Run(name, func(b *testing.B) {
					mem := memory.DefaultAllocator
					ctx := compute.WithAllocator(context.Background(), mem)

					const rowsPerChunk = 4096
					values, indices := makeChunkedBinaryTakeInputs(mem, typ, numChunks, rowsPerChunk, selectivity)
					defer values.Release()
					defer indices.Release()

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
							&compute.ChunkedDatum{Value: values},
							&compute.ArrayDatum{Value: indices.Data()})
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

func makeChunkedBinaryTakeInputs(mem memory.Allocator, typ arrow.DataType, numChunks, rowsPerChunk, selectivity int) (*arrow.Chunked, arrow.Array) {
	chunks := make([]arrow.Array, numChunks)
	value := []byte("0123456789abcdefghijklmnopqrstuv")
	for i := range chunks {
		bldr := array.NewBinaryBuilder(mem, typ.(arrow.BinaryDataType))
		bldr.Reserve(rowsPerChunk)
		bldr.ReserveData(rowsPerChunk * len(value))
		for j := 0; j < rowsPerChunk; j++ {
			bldr.Append(value)
		}
		chunks[i] = bldr.NewArray()
		bldr.Release()
	}

	values := arrow.NewChunked(typ, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}

	totalRows := numChunks * rowsPerChunk
	numIndices := totalRows * selectivity / 100
	indicesBldr := array.NewInt64Builder(mem)
	indicesBldr.Reserve(numIndices)
	for i := 0; i < numIndices; i++ {
		idx := (int64(i)*1103515245 + 12345) % int64(totalRows)
		indicesBldr.Append(idx)
	}
	indices := indicesBldr.NewArray()
	indicesBldr.Release()
	return values, indices
}
