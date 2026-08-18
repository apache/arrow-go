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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

var chunkedTakeIndexTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8,
	arrow.PrimitiveTypes.Uint8,
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Uint16,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Uint32,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint64,
}

var chunkedTakeBinaryTypes = []arrow.DataType{
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.Binary,
	arrow.BinaryTypes.LargeString,
	arrow.BinaryTypes.LargeBinary,
}

func TestChunkedBinaryTakeIndexTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(context.Background(), mem)

	for _, typ := range chunkedTakeBinaryTypes {
		t.Run(typ.String(), func(t *testing.T) {
			values := makeChunkedTakeValues(mem, typ)
			defer values.Release()

			for _, indexType := range chunkedTakeIndexTypes {
				t.Run(indexType.String(), func(t *testing.T) {
					indices0Full := makeChunkedTakeIndexArray(mem, indexType,
						[]string{"77", "0", "5", "99", "3", "77"},
						[]bool{true, true, true, false, true, true})
					indices0 := array.NewSlice(indices0Full, 1, 5)
					indices1Full := makeChunkedTakeIndexArray(mem, indexType,
						[]string{"77", "2", "1", "77"}, nil)
					indices1 := array.NewSlice(indices1Full, 1, 3)

					indices := arrow.NewChunked(indexType, []arrow.Array{indices0, indices1})
					indices0Full.Release()
					indices0.Release()
					indices1Full.Release()
					indices1.Release()
					defer indices.Release()

					result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
						&compute.ChunkedDatum{Value: values},
						&compute.ChunkedDatum{Value: indices})
					require.NoError(t, err)
					defer result.Release()

					actual := result.(*compute.ChunkedDatum).Value
					require.Equal(t, 6, actual.Len())
					require.Len(t, actual.Chunks(), 2)
					require.Equal(t, 4, actual.Chunk(0).Len())
					require.Equal(t, 2, actual.Chunk(1).Len())

					expected0 := newTestBinaryArray(mem, typ,
						[][]byte{[]byte("zero"), []byte("five"), nil, []byte("three")},
						[]bool{true, true, false, true})
					expected1 := newTestBinaryArray(mem, typ,
						[][]byte{[]byte("two"), []byte("one")}, nil)
					expected := arrow.NewChunked(typ, []arrow.Array{expected0, expected1})
					expected0.Release()
					expected1.Release()
					defer expected.Release()
					require.True(t, array.ChunkedEqual(expected, actual))
				})
			}
		})
	}

	mem.AssertSize(t, 0)
}

func TestChunkedBinaryTakeBoundsChecks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(context.Background(), mem)

	for _, typ := range chunkedTakeBinaryTypes {
		t.Run(typ.String(), func(t *testing.T) {
			values := makeChunkedTakeValues(mem, typ)
			defer values.Release()

			for _, indexType := range chunkedTakeIndexTypes {
				cases := []struct {
					name  string
					value string
				}{
					{name: "upper_bound", value: "6"},
				}
				if arrow.IsSignedInteger(indexType.ID()) {
					cases = append(cases, struct {
						name  string
						value string
					}{name: "negative", value: "-1"})
				}

				for _, tc := range cases {
					t.Run(indexType.String()+"/"+tc.name, func(t *testing.T) {
						indices := makeChunkedTakeIndexArray(mem, indexType, []string{tc.value}, nil)
						defer indices.Release()

						_, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
							&compute.ChunkedDatum{Value: values},
							&compute.ArrayDatum{Value: indices.Data()})
						require.ErrorIs(t, err, arrow.ErrIndex)
					})
				}
			}
		})
	}

	mem.AssertSize(t, 0)
}

func TestChunkedBinaryTakeEmptyIndexChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(context.Background(), mem)

	for _, typ := range chunkedTakeBinaryTypes {
		t.Run(typ.String(), func(t *testing.T) {
			values := makeChunkedTakeValues(mem, typ)
			defer values.Release()

			for _, indexType := range chunkedTakeIndexTypes {
				t.Run(indexType.String(), func(t *testing.T) {
					t.Run("zero_chunks", func(t *testing.T) {
						indices := arrow.NewChunked(indexType, nil)
						defer indices.Release()

						result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
							&compute.ChunkedDatum{Value: values},
							&compute.ChunkedDatum{Value: indices})
						require.NoError(t, err)
						defer result.Release()

						actual := result.(*compute.ChunkedDatum).Value
						require.Equal(t, 0, actual.Len())
						require.Empty(t, actual.Chunks())
					})

					for _, tc := range []struct {
						name          string
						chunkedValues [][]string
						wantLen       int
						wantChunks    int
					}{
						{name: "empty_chunk", chunkedValues: [][]string{nil}, wantLen: 0, wantChunks: 0},
						{name: "leading_empty", chunkedValues: [][]string{nil, {"0", "5"}}, wantLen: 2, wantChunks: 1},
						{name: "trailing_empty", chunkedValues: [][]string{{"0", "5"}, nil}, wantLen: 2, wantChunks: 1},
					} {
						t.Run(tc.name, func(t *testing.T) {
							chunks := make([]arrow.Array, len(tc.chunkedValues))
							for i, chunkValues := range tc.chunkedValues {
								chunks[i] = makeChunkedTakeIndexArray(mem, indexType, chunkValues, nil)
							}
							indices := arrow.NewChunked(indexType, chunks)
							for _, chunk := range chunks {
								chunk.Release()
							}
							defer indices.Release()

							result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
								&compute.ChunkedDatum{Value: values},
								&compute.ChunkedDatum{Value: indices})
							require.NoError(t, err)
							defer result.Release()

							actual := result.(*compute.ChunkedDatum).Value
							require.Equal(t, tc.wantLen, actual.Len())
							require.Len(t, actual.Chunks(), tc.wantChunks)
							if tc.wantChunks == 1 {
								require.Equal(t, 2, actual.Chunk(0).Len())
							}
						})
					}
				})
			}
		})
	}

	mem.AssertSize(t, 0)
}

func makeChunkedTakeValues(mem memory.Allocator, typ arrow.DataType) *arrow.Chunked {
	full := newTestBinaryArray(mem, typ,
		[][]byte{[]byte("prefix"), []byte("zero"), []byte("one"), []byte("two"), []byte("three"), []byte("suffix")}, nil)
	middle := array.NewSlice(full, 1, 5)
	tail := newTestBinaryArray(mem, typ, [][]byte{[]byte("four"), []byte("five")}, nil)
	empty0 := newTestBinaryArray(mem, typ, nil, nil)
	empty1 := newTestBinaryArray(mem, typ, nil, nil)

	values := arrow.NewChunked(typ, []arrow.Array{empty0, middle, tail, empty1})
	full.Release()
	middle.Release()
	tail.Release()
	empty0.Release()
	empty1.Release()
	return values
}

func makeChunkedTakeIndexArray(mem memory.Allocator, typ arrow.DataType, values []string, valid []bool) arrow.Array {
	bldr := array.NewBuilder(mem, typ)
	bldr.Reserve(len(values))
	for i, value := range values {
		if len(valid) != 0 && !valid[i] {
			bldr.AppendNull()
			continue
		}
		if err := bldr.AppendValueFromString(value); err != nil {
			panic("failed to build take index: " + err.Error())
		}
	}
	result := bldr.NewArray()
	bldr.Release()
	return result
}
