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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type bulkUnionBuilderFactory func(memory.Allocator) array.UnionBuilder

var bulkUnionBuilderFactories = []struct {
	name string
	new  bulkUnionBuilderFactory
}{
	{"sparse", func(mem memory.Allocator) array.UnionBuilder {
		return array.NewSparseUnionBuilder(mem, bulkUnionType(arrow.SparseMode).(*arrow.SparseUnionType))
	}},
	{"dense", func(mem memory.Allocator) array.UnionBuilder {
		return array.NewDenseUnionBuilder(mem, bulkUnionType(arrow.DenseMode).(*arrow.DenseUnionType))
	}},
}

func bulkUnionType(mode arrow.UnionMode) arrow.UnionType {
	fields := []arrow.Field{
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	codes := []arrow.UnionTypeCode{8, 13, 7}
	return arrow.UnionOf(mode, fields, codes)
}

func TestUnionBuilderBulkAppendNullsAndEmptyValues(t *testing.T) {
	for _, tc := range bulkUnionBuilderFactories {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			builder := tc.new(mem)
			defer builder.Release()

			builder.AppendNulls(0)
			builder.AppendEmptyValues(0)
			builder.AppendNulls(-1)
			builder.AppendEmptyValues(-1)
			require.Zero(t, builder.Len())
			require.Zero(t, builder.Cap())

			appendBulkUnionPrefix(builder, 3)
			builder.AppendNulls(5)
			builder.AppendEmptyValues(4)

			result := builder.NewArray().(array.Union)
			defer result.Release()
			require.NoError(t, result.ValidateFull())

			assert.Equal(t, 12, result.Len())
			assert.Equal(t, []arrow.UnionTypeCode{8, 13, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8}, result.RawTypeCodes())
		})
	}
}

func TestUnionBuilderZeroBulkAppendDoesNotMutateChildren(t *testing.T) {
	tests := []struct {
		name string
		new  func(memory.Allocator) array.UnionBuilder
	}{
		{"sparse", func(mem memory.Allocator) array.UnionBuilder {
			return array.NewEmptySparseUnionBuilder(mem)
		}},
		{"dense", func(mem memory.Allocator) array.UnionBuilder {
			return array.NewEmptyDenseUnionBuilder(mem)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			builder := tc.new(mem)
			defer builder.Release()

			builder.AppendNulls(0)
			builder.AppendEmptyValues(0)
			builder.AppendNulls(-1)
			builder.AppendEmptyValues(-1)

			assert.Zero(t, builder.Len())
			assert.Zero(t, builder.Cap())
		})
	}
}

func TestUnionBuilderBulkAppendMatchesScalar(t *testing.T) {
	starts := []int{0, 1, 2, 7, 8, 15}
	batchSizes := []int{-1, 0, 1, 2, 7, 16, 17}
	operations := []struct {
		name   string
		bulk   func(array.UnionBuilder, int)
		scalar func(array.UnionBuilder, int)
	}{
		{
			name: "nulls",
			bulk: func(builder array.UnionBuilder, n int) {
				builder.AppendNulls(n)
			},
			scalar: func(builder array.UnionBuilder, n int) {
				for i := 0; i < n; i++ {
					builder.AppendNull()
				}
			},
		},
		{
			name: "empty_values",
			bulk: func(builder array.UnionBuilder, n int) {
				builder.AppendEmptyValues(n)
			},
			scalar: func(builder array.UnionBuilder, n int) {
				for i := 0; i < n; i++ {
					builder.AppendEmptyValue()
				}
			},
		},
	}

	for _, factory := range bulkUnionBuilderFactories {
		t.Run(factory.name, func(t *testing.T) {
			for operationIndex, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					reuseOperation := operations[1-operationIndex]
					for _, start := range starts {
						for _, batchSize := range batchSizes {
							t.Run(fmt.Sprintf("start_%d_batch_%d", start, batchSize), func(t *testing.T) {
								mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
								defer mem.AssertSize(t, 0)

								bulk := factory.new(mem)
								defer bulk.Release()
								scalar := factory.new(mem)
								defer scalar.Release()

								appendBulkUnionPrefix(bulk, start)
								appendBulkUnionPrefix(scalar, start)
								operation.bulk(bulk, batchSize)
								operation.scalar(scalar, batchSize)
								assertUnionBuilderArrayParity(t, bulk, scalar)

								appendBulkUnionPrefix(bulk, start)
								appendBulkUnionPrefix(scalar, start)
								reuseOperation.bulk(bulk, 9)
								reuseOperation.scalar(scalar, 9)
								assertUnionBuilderArrayParity(t, bulk, scalar)
							})
						}
					}
				})
			}
		})
	}
}

func appendBulkUnionPrefix(builder array.UnionBuilder, n int) {
	codes := []arrow.UnionTypeCode{8, 13, 7}
	for i := 0; i < n; i++ {
		childID := i % len(codes)
		builder.Append(codes[childID])

		switch childID {
		case 0:
			builder.Child(childID).(*array.StringBuilder).Append(fmt.Sprintf("value-%d", i))
		case 1:
			builder.Child(childID).(*array.Int32Builder).Append(int32(i))
		case 2:
			builder.Child(childID).(*array.Float64Builder).Append(float64(i))
		}

		if builder.Mode() == arrow.SparseMode {
			for i := 0; i < len(codes); i++ {
				if i != childID {
					builder.Child(i).AppendEmptyValue()
				}
			}
		}
	}
}

func assertUnionBuilderArrayParity(t *testing.T, bulk, scalar array.UnionBuilder) {
	t.Helper()

	assert.Equal(t, scalar.Len(), bulk.Len())

	bulkArray := bulk.NewArray().(array.Union)
	defer bulkArray.Release()
	scalarArray := scalar.NewArray().(array.Union)
	defer scalarArray.Release()

	require.NoError(t, bulkArray.ValidateFull())
	require.NoError(t, scalarArray.ValidateFull())
	assert.True(t, array.Equal(bulkArray, scalarArray))
}

func BenchmarkUnionBuilderBulkAppend(b *testing.B) {
	for _, tc := range bulkUnionBuilderFactories {
		b.Run(tc.name, func(b *testing.B) {
			for _, rows := range []int{1, 16, 1024, 65536} {
				b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
					b.Run("nulls", func(b *testing.B) {
						benchmarkUnionBuilderBulkAppend(b, tc.new, rows, false)
					})
					b.Run("empty", func(b *testing.B) {
						benchmarkUnionBuilderBulkAppend(b, tc.new, rows, true)
					})
				})
			}
		})
	}
}

func benchmarkUnionBuilderBulkAppend(b *testing.B, factory bulkUnionBuilderFactory, rows int, empty bool) {
	builder := factory(memory.DefaultAllocator)
	defer builder.Release()
	b.ReportAllocs()

	for b.Loop() {
		if empty {
			builder.AppendEmptyValues(rows)
		} else {
			builder.AppendNulls(rows)
		}

		arr := builder.NewArray()
		arr.Release()
	}
}
