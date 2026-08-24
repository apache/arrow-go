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

type binaryBuilderFactory func(memory.Allocator) array.Builder

var binaryBuilderFactories = []struct {
	name string
	new  binaryBuilderFactory
}{
	{"binary", func(mem memory.Allocator) array.Builder {
		return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	}},
	{"large_binary", func(mem memory.Allocator) array.Builder {
		return array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	}},
	{"string", func(mem memory.Allocator) array.Builder {
		return array.NewStringBuilder(mem)
	}},
	{"large_string", func(mem memory.Allocator) array.Builder {
		return array.NewLargeStringBuilder(mem)
	}},
}

func TestBinaryBuilderBulkAppendNullsAndEmptyValues(t *testing.T) {
	for _, tc := range binaryBuilderFactories {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			builder := tc.new(mem)
			defer builder.Release()
			appendBinaryBuilderValue(builder, "abc")
			builder.AppendNulls(5)
			builder.AppendEmptyValues(4)
			appendBinaryBuilderValue(builder, "de")

			arr := builder.NewArray().(array.BinaryLike)
			defer arr.Release()
			require.NoError(t, arr.(interface{ ValidateFull() error }).ValidateFull())

			assert.Equal(t, 11, arr.Len())
			assert.Equal(t, 5, arr.NullN())
			assert.Equal(t, []byte("abcde"), arr.ValueBytes())
			for i := range arr.Len() {
				assert.Equal(t, i == 0 || i >= 6, arr.IsValid(i), "value %d", i)
				switch i {
				case 0:
					assert.Equal(t, int64(0), arr.ValueOffset64(i))
					assert.Equal(t, 3, arr.ValueLen(i))
				case 10:
					assert.Equal(t, int64(3), arr.ValueOffset64(i))
					assert.Equal(t, 2, arr.ValueLen(i))
				default:
					assert.Equal(t, int64(3), arr.ValueOffset64(i))
					assert.Zero(t, arr.ValueLen(i))
				}
			}
		})
	}
}

func appendBinaryBuilderValue(builder array.Builder, value string) {
	switch builder := builder.(type) {
	case *array.BinaryBuilder:
		builder.Append([]byte(value))
	case *array.StringBuilder:
		builder.Append(value)
	case *array.LargeStringBuilder:
		builder.Append(value)
	default:
		panic(fmt.Sprintf("unexpected binary builder %T", builder))
	}
}

func TestBinaryBuilderBulkAppendMatchesScalar(t *testing.T) {
	starts := []int{0, 1, 7, 8, 9, 15, 16, 17}
	batchSizes := []int{-1, 0, 1, 2, 7, 8, 9, 16, 17}
	operations := []struct {
		name   string
		bulk   func(array.Builder, int)
		scalar func(array.Builder, int)
	}{
		{
			name: "nulls",
			bulk: func(builder array.Builder, n int) {
				builder.AppendNulls(n)
			},
			scalar: func(builder array.Builder, n int) {
				for i := 0; i < n; i++ {
					builder.AppendNull()
				}
			},
		},
		{
			name: "empty_values",
			bulk: func(builder array.Builder, n int) {
				builder.AppendEmptyValues(n)
			},
			scalar: func(builder array.Builder, n int) {
				for i := 0; i < n; i++ {
					builder.AppendEmptyValue()
				}
			},
		},
	}

	for _, factory := range binaryBuilderFactories {
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

								appendBinaryBuilderPrefix(bulk, start)
								appendBinaryBuilderPrefix(scalar, start)
								operation.bulk(bulk, batchSize)
								operation.scalar(scalar, batchSize)
								assertBinaryBuilderArrayParity(t, bulk, scalar)

								appendBinaryBuilderPrefix(bulk, start)
								appendBinaryBuilderPrefix(scalar, start)
								reuseOperation.bulk(bulk, 9)
								reuseOperation.scalar(scalar, 9)
								assertBinaryBuilderArrayParity(t, bulk, scalar)
							})
						}
					}
				})
			}
		})
	}
}

func appendBinaryBuilderPrefix(builder array.Builder, n int) {
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			appendBinaryBuilderValue(builder, fmt.Sprintf("value-%d", i))
		case 1:
			builder.AppendNull()
		case 2:
			builder.AppendEmptyValue()
		case 3:
			appendBinaryBuilderValue(builder, fmt.Sprintf("tail-%d", i))
		}
	}
}

func assertBinaryBuilderArrayParity(t *testing.T, bulk, scalar array.Builder) {
	t.Helper()

	assert.Equal(t, scalar.Len(), bulk.Len())
	assert.Equal(t, scalar.NullN(), bulk.NullN())

	bulkArray := bulk.NewArray()
	defer bulkArray.Release()
	scalarArray := scalar.NewArray()
	defer scalarArray.Release()

	require.NoError(t, bulkArray.(interface{ ValidateFull() error }).ValidateFull())
	require.NoError(t, scalarArray.(interface{ ValidateFull() error }).ValidateFull())
	assert.True(t, array.Equal(bulkArray, scalarArray))
}

func BenchmarkBinaryBuilderBulkAppend(b *testing.B) {
	for _, tc := range binaryBuilderFactories {
		b.Run(tc.name, func(b *testing.B) {
			for _, rows := range []int{1, 16, 1024, 65536} {
				b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
					b.Run("nulls", func(b *testing.B) {
						benchmarkBinaryBuilderBulkAppend(b, tc.new, rows, false)
					})
					b.Run("empty", func(b *testing.B) {
						benchmarkBinaryBuilderBulkAppend(b, tc.new, rows, true)
					})
				})
			}
		})
	}
}

func benchmarkBinaryBuilderBulkAppend(b *testing.B, factory binaryBuilderFactory, rows int, empty bool) {
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
