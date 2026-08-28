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

func TestBinaryBuilderBulkAppendValuesPreservesNullPayload(t *testing.T) {
	values := []string{"", "one", "世界", "", "five", "six"}
	valid := []bool{true, false, true, true, false, true}
	suffix := []string{"tail", ""}

	for _, factory := range binaryBuilderFactories {
		t.Run(factory.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			builder := factory.new(mem)
			defer builder.Release()
			appendBinaryBuilderValue(builder, "prefix")
			builder.AppendNull()
			builder.AppendEmptyValue()
			appendBinaryBuilderValues(builder, values, valid)
			appendBinaryBuilderValues(builder, suffix, nil)

			arr := builder.NewArray().(array.BinaryLike)
			defer arr.Release()
			require.NoError(t, arr.(interface{ ValidateFull() error }).ValidateFull())

			expectedValues := append([]string{"prefix", "", ""}, values...)
			expectedValues = append(expectedValues, suffix...)
			expectedValid := append([]bool{true, false, true}, valid...)
			expectedValid = append(expectedValid, true, true)
			expectedData := make([]byte, 0)
			for _, value := range expectedValues {
				expectedData = append(expectedData, []byte(value)...)
			}

			assert.Equal(t, len(expectedValues), arr.Len())
			assert.Equal(t, 3, arr.NullN())
			assert.Equal(t, expectedData, arr.ValueBytes())

			dataOffset := int64(0)
			for i, value := range expectedValues {
				assert.Equal(t, expectedValid[i], arr.IsValid(i), "value %d", i)
				assert.Equal(t, dataOffset, arr.ValueOffset64(i), "value %d offset", i)
				assert.Equal(t, len(value), arr.ValueLen(i), "value %d length", i)
				dataOffset += int64(len(value))
			}
		})
	}
}

func appendBinaryBuilderValues(builder array.Builder, values []string, valid []bool) {
	switch builder := builder.(type) {
	case *array.BinaryBuilder:
		binaryValues := make([][]byte, len(values))
		for i, value := range values {
			binaryValues[i] = []byte(value)
		}
		builder.AppendValues(binaryValues, valid)
	case *array.StringBuilder:
		builder.AppendValues(values, valid)
	case *array.LargeStringBuilder:
		builder.AppendValues(values, valid)
	default:
		panic(fmt.Sprintf("unexpected binary builder %T", builder))
	}
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

func BenchmarkBinaryBuilderScalarAppend(b *testing.B) {
	const rows = 64 * 1024

	b.Run("append", func(b *testing.B) {
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer builder.Release()
		value := []byte("data")
		builder.Resize(rows)
		builder.ReserveData(rows * len(value))
		builder.Resize(0)

		b.SetBytes(int64(rows * len(value)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			builder.Resize(rows)
			b.StartTimer()
			for range rows {
				builder.Append(value)
			}
			b.StopTimer()
			builder.Resize(0)
			b.StartTimer()
		}
	})

	b.Run("append_null", func(b *testing.B) {
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer builder.Release()
		builder.Resize(rows)
		builder.Resize(0)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			builder.Resize(rows)
			b.StartTimer()
			for range rows {
				builder.AppendNull()
			}
			b.StopTimer()
			builder.Resize(0)
			b.StartTimer()
		}
	})

	b.Run("append_empty_value", func(b *testing.B) {
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer builder.Release()
		builder.Resize(rows)
		builder.Resize(0)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			builder.Resize(rows)
			b.StartTimer()
			for range rows {
				builder.AppendEmptyValue()
			}
			b.StopTimer()
			builder.Resize(0)
			b.StartTimer()
		}
	})
}

type binaryBuilderValuesFactory struct {
	name         string
	new          binaryBuilderFactory
	values       func(rows, width int) any
	appendValues func(array.Builder, any, []bool)
}

var binaryBuilderValuesFactories = []binaryBuilderValuesFactory{
	{
		name: "binary",
		new: func(mem memory.Allocator) array.Builder {
			return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		},
		values: func(rows, width int) any {
			value := make([]byte, width)
			for i := range value {
				value[i] = byte('a' + i%26)
			}
			values := make([][]byte, rows)
			for i := range values {
				values[i] = value
			}
			return values
		},
		appendValues: func(builder array.Builder, values any, valid []bool) {
			builder.(*array.BinaryBuilder).AppendValues(values.([][]byte), valid)
		},
	},
	{
		name: "large_binary",
		new: func(mem memory.Allocator) array.Builder {
			return array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
		},
		values: func(rows, width int) any {
			value := make([]byte, width)
			for i := range value {
				value[i] = byte('a' + i%26)
			}
			values := make([][]byte, rows)
			for i := range values {
				values[i] = value
			}
			return values
		},
		appendValues: func(builder array.Builder, values any, valid []bool) {
			builder.(*array.BinaryBuilder).AppendValues(values.([][]byte), valid)
		},
	},
	{
		name: "string",
		new: func(mem memory.Allocator) array.Builder {
			return array.NewStringBuilder(mem)
		},
		values: func(rows, width int) any {
			value := make([]byte, width)
			for i := range value {
				value[i] = byte('a' + i%26)
			}
			values := make([]string, rows)
			for i := range values {
				values[i] = string(value)
			}
			return values
		},
		appendValues: func(builder array.Builder, values any, valid []bool) {
			builder.(*array.StringBuilder).AppendValues(values.([]string), valid)
		},
	},
	{
		name: "large_string",
		new: func(mem memory.Allocator) array.Builder {
			return array.NewLargeStringBuilder(mem)
		},
		values: func(rows, width int) any {
			value := make([]byte, width)
			for i := range value {
				value[i] = byte('a' + i%26)
			}
			values := make([]string, rows)
			for i := range values {
				values[i] = string(value)
			}
			return values
		},
		appendValues: func(builder array.Builder, values any, valid []bool) {
			builder.(*array.LargeStringBuilder).AppendValues(values.([]string), valid)
		},
	},
}

func BenchmarkBinaryBuilderAppendValues(b *testing.B) {
	validityPatterns := []struct {
		name  string
		valid func(rows int) []bool
	}{
		{name: "all_valid", valid: func(int) []bool { return nil }},
		{
			name: "10pct_null",
			valid: func(rows int) []bool {
				valid := make([]bool, rows)
				for i := range valid {
					valid[i] = i%10 != 0
				}
				return valid
			},
		},
		{
			name: "50pct_null",
			valid: func(rows int) []bool {
				valid := make([]bool, rows)
				for i := range valid {
					valid[i] = i%2 != 0
				}
				return valid
			},
		},
	}

	for _, factory := range binaryBuilderValuesFactories {
		for _, rows := range []int{1024, 65536} {
			for _, width := range []int{4, 16, 64, 256} {
				values := factory.values(rows, width)
				for _, pattern := range validityPatterns {
					valid := pattern.valid(rows)
					name := fmt.Sprintf("%s/rows_%d/width_%d/%s", factory.name, rows, width, pattern.name)
					b.Run(name, func(b *testing.B) {
						builder := factory.new(memory.DefaultAllocator)
						defer builder.Release()
						b.SetBytes(int64(rows * width))
						b.ReportAllocs()

						for b.Loop() {
							factory.appendValues(builder, values, valid)
							arr := builder.NewArray()
							arr.Release()
						}
					})
				}
			}
		}
	}
}
