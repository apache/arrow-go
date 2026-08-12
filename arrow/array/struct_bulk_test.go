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

func TestStructBuilderBulkAppendNullsAndEmptyValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	nestedType := arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32})
	dtype := arrow.StructOf(
		arrow.Field{Name: "integer", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "string", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "nested", Type: nestedType},
	)
	builder := array.NewStructBuilder(mem, dtype)
	defer builder.Release()

	appendStructBuilderValue(builder, 10, "a", true, 100)
	builder.AppendNulls(5)
	builder.AppendEmptyValues(4)
	appendStructBuilderValue(builder, 20, "b", false, 200)

	arr := builder.NewStructArray()
	defer arr.Release()
	require.NoError(t, arr.ValidateFull())
	require.Equal(t, 11, arr.Len())
	require.Equal(t, 5, arr.NullN())

	for i := range arr.Len() {
		wantValid := i == 0 || i >= 6
		assert.Equal(t, wantValid, arr.IsValid(i), "parent value %d", i)
		for field := 0; field < arr.NumField(); field++ {
			assert.Equal(t, wantValid, arr.Field(field).IsValid(i), "field %d value %d", field, i)
		}
	}

	assert.Equal(t, []int64{10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20}, arr.Field(0).(*array.Int64).Int64Values())
	assert.Equal(t, "a", arr.Field(1).(*array.String).Value(0))
	assert.Equal(t, "b", arr.Field(1).(*array.String).Value(10))
	for i := 1; i < 10; i++ {
		assert.Empty(t, arr.Field(1).(*array.String).Value(i))
	}

	nested := arr.Field(3).(*array.Struct)
	assert.Equal(t, 5, nested.NullN())
	nestedValues := nested.Field(0).(*array.Int32)
	assert.Equal(t, []int32{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 200}, nestedValues.Int32Values())
	for i := range nestedValues.Len() {
		assert.Equal(t, i == 0 || i >= 6, nestedValues.IsValid(i), "nested value %d", i)
	}
}

func appendStructBuilderValue(builder *array.StructBuilder, integer int64, str string, boolean bool, nestedValue int32) {
	builder.Append(true)
	builder.FieldBuilder(0).(*array.Int64Builder).Append(integer)
	builder.FieldBuilder(1).(*array.StringBuilder).Append(str)
	builder.FieldBuilder(2).(*array.BooleanBuilder).Append(boolean)
	nested := builder.FieldBuilder(3).(*array.StructBuilder)
	nested.Append(true)
	nested.FieldBuilder(0).(*array.Int32Builder).Append(nestedValue)
}

func BenchmarkStructBuilderBulkAppend(b *testing.B) {
	for _, fields := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("fields_%d", fields), func(b *testing.B) {
			for _, rows := range []int{1, 1024, 65536} {
				b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
					b.Run("nulls", func(b *testing.B) {
						benchmarkStructBuilderBulkAppend(b, fields, rows, false)
					})
					b.Run("empty", func(b *testing.B) {
						benchmarkStructBuilderBulkAppend(b, fields, rows, true)
					})
				})
			}
		})
	}
}

func benchmarkStructBuilderBulkAppend(b *testing.B, fields, rows int, empty bool) {
	structFields := make([]arrow.Field, fields)
	for i := range structFields {
		structFields[i] = arrow.Field{Name: fmt.Sprintf("field_%d", i), Type: arrow.PrimitiveTypes.Int64}
	}
	builder := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(structFields...))
	defer builder.Release()
	b.ReportAllocs()

	for b.Loop() {
		if empty {
			builder.AppendEmptyValues(rows)
		} else {
			builder.AppendNulls(rows)
		}

		arr := builder.NewStructArray()
		arr.Release()
	}
}
