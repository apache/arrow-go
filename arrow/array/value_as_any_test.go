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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueAsAnyPrimitives(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	t.Run("int8 native vs marshal", func(t *testing.T) {
		b := array.NewInt8Builder(mem)
		defer b.Release()
		b.AppendValues([]int8{1, -2}, []bool{true, true})
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, int8(1), array.ValueAsAny(arr, 0))
		assert.Equal(t, int8(-2), array.ValueAsAny(arr, 1))
		assert.Nil(t, array.ValueAsAny(arr, 2))

		// GetOneForMarshal widens int8 to float64 for JSON safety.
		assert.Equal(t, float64(1), arr.GetOneForMarshal(0))
		assert.NotEqual(t, arr.GetOneForMarshal(0), array.ValueAsAny(arr, 0))
	})

	t.Run("uint8 native vs marshal", func(t *testing.T) {
		b := array.NewUint8Builder(mem)
		defer b.Release()
		b.Append(255)
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, uint8(255), array.ValueAsAny(arr, 0))
		assert.Equal(t, float64(255), arr.GetOneForMarshal(0))
	})

	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.AppendValues([]int64{42, 0}, []bool{true, false})
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, int64(42), array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
	})

	t.Run("boolean", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.AppendValues([]bool{true, false}, []bool{true, false})
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, true, array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
	})

	t.Run("float64 keeps NaN", func(t *testing.T) {
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		b.Append(math.NaN())
		b.Append(math.Inf(1))
		arr := b.NewArray()
		defer arr.Release()

		got := array.ValueAsAny(arr, 0).(float64)
		assert.True(t, math.IsNaN(got))
		assert.Equal(t, math.Inf(1), array.ValueAsAny(arr, 1))
		assert.Equal(t, "NaN", arr.GetOneForMarshal(0))
		assert.Equal(t, "+Inf", arr.GetOneForMarshal(1))
	})

	t.Run("string and binary", func(t *testing.T) {
		sb := array.NewStringBuilder(mem)
		defer sb.Release()
		sb.Append("hello")
		sb.AppendNull()
		sarr := sb.NewArray()
		defer sarr.Release()
		assert.Equal(t, "hello", array.ValueAsAny(sarr, 0))
		assert.Nil(t, array.ValueAsAny(sarr, 1))

		bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bb.Release()
		bb.Append([]byte{0x01, 0x02})
		barr := bb.NewArray()
		defer barr.Release()
		assert.Equal(t, []byte{0x01, 0x02}, array.ValueAsAny(barr, 0))
	})

	t.Run("float16", func(t *testing.T) {
		b := array.NewFloat16Builder(mem)
		defer b.Release()
		b.Append(float16.New(1.5))
		arr := b.NewArray()
		defer arr.Release()

		got, ok := array.ValueAsAny(arr, 0).(float16.Num)
		require.True(t, ok)
		assert.Equal(t, float32(1.5), got.Float32())
		assert.Equal(t, float32(1.5), arr.GetOneForMarshal(0))
	})
}

func TestValueAsAnyTemporalAndDecimal(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	t.Run("timestamp", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}
		b := array.NewTimestampBuilder(mem, dt)
		defer b.Release()
		b.Append(arrow.Timestamp(1_700_000_000))
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, arrow.Timestamp(1_700_000_000), array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
		_, isString := arr.GetOneForMarshal(0).(string)
		assert.True(t, isString)
	})

	t.Run("date32", func(t *testing.T) {
		b := array.NewDate32Builder(mem)
		defer b.Release()
		b.Append(arrow.Date32(10))
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, arrow.Date32(10), array.ValueAsAny(arr, 0))
		_, isString := arr.GetOneForMarshal(0).(string)
		assert.True(t, isString)
	})

	t.Run("duration", func(t *testing.T) {
		b := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Millisecond})
		defer b.Release()
		b.Append(arrow.Duration(250))
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, arrow.Duration(250), array.ValueAsAny(arr, 0))
		assert.Equal(t, "250ms", arr.GetOneForMarshal(0))
	})

	t.Run("decimal128", func(t *testing.T) {
		dtype := &arrow.Decimal128Type{Precision: 10, Scale: 2}
		b := array.NewDecimal128Builder(mem, dtype)
		defer b.Release()
		n, err := decimal128.FromString("12.34", dtype.Precision, dtype.Scale)
		require.NoError(t, err)
		b.Append(n)
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, n, array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
		_, isString := arr.GetOneForMarshal(0).(string)
		assert.True(t, isString)
	})
}

func TestValueAsAnyNested(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	t.Run("list", func(t *testing.T) {
		b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int8)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int8Builder)

		b.Append(true)
		vb.AppendValues([]int8{1, 2}, nil)
		b.AppendNull()
		b.Append(true)
		vb.Append(3)

		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, []any{int8(1), int8(2)}, array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
		assert.Equal(t, []any{int8(3)}, array.ValueAsAny(arr, 2))
	})

	t.Run("struct", func(t *testing.T) {
		fields := []arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		b := array.NewStructBuilder(mem, arrow.StructOf(fields...))
		defer b.Release()
		nb := b.FieldBuilder(0).(*array.Int8Builder)
		sb := b.FieldBuilder(1).(*array.StringBuilder)

		b.Append(true)
		nb.Append(7)
		sb.Append("x")
		b.AppendNull()
		nb.AppendNull()
		sb.AppendNull()

		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, []any{[]any{"n", int8(7)}, []any{"s", "x"}}, array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 1))
	})

	t.Run("struct duplicate field names", func(t *testing.T) {
		fields := []arrow.Field{
			{Name: "dup", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "dup", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		b := array.NewStructBuilder(mem, arrow.StructOf(fields...))
		defer b.Release()
		i32 := b.FieldBuilder(0).(*array.Int32Builder)
		i64 := b.FieldBuilder(1).(*array.Int64Builder)

		b.Append(true)
		i32.Append(11)
		i64.Append(22)

		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, []any{[]any{"dup", int32(11)}, []any{"dup", int64(22)}}, array.ValueAsAny(arr, 0))
	})

	t.Run("null array", func(t *testing.T) {
		arr := array.NewNull(3)
		defer arr.Release()
		assert.Nil(t, array.ValueAsAny(arr, 0))
		assert.Nil(t, array.ValueAsAny(arr, 2))
	})

	t.Run("dictionary", func(t *testing.T) {
		b := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8,
			ValueType: arrow.BinaryTypes.String,
		})
		defer b.Release()
		db := b.(*array.BinaryDictionaryBuilder)
		require.NoError(t, db.Append([]byte("a")))
		require.NoError(t, db.Append([]byte("b")))
		db.AppendNull()
		require.NoError(t, db.Append([]byte("a")))
		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, "a", array.ValueAsAny(arr, 0))
		assert.Equal(t, "b", array.ValueAsAny(arr, 1))
		assert.Nil(t, array.ValueAsAny(arr, 2))
		assert.Equal(t, "a", array.ValueAsAny(arr, 3))
	})
}

func TestValueAsAnyUnionKeepsTypeIDWhenChildNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	codes := []arrow.UnionTypeCode{0, 1}

	t.Run("sparse", func(t *testing.T) {
		b := array.NewSparseUnionBuilder(mem, arrow.SparseUnionOf(fields, codes))
		defer b.Release()
		i8b := b.Child(0).(*array.Int8Builder)
		strb := b.Child(1).(*array.StringBuilder)

		b.Append(0)
		i8b.Append(5)
		strb.AppendEmptyValue()

		b.AppendNull()

		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, []any{arrow.UnionTypeCode(0), int8(5)}, array.ValueAsAny(arr, 0))
		assert.Equal(t, []any{arrow.UnionTypeCode(0), nil}, array.ValueAsAny(arr, 1))
	})

	t.Run("dense", func(t *testing.T) {
		b := array.NewDenseUnionBuilder(mem, arrow.DenseUnionOf(fields, codes))
		defer b.Release()
		i8b := b.Child(0).(*array.Int8Builder)

		b.Append(0)
		i8b.Append(5)

		b.AppendNull()

		arr := b.NewArray()
		defer arr.Release()

		assert.Equal(t, []any{arrow.UnionTypeCode(0), int8(5)}, array.ValueAsAny(arr, 0))
		assert.Equal(t, []any{arrow.UnionTypeCode(0), nil}, array.ValueAsAny(arr, 1))
	})
}
