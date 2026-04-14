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

package arreflect

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setValueAt[T any](t *testing.T, arr arrow.Array, i int) T {
	t.Helper()
	var got T
	require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, i))
	return got
}

func TestSetValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("bool", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(true)
		b.AppendNull()
		arr := b.NewBooleanArray()
		defer arr.Release()

		got := setValueAt[bool](t, arr, 0)
		assert.True(t, got, "expected true, got false")

		got = true
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.False(t, got, "expected false (null → zero), got true")
	})

	t.Run("string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append("hello")
		arr := b.NewStringArray()
		defer arr.Release()

		got := setValueAt[string](t, arr, 0)
		assert.Equal(t, "hello", got)
	})

	t.Run("binary", func(t *testing.T) {
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		b.Append([]byte("data"))
		arr := b.NewBinaryArray()
		defer arr.Release()

		got := setValueAt[[]byte](t, arr, 0)
		assert.Equal(t, "data", string(got))
	})

	t.Run("unsupported type error", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(true)
		arr := b.NewBooleanArray()
		defer arr.Release()

		var got int32
		err := setValue(reflect.ValueOf(&got).Elem(), arr, 0)
		assert.Error(t, err, "expected error for bool→int32 mismatch")
	})

	t.Run("pointer allocation", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append("ptr")
		b.AppendNull()
		arr := b.NewStringArray()
		defer arr.Release()

		got := setValueAt[*string](t, arr, 0)
		if assert.NotNil(t, got) {
			assert.Equal(t, "ptr", *got)
		}

		got = new(string)
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Nil(t, got, "expected nil for null, got %v", got)
	})
}

func TestSetPrimitiveValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("int32", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.Append(42)
		b.AppendNull()
		arr := b.NewInt32Array()
		defer arr.Release()

		got := setValueAt[int32](t, arr, 0)
		assert.Equal(t, int32(42), got)

		got = 99
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, int32(0), got, "expected 0 for null, got %d", got)
	})

	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.Append(int64(1 << 40))
		arr := b.NewInt64Array()
		defer arr.Release()

		var got int64
		require.NoError(t, setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0))
		assert.Equal(t, int64(1<<40), got)
	})

	t.Run("uint8", func(t *testing.T) {
		b := array.NewUint8Builder(mem)
		defer b.Release()
		b.Append(255)
		arr := b.NewUint8Array()
		defer arr.Release()

		var got uint8
		require.NoError(t, setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0))
		assert.Equal(t, uint8(255), got)
	})

	t.Run("float64", func(t *testing.T) {
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		b.Append(3.14)
		arr := b.NewFloat64Array()
		defer arr.Release()

		var got float64
		require.NoError(t, setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0))
		assert.Equal(t, 3.14, got)
	})

	t.Run("type mismatch returns error", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.Append(10)
		arr := b.NewInt32Array()
		defer arr.Release()

		var got float64
		err := setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0)
		assert.Error(t, err, "expected error for int32→float64 mismatch")
	})
}

func TestSetTemporalValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("timestamp", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Second}
		b := array.NewTimestampBuilder(mem, dt)
		defer b.Release()
		now := time.Unix(1700000000, 0).UTC()
		b.Append(arrow.Timestamp(now.Unix()))
		arr := b.NewArray().(*array.Timestamp)
		defer arr.Release()

		got := setValueAt[time.Time](t, arr, 0)
		assert.True(t, got.Equal(now), "expected %v, got %v", now, got)
	})

	t.Run("date32", func(t *testing.T) {
		b := array.NewDate32Builder(mem)
		defer b.Release()
		b.Append(arrow.Date32(19000))
		arr := b.NewArray().(*array.Date32)
		defer arr.Release()

		got := setValueAt[time.Time](t, arr, 0)
		expected := arrow.Date32(19000).ToTime()
		assert.True(t, got.Equal(expected), "expected %v, got %v", expected, got)
	})

	t.Run("duration", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(mem, dt)
		defer b.Release()
		b.Append(arrow.Duration(5))
		arr := b.NewArray().(*array.Duration)
		defer arr.Release()

		got := setValueAt[time.Duration](t, arr, 0)
		expected := 5 * time.Second
		assert.Equal(t, expected, got)
	})

	t.Run("null temporal", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Second}
		b := array.NewTimestampBuilder(mem, dt)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray().(*array.Timestamp)
		defer arr.Release()

		got := setValueAt[*time.Time](t, arr, 0)
		assert.Nil(t, got, "expected nil for null timestamp pointer")
	})

	t.Run("time32", func(t *testing.T) {
		dt := &arrow.Time32Type{Unit: arrow.Millisecond}
		b := array.NewTime32Builder(mem, dt)
		defer b.Release()
		// 10h30m0s500ms = (10*3600 + 30*60)*1000 + 500 = 37800500 ms
		b.Append(arrow.Time32(37800500))
		arr := b.NewArray()
		defer arr.Release()

		var got time.Time
		v := reflect.ValueOf(&got).Elem()
		require.NoError(t, setValue(v, arr, 0))
		assert.True(t, got.Hour() == 10 && got.Minute() == 30 && got.Second() == 0 && got.Nanosecond()/1_000_000 == 500,
			"time32: got %v, want 10:30:00.500", got)
	})

	t.Run("time64", func(t *testing.T) {
		dt := &arrow.Time64Type{Unit: arrow.Nanosecond}
		b := array.NewTime64Builder(mem, dt)
		defer b.Release()
		// 10h30m0s123456789ns
		nanos := int64(10*3600+30*60)*1_000_000_000 + 123456789
		b.Append(arrow.Time64(nanos))
		arr := b.NewArray()
		defer arr.Release()

		var got time.Time
		v := reflect.ValueOf(&got).Elem()
		require.NoError(t, setValue(v, arr, 0))
		assert.True(t, got.Hour() == 10 && got.Minute() == 30 && got.Second() == 0 && got.Nanosecond() == 123456789,
			"time64: got %v, want 10:30:00.123456789", got)
	})
}

func TestSetDecimalValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("decimal128", func(t *testing.T) {
		dt := &arrow.Decimal128Type{Precision: 10, Scale: 2}
		b := array.NewDecimal128Builder(mem, dt)
		defer b.Release()
		num := decimal128.New(0, 12345)
		b.Append(num)
		b.AppendNull()
		arr := b.NewDecimal128Array()
		defer arr.Release()

		got := setValueAt[decimal128.Num](t, arr, 0)
		assert.Equal(t, num, got)

		gotPtr := setValueAt[*decimal128.Num](t, arr, 1)
		assert.Nil(t, gotPtr, "expected nil for null decimal128")
	})

	t.Run("decimal256", func(t *testing.T) {
		dt := &arrow.Decimal256Type{Precision: 20, Scale: 4}
		b := array.NewDecimal256Builder(mem, dt)
		defer b.Release()
		num := decimal256.New(0, 0, 0, 9876)
		b.Append(num)
		arr := b.NewDecimal256Array()
		defer arr.Release()

		got := setValueAt[decimal256.Num](t, arr, 0)
		assert.Equal(t, num, got)
	})

	t.Run("decimal32", func(t *testing.T) {
		dt := &arrow.Decimal32Type{Precision: 9, Scale: 2}
		b := array.NewDecimal32Builder(mem, dt)
		defer b.Release()
		num := decimal.Decimal32(12345)
		b.Append(num)
		b.AppendNull()
		arr := b.NewArray().(*array.Decimal32)
		defer arr.Release()

		got := setValueAt[decimal.Decimal32](t, arr, 0)
		assert.Equal(t, num, got)

		gotPtr := setValueAt[*decimal.Decimal32](t, arr, 1)
		assert.Nil(t, gotPtr, "expected nil for null decimal32")
	})

	t.Run("decimal64", func(t *testing.T) {
		dt := &arrow.Decimal64Type{Precision: 18, Scale: 3}
		b := array.NewDecimal64Builder(mem, dt)
		defer b.Release()
		num := decimal.Decimal64(987654321)
		b.Append(num)
		arr := b.NewArray().(*array.Decimal64)
		defer arr.Release()

		got := setValueAt[decimal.Decimal64](t, arr, 0)
		assert.Equal(t, num, got)
	})
}

func TestSetStructValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic struct", func(t *testing.T) {
		nameArr := func() *array.String {
			b := array.NewStringBuilder(mem)
			defer b.Release()
			b.Append("Alice")
			b.Append("Bob")
			return b.NewStringArray()
		}()
		defer nameArr.Release()

		ageArr := func() *array.Int32 {
			b := array.NewInt32Builder(mem)
			defer b.Release()
			b.Append(30)
			b.Append(25)
			return b.NewInt32Array()
		}()
		defer ageArr.Release()

		sa, err := array.NewStructArray(
			[]arrow.Array{nameArr, ageArr},
			[]string{"Name", "Age"},
		)
		require.NoError(t, err)
		defer sa.Release()

		type Person struct {
			Name string
			Age  int32
		}

		var got Person
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), sa, 0))
		assert.Equal(t, "Alice", got.Name)
		assert.Equal(t, int32(30), got.Age)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), sa, 1))
		assert.Equal(t, "Bob", got.Name)
		assert.Equal(t, int32(25), got.Age)
	})

	t.Run("arrow tag mapping", func(t *testing.T) {
		nameArr := func() *array.String {
			b := array.NewStringBuilder(mem)
			defer b.Release()
			b.Append("Charlie")
			return b.NewStringArray()
		}()
		defer nameArr.Release()

		sa, err := array.NewStructArray(
			[]arrow.Array{nameArr},
			[]string{"full_name"},
		)
		require.NoError(t, err)
		defer sa.Release()

		type TaggedPerson struct {
			FullName string `arrow:"full_name"`
		}

		var got TaggedPerson
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), sa, 0))
		assert.Equal(t, "Charlie", got.FullName)
	})

	t.Run("missing arrow field leaves go field zero", func(t *testing.T) {
		nameArr := func() *array.String {
			b := array.NewStringBuilder(mem)
			defer b.Release()
			b.Append("Dave")
			return b.NewStringArray()
		}()
		defer nameArr.Release()

		sa, err := array.NewStructArray(
			[]arrow.Array{nameArr},
			[]string{"Name"},
		)
		require.NoError(t, err)
		defer sa.Release()

		type PersonWithExtra struct {
			Name  string
			Email string
		}

		var got PersonWithExtra
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), sa, 0))
		assert.Equal(t, "Dave", got.Name)
		assert.Equal(t, "", got.Email)
	})
}

func TestSetListValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("list of int32", func(t *testing.T) {
		vb := array.NewInt32Builder(mem)
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()

		vb = lb.ValueBuilder().(*array.Int32Builder)
		lb.Append(true)
		vb.AppendValues([]int32{1, 2, 3}, nil)
		lb.Append(true)
		vb.AppendValues([]int32{4, 5}, nil)
		lb.AppendNull()

		arr := lb.NewListArray()
		defer arr.Release()

		got := setValueAt[[]int32](t, arr, 0)
		assert.Equal(t, []int32{1, 2, 3}, got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, []int32{4, 5}, got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 2))
		assert.Nil(t, got, "expected nil slice for null list, got %v", got)
	})

	t.Run("nested list of lists", func(t *testing.T) {
		inner := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer inner.Release()
		outer := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
		defer outer.Release()

		innerVB := inner.ValueBuilder().(*array.Int32Builder)

		inner.Append(true)
		innerVB.AppendValues([]int32{1, 2}, nil)
		inner.Append(true)
		innerVB.AppendValues([]int32{3}, nil)
		innerArr := inner.NewListArray()
		defer innerArr.Release()

		outerVB := outer.ValueBuilder().(*array.ListBuilder)
		outerInnerVB := outerVB.ValueBuilder().(*array.Int32Builder)
		outer.Append(true)
		outerVB.Append(true)
		outerInnerVB.AppendValues([]int32{10, 20}, nil)
		outerVB.Append(true)
		outerInnerVB.AppendValues([]int32{30}, nil)

		outerArr := outer.NewListArray()
		defer outerArr.Release()

		var got [][]int32
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), outerArr, 0))
		require.Len(t, got, 2, "expected 2 inner slices, got %d", len(got))
		assert.Equal(t, []int32{10, 20}, got[0])
		assert.Equal(t, []int32{30}, got[1])
	})

	t.Run("large list view of int32", func(t *testing.T) {
		lvb := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lvb.Release()
		vb := lvb.ValueBuilder().(*array.Int32Builder)

		lvb.AppendWithSize(true, 2)
		vb.AppendValues([]int32{1, 2}, nil)
		lvb.AppendWithSize(true, 1)
		vb.AppendValues([]int32{3}, nil)

		arr := lvb.NewLargeListViewArray()
		defer arr.Release()

		got := setValueAt[[]int32](t, arr, 0)
		assert.Equal(t, []int32{1, 2}, got, "row 0: expected [1,2], got %v", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, []int32{3}, got, "row 1: expected [3], got %v", got)
	})
}

func TestSetMapValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("map string to int32", func(t *testing.T) {
		mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
		defer mb.Release()

		kb := mb.KeyBuilder().(*array.StringBuilder)
		ib := mb.ItemBuilder().(*array.Int32Builder)

		mb.Append(true)
		kb.Append("a")
		ib.Append(1)
		kb.Append("b")
		ib.Append(2)

		mb.Append(true)
		kb.Append("x")
		ib.Append(10)

		mb.AppendNull()

		arr := mb.NewMapArray()
		defer arr.Release()

		got := setValueAt[map[string]int32](t, arr, 0)
		assert.Equal(t, int32(1), got["a"])
		assert.Equal(t, int32(2), got["b"])

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, int32(10), got["x"])

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 2))
		assert.Nil(t, got, "expected nil map for null, got %v", got)
	})
}

func TestSetFixedSizeListValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("go array", func(t *testing.T) {
		b := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Int32)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int32Builder)

		b.Append(true)
		vb.AppendValues([]int32{10, 20, 30}, nil)
		b.Append(true)
		vb.AppendValues([]int32{40, 50, 60}, nil)
		b.AppendNull()

		arr := b.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		got := setValueAt[[3]int32](t, arr, 0)
		assert.Equal(t, [3]int32{10, 20, 30}, got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, [3]int32{40, 50, 60}, got)

		got = [3]int32{1, 2, 3}
		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 2))
		assert.Equal(t, [3]int32{}, got, "expected zero array for null, got %v", got)
	})

	t.Run("go slice", func(t *testing.T) {
		b := array.NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Int32)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int32Builder)

		b.Append(true)
		vb.AppendValues([]int32{7, 8}, nil)

		arr := b.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		got := setValueAt[[]int32](t, arr, 0)
		assert.Equal(t, []int32{7, 8}, got)
	})

	t.Run("size mismatch returns error", func(t *testing.T) {
		b := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Int32)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int32Builder)
		b.Append(true)
		vb.AppendValues([]int32{1, 2, 3}, nil)

		arr := b.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		var got [2]int32
		err := setValue(reflect.ValueOf(&got).Elem(), arr, 0)
		assert.Error(t, err, "expected error for size mismatch")
	})
}

func TestSetDictionaryValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("dictionary int8 to string", func(t *testing.T) {
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
		bldr := array.NewDictionaryBuilder(mem, dt)
		defer bldr.Release()
		db := bldr.(*array.BinaryDictionaryBuilder)

		db.AppendString("foo")
		db.AppendString("bar")
		db.AppendString("foo")
		db.AppendNull()

		arr := bldr.NewDictionaryArray()
		defer arr.Release()

		got := setValueAt[string](t, arr, 0)
		assert.Equal(t, "foo", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 1))
		assert.Equal(t, "bar", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 2))
		assert.Equal(t, "foo", got)

		gotPtr := setValueAt[*string](t, arr, 3)
		assert.Nil(t, gotPtr, "expected nil for null dictionary entry")
	})
}

func TestSetRunEndEncodedValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("ree int32 to string", func(t *testing.T) {
		b := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
		defer b.Release()
		vb := b.ValueBuilder().(*array.StringBuilder)

		b.Append(3)
		vb.Append("aaa")
		b.Append(2)
		vb.Append("bbb")

		arr := b.NewRunEndEncodedArray()
		defer arr.Release()

		got := setValueAt[string](t, arr, 0)
		assert.Equal(t, "aaa", got, "expected aaa at logical 0, got %q", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 2))
		assert.Equal(t, "aaa", got, "expected aaa at logical 2, got %q", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 3))
		assert.Equal(t, "bbb", got, "expected bbb at logical 3, got %q", got)

		require.NoError(t, setValue(reflect.ValueOf(&got).Elem(), arr, 4))
		assert.Equal(t, "bbb", got, "expected bbb at logical 4, got %q", got)
	})
}
