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
)

func TestSetValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("bool", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(true)
		b.AppendNull()
		arr := b.NewBooleanArray()
		defer arr.Release()

		var got bool
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if !got {
			t.Errorf("expected true, got false")
		}

		got = true
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got {
			t.Errorf("expected false (null → zero), got true")
		}
	})

	t.Run("string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append("hello")
		arr := b.NewStringArray()
		defer arr.Release()

		var got string
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != "hello" {
			t.Errorf("expected hello, got %q", got)
		}
	})

	t.Run("binary", func(t *testing.T) {
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		b.Append([]byte("data"))
		arr := b.NewBinaryArray()
		defer arr.Release()

		var got []byte
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if string(got) != "data" {
			t.Errorf("expected data, got %q", got)
		}
	})

	t.Run("unsupported type error", func(t *testing.T) {
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(true)
		arr := b.NewBooleanArray()
		defer arr.Release()

		var got int32
		err := setValue(reflect.ValueOf(&got).Elem(), arr, 0)
		if err == nil {
			t.Error("expected error for bool→int32 mismatch")
		}
	})

	t.Run("pointer allocation", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append("ptr")
		b.AppendNull()
		arr := b.NewStringArray()
		defer arr.Release()

		var got *string
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got == nil || *got != "ptr" {
			t.Errorf("expected ptr, got %v", got)
		}

		got = new(string)
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil for null, got %v", got)
		}
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

		var got int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != 42 {
			t.Errorf("expected 42, got %d", got)
		}

		got = 99
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("expected 0 for null, got %d", got)
		}
	})

	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.Append(int64(1 << 40))
		arr := b.NewInt64Array()
		defer arr.Release()

		var got int64
		if err := setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != int64(1<<40) {
			t.Errorf("expected large int64, got %d", got)
		}
	})

	t.Run("uint8", func(t *testing.T) {
		b := array.NewUint8Builder(mem)
		defer b.Release()
		b.Append(255)
		arr := b.NewUint8Array()
		defer arr.Release()

		var got uint8
		if err := setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != 255 {
			t.Errorf("expected 255, got %d", got)
		}
	})

	t.Run("float64", func(t *testing.T) {
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		b.Append(3.14)
		arr := b.NewFloat64Array()
		defer arr.Release()

		var got float64
		if err := setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != 3.14 {
			t.Errorf("expected 3.14, got %f", got)
		}
	})

	t.Run("type mismatch returns error", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.Append(10)
		arr := b.NewInt32Array()
		defer arr.Release()

		var got float64
		err := setPrimitiveValue(reflect.ValueOf(&got).Elem(), arr, 0)
		if err == nil {
			t.Error("expected error for int32→float64 mismatch")
		}
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

		var got time.Time
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if !got.Equal(now) {
			t.Errorf("expected %v, got %v", now, got)
		}
	})

	t.Run("date32", func(t *testing.T) {
		b := array.NewDate32Builder(mem)
		defer b.Release()
		b.Append(arrow.Date32(19000))
		arr := b.NewArray().(*array.Date32)
		defer arr.Release()

		var got time.Time
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		expected := arrow.Date32(19000).ToTime()
		if !got.Equal(expected) {
			t.Errorf("expected %v, got %v", expected, got)
		}
	})

	t.Run("duration", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(mem, dt)
		defer b.Release()
		b.Append(arrow.Duration(5))
		arr := b.NewArray().(*array.Duration)
		defer arr.Release()

		var got time.Duration
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		expected := 5 * time.Second
		if got != expected {
			t.Errorf("expected %v, got %v", expected, got)
		}
	})

	t.Run("null temporal", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Second}
		b := array.NewTimestampBuilder(mem, dt)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray().(*array.Timestamp)
		defer arr.Release()

		var got *time.Time
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil for null timestamp pointer")
		}
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

		var got decimal128.Num
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != num {
			t.Errorf("expected %v, got %v", num, got)
		}

		var gotPtr *decimal128.Num
		if err := setValue(reflect.ValueOf(&gotPtr).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if gotPtr != nil {
			t.Errorf("expected nil for null decimal128")
		}
	})

	t.Run("decimal256", func(t *testing.T) {
		dt := &arrow.Decimal256Type{Precision: 20, Scale: 4}
		b := array.NewDecimal256Builder(mem, dt)
		defer b.Release()
		num := decimal256.New(0, 0, 0, 9876)
		b.Append(num)
		arr := b.NewDecimal256Array()
		defer arr.Release()

		var got decimal256.Num
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != num {
			t.Errorf("expected %v, got %v", num, got)
		}
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

		var got decimal.Decimal32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != num {
			t.Errorf("expected %v, got %v", num, got)
		}

		var gotPtr *decimal.Decimal32
		if err := setValue(reflect.ValueOf(&gotPtr).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if gotPtr != nil {
			t.Errorf("expected nil for null decimal32")
		}
	})

	t.Run("decimal64", func(t *testing.T) {
		dt := &arrow.Decimal64Type{Precision: 18, Scale: 3}
		b := array.NewDecimal64Builder(mem, dt)
		defer b.Release()
		num := decimal.Decimal64(987654321)
		b.Append(num)
		arr := b.NewArray().(*array.Decimal64)
		defer arr.Release()

		var got decimal.Decimal64
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != num {
			t.Errorf("expected %v, got %v", num, got)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		defer sa.Release()

		type Person struct {
			Name string
			Age  int32
		}

		var got Person
		if err := setValue(reflect.ValueOf(&got).Elem(), sa, 0); err != nil {
			t.Fatal(err)
		}
		if got.Name != "Alice" || got.Age != 30 {
			t.Errorf("expected Alice/30, got %+v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), sa, 1); err != nil {
			t.Fatal(err)
		}
		if got.Name != "Bob" || got.Age != 25 {
			t.Errorf("expected Bob/25, got %+v", got)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		defer sa.Release()

		type TaggedPerson struct {
			FullName string `arrow:"full_name"`
		}

		var got TaggedPerson
		if err := setValue(reflect.ValueOf(&got).Elem(), sa, 0); err != nil {
			t.Fatal(err)
		}
		if got.FullName != "Charlie" {
			t.Errorf("expected Charlie, got %q", got.FullName)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		defer sa.Release()

		type PersonWithExtra struct {
			Name  string
			Email string
		}

		var got PersonWithExtra
		if err := setValue(reflect.ValueOf(&got).Elem(), sa, 0); err != nil {
			t.Fatal(err)
		}
		if got.Name != "Dave" {
			t.Errorf("expected Dave, got %q", got.Name)
		}
		if got.Email != "" {
			t.Errorf("expected empty Email, got %q", got.Email)
		}
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

		var got []int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int32{1, 2, 3}) {
			t.Errorf("expected [1,2,3], got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int32{4, 5}) {
			t.Errorf("expected [4,5], got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 2); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil slice for null list, got %v", got)
		}
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
		if err := setValue(reflect.ValueOf(&got).Elem(), outerArr, 0); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 inner slices, got %d", len(got))
		}
		if !reflect.DeepEqual(got[0], []int32{10, 20}) {
			t.Errorf("expected [10,20], got %v", got[0])
		}
		if !reflect.DeepEqual(got[1], []int32{30}) {
			t.Errorf("expected [30], got %v", got[1])
		}
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

		var got []int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int32{1, 2}) {
			t.Errorf("row 0: expected [1,2], got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int32{3}) {
			t.Errorf("row 1: expected [3], got %v", got)
		}
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

		var got map[string]int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got["a"] != 1 || got["b"] != 2 {
			t.Errorf("expected {a:1, b:2}, got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got["x"] != 10 {
			t.Errorf("expected {x:10}, got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 2); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil map for null, got %v", got)
		}
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

		var got [3]int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != [3]int32{10, 20, 30} {
			t.Errorf("expected [10,20,30], got %v", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got != [3]int32{40, 50, 60} {
			t.Errorf("expected [40,50,60], got %v", got)
		}

		got = [3]int32{1, 2, 3}
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 2); err != nil {
			t.Fatal(err)
		}
		if got != ([3]int32{}) {
			t.Errorf("expected zero array for null, got %v", got)
		}
	})

	t.Run("go slice", func(t *testing.T) {
		b := array.NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Int32)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int32Builder)

		b.Append(true)
		vb.AppendValues([]int32{7, 8}, nil)

		arr := b.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		var got []int32
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, []int32{7, 8}) {
			t.Errorf("expected [7,8], got %v", got)
		}
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
		if err == nil {
			t.Error("expected error for size mismatch")
		}
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

		var got string
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != "foo" {
			t.Errorf("expected foo, got %q", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 1); err != nil {
			t.Fatal(err)
		}
		if got != "bar" {
			t.Errorf("expected bar, got %q", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 2); err != nil {
			t.Fatal(err)
		}
		if got != "foo" {
			t.Errorf("expected foo, got %q", got)
		}

		var gotPtr *string
		if err := setValue(reflect.ValueOf(&gotPtr).Elem(), arr, 3); err != nil {
			t.Fatal(err)
		}
		if gotPtr != nil {
			t.Errorf("expected nil for null dictionary entry")
		}
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

		var got string
		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 0); err != nil {
			t.Fatal(err)
		}
		if got != "aaa" {
			t.Errorf("expected aaa at logical 0, got %q", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 2); err != nil {
			t.Fatal(err)
		}
		if got != "aaa" {
			t.Errorf("expected aaa at logical 2, got %q", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 3); err != nil {
			t.Fatal(err)
		}
		if got != "bbb" {
			t.Errorf("expected bbb at logical 3, got %q", got)
		}

		if err := setValue(reflect.ValueOf(&got).Elem(), arr, 4); err != nil {
			t.Fatal(err)
		}
		if got != "bbb" {
			t.Errorf("expected bbb at logical 4, got %q", got)
		}
	})
}
