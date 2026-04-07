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

func TestBuildPrimitiveArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.Len() != 5 {
			t.Errorf("expected 5, got %d", arr.Len())
		}
		if arr.DataType().ID() != arrow.INT32 {
			t.Errorf("expected INT32, got %v", arr.DataType())
		}
		typed := arr.(*array.Int32)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] want %d, got %d", i, want, typed.Value(i))
			}
		}
	})

	t.Run("string", func(t *testing.T) {
		vals := []string{"hello", "world", "foo"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.STRING {
			t.Errorf("expected STRING, got %v", arr.DataType())
		}
		typed := arr.(*array.String)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] want %q, got %q", i, want, typed.Value(i))
			}
		}
	})

	t.Run("pointer_with_null", func(t *testing.T) {
		v1, v3 := int32(10), int32(30)
		vals := []*int32{&v1, nil, &v3}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if !arr.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
		typed := arr.(*array.Int32)
		if typed.Value(0) != 10 || typed.Value(2) != 30 {
			t.Error("unexpected values")
		}
	})

	t.Run("bool", func(t *testing.T) {
		vals := []bool{true, false, true}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.BOOL {
			t.Errorf("expected BOOL, got %v", arr.DataType())
		}
		typed := arr.(*array.Boolean)
		if !typed.Value(0) || typed.Value(1) || !typed.Value(2) {
			t.Error("unexpected bool values")
		}
	})

	t.Run("binary", func(t *testing.T) {
		vals := [][]byte{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.BINARY {
			t.Errorf("expected BINARY, got %v", arr.DataType())
		}
	})

	t.Run("numeric_types", func(t *testing.T) {
		cases := []struct {
			vals any
			id   arrow.Type
		}{
			{[]int8{1, -2, 3}, arrow.INT8},
			{[]int16{100, -200}, arrow.INT16},
			{[]int64{1000, -2000}, arrow.INT64},
			{[]uint8{1, 2, 3}, arrow.UINT8},
			{[]uint16{1, 2}, arrow.UINT16},
			{[]uint32{1, 2}, arrow.UINT32},
			{[]uint64{1, 2}, arrow.UINT64},
			{[]float32{1.0, 2.0}, arrow.FLOAT32},
			{[]float64{1.1, 2.2}, arrow.FLOAT64},
		}
		for _, tc := range cases {
			arr, err := buildArray(reflect.ValueOf(tc.vals), tagOpts{}, mem)
			if err != nil {
				t.Fatalf("type %v: %v", tc.id, err)
			}
			if arr.DataType().ID() != tc.id {
				t.Errorf("expected %v, got %v", tc.id, arr.DataType())
			}
			arr.Release()
		}
	})
}

func TestBuildTemporalArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("time_time", func(t *testing.T) {
		now := time.Now().UTC()
		vals := []time.Time{now, now.Add(time.Hour)}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.TIMESTAMP {
			t.Errorf("expected TIMESTAMP, got %v", arr.DataType())
		}
		typed := arr.(*array.Timestamp)
		for i, want := range vals {
			if typed.Value(i) != arrow.Timestamp(want.UnixNano()) {
				t.Errorf("[%d] timestamp mismatch", i)
			}
		}
	})

	t.Run("time_duration", func(t *testing.T) {
		vals := []time.Duration{time.Second, time.Minute, time.Hour}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DURATION {
			t.Errorf("expected DURATION, got %v", arr.DataType())
		}
		typed := arr.(*array.Duration)
		for i, want := range vals {
			if typed.Value(i) != arrow.Duration(want.Nanoseconds()) {
				t.Errorf("[%d] duration mismatch", i)
			}
		}
	})
}

func TestBuildDecimalArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("decimal128", func(t *testing.T) {
		vals := []decimal128.Num{
			decimal128.New(0, 100),
			decimal128.New(0, 200),
			decimal128.New(0, 300),
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DECIMAL128 {
			t.Errorf("expected DECIMAL128, got %v", arr.DataType())
		}
		typed := arr.(*array.Decimal128)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] decimal128 mismatch", i)
			}
		}
	})

	t.Run("decimal256", func(t *testing.T) {
		vals := []decimal256.Num{
			decimal256.New(0, 0, 0, 100),
			decimal256.New(0, 0, 0, 200),
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DECIMAL256 {
			t.Errorf("expected DECIMAL256, got %v", arr.DataType())
		}
		typed := arr.(*array.Decimal256)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] decimal256 mismatch", i)
			}
		}
	})

	t.Run("decimal128_custom_opts", func(t *testing.T) {
		vals := []decimal128.Num{decimal128.New(0, 12345)}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 10, DecimalScale: 3}
		arr, err := buildArray(reflect.ValueOf(vals), opts, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		dt := arr.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 10 || dt.Scale != 3 {
			t.Errorf("expected p=10 s=3, got p=%d s=%d", dt.Precision, dt.Scale)
		}
	})

	t.Run("decimal32", func(t *testing.T) {
		vals := []decimal.Decimal32{100, 200, 300}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DECIMAL32 {
			t.Errorf("expected DECIMAL32, got %v", arr.DataType())
		}
		typed := arr.(*array.Decimal32)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] decimal32 mismatch: got %v, want %v", i, typed.Value(i), want)
			}
		}
	})

	t.Run("decimal64", func(t *testing.T) {
		vals := []decimal.Decimal64{1000, 2000}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DECIMAL64 {
			t.Errorf("expected DECIMAL64, got %v", arr.DataType())
		}
		typed := arr.(*array.Decimal64)
		for i, want := range vals {
			if typed.Value(i) != want {
				t.Errorf("[%d] decimal64 mismatch: got %v, want %v", i, typed.Value(i), want)
			}
		}
	})

	t.Run("decimal32_custom_opts", func(t *testing.T) {
		vals := []decimal.Decimal32{12345}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 9, DecimalScale: 2}
		arr, err := buildArray(reflect.ValueOf(vals), opts, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		dt := arr.DataType().(*arrow.Decimal32Type)
		if dt.Precision != 9 || dt.Scale != 2 {
			t.Errorf("expected p=9 s=2, got p=%d s=%d", dt.Precision, dt.Scale)
		}
	})
}

type buildSimpleStruct struct {
	X int32
	Y string
}

type buildNestedStruct struct {
	A int32
	B buildSimpleStruct
}

type buildNullableStruct struct {
	X *int32
	Y *string
}

func TestBuildStructArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("simple", func(t *testing.T) {
		vals := []buildSimpleStruct{
			{X: 1, Y: "one"},
			{X: 2, Y: "two"},
			{X: 3, Y: "three"},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.STRUCT {
			t.Fatalf("expected STRUCT, got %v", arr.DataType())
		}
		typed := arr.(*array.Struct)
		if typed.Len() != 3 {
			t.Errorf("expected 3, got %d", typed.Len())
		}
		xArr := typed.Field(0).(*array.Int32)
		yArr := typed.Field(1).(*array.String)
		for i, want := range vals {
			if xArr.Value(i) != want.X {
				t.Errorf("[%d] X: want %d, got %d", i, want.X, xArr.Value(i))
			}
			if yArr.Value(i) != want.Y {
				t.Errorf("[%d] Y: want %q, got %q", i, want.Y, yArr.Value(i))
			}
		}
	})

	t.Run("pointer_null_row", func(t *testing.T) {
		v1 := buildSimpleStruct{X: 42, Y: "answer"}
		vals := []*buildSimpleStruct{&v1, nil}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.Len() != 2 {
			t.Errorf("expected 2, got %d", arr.Len())
		}
		if !arr.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
	})

	t.Run("nullable_fields", func(t *testing.T) {
		x1 := int32(10)
		y1 := "hello"
		vals := []buildNullableStruct{
			{X: &x1, Y: &y1},
			{X: nil, Y: nil},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		typed := arr.(*array.Struct)
		if !typed.Field(0).IsNull(1) {
			t.Error("expected X[1] to be null")
		}
		if !typed.Field(1).IsNull(1) {
			t.Error("expected Y[1] to be null")
		}
	})

	t.Run("nested_struct", func(t *testing.T) {
		vals := []buildNestedStruct{
			{A: 1, B: buildSimpleStruct{X: 10, Y: "inner1"}},
			{A: 2, B: buildSimpleStruct{X: 20, Y: "inner2"}},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.STRUCT {
			t.Fatalf("expected STRUCT, got %v", arr.DataType())
		}
		typed := arr.(*array.Struct)
		aArr := typed.Field(0).(*array.Int32)
		if aArr.Value(0) != 1 || aArr.Value(1) != 2 {
			t.Error("unexpected A values")
		}
		bArr := typed.Field(1).(*array.Struct)
		bxArr := bArr.Field(0).(*array.Int32)
		if bxArr.Value(0) != 10 || bxArr.Value(1) != 20 {
			t.Error("unexpected B.X values")
		}
	})
}

func TestBuildListArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_lists", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.LIST {
			t.Fatalf("expected LIST, got %v", arr.DataType())
		}
		typed := arr.(*array.List)
		if typed.Len() != 3 {
			t.Errorf("expected 3, got %d", typed.Len())
		}
		if typed.ListValues().(*array.Int32).Len() != 6 {
			t.Errorf("expected 6 total values")
		}
	})

	t.Run("null_inner", func(t *testing.T) {
		vals := [][]int32{{1, 2}, nil, {3}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if !arr.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
	})

	t.Run("string_lists", func(t *testing.T) {
		vals := [][]string{{"a", "b"}, {"c"}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.LIST {
			t.Fatalf("expected LIST, got %v", arr.DataType())
		}
	})

	t.Run("nested", func(t *testing.T) {
		vals := [][][]int32{{{1, 2}, {3}}, {{4, 5, 6}}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.LIST {
			t.Fatalf("expected outer LIST, got %v", arr.DataType())
		}
		outer := arr.(*array.List)
		if outer.Len() != 2 {
			t.Errorf("expected 2 outer rows, got %d", outer.Len())
		}
		if outer.ListValues().DataType().ID() != arrow.LIST {
			t.Fatalf("expected inner LIST, got %v", outer.ListValues().DataType())
		}
	})
}

func TestBuildMapArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("string_int32", func(t *testing.T) {
		vals := []map[string]int32{
			{"a": 1, "b": 2},
			{"c": 3},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.MAP {
			t.Fatalf("expected MAP, got %v", arr.DataType())
		}
		if arr.(*array.Map).Len() != 2 {
			t.Errorf("expected 2, got %d", arr.Len())
		}
	})

	t.Run("null_map", func(t *testing.T) {
		vals := []map[string]int32{{"a": 1}, nil}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if !arr.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
	})

	t.Run("entry_count", func(t *testing.T) {
		vals := []map[string]int32{{"x": 10, "y": 20, "z": 30}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		kvArr := arr.(*array.Map).ListValues().(*array.Struct)
		if kvArr.Len() != 3 {
			t.Errorf("expected 3 key-value pairs, got %d", kvArr.Len())
		}
	})
}

func TestBuildFixedSizeListArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_n3", func(t *testing.T) {
		vals := [][3]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.FIXED_SIZE_LIST {
			t.Fatalf("expected FIXED_SIZE_LIST, got %v", arr.DataType())
		}
		typed := arr.(*array.FixedSizeList)
		if typed.Len() != 3 {
			t.Errorf("expected 3, got %d", typed.Len())
		}
		if typed.DataType().(*arrow.FixedSizeListType).Len() != 3 {
			t.Error("expected fixed size 3")
		}
		values := typed.ListValues().(*array.Int32)
		if values.Len() != 9 {
			t.Errorf("expected 9 values, got %d", values.Len())
		}
		if values.Value(0) != 1 || values.Value(3) != 4 || values.Value(6) != 7 {
			t.Error("unexpected values")
		}
	})

	t.Run("float64_n2", func(t *testing.T) {
		vals := [][2]float64{{1.0, 2.0}, {3.0, 4.0}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.FIXED_SIZE_LIST {
			t.Fatalf("expected FIXED_SIZE_LIST, got %v", arr.DataType())
		}
		if arr.DataType().(*arrow.FixedSizeListType).Len() != 2 {
			t.Error("expected fixed size 2")
		}
	})
}

func TestBuildDictionaryArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("string_dict", func(t *testing.T) {
		vals := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DICTIONARY {
			t.Fatalf("expected DICTIONARY, got %v", arr.DataType())
		}
		typed := arr.(*array.Dictionary)
		if typed.Len() != 6 {
			t.Errorf("expected 6, got %d", typed.Len())
		}
		if typed.Dictionary().Len() != 3 {
			t.Errorf("expected 3 unique, got %d", typed.Dictionary().Len())
		}
	})

	t.Run("int32_dict", func(t *testing.T) {
		vals := []int32{1, 2, 1, 3, 2, 1}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DICTIONARY {
			t.Fatalf("expected DICTIONARY, got %v", arr.DataType())
		}
		typed := arr.(*array.Dictionary)
		if typed.Len() != 6 {
			t.Errorf("expected 6, got %d", typed.Len())
		}
		if typed.Dictionary().Len() != 3 {
			t.Errorf("expected 3 unique, got %d", typed.Dictionary().Len())
		}
	})

	t.Run("index_type_is_int32", func(t *testing.T) {
		vals := []string{"x", "y", "z"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		dt := arr.DataType().(*arrow.DictionaryType)
		if dt.IndexType.ID() != arrow.INT32 {
			t.Errorf("expected INT32 index, got %v", dt.IndexType)
		}
	})
}

func TestBuildRunEndEncodedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_runs", func(t *testing.T) {
		vals := []int32{1, 1, 1, 2, 2, 3}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.RUN_END_ENCODED {
			t.Fatalf("expected RUN_END_ENCODED, got %v", arr.DataType())
		}
		ree := arr.(*array.RunEndEncoded)
		if ree.Len() != 6 {
			t.Errorf("expected 6, got %d", ree.Len())
		}
		runEnds := ree.RunEndsArr().(*array.Int32)
		if runEnds.Len() != 3 {
			t.Errorf("expected 3 runs, got %d", runEnds.Len())
		}
		if runEnds.Value(0) != 3 || runEnds.Value(1) != 5 || runEnds.Value(2) != 6 {
			t.Errorf("unexpected run ends: %d %d %d",
				runEnds.Value(0), runEnds.Value(1), runEnds.Value(2))
		}
		values := ree.Values().(*array.Int32)
		if values.Len() != 3 {
			t.Errorf("expected 3 values, got %d", values.Len())
		}
		if values.Value(0) != 1 || values.Value(1) != 2 || values.Value(2) != 3 {
			t.Errorf("unexpected values: %d %d %d",
				values.Value(0), values.Value(1), values.Value(2))
		}
	})

	t.Run("string_runs", func(t *testing.T) {
		vals := []string{"a", "a", "b", "b", "b", "c"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.RUN_END_ENCODED {
			t.Fatalf("expected RUN_END_ENCODED, got %v", arr.DataType())
		}
		ree := arr.(*array.RunEndEncoded)
		if ree.Len() != 6 {
			t.Errorf("expected 6, got %d", ree.Len())
		}
		if ree.RunEndsArr().Len() != 3 {
			t.Errorf("expected 3 runs, got %d", ree.RunEndsArr().Len())
		}
	})

	t.Run("single_run", func(t *testing.T) {
		vals := []int32{42, 42, 42}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		if ree.Len() != 3 {
			t.Errorf("expected 3, got %d", ree.Len())
		}
		runEnds := ree.RunEndsArr().(*array.Int32)
		if runEnds.Len() != 1 || runEnds.Value(0) != 3 {
			t.Errorf("expected 1 run ending at 3, got %d runs, end=%d",
				runEnds.Len(), runEnds.Value(0))
		}
	})

	t.Run("all_distinct", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		if ree.Len() != 5 {
			t.Errorf("expected 5, got %d", ree.Len())
		}
		if ree.RunEndsArr().Len() != 5 {
			t.Errorf("expected 5 runs for all-distinct, got %d", ree.RunEndsArr().Len())
		}
	})
}

func TestBuildListViewArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_listview", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.LIST_VIEW {
			t.Fatalf("expected LIST_VIEW, got %v", arr.DataType())
		}
		typed := arr.(*array.ListView)
		if typed.Len() != 3 {
			t.Errorf("expected 3, got %d", typed.Len())
		}
	})

	t.Run("null_entry", func(t *testing.T) {
		vals := [][]int32{{1, 2}, nil, {3}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if !arr.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
	})

	t.Run("string_listview", func(t *testing.T) {
		vals := [][]string{{"hello", "world"}, {"foo"}, {"a", "b", "c"}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.LIST_VIEW {
			t.Fatalf("expected LIST_VIEW, got %v", arr.DataType())
		}
		if arr.Len() != 3 {
			t.Errorf("expected 3, got %d", arr.Len())
		}
	})

	t.Run("total_values", func(t *testing.T) {
		vals := [][]int32{{10, 20}, {30}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		allVals := arr.(*array.ListView).ListValues().(*array.Int32)
		if allVals.Len() != 3 {
			t.Errorf("expected 3 total values, got %d", allVals.Len())
		}
	})
}

func TestBuildTemporalTaggedArray(t *testing.T) {
	mem := memory.NewGoAllocator()

	// reference time-of-day: 2024-01-15 10:30:00 UTC
	ref := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	t.Run("date32", func(t *testing.T) {
		vals := []time.Time{ref, ref.AddDate(0, 0, 1)}
		opts := tagOpts{Temporal: "date32"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DATE32 {
			t.Errorf("expected DATE32, got %v", arr.DataType().ID())
		}
		if arr.Len() != 2 {
			t.Errorf("expected len 2, got %d", arr.Len())
		}
		// roundtrip: convert back and check date
		d32arr := arr.(*array.Date32)
		got0 := d32arr.Value(0).ToTime()
		if got0.Year() != ref.Year() || got0.Month() != ref.Month() || got0.Day() != ref.Day() {
			t.Errorf("date32 roundtrip: got %v, want %v", got0, ref)
		}
	})

	t.Run("date64", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "date64"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.DATE64 {
			t.Errorf("expected DATE64, got %v", arr.DataType().ID())
		}
		d64arr := arr.(*array.Date64)
		got0 := d64arr.Value(0).ToTime()
		if got0.Year() != ref.Year() || got0.Month() != ref.Month() || got0.Day() != ref.Day() {
			t.Errorf("date64 roundtrip: got %v, want %v", got0, ref)
		}
	})

	t.Run("time32", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "time32"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.TIME32 {
			t.Errorf("expected TIME32, got %v", arr.DataType().ID())
		}
		if arr.Len() != 1 {
			t.Errorf("expected len 1, got %d", arr.Len())
		}
		t32arr := arr.(*array.Time32)
		val := t32arr.Value(0)
		if val == 0 {
			t.Error("expected non-zero time32 value")
		}
	})

	t.Run("time64", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "time64"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer arr.Release()
		if arr.DataType().ID() != arrow.TIME64 {
			t.Errorf("expected TIME64, got %v", arr.DataType().ID())
		}
		t64arr := arr.(*array.Time64)
		unit := arr.DataType().(*arrow.Time64Type).Unit
		got0 := t64arr.Value(0).ToTime(unit)
		if got0.Hour() != ref.Hour() || got0.Minute() != ref.Minute() || got0.Second() != ref.Second() {
			t.Errorf("time64 roundtrip: got %v, want %v", got0, ref)
		}
	})
}
