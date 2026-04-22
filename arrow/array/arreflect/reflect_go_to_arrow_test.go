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

func TestBuildPrimitiveArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("int32", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, 5, arr.Len())
		assert.Equal(t, arrow.INT32, arr.DataType().ID())
		typed := arr.(*array.Int32)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] value mismatch", i)
		}
	})

	t.Run("multi_level_pointer_int32", func(t *testing.T) {
		v := int32(42)
		pv := &v
		var nilPv *int32
		vals := []**int32{&pv, &nilPv, &pv}
		arr := mustBuildDefault(t, vals, mem)
		assertMultiLevelPtrNullPattern(t, arr)
		assert.Equal(t, int32(42), arr.(*array.Int32).Value(0))
	})

	t.Run("pointer_with_null", func(t *testing.T) {
		v1, v3 := int32(10), int32(30)
		vals := []*int32{&v1, nil, &v3}
		arr := mustBuildDefault(t, vals, mem)
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
		typed := arr.(*array.Int32)
		assert.Equal(t, int32(10), typed.Value(0))
		assert.Equal(t, int32(30), typed.Value(2))
	})

	t.Run("bool", func(t *testing.T) {
		vals := []bool{true, false, true}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.BOOL, arr.DataType().ID())
		typed := arr.(*array.Boolean)
		assert.True(t, typed.Value(0), "expected Value(0) to be true")
		assert.False(t, typed.Value(1), "expected Value(1) to be false")
		assert.True(t, typed.Value(2), "expected Value(2) to be true")
	})

	t.Run("binary", func(t *testing.T) {
		vals := [][]byte{{1, 2, 3}, {4, 5}, {6}}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.BINARY, arr.DataType().ID())
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
			{[]int{1, -2, 3}, arrow.INT64},
			{[]uint{1, 2, 3}, arrow.UINT64},
		}
		for _, tc := range cases {
			arr, err := buildArray(reflect.ValueOf(tc.vals), tagOpts{}, mem)
			require.NoError(t, err, "type %v", tc.id)
			assert.Equal(t, tc.id, arr.DataType().ID(), "expected %v, got %v", tc.id, arr.DataType())
			arr.Release()
		}
	})
}

func TestBuildTemporalArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("time_time", func(t *testing.T) {
		now := time.Now().UTC()
		vals := []time.Time{now, now.Add(time.Hour)}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.TIMESTAMP, arr.DataType().ID())
		typed := arr.(*array.Timestamp)
		for i, want := range vals {
			assert.Equal(t, arrow.Timestamp(want.UnixNano()), typed.Value(i), "[%d] timestamp mismatch", i)
		}
	})

	t.Run("time_duration", func(t *testing.T) {
		vals := []time.Duration{time.Second, time.Minute, time.Hour}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.DURATION, arr.DataType().ID())
		typed := arr.(*array.Duration)
		for i, want := range vals {
			assert.Equal(t, arrow.Duration(want.Nanoseconds()), typed.Value(i), "[%d] duration mismatch", i)
		}
	})
}

func TestBuildDecimalArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("decimal128", func(t *testing.T) {
		vals := []decimal128.Num{
			decimal128.New(0, 100),
			decimal128.New(0, 200),
			decimal128.New(0, 300),
		}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.DECIMAL128, arr.DataType().ID())
		typed := arr.(*array.Decimal128)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal128 mismatch", i)
		}
	})

	t.Run("decimal256", func(t *testing.T) {
		vals := []decimal256.Num{
			decimal256.New(0, 0, 0, 100),
			decimal256.New(0, 0, 0, 200),
		}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.DECIMAL256, arr.DataType().ID())
		typed := arr.(*array.Decimal256)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal256 mismatch", i)
		}
	})

	t.Run("decimal128_custom_opts", func(t *testing.T) {
		vals := []decimal128.Num{decimal128.New(0, 12345)}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 10, DecimalScale: 3}
		arr := mustBuildArray(t, vals, opts, mem)
		dt := arr.DataType().(*arrow.Decimal128Type)
		assert.Equal(t, int32(10), dt.Precision, "expected p=10, got p=%d", dt.Precision)
		assert.Equal(t, int32(3), dt.Scale, "expected s=3, got s=%d", dt.Scale)
	})

	t.Run("decimal32", func(t *testing.T) {
		vals := []decimal.Decimal32{100, 200, 300}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.DECIMAL32, arr.DataType().ID())
		typed := arr.(*array.Decimal32)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal32 mismatch", i)
		}
	})

	t.Run("decimal64", func(t *testing.T) {
		vals := []decimal.Decimal64{1000, 2000}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.DECIMAL64, arr.DataType().ID())
		typed := arr.(*array.Decimal64)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal64 mismatch", i)
		}
	})

	t.Run("decimal32_custom_opts", func(t *testing.T) {
		vals := []decimal.Decimal32{12345}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 9, DecimalScale: 2}
		arr := mustBuildArray(t, vals, opts, mem)
		dt := arr.DataType().(*arrow.Decimal32Type)
		assert.Equal(t, int32(9), dt.Precision, "expected p=9, got p=%d", dt.Precision)
		assert.Equal(t, int32(2), dt.Scale, "expected s=2, got s=%d", dt.Scale)
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
	mem := checkedMem(t)

	t.Run("simple", func(t *testing.T) {
		vals := []buildSimpleStruct{
			{X: 1, Y: "one"},
			{X: 2, Y: "two"},
			{X: 3, Y: "three"},
		}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.STRUCT, arr.DataType().ID(), "expected STRUCT, got %v", arr.DataType())
		typed := arr.(*array.Struct)
		assert.Equal(t, 3, typed.Len())
		xArr := typed.Field(0).(*array.Int32)
		yArr := typed.Field(1).(*array.String)
		for i, want := range vals {
			assert.Equal(t, want.X, xArr.Value(i), "[%d] X mismatch", i)
			assert.Equal(t, want.Y, yArr.Value(i), "[%d] Y mismatch", i)
		}
	})

	t.Run("pointer_null_row", func(t *testing.T) {
		v1 := buildSimpleStruct{X: 42, Y: "answer"}
		vals := []*buildSimpleStruct{&v1, nil}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, 2, arr.Len())
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("nullable_fields", func(t *testing.T) {
		x1 := int32(10)
		y1 := "hello"
		vals := []buildNullableStruct{
			{X: &x1, Y: &y1},
			{X: nil, Y: nil},
		}
		arr := mustBuildDefault(t, vals, mem)
		typed := arr.(*array.Struct)
		assert.True(t, typed.Field(0).IsNull(1), "expected X[1] to be null")
		assert.True(t, typed.Field(1).IsNull(1), "expected Y[1] to be null")
	})

	t.Run("nested_struct", func(t *testing.T) {
		vals := []buildNestedStruct{
			{A: 1, B: buildSimpleStruct{X: 10, Y: "inner1"}},
			{A: 2, B: buildSimpleStruct{X: 20, Y: "inner2"}},
		}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.STRUCT, arr.DataType().ID(), "expected STRUCT, got %v", arr.DataType())
		typed := arr.(*array.Struct)
		aArr := typed.Field(0).(*array.Int32)
		assert.Equal(t, int32(1), aArr.Value(0))
		assert.Equal(t, int32(2), aArr.Value(1))
		bArr := typed.Field(1).(*array.Struct)
		bxArr := bArr.Field(0).(*array.Int32)
		assert.Equal(t, int32(10), bxArr.Value(0))
		assert.Equal(t, int32(20), bxArr.Value(1))
	})

	t.Run("multi_level_pointer_struct", func(t *testing.T) {
		type S struct {
			X int32
		}
		s := S{X: 99}
		ps := &s
		var nilPs *S
		vals := []**S{&ps, &nilPs, &ps}
		arr := mustBuildDefault(t, vals, mem)
		assertMultiLevelPtrNullPattern(t, arr)
		sa := arr.(*array.Struct)
		xArr := sa.Field(0).(*array.Int32)
		assert.Equal(t, int32(99), xArr.Value(0))
		assert.Equal(t, int32(99), xArr.Value(2))
	})

	t.Run("nil_embedded_pointer_promoted_field", func(t *testing.T) {
		// Regression: reflect.Value.FieldByIndex panics when traversing a nil
		// embedded pointer; promoted fields must become null instead.
		type Inner struct {
			City string
			Zip  int32
		}
		type Outer struct {
			Name string
			*Inner
		}
		vals := []Outer{
			{Name: "Alice", Inner: &Inner{City: "NYC", Zip: 10001}},
			{Name: "Bob", Inner: nil},
			{Name: "Carol", Inner: &Inner{City: "LA", Zip: 90001}},
		}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.STRUCT, arr.DataType().ID())
		sa := arr.(*array.Struct)
		require.Equal(t, 3, sa.Len())
		require.Equal(t, 3, sa.NumField(), "expected 3 promoted fields (Name, City, Zip)")

		nameArr := sa.Field(0).(*array.String)
		cityArr := sa.Field(1).(*array.String)
		zipArr := sa.Field(2).(*array.Int32)

		assert.Equal(t, "Alice", nameArr.Value(0))
		assert.False(t, cityArr.IsNull(0))
		assert.Equal(t, "NYC", cityArr.Value(0))
		assert.False(t, zipArr.IsNull(0))
		assert.Equal(t, int32(10001), zipArr.Value(0))

		assert.Equal(t, "Bob", nameArr.Value(1))
		assert.True(t, cityArr.IsNull(1), "City should be null when *Inner is nil")
		assert.True(t, zipArr.IsNull(1), "Zip should be null when *Inner is nil")

		assert.Equal(t, "Carol", nameArr.Value(2))
		assert.False(t, cityArr.IsNull(2))
		assert.Equal(t, "LA", cityArr.Value(2))
		assert.False(t, zipArr.IsNull(2))
		assert.Equal(t, int32(90001), zipArr.Value(2))
	})
}

func TestBuildListArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("int32_lists", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}, {6}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected LIST, got %v", arr.DataType())
		typed := arr.(*array.List)
		assert.Equal(t, 3, typed.Len())
		assert.Equal(t, 6, typed.ListValues().(*array.Int32).Len(), "expected 6 total values")
	})

	t.Run("null_inner", func(t *testing.T) {
		vals := [][]int32{{1, 2}, nil, {3}}
		arr := mustBuildDefault(t, vals, mem)
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("nil_pointer_list_element", func(t *testing.T) {
		a := []int32{1, 2}
		vals := []*[]int32{&a, nil, &a}
		arr := mustBuildDefault(t, vals, mem)
		assert.Equal(t, arrow.LIST, arr.DataType().ID())
		assertMultiLevelPtrNullPattern(t, arr)
	})

	t.Run("multi_level_pointer_list", func(t *testing.T) {
		a := []int32{1, 2}
		pa := &a
		var nilPa *[]int32
		vals := []**[]int32{&pa, &nilPa, &pa}
		arr := mustBuildDefault(t, vals, mem)
		assertMultiLevelPtrNullPattern(t, arr)
	})

	t.Run("string_lists", func(t *testing.T) {
		vals := [][]string{{"a", "b"}, {"c"}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected LIST, got %v", arr.DataType())
	})

	t.Run("nested", func(t *testing.T) {
		vals := [][][]int32{{{1, 2}, {3}}, {{4, 5, 6}}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected outer LIST, got %v", arr.DataType())
		outer := arr.(*array.List)
		assert.Equal(t, 2, outer.Len(), "expected 2 outer rows, got %d", outer.Len())
		require.Equal(t, arrow.LIST, outer.ListValues().DataType().ID(), "expected inner LIST, got %v", outer.ListValues().DataType())
	})
}

func TestBuildMapArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("string_int32", func(t *testing.T) {
		vals := []map[string]int32{
			{"a": 1, "b": 2},
			{"c": 3},
		}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.MAP, arr.DataType().ID(), "expected MAP, got %v", arr.DataType())
		assert.Equal(t, 2, arr.(*array.Map).Len())
	})

	t.Run("null_map", func(t *testing.T) {
		vals := []map[string]int32{{"a": 1}, nil}
		arr := mustBuildDefault(t, vals, mem)
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("entry_count", func(t *testing.T) {
		vals := []map[string]int32{{"x": 10, "y": 20, "z": 30}}
		arr := mustBuildDefault(t, vals, mem)
		kvArr := arr.(*array.Map).ListValues().(*array.Struct)
		assert.Equal(t, 3, kvArr.Len(), "expected 3 key-value pairs, got %d", kvArr.Len())
	})

	t.Run("multi_level_pointer_map", func(t *testing.T) {
		m := map[string]int32{"x": 1}
		pm := &m
		var nilPm *map[string]int32
		vals := []**map[string]int32{&pm, &nilPm, &pm}
		arr := mustBuildDefault(t, vals, mem)
		assertMultiLevelPtrNullPattern(t, arr)
	})
}

func TestBuildFixedSizeListArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("int32_n3", func(t *testing.T) {
		vals := [][3]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.FIXED_SIZE_LIST, arr.DataType().ID(), "expected FIXED_SIZE_LIST, got %v", arr.DataType())
		typed := arr.(*array.FixedSizeList)
		assert.Equal(t, 3, typed.Len())
		assert.Equal(t, int32(3), typed.DataType().(*arrow.FixedSizeListType).Len(), "expected fixed size 3")
		values := typed.ListValues().(*array.Int32)
		assert.Equal(t, 9, values.Len())
		assert.Equal(t, int32(1), values.Value(0))
		assert.Equal(t, int32(4), values.Value(3))
		assert.Equal(t, int32(7), values.Value(6))
	})

	t.Run("float64_n2", func(t *testing.T) {
		vals := [][2]float64{{1.0, 2.0}, {3.0, 4.0}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.FIXED_SIZE_LIST, arr.DataType().ID(), "expected FIXED_SIZE_LIST, got %v", arr.DataType())
		assert.Equal(t, int32(2), arr.DataType().(*arrow.FixedSizeListType).Len(), "expected fixed size 2")
	})

	t.Run("nil_slice_appends_null", func(t *testing.T) {
		bldr := array.NewFixedSizeListBuilder(mem, int32(3), arrow.PrimitiveTypes.Int32)
		defer bldr.Release()

		var nilSlice []int32
		err := appendValue(bldr, reflect.ValueOf(&nilSlice).Elem())
		require.NoError(t, err)

		bldr.Append(true)
		vb := bldr.ValueBuilder().(*array.Int32Builder)
		vb.AppendValues([]int32{1, 2, 3}, nil)

		arr := bldr.NewArray()
		defer arr.Release()
		assert.True(t, arr.IsNull(0), "nil slice should be null")
		assert.False(t, arr.IsNull(1), "non-nil should not be null")
	})

	t.Run("multi_level_pointer_fixed_size_list", func(t *testing.T) {
		a := [3]int32{1, 2, 3}
		pa := &a
		var nilPa *[3]int32
		vals := []**[3]int32{&pa, &nilPa, &pa}
		arr := mustBuildDefault(t, vals, mem)
		assertMultiLevelPtrNullPattern(t, arr)
	})
}

func TestBuildDictionaryArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("string_dict", func(t *testing.T) {
		vals := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
		arr := mustBuildArray(t, vals, tagOpts{Dict: true}, mem)
		require.Equal(t, arrow.DICTIONARY, arr.DataType().ID(), "expected DICTIONARY, got %v", arr.DataType())
		typed := arr.(*array.Dictionary)
		assert.Equal(t, 6, typed.Len())
		assert.Equal(t, 3, typed.Dictionary().Len(), "expected 3 unique, got %d", typed.Dictionary().Len())
	})

	t.Run("int32_dict", func(t *testing.T) {
		vals := []int32{1, 2, 1, 3, 2, 1}
		arr := mustBuildArray(t, vals, tagOpts{Dict: true}, mem)
		require.Equal(t, arrow.DICTIONARY, arr.DataType().ID(), "expected DICTIONARY, got %v", arr.DataType())
		typed := arr.(*array.Dictionary)
		assert.Equal(t, 6, typed.Len())
		assert.Equal(t, 3, typed.Dictionary().Len(), "expected 3 unique, got %d", typed.Dictionary().Len())
	})

	t.Run("index_type_is_int32", func(t *testing.T) {
		vals := []string{"x", "y", "z"}
		arr := mustBuildArray(t, vals, tagOpts{Dict: true}, mem)
		dt := arr.DataType().(*arrow.DictionaryType)
		assert.Equal(t, arrow.INT32, dt.IndexType.ID(), "expected INT32 index, got %v", dt.IndexType)
	})

	t.Run("bool_dict_returns_error", func(t *testing.T) {
		_, err := buildArray(reflect.ValueOf([]bool{true, false}), tagOpts{Dict: true}, mem)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("pointer_string_with_nil", func(t *testing.T) {
		s := "hello"
		vals := []*string{&s, nil, &s}
		arr := mustBuildArray(t, vals, tagOpts{Dict: true}, mem)
		typed := arr.(*array.Dictionary)
		assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
		assert.Equal(t, 3, arr.Len())
		assert.False(t, arr.IsNull(0))
		assert.True(t, arr.IsNull(1))
		assert.False(t, arr.IsNull(2))
		assert.Equal(t, 1, typed.Dictionary().Len(), "expected 1 unique value")
	})

	t.Run("multi_level_pointer_string", func(t *testing.T) {
		s := "world"
		ps := &s
		var nilPs *string
		vals := []**string{&ps, &nilPs, &ps}
		arr := mustBuildArray(t, vals, tagOpts{Dict: true}, mem)
		typed := arr.(*array.Dictionary)
		assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
		assertMultiLevelPtrNullPattern(t, arr)
		assert.Equal(t, 1, typed.Dictionary().Len(), "expected 1 unique value")
	})
}

func TestBuildRunEndEncodedArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("int32_runs", func(t *testing.T) {
		vals := []int32{1, 1, 1, 2, 2, 3}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		require.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID(), "expected RUN_END_ENCODED, got %v", arr.DataType())
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 6, ree.Len())
		runEnds := ree.RunEndsArr().(*array.Int32)
		assert.Equal(t, 3, runEnds.Len(), "expected 3 runs, got %d", runEnds.Len())
		assert.Equal(t, int32(3), runEnds.Value(0))
		assert.Equal(t, int32(5), runEnds.Value(1))
		assert.Equal(t, int32(6), runEnds.Value(2))
		values := ree.Values().(*array.Int32)
		assert.Equal(t, 3, values.Len(), "expected 3 values, got %d", values.Len())
		assert.Equal(t, int32(1), values.Value(0))
		assert.Equal(t, int32(2), values.Value(1))
		assert.Equal(t, int32(3), values.Value(2))
	})

	t.Run("string_runs", func(t *testing.T) {
		vals := []string{"a", "a", "b", "b", "b", "c"}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		require.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID(), "expected RUN_END_ENCODED, got %v", arr.DataType())
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 6, ree.Len())
		assert.Equal(t, 3, ree.RunEndsArr().Len(), "expected 3 runs, got %d", ree.RunEndsArr().Len())
	})

	t.Run("single_run", func(t *testing.T) {
		vals := []int32{42, 42, 42}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 3, ree.Len())
		runEnds := ree.RunEndsArr().(*array.Int32)
		assert.Equal(t, 1, runEnds.Len())
		assert.Equal(t, int32(3), runEnds.Value(0))
	})

	t.Run("all_distinct", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 5, ree.Len())
		assert.Equal(t, 5, ree.RunEndsArr().Len(), "expected 5 runs for all-distinct, got %d", ree.RunEndsArr().Len())
	})

	t.Run("pointer_value_equality", func(t *testing.T) {
		x1 := "x"
		x2 := "x"
		y := "y"
		vals := []*string{&x1, &x2, &y}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 2, ree.RunEndsArr().Len(), "expected 2 runs (x+x coalesced, y), got %d", ree.RunEndsArr().Len())
	})

	t.Run("ree_with_temporal_date32", func(t *testing.T) {
		t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		t2 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
		vals := []time.Time{t1, t1, t2}
		arr := mustBuildArray(t, vals, tagOpts{REE: true, Temporal: "date32"}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 3, ree.Len())
		assert.Equal(t, arrow.DATE32, ree.Values().DataType().ID())
	})
}

func TestBuildViewArray(t *testing.T) {
	mem := checkedMem(t)

	t.Run("string→STRING_VIEW", func(t *testing.T) {
		arr := mustBuildArray(t, []string{"a", "b"}, tagOpts{View: true}, mem)
		assert.Equal(t, arrow.STRING_VIEW, arr.DataType().ID())
		sv := arr.(*array.StringView)
		assert.Equal(t, "a", sv.Value(0))
		assert.Equal(t, "b", sv.Value(1))
	})

	t.Run("[]byte→BINARY_VIEW", func(t *testing.T) {
		arr := mustBuildArray(t, [][]byte{{1, 2}, {3}}, tagOpts{View: true}, mem)
		assert.Equal(t, arrow.BINARY_VIEW, arr.DataType().ID())
	})

	t.Run("int32_view", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}}
		arr := mustBuildArray(t, vals, tagOpts{View: true}, mem)
		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
		typed := arr.(*array.ListView)
		assert.Equal(t, 2, typed.Len())
	})

	t.Run("nil_outer_listview", func(t *testing.T) {
		var nilSlice [][]int32
		arr := mustBuildArray(t, nilSlice, tagOpts{View: true}, mem)
		assert.Equal(t, 0, arr.Len())
	})

	t.Run("string_listview", func(t *testing.T) {
		vals := [][]string{{"a", "b"}, {"c"}}
		arr := mustBuildArray(t, vals, tagOpts{View: true}, mem)
		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
		lv := arr.DataType().(*arrow.ListViewType)
		assert.Equal(t, arrow.STRING_VIEW, lv.Elem().ID())
	})

	t.Run("null_in_listview", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, nil, {4, 5}}
		arr := mustBuildArray(t, vals, tagOpts{View: true}, mem)
		allVals := arr.(*array.ListView).ListValues().(*array.Int32)
		assert.Equal(t, 5, allVals.Len())
	})

	t.Run("nil_pointer_view_element", func(t *testing.T) {
		a := []int32{1, 2}
		vals := []*[]int32{&a, nil}
		arr := mustBuildArray(t, vals, tagOpts{View: true}, mem)
		assert.True(t, arr.IsNull(1))
	})
}

func TestBuildTemporalTaggedArray(t *testing.T) {
	mem := checkedMem(t)

	ref := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	t.Run("date32", func(t *testing.T) {
		vals := []time.Time{ref, ref.AddDate(0, 0, 1)}
		opts := tagOpts{Temporal: "date32"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		require.NoError(t, err, "unexpected error")
		defer arr.Release()
		assert.Equal(t, arrow.DATE32, arr.DataType().ID())
		assert.Equal(t, 2, arr.Len())
		d32arr := arr.(*array.Date32)
		got0 := d32arr.Value(0).ToTime()
		assert.Equal(t, ref.Year(), got0.Year())
		assert.Equal(t, ref.Month(), got0.Month())
		assert.Equal(t, ref.Day(), got0.Day())
	})

	t.Run("date64", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "date64"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		require.NoError(t, err, "unexpected error")
		defer arr.Release()
		assert.Equal(t, arrow.DATE64, arr.DataType().ID())
		d64arr := arr.(*array.Date64)
		got0 := d64arr.Value(0).ToTime()
		assert.Equal(t, ref.Year(), got0.Year())
		assert.Equal(t, ref.Month(), got0.Month())
		assert.Equal(t, ref.Day(), got0.Day())
	})

	t.Run("time32", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "time32"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		require.NoError(t, err, "unexpected error")
		defer arr.Release()
		assert.Equal(t, arrow.TIME32, arr.DataType().ID())
		assert.Equal(t, 1, arr.Len())
		t32arr := arr.(*array.Time32)
		unit := arr.DataType().(*arrow.Time32Type).Unit
		got0 := t32arr.Value(0).ToTime(unit)
		assert.Equal(t, ref.Hour(), got0.Hour())
		assert.Equal(t, ref.Minute(), got0.Minute())
		assert.Equal(t, ref.Second(), got0.Second())
		refWithMs := time.Date(ref.Year(), ref.Month(), ref.Day(), ref.Hour(), ref.Minute(), ref.Second(), 500_000_000, ref.Location())
		svMs := reflect.ValueOf([]time.Time{refWithMs})
		arrMs, err := buildTemporalArray(svMs, tagOpts{Temporal: "time32"}, mem)
		require.NoError(t, err, "time32 with ms")
		defer arrMs.Release()
		t32ms := arrMs.(*array.Time32)
		unitMs := arrMs.DataType().(*arrow.Time32Type).Unit
		gotMs := t32ms.Value(0).ToTime(unitMs)
		assert.Equal(t, 500, gotMs.Nanosecond()/1_000_000, "time32 millisecond: got %d ms, want 500 ms", gotMs.Nanosecond()/1_000_000)
	})

	t.Run("time64", func(t *testing.T) {
		vals := []time.Time{ref}
		opts := tagOpts{Temporal: "time64"}
		sv := reflect.ValueOf(vals)
		arr, err := buildTemporalArray(sv, opts, mem)
		require.NoError(t, err, "unexpected error")
		defer arr.Release()
		assert.Equal(t, arrow.TIME64, arr.DataType().ID())
		t64arr := arr.(*array.Time64)
		unit := arr.DataType().(*arrow.Time64Type).Unit
		got0 := t64arr.Value(0).ToTime(unit)
		assert.Equal(t, ref.Hour(), got0.Hour())
		assert.Equal(t, ref.Minute(), got0.Minute())
		assert.Equal(t, ref.Second(), got0.Second())
		refWithNanos := time.Date(ref.Year(), ref.Month(), ref.Day(), ref.Hour(), ref.Minute(), ref.Second(), 123456789, ref.Location())
		sv64 := reflect.ValueOf([]time.Time{refWithNanos})
		arr64, err := buildTemporalArray(sv64, tagOpts{Temporal: "time64"}, mem)
		require.NoError(t, err, "time64 with nanos")
		defer arr64.Release()
		t64arr64 := arr64.(*array.Time64)
		unit64 := arr64.DataType().(*arrow.Time64Type).Unit
		got64 := t64arr64.Value(0).ToTime(unit64)
		assert.Equal(t, refWithNanos.Nanosecond(), got64.Nanosecond(),
			"time64 nanosecond: got %d, want %d", got64.Nanosecond(), refWithNanos.Nanosecond())
	})
}

func TestNilByteSliceIsNull(t *testing.T) {
	mem := memory.NewGoAllocator()
	arr, err := FromSlice([][]byte{[]byte("hello"), nil}, mem)
	require.NoError(t, err)
	defer arr.Release()
	assert.False(t, arr.IsNull(0), "non-nil byte slice should not be null")
	assert.True(t, arr.IsNull(1), "nil byte slice should be null")
}

func TestAppendToDictBuilderAllTypes(t *testing.T) {
	mem := checkedMem(t)

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"int8", func(t *testing.T) {
			arr := mustBuildArray(t, []int8{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"int16", func(t *testing.T) {
			arr := mustBuildArray(t, []int16{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"int64", func(t *testing.T) {
			arr := mustBuildArray(t, []int64{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"uint8", func(t *testing.T) {
			arr := mustBuildArray(t, []uint8{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"uint16", func(t *testing.T) {
			arr := mustBuildArray(t, []uint16{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"uint32", func(t *testing.T) {
			arr := mustBuildArray(t, []uint32{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"uint64", func(t *testing.T) {
			arr := mustBuildArray(t, []uint64{1, 2, 1, 3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"float32", func(t *testing.T) {
			arr := mustBuildArray(t, []float32{1.1, 2.2, 1.1, 3.3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"float64", func(t *testing.T) {
			arr := mustBuildArray(t, []float64{1.1, 2.2, 1.1, 3.3}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 3, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"binary bytes", func(t *testing.T) {
			arr := mustBuildArray(t, [][]byte{[]byte("a"), []byte("b"), []byte("a")}, tagOpts{Dict: true}, mem)
			assert.Equal(t, 2, arr.(*array.Dictionary).Dictionary().Len())
		}},
		{"binary nil is null", func(t *testing.T) {
			arr := mustBuildArray(t, [][]byte{[]byte("a"), nil, []byte("a")}, tagOpts{Dict: true}, mem)
			assert.True(t, arr.IsNull(1))
			assert.Equal(t, 1, arr.(*array.Dictionary).Dictionary().Len())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}

	t.Run("binary with unsupported kind returns error", func(t *testing.T) {
		db := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary,
		}).(*array.BinaryDictionaryBuilder)
		defer db.Release()
		err := appendToDictBuilder(db, reflect.ValueOf(int32(7)))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("unsupported dict builder type returns error", func(t *testing.T) {
		db := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: &arrow.Decimal128Type{Precision: 10, Scale: 2},
		})
		defer db.Release()
		err := appendToDictBuilder(db, reflect.ValueOf(decimal128.New(0, 1)))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestAppendListElementDirect(t *testing.T) {
	mem := checkedMem(t)

	t.Run("nil slice appends null", func(t *testing.T) {
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()
		var empty []int32
		require.NoError(t, appendListElement(lb, reflect.ValueOf(empty)))
		arr := lb.NewArray()
		defer arr.Release()
		assert.True(t, arr.IsNull(0))
	})

	t.Run("large list builder", func(t *testing.T) {
		lb := array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()
		require.NoError(t, appendListElement(lb, reflect.ValueOf([]int32{1, 2, 3})))
		arr := lb.NewArray().(*array.LargeList)
		defer arr.Release()
		assert.Equal(t, 1, arr.Len())
		vb := arr.ListValues().(*array.Int32)
		assert.Equal(t, 3, vb.Len())
	})

	t.Run("list view builder", func(t *testing.T) {
		lb := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()
		require.NoError(t, appendListElement(lb, reflect.ValueOf([]int32{4, 5})))
		arr := lb.NewArray().(*array.ListView)
		defer arr.Release()
		assert.Equal(t, 1, arr.Len())
	})

	t.Run("large list view builder", func(t *testing.T) {
		lb := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()
		require.NoError(t, appendListElement(lb, reflect.ValueOf([]int32{6})))
		arr := lb.NewArray().(*array.LargeListView)
		defer arr.Release()
		assert.Equal(t, 1, arr.Len())
	})

	t.Run("unexpected builder type returns error", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		err := appendListElement(b, reflect.ValueOf([]int32{1}))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestBuildRunEndEncodedArrayExtras(t *testing.T) {
	mem := checkedMem(t)

	t.Run("empty_slice_direct", func(t *testing.T) {
		empty := reflect.MakeSlice(reflect.TypeOf([]int32{}), 0, 0)
		arr, err := buildRunEndEncodedArray(empty, tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID())
	})

	t.Run("nil_pointer_runs_collapse", func(t *testing.T) {
		s := "x"
		vals := []*string{nil, nil, &s, nil}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 4, ree.Len())
		assert.Equal(t, 3, ree.RunEndsArr().Len(),
			"expected 3 runs (nil,nil + x + nil), got %d", ree.RunEndsArr().Len())
	})

	t.Run("nil_and_non_nil_pointer_are_not_equal", func(t *testing.T) {
		s := "x"
		vals := []*string{nil, &s}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 2, ree.RunEndsArr().Len(),
			"expected 2 runs (nil != &x), got %d", ree.RunEndsArr().Len())
	})

	t.Run("non_comparable_elem_uses_deep_equal", func(t *testing.T) {
		vals := [][]int32{{1, 2}, {1, 2}, {3}}
		arr := mustBuildArray(t, vals, tagOpts{REE: true}, mem)
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 3, ree.Len())
		assert.Equal(t, 2, ree.RunEndsArr().Len(),
			"expected 2 runs via DeepEqual, got %d", ree.RunEndsArr().Len())
	})
}

func TestBuildMapArrayExtras(t *testing.T) {
	mem := checkedMem(t)

	t.Run("pointer_key_type", func(t *testing.T) {
		k1, k2 := "a", "b"
		vals := []map[*string]int32{{&k1: 1, &k2: 2}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.MAP, arr.DataType().ID())
		assert.Equal(t, 1, arr.Len())
	})

	t.Run("pointer_value_type", func(t *testing.T) {
		v1, v2 := int32(1), int32(2)
		vals := []map[string]*int32{{"a": &v1, "b": &v2}}
		arr := mustBuildDefault(t, vals, mem)
		require.Equal(t, arrow.MAP, arr.DataType().ID())
		assert.Equal(t, 1, arr.Len())
	})

	t.Run("unsupported_key_type_errors", func(t *testing.T) {
		vals := []map[complex64]int32{{1 + 2i: 1}}
		_, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("unsupported_value_type_errors", func(t *testing.T) {
		vals := []map[string]complex64{{"a": 1 + 2i}}
		_, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestAppendTemporalValueErrors(t *testing.T) {
	mem := checkedMem(t)
	notATime := reflect.ValueOf(int32(42))

	builderCases := []struct {
		name    string
		builder array.Builder
	}{
		{"timestamp", array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})},
		{"date32", array.NewDate32Builder(mem)},
		{"date64", array.NewDate64Builder(mem)},
		{"time32", array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Millisecond})},
		{"time64", array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Nanosecond})},
	}
	for _, tc := range builderCases {
		t.Run(tc.name+"_requires_time_Time", func(t *testing.T) {
			defer tc.builder.Release()
			err := appendTemporalValue(tc.builder, notATime)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrTypeMismatch)
		})
	}

	t.Run("duration_requires_time_Duration", func(t *testing.T) {
		b := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
		defer b.Release()
		err := appendTemporalValue(b, notATime)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTypeMismatch)
	})

	t.Run("unexpected_builder_type", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		err := appendTemporalValue(b, reflect.ValueOf(time.Now()))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestAppendDecimalValueErrors(t *testing.T) {
	mem := checkedMem(t)
	notDecimal := reflect.ValueOf("not a decimal")

	t.Run("decimal128_wrong_type", func(t *testing.T) {
		b := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
		defer b.Release()
		err := appendDecimalValue(b, notDecimal)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTypeMismatch)
	})

	t.Run("decimal256_wrong_type", func(t *testing.T) {
		b := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 40, Scale: 2})
		defer b.Release()
		err := appendDecimalValue(b, notDecimal)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTypeMismatch)
	})

	t.Run("unexpected_builder_type", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		err := appendDecimalValue(b, reflect.ValueOf(decimal128.New(0, 1)))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestBuildLargeTypes(t *testing.T) {
	mem := checkedMem(t)
	largeOpts := tagOpts{Large: true}

	t.Run("string→LARGE_STRING", func(t *testing.T) {
		arr := mustBuildArray(t, []string{"a", "b", "c"}, largeOpts, mem)
		assert.Equal(t, arrow.LARGE_STRING, arr.DataType().ID())
		ls := arr.(*array.LargeString)
		assert.Equal(t, "a", ls.Value(0))
		assert.Equal(t, "b", ls.Value(1))
		assert.Equal(t, "c", ls.Value(2))
	})

	t.Run("[]byte→LARGE_BINARY", func(t *testing.T) {
		arr := mustBuildArray(t, [][]byte{{1, 2}, {3}}, largeOpts, mem)
		assert.Equal(t, arrow.LARGE_BINARY, arr.DataType().ID())
	})

	t.Run("[]string→LARGE_LIST<LARGE_STRING>", func(t *testing.T) {
		arr := mustBuildArray(t, [][]string{{"a", "b"}, {"c"}}, largeOpts, mem)
		assert.Equal(t, arrow.LARGE_LIST, arr.DataType().ID())
		ll := arr.DataType().(*arrow.LargeListType)
		assert.Equal(t, arrow.LARGE_STRING, ll.Elem().ID())
	})

	t.Run("[][]byte→LARGE_LIST<LARGE_BINARY>", func(t *testing.T) {
		arr := mustBuildArray(t, [][][]byte{{{1}, {2}}, {{3}}}, largeOpts, mem)
		assert.Equal(t, arrow.LARGE_LIST, arr.DataType().ID())
		ll := arr.DataType().(*arrow.LargeListType)
		assert.Equal(t, arrow.LARGE_BINARY, ll.Elem().ID())
	})

	t.Run("view+large→LARGE_LIST_VIEW<STRING_VIEW>", func(t *testing.T) {
		// large→LARGE_LIST, then view→LARGE_LIST_VIEW; view wins on string elem (no LARGE_STRING_VIEW)
		opts := tagOpts{Large: true, View: true}
		arr := mustBuildArray(t, [][]string{{"x"}, {"y", "z"}}, opts, mem)
		assert.Equal(t, arrow.LARGE_LIST_VIEW, arr.DataType().ID())
		llv := arr.DataType().(*arrow.LargeListViewType)
		assert.Equal(t, arrow.STRING_VIEW, llv.Elem().ID())
	})

	t.Run("map<string,string> with large", func(t *testing.T) {
		arr := mustBuildArray(t, []map[string]string{{"k": "v"}}, largeOpts, mem)
		assert.Equal(t, arrow.MAP, arr.DataType().ID())
		mt := arr.DataType().(*arrow.MapType)
		assert.Equal(t, arrow.LARGE_STRING, mt.KeyType().ID())
		assert.Equal(t, arrow.LARGE_STRING, mt.ItemField().Type.ID())
	})

	t.Run("dict+large on string→Dictionary<Int32,STRING> (large ignored for dict)", func(t *testing.T) {
		opts := tagOpts{Large: true, Dict: true}
		arr := mustBuildArray(t, []string{"a", "b", "a"}, opts, mem)
		assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
		dt := arr.DataType().(*arrow.DictionaryType)
		assert.Equal(t, arrow.STRING, dt.ValueType.ID()) // large not applied, library limitation
	})
}

func TestAppendTemporalValueUnitHandling(t *testing.T) {
	mem := checkedMem(t)
	ref := time.Date(2024, 1, 15, 12, 34, 56, 789_000_000, time.UTC)

	timestampCases := []struct {
		name string
		unit arrow.TimeUnit
	}{
		{"timestamp_second", arrow.Second},
		{"timestamp_millisecond", arrow.Millisecond},
		{"timestamp_microsecond", arrow.Microsecond},
		{"timestamp_nanosecond", arrow.Nanosecond},
	}
	for _, tc := range timestampCases {
		t.Run(tc.name, func(t *testing.T) {
			dt := &arrow.TimestampType{Unit: tc.unit}
			b := array.NewTimestampBuilder(mem, dt)
			defer b.Release()
			require.NoError(t, appendTemporalValue(b, reflect.ValueOf(ref)))
			arr := b.NewArray().(*array.Timestamp)
			defer arr.Release()
			got := int64(arr.Value(0))
			want := ref.UnixNano() / int64(tc.unit.Multiplier())
			assert.Equal(t, want, got, "%s: stored value should be scaled by unit", tc.name)
		})
	}

	durationCases := []struct {
		name string
		unit arrow.TimeUnit
		d    time.Duration
	}{
		{"duration_second", arrow.Second, 90 * time.Second},
		{"duration_millisecond", arrow.Millisecond, 1500 * time.Millisecond},
		{"duration_microsecond", arrow.Microsecond, 2500 * time.Microsecond},
		{"duration_nanosecond", arrow.Nanosecond, 12345 * time.Nanosecond},
	}
	for _, tc := range durationCases {
		t.Run(tc.name, func(t *testing.T) {
			dt := &arrow.DurationType{Unit: tc.unit}
			b := array.NewDurationBuilder(mem, dt)
			defer b.Release()
			require.NoError(t, appendTemporalValue(b, reflect.ValueOf(tc.d)))
			arr := b.NewArray().(*array.Duration)
			defer arr.Release()
			got := int64(arr.Value(0))
			want := tc.d.Nanoseconds() / int64(tc.unit.Multiplier())
			assert.Equal(t, want, got, "%s: stored value should be scaled by unit", tc.name)
		})
	}
}

func TestWithLargeErrors(t *testing.T) {
	mem := checkedMem(t)

	t.Run("large on int64 slice errors", func(t *testing.T) {
		_, err := FromSlice([]int64{1, 2, 3}, mem, WithLarge())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
		assert.Contains(t, err.Error(), "large option has no effect")
	})

	t.Run("large on float32 slice errors", func(t *testing.T) {
		_, err := FromSlice([]float32{1.0}, mem, WithLarge())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("large on struct with no string fields errors", func(t *testing.T) {
		type NoStrings struct {
			X int32
			Y float64
		}
		_, err := FromSlice([]NoStrings{{1, 2.0}}, mem, WithLarge())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
		assert.Contains(t, err.Error(), "large option has no effect")
	})

	t.Run("large on string slice succeeds", func(t *testing.T) {
		arr, err := FromSlice([]string{"a"}, mem, WithLarge())
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.LARGE_STRING, arr.DataType().ID())
	})
}

func TestAppendValueViewBuilders(t *testing.T) {
	mem := checkedMem(t)

	t.Run("StringViewBuilder appends string value", func(t *testing.T) {
		b := array.NewStringViewBuilder(mem)
		defer b.Release()
		err := appendValue(b, reflect.ValueOf("hello"))
		require.NoError(t, err)
		arr := b.NewArray()
		defer arr.Release()
		assert.Equal(t, 1, arr.Len())
		assert.Equal(t, "hello", arr.(*array.StringView).Value(0))
	})

	t.Run("BinaryViewBuilder appends binary value", func(t *testing.T) {
		b := array.NewBinaryViewBuilder(mem)
		defer b.Release()
		err := appendValue(b, reflect.ValueOf([]byte{1, 2, 3}))
		require.NoError(t, err)
		arr := b.NewArray()
		defer arr.Release()
		assert.Equal(t, 1, arr.Len())
		assert.Equal(t, []byte{1, 2, 3}, arr.(*array.BinaryView).Value(0))
	})

	t.Run("BinaryViewBuilder appends null for nil slice", func(t *testing.T) {
		b := array.NewBinaryViewBuilder(mem)
		defer b.Release()
		var nilSlice []byte
		err := appendValue(b, reflect.ValueOf(nilSlice))
		require.NoError(t, err)
		arr := b.NewArray()
		defer arr.Release()
		assert.True(t, arr.IsNull(0))
	})
}
