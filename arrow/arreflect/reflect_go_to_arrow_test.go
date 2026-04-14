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
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 5, arr.Len())
		assert.Equal(t, arrow.INT32, arr.DataType().ID())
		typed := arr.(*array.Int32)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] value mismatch", i)
		}
	})

	t.Run("string", func(t *testing.T) {
		vals := []string{"hello", "world", "foo"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.STRING, arr.DataType().ID())
		typed := arr.(*array.String)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] value mismatch", i)
		}
	})

	t.Run("pointer_with_null", func(t *testing.T) {
		v1, v3 := int32(10), int32(30)
		vals := []*int32{&v1, nil, &v3}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
		typed := arr.(*array.Int32)
		assert.Equal(t, int32(10), typed.Value(0))
		assert.Equal(t, int32(30), typed.Value(2))
	})

	t.Run("bool", func(t *testing.T) {
		vals := []bool{true, false, true}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.BOOL, arr.DataType().ID())
		typed := arr.(*array.Boolean)
		assert.True(t, typed.Value(0), "expected Value(0) to be true")
		assert.False(t, typed.Value(1), "expected Value(1) to be false")
		assert.True(t, typed.Value(2), "expected Value(2) to be true")
	})

	t.Run("binary", func(t *testing.T) {
		vals := [][]byte{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("time_time", func(t *testing.T) {
		now := time.Now().UTC()
		vals := []time.Time{now, now.Add(time.Hour)}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.TIMESTAMP, arr.DataType().ID())
		typed := arr.(*array.Timestamp)
		for i, want := range vals {
			assert.Equal(t, arrow.Timestamp(want.UnixNano()), typed.Value(i), "[%d] timestamp mismatch", i)
		}
	})

	t.Run("time_duration", func(t *testing.T) {
		vals := []time.Duration{time.Second, time.Minute, time.Hour}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.DURATION, arr.DataType().ID())
		typed := arr.(*array.Duration)
		for i, want := range vals {
			assert.Equal(t, arrow.Duration(want.Nanoseconds()), typed.Value(i), "[%d] duration mismatch", i)
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
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.DECIMAL256, arr.DataType().ID())
		typed := arr.(*array.Decimal256)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal256 mismatch", i)
		}
	})

	t.Run("decimal128_custom_opts", func(t *testing.T) {
		vals := []decimal128.Num{decimal128.New(0, 12345)}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 10, DecimalScale: 3}
		arr, err := buildArray(reflect.ValueOf(vals), opts, mem)
		require.NoError(t, err)
		defer arr.Release()
		dt := arr.DataType().(*arrow.Decimal128Type)
		assert.Equal(t, int32(10), dt.Precision, "expected p=10, got p=%d", dt.Precision)
		assert.Equal(t, int32(3), dt.Scale, "expected s=3, got s=%d", dt.Scale)
	})

	t.Run("decimal32", func(t *testing.T) {
		vals := []decimal.Decimal32{100, 200, 300}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.DECIMAL32, arr.DataType().ID())
		typed := arr.(*array.Decimal32)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal32 mismatch", i)
		}
	})

	t.Run("decimal64", func(t *testing.T) {
		vals := []decimal.Decimal64{1000, 2000}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.DECIMAL64, arr.DataType().ID())
		typed := arr.(*array.Decimal64)
		for i, want := range vals {
			assert.Equal(t, want, typed.Value(i), "[%d] decimal64 mismatch", i)
		}
	})

	t.Run("decimal32_custom_opts", func(t *testing.T) {
		vals := []decimal.Decimal32{12345}
		opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 9, DecimalScale: 2}
		arr, err := buildArray(reflect.ValueOf(vals), opts, mem)
		require.NoError(t, err)
		defer arr.Release()
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
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("simple", func(t *testing.T) {
		vals := []buildSimpleStruct{
			{X: 1, Y: "one"},
			{X: 2, Y: "two"},
			{X: 3, Y: "three"},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		typed := arr.(*array.Struct)
		assert.True(t, typed.Field(0).IsNull(1), "expected X[1] to be null")
		assert.True(t, typed.Field(1).IsNull(1), "expected Y[1] to be null")
	})

	t.Run("nested_struct", func(t *testing.T) {
		vals := []buildNestedStruct{
			{A: 1, B: buildSimpleStruct{X: 10, Y: "inner1"}},
			{A: 2, B: buildSimpleStruct{X: 20, Y: "inner2"}},
		}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
}

func TestBuildListArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_lists", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected LIST, got %v", arr.DataType())
		typed := arr.(*array.List)
		assert.Equal(t, 3, typed.Len())
		assert.Equal(t, 6, typed.ListValues().(*array.Int32).Len(), "expected 6 total values")
	})

	t.Run("null_inner", func(t *testing.T) {
		vals := [][]int32{{1, 2}, nil, {3}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("nil_pointer_list_element", func(t *testing.T) {
		a := []int32{1, 2}
		vals := []*[]int32{&a, nil, &a}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.LIST, arr.DataType().ID())
		assert.Equal(t, 3, arr.Len())
		assert.False(t, arr.IsNull(0))
		assert.True(t, arr.IsNull(1))
		assert.False(t, arr.IsNull(2))
	})

	t.Run("string_lists", func(t *testing.T) {
		vals := [][]string{{"a", "b"}, {"c"}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected LIST, got %v", arr.DataType())
	})

	t.Run("nested", func(t *testing.T) {
		vals := [][][]int32{{{1, 2}, {3}}, {{4, 5, 6}}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.LIST, arr.DataType().ID(), "expected outer LIST, got %v", arr.DataType())
		outer := arr.(*array.List)
		assert.Equal(t, 2, outer.Len(), "expected 2 outer rows, got %d", outer.Len())
		require.Equal(t, arrow.LIST, outer.ListValues().DataType().ID(), "expected inner LIST, got %v", outer.ListValues().DataType())
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
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.MAP, arr.DataType().ID(), "expected MAP, got %v", arr.DataType())
		assert.Equal(t, 2, arr.(*array.Map).Len())
	})

	t.Run("null_map", func(t *testing.T) {
		vals := []map[string]int32{{"a": 1}, nil}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("entry_count", func(t *testing.T) {
		vals := []map[string]int32{{"x": 10, "y": 20, "z": 30}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		kvArr := arr.(*array.Map).ListValues().(*array.Struct)
		assert.Equal(t, 3, kvArr.Len(), "expected 3 key-value pairs, got %d", kvArr.Len())
	})
}

func TestBuildFixedSizeListArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_n3", func(t *testing.T) {
		vals := [][3]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.FIXED_SIZE_LIST, arr.DataType().ID(), "expected FIXED_SIZE_LIST, got %v", arr.DataType())
		assert.Equal(t, int32(2), arr.DataType().(*arrow.FixedSizeListType).Len(), "expected fixed size 2")
	})

	t.Run("nil_slice_appends_null", func(t *testing.T) {
		bldr := array.NewFixedSizeListBuilder(mem, int32(3), arrow.PrimitiveTypes.Int32)
		defer bldr.Release()

		var nilSlice []int32
		err := appendValue(bldr, reflect.ValueOf(&nilSlice).Elem(), tagOpts{})
		require.NoError(t, err)

		bldr.Append(true)
		vb := bldr.ValueBuilder().(*array.Int32Builder)
		vb.AppendValues([]int32{1, 2, 3}, nil)

		arr := bldr.NewArray()
		defer arr.Release()
		assert.True(t, arr.IsNull(0), "nil slice should be null")
		assert.False(t, arr.IsNull(1), "non-nil should not be null")
	})
}

func TestBuildDictionaryArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("string_dict", func(t *testing.T) {
		vals := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.DICTIONARY, arr.DataType().ID(), "expected DICTIONARY, got %v", arr.DataType())
		typed := arr.(*array.Dictionary)
		assert.Equal(t, 6, typed.Len())
		assert.Equal(t, 3, typed.Dictionary().Len(), "expected 3 unique, got %d", typed.Dictionary().Len())
	})

	t.Run("int32_dict", func(t *testing.T) {
		vals := []int32{1, 2, 1, 3, 2, 1}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.DICTIONARY, arr.DataType().ID(), "expected DICTIONARY, got %v", arr.DataType())
		typed := arr.(*array.Dictionary)
		assert.Equal(t, 6, typed.Len())
		assert.Equal(t, 3, typed.Dictionary().Len(), "expected 3 unique, got %d", typed.Dictionary().Len())
	})

	t.Run("index_type_is_int32", func(t *testing.T) {
		vals := []string{"x", "y", "z"}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
		assert.Equal(t, 3, arr.Len())
		assert.False(t, arr.IsNull(0))
		assert.True(t, arr.IsNull(1))
		assert.False(t, arr.IsNull(2))
	})
}

func TestBuildRunEndEncodedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_runs", func(t *testing.T) {
		vals := []int32{1, 1, 1, 2, 2, 3}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
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
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID(), "expected RUN_END_ENCODED, got %v", arr.DataType())
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 6, ree.Len())
		assert.Equal(t, 3, ree.RunEndsArr().Len(), "expected 3 runs, got %d", ree.RunEndsArr().Len())
	})

	t.Run("single_run", func(t *testing.T) {
		vals := []int32{42, 42, 42}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 3, ree.Len())
		runEnds := ree.RunEndsArr().(*array.Int32)
		assert.Equal(t, 1, runEnds.Len())
		assert.Equal(t, int32(3), runEnds.Value(0))
	})

	t.Run("all_distinct", func(t *testing.T) {
		vals := []int32{1, 2, 3, 4, 5}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 5, ree.Len())
		assert.Equal(t, 5, ree.RunEndsArr().Len(), "expected 5 runs for all-distinct, got %d", ree.RunEndsArr().Len())
	})

	t.Run("pointer_value_equality", func(t *testing.T) {
		x1 := "x"
		x2 := "x"
		y := "y"
		vals := []*string{&x1, &x2, &y}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true}, mem)
		require.NoError(t, err, "unexpected error")
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 2, ree.RunEndsArr().Len(), "expected 2 runs (x+x coalesced, y), got %d", ree.RunEndsArr().Len())
	})

	t.Run("ree_with_temporal_date32", func(t *testing.T) {
		t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		t2 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
		vals := []time.Time{t1, t1, t2}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{REE: true, Temporal: "date32"}, mem)
		require.NoError(t, err)
		defer arr.Release()
		ree := arr.(*array.RunEndEncoded)
		assert.Equal(t, 3, ree.Len())
		assert.Equal(t, arrow.DATE32, ree.Values().DataType().ID())
	})
}

func TestBuildListViewArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("int32_listview", func(t *testing.T) {
		vals := [][]int32{{1, 2, 3}, {4, 5}, {6}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.LIST_VIEW, arr.DataType().ID(), "expected LIST_VIEW, got %v", arr.DataType())
		typed := arr.(*array.ListView)
		assert.Equal(t, 3, typed.Len())
	})

	t.Run("null_entry", func(t *testing.T) {
		vals := [][]int32{{1, 2}, nil, {3}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
	})

	t.Run("string_listview", func(t *testing.T) {
		vals := [][]string{{"hello", "world"}, {"foo"}, {"a", "b", "c"}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, arrow.LIST_VIEW, arr.DataType().ID(), "expected LIST_VIEW, got %v", arr.DataType())
		assert.Equal(t, 3, arr.Len())
	})

	t.Run("total_values", func(t *testing.T) {
		vals := [][]int32{{10, 20}, {30}}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		allVals := arr.(*array.ListView).ListValues().(*array.Int32)
		assert.Equal(t, 3, allVals.Len(), "expected 3 total values, got %d", allVals.Len())
	})

	t.Run("nil_pointer_listview_element", func(t *testing.T) {
		a := []int32{1, 2}
		vals := []*[]int32{&a, nil, &a}
		arr, err := buildArray(reflect.ValueOf(vals), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
		assert.Equal(t, 3, arr.Len())
		assert.False(t, arr.IsNull(0))
		assert.True(t, arr.IsNull(1))
		assert.False(t, arr.IsNull(2))
	})
}

func TestBuildTemporalTaggedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

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
