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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s = []int32{1, 2, 3, 4}

		f1Lengths = []int{3, 0, 3, 4}
		f1Offsets = []int32{0, 3, 3, 6, 10}
		f1Valids  = []bool{true, false, true, true}

		isValid = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	for i := 0; i < 10; i++ {
		f1b := sb.FieldBuilder(0).(*array.ListBuilder)
		f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
		f2b := sb.FieldBuilder(1).(*array.Int32Builder)

		if got, want := sb.NumField(), 2; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		sb.Resize(len(f1Lengths))
		f1vb.Resize(len(f1s))
		f2b.Resize(len(f2s))

		pos := 0
		for i, length := range f1Lengths {
			f1b.Append(f1Valids[i])
			for j := 0; j < length; j++ {
				f1vb.Append(f1s[pos])
				pos++
			}
			f2b.Append(f2s[i])
		}

		for _, valid := range isValid {
			sb.Append(valid)
		}

		arr := sb.NewArray().(*array.Struct)
		defer arr.Release()

		arr.Retain()
		arr.Release()

		if got, want := arr.DataType().ID(), arrow.STRUCT; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		for i, valid := range isValid {
			if got, want := arr.IsValid(i), valid; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		{
			f1arr := arr.Field(0).(*array.List)
			if got, want := f1arr.Len(), len(f1Lengths); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range f1Lengths {
				if got, want := f1arr.IsValid(i), f1Valids[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := f1arr.IsNull(i), f1Lengths[i] == 0; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}

			}

			if got, want := f1arr.Offsets(), f1Offsets; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			varr := f1arr.ListValues().(*array.Uint8)
			if got, want := varr.Uint8Values(), f1s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		}

		{
			f2arr := arr.Field(1).(*array.Int32)
			if got, want := f2arr.Len(), len(f2s); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if got, want := f2arr.Int32Values(), f2s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		}
	}
}

func TestStructStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := arrow.StructOf(
		arrow.Field{Name: "nullable_bool", Type: new(arrow.BooleanType), Nullable: true},
		arrow.Field{Name: "non_nullable_bool", Type: new(arrow.BooleanType)},
	)

	builder := array.NewStructBuilder(memory.DefaultAllocator, dt)
	nullableBld := builder.FieldBuilder(0).(*array.BooleanBuilder)
	nonNullableBld := builder.FieldBuilder(1).(*array.BooleanBuilder)

	builder.Append(true)
	nullableBld.Append(true)
	nonNullableBld.Append(true)

	builder.Append(true)
	nullableBld.AppendNull()
	nonNullableBld.Append(true)

	builder.AppendNull()

	arr := builder.NewArray().(*array.Struct)

	// 2. create array via AppendValueFromString
	b1 := array.NewStructBuilder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Struct)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestStructArrayEmpty(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	sb := array.NewStructBuilder(pool, arrow.StructOf())
	defer sb.Release()

	if got, want := sb.NumField(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	arr := sb.NewArray().(*array.Struct)

	if got, want := arr.Len(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NumField(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestStructArrayBulkAppend(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s = []int32{1, 2, 3, 4}

		f1Lengths = []int{3, 0, 3, 4}
		f1Offsets = []int32{0, 3, 3, 6, 10}
		f1Valids  = []bool{true, false, true, true}

		isValid = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	for i := 0; i < 10; i++ {
		f1b := sb.FieldBuilder(0).(*array.ListBuilder)
		f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
		f2b := sb.FieldBuilder(1).(*array.Int32Builder)

		if got, want := sb.NumField(), 2; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		sb.Resize(len(f1Lengths))
		f1vb.Resize(len(f1s))
		f2b.Resize(len(f2s))

		sb.AppendValues(isValid)
		f1b.AppendValues(f1Offsets, f1Valids)
		f1vb.AppendValues(f1s, nil)
		f2b.AppendValues(f2s, nil)

		arr := sb.NewArray().(*array.Struct)
		defer arr.Release()

		if got, want := arr.DataType().ID(), arrow.STRUCT; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		for i, valid := range isValid {
			if got, want := arr.IsValid(i), valid; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		{
			f1arr := arr.Field(0).(*array.List)
			if got, want := f1arr.Len(), len(f1Lengths); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range f1Lengths {
				if got, want := f1arr.IsValid(i), f1Valids[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := f1arr.IsNull(i), f1Lengths[i] == 0; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}

			}

			if got, want := f1arr.Offsets(), f1Offsets; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			varr := f1arr.ListValues().(*array.Uint8)
			if got, want := varr.Uint8Values(), f1s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		}

		{
			f2arr := arr.Field(1).(*array.Int32)
			if got, want := f2arr.Len(), len(f2s); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if got, want := f2arr.Int32Values(), f2s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		}
	}
}

func TestStructArrayStringer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []float64{1.1, 1.2, 1.3, 1.4}
		f2s = []int32{1, 2, 3, 4}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range f1s {
		sb.Append(true)
		switch i {
		case 1:
			f1b.AppendNull()
			f2b.Append(f2s[i])
		case 2:
			f1b.Append(f1s[i])
			f2b.AppendNull()
		default:
			f1b.Append(f1s[i])
			f2b.Append(f2s[i])
		}
	}
	assert.NoError(t, sb.AppendValueFromString(`{"f1": 1.1, "f2": 1}`))
	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	assert.Equal(t, `{"f1":1.1,"f2":1}`, arr.ValueStr(4))
	want := "{[1.1 (null) 1.3 1.4 1.1] [1 2 (null) 4 1]}"
	got := arr.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArraySlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s    = []float64{1.1, 1.2, 1.3, 1.4}
		f2s    = []int32{1, 2, 3, 4}
		valids = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)

	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range f1s {
		sb.Append(valids[i])
		switch i {
		case 1:
			f1b.AppendNull()
			f2b.Append(f2s[i])
		case 2:
			f1b.Append(f1s[i])
			f2b.AppendNull()
		default:
			f1b.Append(f1s[i])
			f2b.Append(f2s[i])
		}
	}

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	// Slice
	arrSlice := array.NewSlice(arr, 2, 4).(*array.Struct)
	defer arrSlice.Release()

	want := "{[1.3 1.4] [(null) 4]}"
	got := arrSlice.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArrayNullBitmap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s    = []float64{1.1, 1.2, 1.3, 1.4}
		f2s    = []int32{1, 2, 3, 4}
		valids = []bool{true, true, true, false}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)

	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	sb.AppendValues(valids)
	for i := range f1s {
		f1b.Append(f1s[i])
		switch i {
		case 1:
			f2b.AppendNull()
		default:
			f2b.Append(f2s[i])
		}
	}

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	want := "{[1.1 1.2 1.3 (null)] [1 (null) 3 (null)]}"
	got := arr.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArrayStringerMasksRequiredChildWithoutNullBitmap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	childData := array.NewData(arrow.PrimitiveTypes.Int32, 3, []*memory.Buffer{
		nil,
		memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{1, 2, 3})),
	}, nil, 0, 0)
	defer childData.Release()
	child := array.MakeFromData(childData)
	defer child.Release()
	require.Empty(t, child.NullBitmapBytes())

	fields := []arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}
	nullBitmap := memory.NewBufferBytes([]byte{0x05})

	arr, err := array.NewStructArrayWithFieldsAndNulls([]arrow.Array{child}, fields, nullBitmap, 1, 0)
	require.NoError(t, err)
	defer arr.Release()

	assert.NotPanics(t, func() {
		assert.Equal(t, "{[1 (null) 3]}", arr.String())
	})
}

func TestNewStructArrayWithNullsValidatesOffset(t *testing.T) {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int32{1, 2, 3}, nil)
	child := builder.NewArray()
	defer child.Release()

	for _, test := range []struct {
		name   string
		offset int
		length int
	}{
		{name: "zero", offset: 0, length: 3},
		{name: "sliced", offset: 1, length: 2},
		{name: "empty", offset: 3, length: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			arr, err := array.NewStructArrayWithNulls(
				[]arrow.Array{child}, []string{"value"}, nil, 0, test.offset)
			require.NoError(t, err)
			defer arr.Release()
			assert.Equal(t, test.length, arr.Len())
			assert.Equal(t, test.length, arr.Field(0).Len())
		})
	}

	for _, offset := range []int{-1, 4} {
		arr, err := array.NewStructArrayWithNulls(
			[]arrow.Array{child}, []string{"value"}, nil, 0, offset)
		require.ErrorIs(t, err, arrow.ErrInvalid)
		require.Nil(t, arr)
	}

	_, err := array.NewStructArrayWithNulls(
		[]arrow.Array{child}, []string{"value"}, nil, 1, 0)
	require.ErrorIs(t, err, arrow.ErrInvalid)

	fields := []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}
	for _, offset := range []int{-1, 4} {
		arr, err := array.NewStructArrayWithFieldsAndNulls(
			[]arrow.Array{child}, fields, nil, 0, offset)
		require.ErrorIs(t, err, arrow.ErrInvalid)
		require.Nil(t, arr)
	}
}

func TestStructStringMasksParentValidityAtPhysicalOffset(t *testing.T) {
	fields := []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}
	parentNulls := memory.NewBufferBytes([]byte{0x05})

	t.Run("child validity bitmap", func(t *testing.T) {
		values := make([]int32, 11)
		for i := range values {
			values[i] = int32(i)
		}
		validity := memory.NewBufferBytes([]byte{0xff, 0x07})
		valueBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(values))
		childData := array.NewData(arrow.PrimitiveTypes.Int32, len(values), []*memory.Buffer{
			validity, valueBuffer,
		}, nil, 0, 0)
		defer childData.Release()
		child := array.MakeFromData(childData)
		defer child.Release()
		childSlice := array.NewSlice(child, 8, 11)
		defer childSlice.Release()

		arr, err := array.NewStructArrayWithFieldsAndNulls(
			[]arrow.Array{childSlice}, fields, parentNulls, 1, 1)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, "{[(null) 10]}", arr.String())
	})

	t.Run("child without validity bitmap", func(t *testing.T) {
		builder := array.NewInt32Builder(memory.DefaultAllocator)
		defer builder.Release()
		values := make([]int32, 11)
		for i := range values {
			values[i] = int32(i)
		}
		builder.AppendValues(values, nil)
		child := builder.NewArray()
		defer child.Release()
		childSlice := array.NewSlice(child, 8, 11)
		defer childSlice.Release()

		arr, err := array.NewStructArrayWithFieldsAndNulls(
			[]arrow.Array{childSlice}, fields, parentNulls, 1, 1)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, "{[(null) 10]}", arr.String())
	})

	t.Run("identical bitmap with different physical offsets", func(t *testing.T) {
		values := []int32{10, 11, 12, 13}
		validity := memory.NewBufferBytes([]byte{0x0d})
		valueBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(values))
		childData := array.NewData(arrow.PrimitiveTypes.Int32, len(values), []*memory.Buffer{
			validity, valueBuffer,
		}, nil, 0, 0)
		defer childData.Release()
		child := array.MakeFromData(childData)
		defer child.Release()
		childSlice := array.NewSlice(child, 1, 4)
		defer childSlice.Release()
		parentNulls := memory.NewBufferBytes([]byte{0x0d})
		defer parentNulls.Release()

		arr, err := array.NewStructArrayWithFieldsAndNulls(
			[]arrow.Array{childSlice}, fields, parentNulls, 1, 1)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, "{[(null) 13]}", arr.String())
	})
}

func TestStructArrayUnmarshalJSONMissingFields(t *testing.T) {
	pool := memory.NewGoAllocator()

	var (
		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
			{
				Name: "f3", Type: arrow.StructOf(
					[]arrow.Field{
						{Name: "f3_1", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_2", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_3", Type: arrow.BinaryTypes.String, Nullable: false},
					}...,
				),
			},
		}
		dtype = arrow.StructOf(fields...)
	)

	tests := []struct {
		name      string
		jsonInput string
		want      string
		panicErr  error
	}{
		{
			name:      "missing required field",
			jsonInput: `[{"f2": 3, "f3": {"f3_1": "test"}}]`,
			panicErr:  errors.New("arrow/array: index out of range"),
			want:      "",
		},
		{
			name:      "missing optional fields",
			jsonInput: `[{"f2": 3, "f3": {"f3_3": "test"}}]`,
			panicErr:  nil,
			want:      `{[(null)] [3] {[(null)] [(null)] ["test"]}}`,
		},
		{
			name:      "explicit null in required field",
			jsonInput: `[{"f2": 3, "f3": {"f3_3": null}}]`,
			panicErr:  errors.New("field 'f3_3' is non-nullable but got null"),
			want:      "",
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {
				sb := array.NewStructBuilder(pool, dtype)
				defer sb.Release()

				defer func() {
					e := recover()
					if e == nil && tc.panicErr != nil {
						t.Fatalf("did not panic, expected panic: %v", tc.panicErr)
					} else if e != nil && tc.panicErr == nil {
						t.Fatalf("unexpected panic: %v", e)
					} else if e != nil && tc.panicErr != nil && fmt.Errorf("%s", e).Error() != tc.panicErr.Error() {
						t.Fatalf("invalid error. got=%v, want=%v", e, tc.panicErr.Error())
					}
				}()

				err := sb.UnmarshalJSON([]byte(tc.jsonInput))
				if err != nil {
					panic(err)
				}

				arr := sb.NewArray().(*array.Struct)
				defer arr.Release()

				got := arr.String()
				if got != tc.want {
					t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, tc.want)
				}
			},
		)
	}
}

func TestCreateStructWithNulls(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	sb.AppendNulls(100)

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	assert.EqualValues(t, 100, arr.Len())
	assert.EqualValues(t, 100, arr.NullN())

	arr2, err := array.NewStructArrayWithFieldsAndNulls(
		[]arrow.Array{arr.Field(0), arr.Field(1)}, fields, arr.Data().Buffers()[0], arr.NullN(), 0)
	require.NoError(t, err)
	defer arr2.Release()

	assert.True(t, array.Equal(arr, arr2))
}
