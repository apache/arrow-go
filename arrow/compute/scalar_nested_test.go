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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type denseUnionExtensionArray struct {
	array.ExtensionArrayBase
}

func (a *denseUnionExtensionArray) ValueStr(i int) string {
	if a.IsNull(i) {
		return array.NullValueStr
	}
	return "dense_union"
}

type denseUnionExtensionType struct {
	arrow.ExtensionBase
}

func (denseUnionExtensionType) ArrayType() reflect.Type {
	return reflect.TypeOf(denseUnionExtensionArray{})
}

func (denseUnionExtensionType) ExtensionName() string {
	return "compute-test.dense-union"
}

func (t *denseUnionExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	rhs, ok := other.(*denseUnionExtensionType)
	return ok && arrow.TypeEqual(t.StorageType(), rhs.StorageType())
}

func (denseUnionExtensionType) Serialize() string { return "" }

func (t *denseUnionExtensionType) Deserialize(storage arrow.DataType, _ string) (arrow.ExtensionType, error) {
	return &denseUnionExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: storage}}, nil
}

type runEndExtensionArray struct {
	array.ExtensionArrayBase
}

func (a *runEndExtensionArray) ValueStr(i int) string {
	return a.Storage().ValueStr(i)
}

type runEndExtensionType struct {
	arrow.ExtensionBase
}

func (runEndExtensionType) ArrayType() reflect.Type {
	return reflect.TypeOf(runEndExtensionArray{})
}

func (runEndExtensionType) ExtensionName() string {
	return "compute-test.run-end"
}

func (t *runEndExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	rhs, ok := other.(*runEndExtensionType)
	return ok && arrow.TypeEqual(t.StorageType(), rhs.StorageType())
}

func (runEndExtensionType) Serialize() string { return "" }

func (t *runEndExtensionType) Deserialize(storage arrow.DataType, _ string) (arrow.ExtensionType, error) {
	return &runEndExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: storage}}, nil
}

func listElementInput(t *testing.T, mem memory.Allocator, typ arrow.DataType, values string) arrow.Array {
	arr, _, err := array.FromJSON(mem, typ, strings.NewReader(values))
	require.NoError(t, err)
	return arr
}

func TestListElement(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	types := []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32),
	}
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			input := listElementInput(t, mem, typ, `[[1, 2], [3, 4], null, [5, 6]]`)
			defer input.Release()
			expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[2, 4, null, 6]`)
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
		})
	}
}

func TestListElementAllIntegerIndexTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	listTypes := []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32),
	}
	indexes := []struct {
		name   string
		typ    arrow.DataType
		scalar scalar.Scalar
	}{
		{name: "int8", typ: arrow.PrimitiveTypes.Int8, scalar: scalar.NewInt8Scalar(1)},
		{name: "int16", typ: arrow.PrimitiveTypes.Int16, scalar: scalar.NewInt16Scalar(1)},
		{name: "int32", typ: arrow.PrimitiveTypes.Int32, scalar: scalar.NewInt32Scalar(1)},
		{name: "int64", typ: arrow.PrimitiveTypes.Int64, scalar: scalar.NewInt64Scalar(1)},
		{name: "uint8", typ: arrow.PrimitiveTypes.Uint8, scalar: scalar.NewUint8Scalar(1)},
		{name: "uint16", typ: arrow.PrimitiveTypes.Uint16, scalar: scalar.NewUint16Scalar(1)},
		{name: "uint32", typ: arrow.PrimitiveTypes.Uint32, scalar: scalar.NewUint32Scalar(1)},
		{name: "uint64", typ: arrow.PrimitiveTypes.Uint64, scalar: scalar.NewUint64Scalar(1)},
	}

	for _, listType := range listTypes {
		t.Run(listType.String(), func(t *testing.T) {
			input := listElementInput(t, mem, listType, `[[10, 20], null]`)
			defer input.Release()
			expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[20, null]`)
			defer expected.Release()

			for _, tc := range indexes {
				tc := tc
				t.Run(tc.name+" scalar", func(t *testing.T) {
					result, err := compute.ListElement(
						context.Background(),
						&compute.ArrayDatum{Value: input.Data()},
						&compute.ScalarDatum{Value: tc.scalar},
					)
					require.NoError(t, err)
					defer result.Release()

					actual := result.(*compute.ArrayDatum).MakeArray()
					defer actual.Release()
					require.NoError(t, array.ValidateFull(actual))
					assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
				})

				t.Run(tc.name+" array", func(t *testing.T) {
					singleInput := listElementInput(t, mem, listType, `[[10, 20]]`)
					defer singleInput.Release()
					index := listElementInput(t, mem, tc.typ, `[1]`)
					defer index.Release()
					expectedSingle := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[20]`)
					defer expectedSingle.Release()

					result, err := compute.ListElement(
						context.Background(),
						&compute.ArrayDatum{Value: singleInput.Data()},
						&compute.ArrayDatum{Value: index.Data()},
					)
					require.NoError(t, err)
					defer result.Release()

					actual := result.(*compute.ArrayDatum).MakeArray()
					defer actual.Release()
					require.NoError(t, array.ValidateFull(actual))
					assert.True(t, array.Equal(expectedSingle, actual), "expected: %s\ngot: %s", expectedSingle, actual)
				})
			}
		})
	}
}

func TestListElementNumericChildren(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	childTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.FixedWidthTypes.Float16,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}
	for _, childType := range childTypes {
		t.Run(childType.String(), func(t *testing.T) {
			listTypes := []arrow.DataType{
				arrow.ListOf(childType),
				arrow.LargeListOf(childType),
				arrow.ListViewOf(childType),
				arrow.LargeListViewOf(childType),
				arrow.FixedSizeListOf(2, childType),
			}
			for _, listType := range listTypes {
				t.Run(listType.String(), func(t *testing.T) {
					input := listElementInput(t, mem, listType, `[[1, 2], [3, 4], null, [5, 6]]`)
					defer input.Release()
					expected := listElementInput(t, mem, childType, `[2, 4, null, 6]`)
					defer expected.Release()

					result, err := compute.ListElement(
						context.Background(),
						&compute.ArrayDatum{Value: input.Data()},
						&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
					)
					require.NoError(t, err)
					defer result.Release()

					actual := result.(*compute.ArrayDatum).MakeArray()
					defer actual.Release()
					require.NoError(t, array.ValidateFull(actual))
					assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
				})
			}
		})
	}
}

func TestListElementPreservesChildNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, null], [2, 3]]`)
	defer input.Release()
	expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[null, 3]`)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt8Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementSlicedInputs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32),
	}
	for _, typ := range tests {
		t.Run(typ.String(), func(t *testing.T) {
			input := listElementInput(t, mem, typ, `[[0, 1], [2, 3], [4, 5], [6, 7]]`)
			defer input.Release()
			sliced := array.NewSlice(input, 1, 3)
			defer sliced.Release()
			expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[3, 5]`)
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: sliced.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)

		})
	}
}

func TestListElementListViewUsesSizes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		input arrow.Array
	}{
		{name: "list view", input: makeListViewWithOutOfOrderOffsets(mem)},
		{name: "large list view", input: makeLargeListViewWithOutOfOrderOffsets(mem)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.input.Release()
			expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[14, 10, 12]`)
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: tc.input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)

			sliced := array.NewSlice(tc.input, 1, int64(tc.input.Len()))
			defer sliced.Release()
			slicedExpected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[10, 12]`)
			defer slicedExpected.Release()
			slicedResult, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: sliced.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			require.NoError(t, err)
			defer slicedResult.Release()
			slicedActual := slicedResult.(*compute.ArrayDatum).MakeArray()
			defer slicedActual.Release()
			assert.True(t, array.Equal(slicedExpected, slicedActual), "expected: %s\ngot: %s", slicedExpected, slicedActual)
		})
	}
}

func TestListElementRejectsInvalidListViewOffsets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		build func(memory.Allocator) arrow.Array
	}{
		{"list view negative offset", func(mem memory.Allocator) arrow.Array {
			builder := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
			builder.ValueBuilder().(*array.Int32Builder).Append(7)
			builder.AppendDimensions(-1, 1)
			result := builder.NewArray()
			builder.Release()
			return result
		}},
		{"list view child overflow", func(mem memory.Allocator) arrow.Array {
			builder := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
			builder.ValueBuilder().(*array.Int32Builder).Append(7)
			builder.AppendDimensions(1, 1)
			result := builder.NewArray()
			builder.Release()
			return result
		}},
		{"large list view negative offset", func(mem memory.Allocator) arrow.Array {
			builder := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
			builder.ValueBuilder().(*array.Int32Builder).Append(7)
			builder.AppendDimensions(-1, 1)
			result := builder.NewArray()
			builder.Release()
			return result
		}},
		{"large list view child overflow", func(mem memory.Allocator) arrow.Array {
			builder := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
			builder.ValueBuilder().(*array.Int32Builder).Append(7)
			builder.AppendDimensions(1, 1)
			result := builder.NewArray()
			builder.Release()
			return result
		}},
		{"large list view offset overflow", func(mem memory.Allocator) arrow.Array {
			builder := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
			builder.AppendDimensions(int(^uint(0)>>1), 1)
			result := builder.NewArray()
			builder.Release()
			return result
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.build(mem)
			defer input.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestListElementRejectsFixedSizeListOffsetOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	valuesBuilder := array.NewInt32Builder(mem)
	valuesBuilder.AppendValues([]int32{10, 20, 30, 40}, nil)
	values := valuesBuilder.NewArray()
	valuesBuilder.Release()
	defer values.Release()

	const offset = int64(6148914691236517205)
	typ := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32)
	data := array.NewData(typ, 1, []*memory.Buffer{nil}, []arrow.ArrayData{values.Data()}, 0, int(offset))
	input := array.NewFixedSizeListData(data)
	data.Release()
	defer input.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

func makeListViewWithOutOfOrderOffsets(mem memory.Allocator) arrow.Array {
	builder := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	values := builder.ValueBuilder().(*array.Int32Builder)
	values.AppendValues([]int32{10, 11, 12, 13, 14, 15}, nil)
	builder.AppendDimensions(4, 2)
	builder.AppendDimensions(0, 1)
	builder.AppendDimensions(2, 2)
	result := builder.NewArray()
	builder.Release()
	return result
}

func makeLargeListViewWithOutOfOrderOffsets(mem memory.Allocator) arrow.Array {
	builder := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	values := builder.ValueBuilder().(*array.Int32Builder)
	values.AppendValues([]int32{10, 11, 12, 13, 14, 15}, nil)
	builder.AppendDimensions(4, 2)
	builder.AppendDimensions(0, 1)
	builder.AppendDimensions(2, 2)
	result := builder.NewArray()
	builder.Release()
	return result
}

func TestListElementNestedListViewChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name    string
		build   func(memory.Allocator) arrow.Array
		elemTyp arrow.DataType
	}{
		{"list view", func(mem memory.Allocator) arrow.Array {
			outer := array.NewListBuilder(mem, arrow.ListViewOf(arrow.PrimitiveTypes.Int32))
			inner := outer.ValueBuilder().(*array.ListViewBuilder)
			values := inner.ValueBuilder().(*array.Int32Builder)
			outer.Append(true)
			values.AppendValues([]int32{10, 11, 20, 21}, nil)
			inner.AppendDimensions(0, 2)
			inner.AppendDimensions(2, 2)
			result := outer.NewArray()
			outer.Release()
			return result
		}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{"large list view", func(mem memory.Allocator) arrow.Array {
			outer := array.NewListBuilder(mem, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32))
			inner := outer.ValueBuilder().(*array.LargeListViewBuilder)
			values := inner.ValueBuilder().(*array.Int32Builder)
			outer.Append(true)
			values.AppendValues([]int32{10, 11, 20, 21}, nil)
			inner.AppendDimensions(0, 2)
			inner.AppendDimensions(2, 2)
			result := outer.NewArray()
			outer.Release()
			return result
		}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.build(mem)
			defer input.Release()
			expected := listElementInput(t, mem, tc.elemTyp, `[[20, 21]]`)
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			require.NoError(t, array.ValidateFull(actual))
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
		})
	}
}

func TestListElementEmptyNestedListViews(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, elemType := range []arrow.DataType{
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
	} {
		t.Run(elemType.String(), func(t *testing.T) {
			builder := array.NewListBuilder(mem, elemType)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			require.Equal(t, 0, actual.Len())
			require.True(t, arrow.TypeEqual(elemType, actual.DataType()))
			require.NoError(t, array.ValidateFull(actual))
		})
	}
}

func TestListElementEmptyRunEndEncodedChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	elemType := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	builder := array.NewListBuilder(mem, elemType)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	require.Equal(t, 0, actual.Len())
	require.True(t, arrow.TypeEqual(elemType, actual.DataType()))
	require.NoError(t, array.ValidateFull(actual))
}

func TestListElementNullParentEmptyListViews(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, elemType := range []arrow.DataType{
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
	} {
		t.Run(elemType.String(), func(t *testing.T) {
			builder := array.NewListBuilder(mem, elemType)
			builder.AppendNull()
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			require.Equal(t, 1, actual.Len())
			require.Equal(t, 1, actual.NullN())
			require.True(t, actual.IsNull(0))
			require.True(t, arrow.TypeEqual(elemType, actual.DataType()))
			require.NoError(t, array.ValidateFull(actual))
		})
	}
}

func TestListElementScalarListWithViewElement(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, values := range []arrow.Array{
		makeListViewWithOutOfOrderOffsets(mem),
		makeLargeListViewWithOutOfOrderOffsets(mem),
	} {
		t.Run(values.DataType().String(), func(t *testing.T) {
			defer values.Release()
			lists := scalar.NewListScalar(values)
			defer lists.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ScalarDatum{Value: lists},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func makeListElementViewValues(mem memory.Allocator, binary bool) arrow.Array {
	values := []string{strings.Repeat("a", 32), strings.Repeat("b", 32)}
	if binary {
		builder := array.NewBinaryViewBuilder(mem)
		builder.SetBlockSize(1)
		for _, value := range values {
			builder.Append([]byte(value))
		}
		result := builder.NewArray()
		builder.Release()
		return result
	}

	builder := array.NewStringViewBuilder(mem)
	builder.SetBlockSize(1)
	for _, value := range values {
		builder.Append(value)
	}
	result := builder.NewArray()
	builder.Release()
	return result
}

func makeListElementArrayWithChild(mem memory.Allocator, elemType arrow.DataType, child arrow.Array) arrow.Array {
	offsetsBuilder := array.NewInt32Builder(mem)
	offsetsBuilder.AppendValues([]int32{0, 1, 2}, nil)
	offsets := offsetsBuilder.NewArray()
	offsetsBuilder.Release()

	data := array.NewData(
		arrow.ListOf(elemType),
		2,
		[]*memory.Buffer{nil, offsets.Data().Buffers()[1]},
		[]arrow.ArrayData{child.Data()},
		0,
		0,
	)
	result := array.NewListData(data)
	data.Release()
	offsets.Release()
	child.Release()
	return result
}

func TestListElementRejectsNestedViewChildren(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	viewType := arrow.BinaryTypes.StringView
	tests := []struct {
		name  string
		build func() arrow.Array
	}{
		{
			name: "string_view",
			build: func() arrow.Array {
				return makeListElementArrayWithChild(mem, viewType, makeListElementViewValues(mem, false))
			},
		},
		{
			name: "binary_view",
			build: func() arrow.Array {
				return makeListElementArrayWithChild(mem, arrow.BinaryTypes.BinaryView, makeListElementViewValues(mem, true))
			},
		},
		{
			name: "struct_string_view",
			build: func() arrow.Array {
				typ := arrow.StructOf(arrow.Field{Name: "value", Type: viewType, Nullable: true})
				builder := array.NewStructBuilder(mem, typ)
				values := builder.FieldBuilder(0).(*array.StringViewBuilder)
				values.SetBlockSize(1)
				for _, value := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
					builder.Append(true)
					values.Append(value)
				}
				child := builder.NewArray()
				builder.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
		{
			name: "list_string_view",
			build: func() arrow.Array {
				typ := arrow.ListOf(viewType)
				builder := array.NewListBuilder(mem, viewType)
				values := builder.ValueBuilder().(*array.StringViewBuilder)
				values.SetBlockSize(1)
				for _, value := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
					builder.Append(true)
					values.Append(value)
				}
				child := builder.NewArray()
				builder.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
		{
			name: "fixed_size_list_string_view",
			build: func() arrow.Array {
				typ := arrow.FixedSizeListOf(2, viewType)
				builder := array.NewFixedSizeListBuilder(mem, 2, viewType)
				values := builder.ValueBuilder().(*array.StringViewBuilder)
				values.SetBlockSize(1)
				for _, value := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
					builder.Append(true)
					values.Append(value)
					values.Append(value)
				}
				child := builder.NewArray()
				builder.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
		{
			name: "dictionary_string_view",
			build: func() arrow.Array {
				typ := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: viewType}
				values := makeListElementViewValues(mem, false)
				indicesBuilder := array.NewInt8Builder(mem)
				indicesBuilder.AppendValues([]int8{0, 1}, nil)
				indices := indicesBuilder.NewArray()
				indicesBuilder.Release()
				child := array.NewDictionaryArray(typ, indices, values)
				indices.Release()
				values.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
		{
			name: "run_end_encoded_string_view",
			build: func() arrow.Array {
				typ := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, viewType)
				builder := array.NewRunEndEncodedBuilder(mem, typ.RunEnds(), typ.Encoded())
				values := builder.ValueBuilder().(*array.StringViewBuilder)
				values.SetBlockSize(1)
				for _, value := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
					builder.Append(1)
					values.Append(value)
				}
				child := builder.NewArray()
				builder.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
		{
			name: "extension_string_view",
			build: func() arrow.Array {
				typ := &denseUnionExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: viewType}}
				storage := makeListElementViewValues(mem, false)
				child := array.NewExtensionArrayWithStorage(typ, storage)
				storage.Release()
				return makeListElementArrayWithChild(mem, typ, child)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.build()
			defer input.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func TestListElementDecimalArrayChildren(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name string
		typ  arrow.DataType
		add  func(array.Builder, int32)
	}{
		{
			name: "decimal32",
			typ:  &arrow.Decimal32Type{Precision: 6, Scale: 2},
			add: func(builder array.Builder, value int32) {
				builder.(*array.Decimal32Builder).Append(decimal.Decimal32(value))
			},
		},
		{
			name: "decimal64",
			typ:  &arrow.Decimal64Type{Precision: 12, Scale: 2},
			add: func(builder array.Builder, value int32) {
				builder.(*array.Decimal64Builder).Append(decimal.Decimal64(value))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listBuilder := array.NewListBuilder(mem, tc.typ)
			values := listBuilder.ValueBuilder()
			for _, row := range [][]int32{{1, 2}, {3, 4}} {
				listBuilder.Append(true)
				for _, value := range row {
					tc.add(values, value)
				}
			}
			input := listBuilder.NewArray()
			listBuilder.Release()
			defer input.Release()

			expectedBuilder := array.NewBuilder(mem, tc.typ)
			tc.add(expectedBuilder, 2)
			tc.add(expectedBuilder, 4)
			expected := expectedBuilder.NewArray()
			expectedBuilder.Release()
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			require.NoError(t, array.ValidateFull(actual))
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
		})
	}
}

func TestListElementScalarListWithUnsupportedDecimalValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name string
		typ  arrow.DataType
	}{
		{name: "decimal32", typ: &arrow.Decimal32Type{Precision: 6, Scale: 2}},
		{name: "decimal64", typ: &arrow.Decimal64Type{Precision: 12, Scale: 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewBuilder(mem, tc.typ)
			switch b := builder.(type) {
			case *array.Decimal32Builder:
				b.Append(decimal.Decimal32(123))
			case *array.Decimal64Builder:
				b.Append(decimal.Decimal64(123))
			default:
				t.Fatalf("unexpected builder type %T", builder)
			}
			values := builder.NewArray()
			builder.Release()
			defer values.Release()

			list := scalar.NewListScalar(values)
			defer list.Release()
			result, err := compute.ListElement(
				context.Background(),
				&compute.ScalarDatum{Value: list},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func TestListElementSingleIndexArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, 2]]`)
	defer input.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[1]`)
	defer index.Release()
	expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[2]`)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ArrayDatum{Value: index.Data()},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementSingleIndexArrayRejectsMismatchedLengths(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, 2], [3, 4]]`)
	defer input.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[1]`)
	defer index.Release()
	result, err := compute.CallFunction(
		context.Background(),
		"list_element",
		nil,
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ArrayDatum{Value: index.Data()},
	)
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestListElementDispatchBest(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	CheckDispatchBest(t, "list_element",
		[]arrow.DataType{listType, arrow.PrimitiveTypes.Int64},
		[]arrow.DataType{listType, arrow.PrimitiveTypes.Int64})
}

func TestListElementFunctionDoc(t *testing.T) {
	fn, ok := compute.GetFunctionRegistry().GetFunction("list_element")
	require.True(t, ok)
	require.NoError(t, fn.Validate())
}

func TestListElementRejectsMultipleIndicesIndependentOfExecutionSpans(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[10, 11], [20, 21]]`)
	defer input.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[0, 1]`)
	defer index.Release()

	chunk0 := array.NewSlice(input, 0, 1)
	defer chunk0.Release()
	chunk1 := array.NewSlice(input, 1, 2)
	defer chunk1.Release()
	chunkedLists := arrow.NewChunked(input.DataType(), []arrow.Array{chunk0, chunk1})
	defer chunkedLists.Release()

	execCtx := compute.DefaultExecCtx()
	execCtx.ChunkSize = 1

	tests := []struct {
		name  string
		ctx   context.Context
		lists compute.Datum
	}{
		{name: "regular execution span", ctx: context.Background(), lists: &compute.ArrayDatum{Value: input.Data()}},
		{name: "chunk size one", ctx: compute.SetExecCtx(context.Background(), execCtx), lists: &compute.ArrayDatum{Value: input.Data()}},
		{name: "chunked lists", ctx: context.Background(), lists: &compute.ChunkedDatum{Value: chunkedLists}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := compute.ListElement(
				tc.ctx,
				tc.lists,
				&compute.ArrayDatum{Value: index.Data()},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func TestListElementSingleIndexChunkedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, 2]]`)
	defer input.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[1]`)
	defer index.Release()
	chunkedIndex := arrow.NewChunked(index.DataType(), []arrow.Array{index})
	defer chunkedIndex.Release()
	expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[2]`)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ChunkedDatum{Value: chunkedIndex},
	)
	require.NoError(t, err)
	defer result.Release()

	chunkedResult, ok := result.(*compute.ChunkedDatum)
	require.True(t, ok)
	require.Len(t, chunkedResult.Value.Chunks(), 1)
	assert.True(t, array.Equal(expected, chunkedResult.Value.Chunk(0)),
		"expected: %s\ngot: %s", expected, chunkedResult.Value.Chunk(0))
}

func TestListElementScalarList(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[10, 20]`)
	defer values.Release()
	list := scalar.NewListScalar(values)
	defer list.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ScalarDatum{Value: list},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ScalarDatum).Value
	assert.True(t, scalar.Equals(scalar.NewInt32Scalar(20), actual), "expected: 20\ngot: %s", actual)
}

func TestListElementRejectsNonListScalar(t *testing.T) {
	result, err := compute.ListElement(
		context.Background(),
		&compute.ScalarDatum{Value: scalar.NewInt32Scalar(7)},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, arrow.ErrType)
}

func TestListElementRejectsNilScalarIndex(t *testing.T) {
	result, err := compute.ListElement(
		context.Background(),
		compute.EmptyDatum{},
		&compute.ScalarDatum{},
	)
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, arrow.ErrType)
}

func TestListElementScalarListWithArrayIndex(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[10, 20]`)
	defer values.Release()
	list := scalar.NewListScalar(values)
	defer list.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[1]`)
	defer index.Release()
	expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[20]`)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ScalarDatum{Value: list},
		&compute.ArrayDatum{Value: index.Data()},
	)
	require.NoError(t, err)
	defer result.Release()

	arrayResult, ok := result.(*compute.ArrayDatum)
	require.True(t, ok)
	actual := arrayResult.MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		input string
		index scalar.Scalar
	}{
		{name: "out of bounds", input: `[[1], [2, 3]]`, index: scalar.NewInt64Scalar(1)},
		{name: "empty list", input: `[[], [1]]`, index: scalar.NewInt64Scalar(0)},
		{name: "negative index", input: `[[1]]`, index: scalar.NewInt64Scalar(-1)},
		{name: "large unsigned index", input: `[[1]]`, index: scalar.NewUint64Scalar(^uint64(0))},
		{name: "null index", input: `[[1]]`, index: scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), tc.input)
			defer input.Release()
			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: tc.index},
			)
			if err == nil && result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestListElementComplexChildren(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name     string
		typ      arrow.DataType
		input    string
		expected string
	}{
		{name: "string", typ: arrow.ListOf(arrow.BinaryTypes.String), input: `[["a", "b"], ["c", "d"]]`, expected: `["b", "d"]`},
		{name: "nested list", typ: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32)), input: `[[[1, 2], [3]], [[4], [5, 6]]]`, expected: `[[3], [5, 6]]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := listElementInput(t, mem, tc.typ, tc.input)
			defer input.Release()
			expected := listElementInput(t, mem, tc.typ.(arrow.ListLikeType).Elem(), tc.expected)
			defer expected.Release()

			result, err := compute.ListElement(
				context.Background(),
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ScalarDatum{Value: scalar.NewUint8Scalar(1)},
			)
			require.NoError(t, err)
			defer result.Release()

			actual := result.(*compute.ArrayDatum).MakeArray()
			defer actual.Release()
			assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
		})
	}
}

func TestListElementMonthDayNanoIntervalChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := arrow.ListOf(arrow.FixedWidthTypes.MonthDayNanoInterval)
	input := listElementInput(t, mem, typ,
		`[[{"months": 1, "days": 2, "nanoseconds": 3}, {"months": 4, "days": 5, "nanoseconds": 6}], [{"months": 7, "days": 8, "nanoseconds": 9}, {"months": 10, "days": 11, "nanoseconds": 12}]]`)
	defer input.Release()
	expected := listElementInput(t, mem, arrow.FixedWidthTypes.MonthDayNanoInterval,
		`[{"months": 4, "days": 5, "nanoseconds": 6}, {"months": 10, "days": 11, "nanoseconds": 12}]`)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementDenseUnionChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(10)
	values.Append(1)
	values.Child(1).(*array.StringBuilder).Append("a")
	builder.Append(true)
	values.Append(1)
	values.Child(1).(*array.StringBuilder).Append("b")
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(20)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewDenseUnionBuilder(mem, unionType)
	expectedBuilder.Append(1)
	expectedBuilder.Child(1).(*array.StringBuilder).Append("a")
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.Int32Builder).Append(20)
	expected := expectedBuilder.NewArray()
	expectedBuilder.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementDenseUnionExtensionChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)
	extType := &denseUnionExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: unionType}}

	builder := array.NewListBuilder(mem, extType)
	values := builder.ValueBuilder().(*array.ExtensionBuilder).StorageBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(10)
	values.Append(1)
	values.Child(1).(*array.StringBuilder).Append("a")
	builder.Append(true)
	values.Append(1)
	values.Child(1).(*array.StringBuilder).Append("b")
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(20)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewDenseUnionBuilder(mem, unionType)
	expectedBuilder.Append(1)
	expectedBuilder.Child(1).(*array.StringBuilder).Append("a")
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.Int32Builder).Append(20)
	expectedStorage := expectedBuilder.NewArray()
	expectedBuilder.Release()
	expected := array.NewExtensionArrayWithStorage(extType, expectedStorage)
	expectedStorage.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	require.True(t, arrow.TypeEqual(extType, actual.DataType()))
	storage := actual.(array.ExtensionArray).Storage()
	require.NoError(t, array.ValidateFull(storage))
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementExtensionRunEndEncodedNullParent(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	storageType := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	extType := &runEndExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: storageType}}

	builder := array.NewListBuilder(mem, extType)
	builder.AppendNull()
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	require.True(t, arrow.TypeEqual(extType, actual.DataType()))
	require.NoError(t, array.ValidateFull(actual))
	storage := actual.(array.ExtensionArray).Storage()
	require.True(t, storage.(*array.RunEndEncoded).Values().IsNull(0))
	require.NoError(t, array.ValidateFull(storage))
}

func TestListElementDenseUnionWithUnusedGenericChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(10)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(20)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewDenseUnionBuilder(mem, unionType)
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.Int32Builder).Append(10)
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.Int32Builder).Append(20)
	expected := expectedBuilder.NewArray()
	expectedBuilder.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementDenseUnionScalarWithUnusedUnsupportedChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "values", Type: arrow.ListViewOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(42)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	listValue, err := scalar.GetScalar(input, 0)
	require.NoError(t, err)
	defer listValue.(scalar.Releasable).Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ScalarDatum{Value: listValue},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ScalarDatum).Value
	require.Equal(t, arrow.DENSE_UNION, actual.DataType().ID())
	assert.Equal(t, int32(42), actual.(scalar.Union).ChildValue().(*scalar.Int32).Value)
}

func TestListElementDenseUnionScalarWithActiveUnsupportedChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{{Name: "values", Type: arrow.ListViewOf(arrow.PrimitiveTypes.Int32), Nullable: true}},
		[]arrow.UnionTypeCode{0},
	)
	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	listBuilder := values.Child(0).(*array.ListViewBuilder)
	builder.Append(true)
	values.Append(0)
	listBuilder.ValueBuilder().(*array.Int32Builder).Append(9)
	listBuilder.AppendDimensions(0, 1)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	listValue := scalar.NewListScalar(input.(*array.List).ListValues())
	defer listValue.Release()
	result, err := compute.ListElement(
		context.Background(),
		&compute.ScalarDatum{Value: listValue},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, arrow.ErrNotImplemented)
}

func TestListElementDenseUnionScalarWithActiveUnsupportedDecimalChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, tc := range []struct {
		name string
		typ  arrow.DataType
		add  func(array.Builder)
	}{
		{
			name: "decimal32",
			typ:  &arrow.Decimal32Type{Precision: 6, Scale: 2},
			add: func(builder array.Builder) {
				builder.(*array.Decimal32Builder).Append(decimal.Decimal32(123))
			},
		},
		{
			name: "decimal64",
			typ:  &arrow.Decimal64Type{Precision: 12, Scale: 2},
			add: func(builder array.Builder) {
				builder.(*array.Decimal64Builder).Append(decimal.Decimal64(123))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unionType := arrow.DenseUnionOf(
				[]arrow.Field{{Name: "value", Type: tc.typ, Nullable: true}},
				[]arrow.UnionTypeCode{0},
			)
			builder := array.NewListBuilder(mem, unionType)
			values := builder.ValueBuilder().(*array.DenseUnionBuilder)
			builder.Append(true)
			values.Append(0)
			tc.add(values.Child(0))
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			listValue := scalar.NewListScalar(input.(*array.List).ListValues())
			defer listValue.Release()
			result, err := compute.ListElement(
				context.Background(),
				&compute.ScalarDatum{Value: listValue},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func TestListElementDenseUnionScalarWithActiveNullUnsupportedChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, tc := range []struct {
		name string
		typ  arrow.DataType
	}{
		{
			name: "binary_view",
			typ:  arrow.BinaryTypes.BinaryView,
		},
		{
			name: "string_view",
			typ:  arrow.BinaryTypes.StringView,
		},
		{
			name: "decimal32",
			typ:  &arrow.Decimal32Type{Precision: 6, Scale: 2},
		},
		{
			name: "decimal64",
			typ:  &arrow.Decimal64Type{Precision: 12, Scale: 2},
		},
		{
			name: "list_view",
			typ:  arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		},
		{
			name: "large_list_view",
			typ:  arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unionType := arrow.DenseUnionOf(
				[]arrow.Field{{Name: "value", Type: tc.typ, Nullable: true}},
				[]arrow.UnionTypeCode{0},
			)
			builder := array.NewListBuilder(mem, unionType)
			values := builder.ValueBuilder().(*array.DenseUnionBuilder)
			builder.Append(true)
			values.Append(0)
			values.Child(0).AppendNull()
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			listValue := scalar.NewListScalar(input.(*array.List).ListValues())
			defer listValue.Release()
			result, err := compute.ListElement(
				context.Background(),
				&compute.ScalarDatum{Value: listValue},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
			)
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}
}

func TestListElementDenseUnionExtensionScalarWithUnusedUnsupportedChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "values", Type: arrow.ListViewOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)
	extType := &denseUnionExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: unionType}}

	builder := array.NewListBuilder(mem, extType)
	values := builder.ValueBuilder().(*array.ExtensionBuilder).StorageBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).Append(42)
	values.Append(0)
	values.Child(0).(*array.Int32Builder).AppendNull()
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	listValue, err := scalar.GetScalar(input, 0)
	require.NoError(t, err)
	defer listValue.(scalar.Releasable).Release()

	for i := int64(0); i < 2; i++ {
		result, err := compute.ListElement(
			context.Background(),
			&compute.ScalarDatum{Value: listValue},
			&compute.ScalarDatum{Value: scalar.NewInt64Scalar(i)},
		)
		require.NoError(t, err)
		defer result.Release()

		actual := result.(*compute.ScalarDatum).Value
		require.True(t, arrow.TypeEqual(extType, actual.DataType()))
		assert.Equal(t, i == 0, actual.IsValid())
		assert.NoError(t, actual.ValidateFull())
		if i == 0 {
			assert.Equal(t, int32(42), actual.(*scalar.Extension).Value.(scalar.Union).ChildValue().(*scalar.Int32).Value)
		}
	}
}

func TestListElementDenseUnionMonthDayNanoIntervalChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "interval", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3})
	builder.Append(true)
	values.Append(0)
	values.Child(0).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 4, Days: 5, Nanoseconds: 6})
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewDenseUnionBuilder(mem, unionType)
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3})
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 4, Days: 5, Nanoseconds: 6})
	expected := expectedBuilder.NewArray()
	expectedBuilder.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementSparseUnionChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	unionType := arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.SparseUnionBuilder)
	appendValue := func(code arrow.UnionTypeCode, number int32, text string) {
		values.Append(code)
		if code == 0 {
			values.Child(0).(*array.Int32Builder).Append(number)
			values.Child(1).(*array.StringBuilder).AppendNull()
		} else {
			values.Child(0).(*array.Int32Builder).AppendNull()
			values.Child(1).(*array.StringBuilder).Append(text)
		}
	}
	builder.Append(true)
	appendValue(0, 10, "")
	appendValue(1, 0, "a")
	builder.Append(true)
	appendValue(1, 0, "b")
	appendValue(0, 20, "")
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewSparseUnionBuilder(mem, unionType)
	expectedBuilder.Append(1)
	expectedBuilder.Child(0).(*array.Int32Builder).AppendNull()
	expectedBuilder.Child(1).(*array.StringBuilder).Append("a")
	expectedBuilder.Append(0)
	expectedBuilder.Child(0).(*array.Int32Builder).Append(20)
	expectedBuilder.Child(1).(*array.StringBuilder).AppendNull()
	expected := expectedBuilder.NewArray()
	expectedBuilder.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestListElementSparseUnionSlicedValuesChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typeIDsBuilder := array.NewInt8Builder(mem)
	typeIDsBuilder.AppendValues([]int8{0, 1, 0}, nil)
	typeIDs := typeIDsBuilder.NewArray()
	typeIDsBuilder.Release()
	defer typeIDs.Release()

	numbersBuilder := array.NewInt32Builder(mem)
	numbersBuilder.Append(10)
	numbersBuilder.AppendNull()
	numbersBuilder.Append(20)
	numbers := numbersBuilder.NewArray()
	numbersBuilder.Release()
	defer numbers.Release()

	textBuilder := array.NewStringBuilder(mem)
	textBuilder.AppendNull()
	textBuilder.Append("a")
	textBuilder.AppendNull()
	texts := textBuilder.NewArray()
	textBuilder.Release()
	defer texts.Release()

	union, err := array.NewSparseUnionFromArraysWithFieldCodes(
		typeIDs,
		[]arrow.Array{numbers, texts},
		[]string{"number", "text"},
		[]arrow.UnionTypeCode{0, 1},
	)
	require.NoError(t, err)
	defer union.Release()

	slicedUnion := array.NewSlice(union, 1, 3)
	defer slicedUnion.Release()

	offsetsBuilder := array.NewInt32Builder(mem)
	offsetsBuilder.AppendValues([]int32{0, 2}, nil)
	offsets := offsetsBuilder.NewArray()
	offsetsBuilder.Release()
	defer offsets.Release()

	data := array.NewData(
		arrow.ListOf(union.DataType()),
		1,
		[]*memory.Buffer{nil, offsets.Data().Buffers()[1]},
		[]arrow.ArrayData{slicedUnion.Data()},
		0,
		0,
	)
	input := array.NewListData(data)
	data.Release()
	defer input.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray().(*array.SparseUnion)
	defer actual.Release()
	require.NoError(t, array.ValidateFull(actual))
	require.Equal(t, []arrow.UnionTypeCode{1}, actual.RawTypeCodes())
	require.True(t, actual.Field(0).IsNull(0))
	require.Equal(t, "a", actual.Field(1).(*array.String).Value(0))
}

func TestListElementSparseUnionPreservesUnusualChildren(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	encodedType := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
	unionType := arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "dictionary", Type: dictionaryType, Nullable: true},
			{Name: "encoded", Type: encodedType, Nullable: true},
		},
		[]arrow.UnionTypeCode{5, 42, 100},
	)

	builder := array.NewListBuilder(mem, unionType)
	values := builder.ValueBuilder().(*array.SparseUnionBuilder)
	numbers := values.Child(0).(*array.Int32Builder)
	dictionaries := values.Child(1).(*array.BinaryDictionaryBuilder)
	encoded := values.Child(2).(*array.RunEndEncodedBuilder)
	encodedValues := encoded.ValueBuilder().(*array.StringBuilder)
	appendValue := func(code arrow.UnionTypeCode, number int32, dictionary, encodedValue string) {
		values.Append(code)
		if code == 5 {
			numbers.Append(number)
		} else {
			numbers.AppendNull()
		}
		if code == 42 {
			require.NoError(t, dictionaries.AppendString(dictionary))
		} else {
			dictionaries.AppendNull()
		}
		encoded.Append(1)
		if encodedValue == "" {
			encodedValues.AppendNull()
		} else {
			encodedValues.Append(encodedValue)
		}
	}

	builder.Append(true)
	appendValue(5, 10, "", "")
	appendValue(42, 0, "a", "")
	builder.Append(true)
	appendValue(100, 0, "", "inactive")
	appendValue(5, 20, "", "")
	builder.Append(false)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	expectedBuilder := array.NewSparseUnionBuilder(mem, unionType)
	expectedNumbers := expectedBuilder.Child(0).(*array.Int32Builder)
	expectedDictionaries := expectedBuilder.Child(1).(*array.BinaryDictionaryBuilder)
	expectedEncoded := expectedBuilder.Child(2).(*array.RunEndEncodedBuilder)
	expectedEncodedValues := expectedEncoded.ValueBuilder().(*array.StringBuilder)
	expectedBuilder.Append(42)
	expectedNumbers.AppendNull()
	require.NoError(t, expectedDictionaries.AppendString("a"))
	expectedEncoded.Append(1)
	expectedEncodedValues.AppendNull()
	expectedBuilder.Append(5)
	expectedNumbers.Append(20)
	expectedDictionaries.AppendNull()
	expectedEncoded.Append(1)
	expectedEncodedValues.AppendNull()
	expectedBuilder.Append(5)
	expectedNumbers.AppendNull()
	expectedDictionaries.AppendNull()
	expectedEncoded.Append(1)
	expectedEncodedValues.AppendNull()
	expected := expectedBuilder.NewArray()
	expectedBuilder.Release()
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	require.NoError(t, array.ValidateFull(actual))
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)

	sliced := array.NewSlice(input, 1, 3)
	defer sliced.Release()
	slicedResult, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: sliced.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(1)},
	)
	require.NoError(t, err)
	defer slicedResult.Release()
	slicedActual := slicedResult.(*compute.ArrayDatum).MakeArray()
	defer slicedActual.Release()
	require.NoError(t, array.ValidateFull(slicedActual))
	slicedUnion := slicedActual.(*array.SparseUnion)
	assert.Equal(t, []arrow.UnionTypeCode{5, 5}, slicedUnion.RawTypeCodes())
	assert.Equal(t, int32(20), slicedUnion.Field(0).(*array.Int32).Value(0))
	assert.True(t, slicedUnion.Field(0).IsNull(1))
}

func TestListElementNestedDictionaryWithNullParent(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.ListOf(arrow.PrimitiveTypes.Int32),
	}
	dictionaryValuesBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	dictionaryValuesBuilder.Append(true)
	dictionaryValuesBuilder.ValueBuilder().(*array.Int32Builder).Append(7)
	dictionaryValues := dictionaryValuesBuilder.NewArray()
	dictionaryValuesBuilder.Release()
	defer dictionaryValues.Release()

	dictionaryIndicesBuilder := array.NewInt8Builder(mem)
	dictionaryIndicesBuilder.Append(0)
	dictionaryIndices := dictionaryIndicesBuilder.NewArray()
	dictionaryIndicesBuilder.Release()
	dictionary := array.NewDictionaryArray(dictionaryType, dictionaryIndices, dictionaryValues)
	dictionaryIndices.Release()
	defer dictionary.Release()

	offsetsBuilder := array.NewInt32Builder(mem)
	offsetsBuilder.Append(0)
	offsetsBuilder.Append(1)
	offsetsBuilder.Append(1)
	offsets := offsetsBuilder.NewArray()
	offsetsBuilder.Release()
	defer offsets.Release()

	validity := memory.NewResizableBuffer(mem)
	validity.Resize(1)
	validity.Bytes()[0] = 0x01
	data := array.NewData(
		arrow.ListOf(dictionaryType),
		2,
		[]*memory.Buffer{validity, offsets.Data().Buffers()[1]},
		[]arrow.ArrayData{dictionary.Data()},
		1,
		0,
	)
	validity.Release()
	input := array.NewListData(data)
	data.Release()
	defer input.Release()

	expectedIndicesBuilder := array.NewInt8Builder(mem)
	expectedIndicesBuilder.Append(0)
	expectedIndicesBuilder.AppendNull()
	expectedIndices := expectedIndicesBuilder.NewArray()
	expectedIndicesBuilder.Release()
	defer expectedIndices.Release()
	expected := array.NewDictionaryArray(dictionaryType, expectedIndices, dictionaryValues)
	defer expected.Release()

	result, err := compute.ListElement(
		context.Background(),
		&compute.ArrayDatum{Value: input.Data()},
		&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
	)
	require.NoError(t, err)
	defer result.Release()

	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	require.NoError(t, array.ValidateFull(actual))
	assert.True(t, array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
	assert.True(t, array.Equal(dictionaryValues, actual.(*array.Dictionary).Dictionary()))
}

func TestListElementDenseUnionRecursiveErrorReleasesTemporaryIndices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sparseType := arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)
	structType := arrow.StructOf(arrow.Field{Name: "union", Type: sparseType, Nullable: true})
	denseType := arrow.DenseUnionOf(
		[]arrow.Field{{Name: "struct", Type: structType, Nullable: true}},
		[]arrow.UnionTypeCode{0},
	)

	builder := array.NewListBuilder(mem, denseType)
	values := builder.ValueBuilder().(*array.DenseUnionBuilder)
	structBuilder := values.Child(0).(*array.StructBuilder)
	unionBuilder := structBuilder.FieldBuilder(0).(*array.SparseUnionBuilder)
	for i := 0; i < 2; i++ {
		builder.Append(true)
		values.Append(0)
		structBuilder.Append(true)
		unionBuilder.Append(0)
		unionBuilder.Child(0).(*array.Int32Builder).Append(int32(i))
		unionBuilder.Child(1).(*array.StringBuilder).AppendNull()
	}
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	for i := 0; i < 20; i++ {
		result, err := compute.ListElement(
			context.Background(),
			&compute.ArrayDatum{Value: input.Data()},
			&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)},
		)
		if err == nil && result != nil {
			result.Release()
		}
		require.Error(t, err)
	}
}

func TestListElementValidatesScalarIndexForEmptyInputs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	empty := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[]`)
	defer empty.Release()
	chunked := arrow.NewChunked(empty.DataType(), []arrow.Array{empty})
	defer chunked.Release()

	tests := []struct {
		name    string
		index   scalar.Scalar
		wantErr bool
	}{
		{name: "null scalar", index: scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64), wantErr: true},
		{name: "negative scalar", index: scalar.NewInt64Scalar(-1), wantErr: true},
		{name: "zero scalar", index: scalar.NewInt64Scalar(0)},
		{name: "large unsigned scalar", index: scalar.NewUint64Scalar(^uint64(0))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, lists := range []compute.Datum{
				&compute.ArrayDatum{Value: empty.Data()},
				&compute.ChunkedDatum{Value: chunked},
			} {
				result, err := compute.ListElement(
					context.Background(),
					lists,
					&compute.ScalarDatum{Value: tc.index},
				)
				if err == nil && result != nil {
					result.Release()
				}
				if tc.wantErr {
					assert.ErrorIs(t, err, arrow.ErrInvalid)
				} else {
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestListElementNullScalarDenseUnionChildren(t *testing.T) {
	for _, unsupported := range []arrow.DataType{
		&arrow.Decimal32Type{Precision: 6, Scale: 2},
		&arrow.Decimal64Type{Precision: 12, Scale: 2},
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
	} {
		for _, unsupportedFirst := range []bool{true, false} {
			fields := []arrow.Field{
				{Name: "unsupported", Type: unsupported, Nullable: true},
				{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			}
			if !unsupportedFirst {
				fields[0], fields[1] = fields[1], fields[0]
			}
			union := arrow.DenseUnionOf(fields, []arrow.UnionTypeCode{3, 7})
			for _, elem := range []arrow.DataType{
				union,
				arrow.StructOf(arrow.Field{Name: "union", Type: union, Nullable: true}),
				&denseUnionExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: union}},
				arrow.DenseUnionOf([]arrow.Field{{Name: "nested", Type: union, Nullable: true}}, []arrow.UnionTypeCode{5}),
			} {
				for _, listType := range []arrow.DataType{arrow.ListOf(elem), arrow.LargeListOf(elem), arrow.FixedSizeListOf(2, elem)} {
					t.Run(fmt.Sprintf("%s/unsupported-first=%t", listType, unsupportedFirst), func(t *testing.T) {
						mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
						defer mem.AssertSize(t, 0)
						ctx := compute.WithAllocator(context.Background(), mem)
						list := scalar.MakeNullScalar(listType)
						defer list.(scalar.Releasable).Release()
						result, err := compute.ListElement(ctx,
							&compute.ScalarDatum{Value: list},
							&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
						if err == nil && result != nil {
							defer result.Release()
						}
						if unsupportedFirst {
							require.ErrorIs(t, err, arrow.ErrNotImplemented)
						} else {
							require.NoError(t, err)
							actual := result.(*compute.ScalarDatum).Value
							require.False(t, actual.IsValid())
							require.NoError(t, actual.ValidateFull())
						}
					})
				}
			}
		}
	}
}

func TestListElementEmptyUnionChild(t *testing.T) {
	for _, elem := range []arrow.DataType{arrow.DenseUnionOf(nil, nil), arrow.SparseUnionOf(nil, nil)} {
		t.Run(elem.ID().String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			builder := array.NewListBuilder(mem, elem)
			builder.AppendNull()
			input := builder.NewArray()
			builder.Release()
			defer input.Release()
			chunked := arrow.NewChunked(input.DataType(), []arrow.Array{input})
			defer chunked.Release()
			for _, lists := range []compute.Datum{
				&compute.ArrayDatum{Value: input.Data()},
				&compute.ChunkedDatum{Value: chunked},
			} {
				result, err := compute.ListElement(compute.WithAllocator(context.Background(), mem),
					lists, &compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
				if err == nil && result != nil {
					result.Release()
				}
				require.ErrorIs(t, err, arrow.ErrNotImplemented)
			}
		})
	}
}

func TestListElementNullScalarValidation(t *testing.T) {
	for _, elem := range []arrow.DataType{
		arrow.DenseUnionOf(nil, nil),
		arrow.SparseUnionOf(nil, nil),
	} {
		t.Run(elem.ID().String(), func(t *testing.T) {
			list := scalar.MakeNullScalar(arrow.ListOf(elem))
			defer list.(scalar.Releasable).Release()
			result, err := compute.ListElement(context.Background(),
				&compute.ScalarDatum{Value: list},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
			if err == nil && result != nil {
				result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrNotImplemented)
		})
	}

	list := scalar.MakeNullScalar(arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer list.(scalar.Releasable).Release()
	for _, index := range []scalar.Scalar{scalar.NewInt64Scalar(-1), scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64)} {
		result, err := compute.ListElement(context.Background(),
			&compute.ScalarDatum{Value: list}, &compute.ScalarDatum{Value: index})
		if err == nil && result != nil {
			result.Release()
		}
		require.ErrorIs(t, err, arrow.ErrInvalid)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := compute.ListElement(ctx,
		&compute.ScalarDatum{Value: list}, &compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
	if err == nil && result != nil {
		result.Release()
	}
	require.ErrorIs(t, err, context.Canceled)

	mapValue := scalar.MakeNullScalar(arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32))
	defer mapValue.(scalar.Releasable).Release()
	result, err = compute.ListElement(context.Background(),
		&compute.ScalarDatum{Value: mapValue}, &compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
	if err == nil && result != nil {
		result.Release()
	}
	require.Error(t, err)
}

func TestListElementNullStructScalarWithDenseUnionChild(t *testing.T) {
	for _, unsupportedFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("unsupported-first=%t", unsupportedFirst), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			fields := []arrow.Field{
				{Name: "unsupported", Type: &arrow.Decimal32Type{Precision: 6, Scale: 2}, Nullable: true},
				{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			}
			childID := 1
			if !unsupportedFirst {
				fields[0], fields[1] = fields[1], fields[0]
				childID = 0
			}
			codes := []arrow.UnionTypeCode{3, 7}
			unionType := arrow.DenseUnionOf(fields, codes)
			builder := array.NewDenseUnionBuilder(mem, unionType)
			builder.Append(codes[childID])
			builder.Child(childID).(*array.Int32Builder).Append(42)
			values := builder.NewArray()
			builder.Release()
			defer values.Release()

			structType := arrow.StructOf(arrow.Field{Name: "union", Type: unionType, Nullable: true})
			validity := memory.NewBufferBytes([]byte{0})
			data := array.NewData(structType, 1, []*memory.Buffer{validity}, []arrow.ArrayData{values.Data()}, 1, 0)
			validity.Release()
			child := array.NewStructData(data)
			data.Release()
			defer child.Release()
			list := scalar.NewListScalar(child)
			defer list.Release()

			result, err := compute.ListElement(compute.WithAllocator(context.Background(), mem),
				&compute.ScalarDatum{Value: list},
				&compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)})
			if err == nil && result != nil {
				defer result.Release()
			}
			if unsupportedFirst {
				require.ErrorIs(t, err, arrow.ErrNotImplemented)
			} else {
				require.NoError(t, err)
				actual := result.(*compute.ScalarDatum).Value
				require.False(t, actual.IsValid())
				require.NoError(t, actual.ValidateFull())
			}
		})
	}
}

func BenchmarkListElement(b *testing.B) {
	for _, size := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			mem := memory.NewGoAllocator()
			input := makeBenchmarkList(mem, size)
			defer input.Release()
			lists := &compute.ArrayDatum{Value: input.Data()}
			index := &compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)}

			b.ReportAllocs()
			b.SetBytes(int64(size * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := compute.ListElement(context.Background(), lists, index)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkListElementNested(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			mem := memory.NewGoAllocator()
			input := makeBenchmarkNestedList(mem, size)
			defer input.Release()
			lists := &compute.ArrayDatum{Value: input.Data()}
			index := &compute.ScalarDatum{Value: scalar.NewInt64Scalar(0)}

			b.ReportAllocs()
			b.SetBytes(int64(size * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := compute.ListElement(context.Background(), lists, index)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func makeBenchmarkList(mem memory.Allocator, length int) arrow.Array {
	builder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	values := builder.ValueBuilder().(*array.Int32Builder)
	builder.Reserve(length)
	values.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(true)
		values.Append(int32(i))
	}
	result := builder.NewArray()
	builder.Release()
	return result
}

func makeBenchmarkNestedList(mem memory.Allocator, length int) arrow.Array {
	outer := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	inner := outer.ValueBuilder().(*array.ListBuilder)
	values := inner.ValueBuilder().(*array.Int32Builder)
	outer.Reserve(length)
	inner.Reserve(length)
	values.Reserve(length)
	for i := 0; i < length; i++ {
		outer.Append(true)
		inner.Append(true)
		values.Append(int32(i))
	}
	result := outer.NewArray()
	outer.Release()
	return result
}
