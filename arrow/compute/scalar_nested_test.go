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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	if result != nil {
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
			if result != nil {
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
			if result != nil {
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
		if result != nil {
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
				if result != nil {
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
