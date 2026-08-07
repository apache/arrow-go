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

func TestListElementSingleIndexArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := listElementInput(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, 2], [3, 4]]`)
	defer input.Release()
	index := listElementInput(t, mem, arrow.PrimitiveTypes.Int64, `[1]`)
	defer index.Release()
	expected := listElementInput(t, mem, arrow.PrimitiveTypes.Int32, `[2, 4]`)
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
