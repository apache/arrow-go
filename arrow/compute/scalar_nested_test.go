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
