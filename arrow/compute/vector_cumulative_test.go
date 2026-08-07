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

func cumulativeInput(t *testing.T, mem memory.Allocator, typ arrow.DataType, values string) arrow.Array {
	arr, _, err := array.FromJSON(mem, typ, strings.NewReader(values))
	require.NoError(t, err)
	return arr
}

func TestCumulativeSum(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	defer input.Release()
	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6, 10]`)
	defer expected.Release()

	result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)

}

func TestCumulativeSumNullsAndStart(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, 2, null, 3]`)
	defer input.Release()

	t.Run("propagate nulls", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, null, null, null]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

	t.Run("skip nulls", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, 3, null, 6]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{SkipNulls: true}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

	t.Run("start value", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[11, null, 13, null, 16]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{
			Start:     scalar.NewInt64Scalar(10),
			SkipNulls: true,
		}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

}

func TestCumulativeSumStartSafeCast(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		typ   arrow.DataType
		start scalar.Scalar
	}{
		{name: "signed integer overflow", typ: arrow.PrimitiveTypes.Int8, start: scalar.NewInt64Scalar(128)},
		{name: "signed integer underflow", typ: arrow.PrimitiveTypes.Int8, start: scalar.NewInt64Scalar(-129)},
		{name: "unsigned integer underflow", typ: arrow.PrimitiveTypes.Uint8, start: scalar.NewInt64Scalar(-1)},
		{name: "float truncation", typ: arrow.PrimitiveTypes.Int32, start: scalar.NewFloat64Scalar(1.5)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, `[0]`)
			defer input.Release()

			result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{
				Start: tc.start,
			}, &compute.ArrayDatum{Value: input.Data()})
			if result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[0]`)
	defer input.Release()
	result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{
		Start: scalar.NewInt64Scalar(127),
	}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.Equal(t, int8(127), actual.(*array.Int8).Value(0))
}

func TestCumulativeSumChunked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	first := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2]`)
	second := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[3, 4]`)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	defer input.Release()
	defer first.Release()
	defer second.Release()

	expectedFirst := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3]`)
	expectedSecond := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[6, 10]`)
	expected := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{expectedFirst, expectedSecond})
	defer expected.Release()
	defer expectedFirst.Release()
	defer expectedSecond.Release()

	result, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{}, &compute.ChunkedDatum{Value: input})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ChunkedDatum{Value: expected}, result, nil, nil)

}

func TestCumulativeSumChecked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[127, 1]`)
	defer input.Release()

	unchecked, err := compute.CumulativeSum(context.Background(), compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer unchecked.Release()
	uncheckedArray := unchecked.(*compute.ArrayDatum).MakeArray()
	defer uncheckedArray.Release()
	assert.Equal(t, int8(-128), uncheckedArray.(*array.Int8).Value(1))

	_, err = compute.CumulativeSumChecked(context.Background(), compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	assert.ErrorIs(t, err, arrow.ErrInvalid)

}

func TestCumulativeSumCheckedIntegerOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name   string
		typ    arrow.DataType
		values string
	}{
		{name: "int8 positive", typ: arrow.PrimitiveTypes.Int8, values: `[127, 1]`},
		{name: "int8 negative", typ: arrow.PrimitiveTypes.Int8, values: `[-128, -1]`},
		{name: "int16 positive", typ: arrow.PrimitiveTypes.Int16, values: `[32767, 1]`},
		{name: "int16 negative", typ: arrow.PrimitiveTypes.Int16, values: `[-32768, -1]`},
		{name: "int32 positive", typ: arrow.PrimitiveTypes.Int32, values: `[2147483647, 1]`},
		{name: "int32 negative", typ: arrow.PrimitiveTypes.Int32, values: `[-2147483648, -1]`},
		{name: "int64 positive", typ: arrow.PrimitiveTypes.Int64, values: `[9223372036854775807, 1]`},
		{name: "int64 negative", typ: arrow.PrimitiveTypes.Int64, values: `[-9223372036854775808, -1]`},
		{name: "uint8", typ: arrow.PrimitiveTypes.Uint8, values: `[255, 1]`},
		{name: "uint16", typ: arrow.PrimitiveTypes.Uint16, values: `[65535, 1]`},
		{name: "uint32", typ: arrow.PrimitiveTypes.Uint32, values: `[4294967295, 1]`},
		{name: "uint64", typ: arrow.PrimitiveTypes.Uint64, values: `[18446744073709551615, 1]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, tc.values)
			defer input.Release()

			result, err := compute.CumulativeSumChecked(context.Background(), compute.CumulativeOptions{},
				&compute.ArrayDatum{Value: input.Data()})
			if result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}
