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
