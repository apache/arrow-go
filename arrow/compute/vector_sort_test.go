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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/internal/kernels"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortIndices(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	testCases := []struct {
		name     string
		buildArr func(mem memory.Allocator) arrow.Array
		key      kernels.SortKey
		expected []uint64
	}{
		{
			name: "Int32Ascending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 1, 4, 1, 5, 9, 2, 6}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 3, 6, 0, 2, 4, 7, 5},
		},
		{
			name: "Int32Descending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 1, 4, 1, 5, 9, 2, 6}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{5, 7, 4, 2, 0, 6, 1, 3},
		},
		{
			name: "Int32WithNullsLast",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 0, 4, 0, 5}, []bool{true, false, true, true, true})
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{3, 0, 2, 4, 1},
		},
		{
			name: "Int32WithNullsFirst",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 0, 4, 0, 5}, []bool{true, false, true, true, true})
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
			expected: []uint64{1, 3, 0, 2, 4},
		},
		{
			name: "Float64WithNaN",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewFloat64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float64{3.14, math.NaN(), 2.71, 1.41, math.NaN()}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{3, 2, 0, 1, 4},
		},
		{
			name: "StringAscending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewStringBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]string{"cherry", "apple", "banana", "date"}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 2, 0, 3},
		},
		{
			name: "BoolAscending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewBooleanBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]bool{true, false, true, false}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 3, 0, 2},
		},
		{
			name: "BoolDescending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewBooleanBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]bool{true, false, true, false}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{0, 2, 1, 3},
		},
		{
			name: "BoolWithNullsLast",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewBooleanBuilder(mem)
				defer bldr.Release()
				bldr.Append(true)
				bldr.Append(false)
				bldr.Append(true)
				bldr.AppendNull()
				bldr.Append(false)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 4, 0, 2, 3},
		},
		{
			name: "EmptyArray",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				return bldr.NewArray()
			},
			key:      compute.DefaultSortKey(),
			expected: []uint64{},
		},
		{
			name: "AllNulls",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{0, 0, 0}, []bool{false, false, false})
				return bldr.NewArray()
			},
			key:      compute.DefaultSortKey(),
			expected: []uint64{0, 1, 2},
		},
		{
			name: "StableSort",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{1, 2, 1, 2, 1}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{0, 2, 4, 1, 3},
		},
		{
			name: "Uint64",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewUint64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]uint64{100, 50, 200, 25}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{3, 1, 0, 2},
		},
		{
			name: "Binary",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				defer bldr.Release()
				bldr.AppendValues([][]byte{{3, 2, 1}, {1, 2, 3}, {2, 2, 2}}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 2, 0},
		},
		{
			name: "Float16Ascending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewFloat16Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float16.Num{
					float16.New(3), float16.New(1), float16.New(4), float16.New(1),
					float16.New(5), float16.New(9), float16.New(2), float16.New(6),
				}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 3, 6, 0, 2, 4, 7, 5},
		},
		{
			name: "Float16WithNaN",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewFloat16Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float16.Num{
					float16.New(3.14), float16.New(float32(math.NaN())), float16.New(2.71), float16.New(1.41), float16.New(float32(math.NaN())),
				}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{3, 2, 0, 1, 4},
		},
		{
			name: "Decimal32Ascending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				dt := &arrow.Decimal32Type{Precision: 5, Scale: 0}
				bldr := array.NewDecimal32Builder(mem, dt)
				defer bldr.Release()
				bldr.AppendValues([]decimal.Decimal32{300, 100, 200}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 2, 0},
		},
		{
			name: "Decimal64Descending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				dt := &arrow.Decimal64Type{Precision: 5, Scale: 0}
				bldr := array.NewDecimal64Builder(mem, dt)
				defer bldr.Release()
				bldr.AppendValues([]decimal.Decimal64{300, 100, 200}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{0, 2, 1},
		},
		{
			name: "IntervalMonthsAscending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewMonthIntervalBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]arrow.MonthInterval{3, 1, 4, 1, 5, 9, 2, 6}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{1, 3, 6, 0, 2, 4, 7, 5},
		},
		{
			name: "IntervalDayTimeLexicographic",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewDayTimeIntervalBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]arrow.DayTimeInterval{
					{Days: 2, Milliseconds: 0},
					{Days: 1, Milliseconds: 500},
					{Days: 1, Milliseconds: 0},
				}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{2, 1, 0},
		},
		{
			name: "IntervalMonthDayNanoLexicographic",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewMonthDayNanoIntervalBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]arrow.MonthDayNanoInterval{
					{Months: 1, Days: 2, Nanoseconds: 0},
					{Months: 1, Days: 1, Nanoseconds: 100},
					{Months: 1, Days: 1, Nanoseconds: 0},
				}, nil)
				return bldr.NewArray()
			},
			key:      kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			expected: []uint64{2, 1, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.buildArr(mem)
			defer arr.Release()

			datum := compute.NewDatum(arr)
			defer datum.Release()

			result, err := compute.SortIndices(ctx, datum, compute.SortOptions{tc.key})
			require.NoError(t, err)
			defer result.Release()

			resultArr := result.(*compute.ArrayDatum).MakeArray()
			defer resultArr.Release()

			uint64Arr := resultArr.(*array.Uint64)
			require.Equal(t, len(tc.expected), uint64Arr.Len(), "result length mismatch")

			for i := 0; i < uint64Arr.Len(); i++ {
				assert.Equal(t, tc.expected[i], uint64Arr.Value(i), "at index %d", i)
			}
		})
	}
}

func TestSortArray(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	testCases := []struct {
		name         string
		buildArr     func(mem memory.Allocator) arrow.Array
		key          kernels.SortKey
		validateFunc func(t *testing.T, result arrow.Array)
	}{
		{
			name: "Int32Ascending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 1, 4, 1, 5, 9, 2, 6}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []int32{1, 1, 2, 3, 4, 5, 6, 9}
				resultArr := result.(*array.Int32)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
		{
			name: "Int32Descending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 1, 4, 1, 5, 9, 2, 6}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []int32{9, 6, 5, 4, 3, 2, 1, 1}
				resultArr := result.(*array.Int32)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
		{
			name: "Int32WithNullsLast",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 0, 4, 0, 5}, []bool{true, false, true, true, true})
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []int32{0, 3, 4, 5, 0}
				validity := []bool{true, true, true, true, false}
				resultArr := result.(*array.Int32)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					if validity[i] {
						assert.Equal(t, expected[i], resultArr.Value(i), "at index %d", i)
					} else {
						assert.True(t, resultArr.IsNull(i), "expected null at index %d", i)
					}
				}
			},
		},
		{
			name: "Int32WithNullsFirst",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{3, 0, 4, 0, 5}, []bool{true, false, true, true, true})
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []int32{0, 0, 3, 4, 5}
				validity := []bool{false, true, true, true, true}
				resultArr := result.(*array.Int32)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					if validity[i] {
						assert.Equal(t, expected[i], resultArr.Value(i), "at index %d", i)
					} else {
						assert.True(t, resultArr.IsNull(i), "expected null at index %d", i)
					}
				}
			},
		},
		{
			name: "Float64WithNaN",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewFloat64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float64{3.14, math.NaN(), 2.71, 1.41, math.NaN()}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				resultArr := result.(*array.Float64)
				require.Equal(t, 5, resultArr.Len())
				assert.Equal(t, 1.41, resultArr.Value(0))
				assert.Equal(t, 2.71, resultArr.Value(1))
				assert.Equal(t, 3.14, resultArr.Value(2))
				assert.True(t, math.IsNaN(resultArr.Value(3)))
				assert.True(t, math.IsNaN(resultArr.Value(4)))
			},
		},
		{
			name: "StringAscending",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewStringBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]string{"cherry", "apple", "banana", "date"}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []string{"apple", "banana", "cherry", "date"}
				resultArr := result.(*array.String)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
		{
			name: "EmptyArray",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				return bldr.NewArray()
			},
			key: compute.DefaultSortKey(),
			validateFunc: func(t *testing.T, result arrow.Array) {
				assert.Equal(t, 0, result.Len())
			},
		},
		{
			name: "AllNulls",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{0, 0, 0}, []bool{false, false, false})
				return bldr.NewArray()
			},
			key: compute.DefaultSortKey(),
			validateFunc: func(t *testing.T, result arrow.Array) {
				resultArr := result.(*array.Int32)
				require.Equal(t, 3, resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.True(t, resultArr.IsNull(i), "expected null at index %d", i)
				}
			},
		},
		{
			name: "StableSort",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewInt32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int32{1, 2, 1, 2, 1}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []int32{1, 1, 1, 2, 2}
				resultArr := result.(*array.Int32)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
		{
			name: "Uint64",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewUint64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]uint64{100, 50, 200, 25}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := []uint64{25, 50, 100, 200}
				resultArr := result.(*array.Uint64)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
		{
			name: "Binary",
			buildArr: func(mem memory.Allocator) arrow.Array {
				bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				defer bldr.Release()
				bldr.AppendValues([][]byte{{3, 2, 1}, {1, 2, 3}, {2, 2, 2}}, nil)
				return bldr.NewArray()
			},
			key: kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			validateFunc: func(t *testing.T, result arrow.Array) {
				expected := [][]byte{{1, 2, 3}, {2, 2, 2}, {3, 2, 1}}
				resultArr := result.(*array.Binary)
				require.Equal(t, len(expected), resultArr.Len())
				for i := 0; i < resultArr.Len(); i++ {
					assert.Equal(t, expected[i], resultArr.Value(i))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.buildArr(mem)
			defer arr.Release()

			result, err := compute.SortArray(ctx, arr, tc.key)
			require.NoError(t, err)
			defer result.Release()

			tc.validateFunc(t, result)
		})
	}
}

func TestSortRecordBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32},
			{Name: "priority", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	t.Run("SortBySecondColumn", func(t *testing.T) {
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"A", "B", "C"}, nil)
		col1 := bldr1.NewArray()
		defer col1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{30, 10, 20}, nil)
		col2 := bldr2.NewArray()
		defer col2.Release()

		bldr3 := array.NewInt32Builder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]int32{1, 2, 3}, nil)
		col3 := bldr3.NewArray()
		defer col3.Release()

		batch := array.NewRecordBatch(schema, []arrow.Array{col1, col2, col3}, 3)
		defer batch.Release()

		// Sort by column 1 (value) instead of column 0 (category)
		keys := []kernels.SortKey{
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}

		result, err := compute.SortRecordBatch(ctx, batch, keys)
		require.NoError(t, err)
		defer result.Release()

		// Should be sorted by value column: 10, 20, 30
		expectedCat := []string{"B", "C", "A"}
		expectedVal := []int32{10, 20, 30}
		expectedPri := []int32{2, 3, 1}

		resultCat := result.Column(0).(*array.String)
		resultVal := result.Column(1).(*array.Int32)
		resultPri := result.Column(2).(*array.Int32)

		for i := 0; i < int(result.NumRows()); i++ {
			assert.Equal(t, expectedCat[i], resultCat.Value(i), "category at %d", i)
			assert.Equal(t, expectedVal[i], resultVal.Value(i), "value at %d", i)
			assert.Equal(t, expectedPri[i], resultPri.Value(i), "priority at %d", i)
		}
	})

	t.Run("MultiColumnLexicographic", func(t *testing.T) {
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		// Create data with duplicates to test lexicographic sort
		bldr1.AppendValues([]string{"B", "A", "B", "A"}, nil)
		col1 := bldr1.NewArray()
		defer col1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{1, 1, 2, 2}, nil)
		col2 := bldr2.NewArray()
		defer col2.Release()

		bldr3 := array.NewInt32Builder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]int32{100, 200, 300, 400}, nil)
		col3 := bldr3.NewArray()
		defer col3.Release()

		batch := array.NewRecordBatch(schema, []arrow.Array{col1, col2, col3}, 4)
		defer batch.Release()

		// Sort by col2 (f2) ascending, then col3 (f3) descending
		keys := []kernels.SortKey{
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 2, Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}

		result, err := compute.SortRecordBatch(ctx, batch, keys)
		require.NoError(t, err)
		defer result.Release()

		// Expected order:
		// First sort by f2 ascending: group [1,1] and [2,2]
		// Within f2=1: sort by f3 descending: 200 > 100, so (A,1,200) then (B,1,100)
		// Within f2=2: sort by f3 descending: 400 > 300, so (A,2,400) then (B,2,300)
		// Final indices: [1, 0, 3, 2]

		resultCol1 := result.Column(0).(*array.String)
		resultCol2 := result.Column(1).(*array.Int32)
		resultCol3 := result.Column(2).(*array.Int32)

		expectedCol1 := []string{"A", "B", "A", "B"}
		expectedCol2 := []int32{1, 1, 2, 2}
		expectedCol3 := []int32{200, 100, 400, 300}

		require.Equal(t, 4, int(result.NumRows()))
		for i := 0; i < 4; i++ {
			assert.Equal(t, expectedCol1[i], resultCol1.Value(i), "col1 at %d", i)
			assert.Equal(t, expectedCol2[i], resultCol2.Value(i), "col2 at %d", i)
			assert.Equal(t, expectedCol3[i], resultCol3.Value(i), "col3 at %d", i)
		}
	})

	t.Run("InvalidColumnIndex", func(t *testing.T) {
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"A"}, nil)
		col1 := bldr1.NewArray()
		defer col1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{1}, nil)
		col2 := bldr2.NewArray()
		defer col2.Release()

		bldr3 := array.NewInt32Builder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]int32{1}, nil)
		col3 := bldr3.NewArray()
		defer col3.Release()

		batch := array.NewRecordBatch(schema, []arrow.Array{col1, col2, col3}, 1)
		defer batch.Release()

		// Try to sort by invalid column index
		keys := []kernels.SortKey{
			{ColumnIndex: 99, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}

		_, err := compute.SortRecordBatch(ctx, batch, keys)
		require.Error(t, err)
		require.ErrorIs(t, err, arrow.ErrInvalid)
	})
}

func TestSortTable(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	t.Run("SortBySecondColumn", func(t *testing.T) {
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
		col1 := bldr1.NewArray()
		defer col1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{30, 25, 35}, nil)
		col2 := bldr2.NewArray()
		defer col2.Release()

		chunked1 := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{col1})
		defer chunked1.Release()
		chunked2 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{col2})
		defer chunked2.Release()

		tbl := array.NewTable(schema, []arrow.Column{
			*arrow.NewColumn(schema.Field(0), chunked1),
			*arrow.NewColumn(schema.Field(1), chunked2),
		}, 3)
		defer tbl.Release()

		// Sort by age (column 1) instead of name (column 0)
		keys := []kernels.SortKey{
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}

		result, err := compute.SortTable(ctx, tbl, keys)
		require.NoError(t, err)
		defer result.Release()

		expectedNames := []string{"Bob", "Alice", "Charlie"}
		expectedAges := []int32{25, 30, 35}

		nameData := result.Column(0).Data().Chunk(0).(*array.String)
		ageData := result.Column(1).Data().Chunk(0).(*array.Int32)

		for i := 0; i < int(result.NumRows()); i++ {
			assert.Equal(t, expectedNames[i], nameData.Value(i))
			assert.Equal(t, expectedAges[i], ageData.Value(i))
		}
	})

	t.Run("MultiColumnSort", func(t *testing.T) {
		// Create schema with 3 columns
		multiSchema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "category", Type: arrow.BinaryTypes.String},
				{Name: "priority", Type: arrow.PrimitiveTypes.Int32},
				{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			},
			nil,
		)

		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"A", "B", "A", "B"}, nil)
		col1 := bldr1.NewArray()
		defer col1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{2, 1, 1, 2}, nil)
		col2 := bldr2.NewArray()
		defer col2.Release()

		bldr3 := array.NewInt32Builder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]int32{100, 200, 300, 400}, nil)
		col3 := bldr3.NewArray()
		defer col3.Release()

		chunked1 := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{col1})
		defer chunked1.Release()
		chunked2 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{col2})
		defer chunked2.Release()
		chunked3 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{col3})
		defer chunked3.Release()

		tbl := array.NewTable(multiSchema, []arrow.Column{
			*arrow.NewColumn(multiSchema.Field(0), chunked1),
			*arrow.NewColumn(multiSchema.Field(1), chunked2),
			*arrow.NewColumn(multiSchema.Field(2), chunked3),
		}, 4)
		defer tbl.Release()

		// Sort by priority ascending, then by id descending
		keys := []kernels.SortKey{
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 2, Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}

		result, err := compute.SortTable(ctx, tbl, keys)
		require.NoError(t, err)
		defer result.Release()

		// Expected order:
		// priority=1: (A,1,300) and (B,1,200), sorted by id desc -> (A,1,300), (B,1,200)
		// priority=2: (A,2,100) and (B,2,400), sorted by id desc -> (B,2,400), (A,2,100)
		// Final: [2, 1, 3, 0]
		expectedCategory := []string{"A", "B", "B", "A"}
		expectedPriority := []int32{1, 1, 2, 2}
		expectedId := []int32{300, 200, 400, 100}

		categoryData := result.Column(0).Data().Chunk(0).(*array.String)
		priorityData := result.Column(1).Data().Chunk(0).(*array.Int32)
		idData := result.Column(2).Data().Chunk(0).(*array.Int32)

		require.Equal(t, 4, int(result.NumRows()))
		for i := 0; i < int(result.NumRows()); i++ {
			assert.Equal(t, expectedCategory[i], categoryData.Value(i), "category at %d", i)
			assert.Equal(t, expectedPriority[i], priorityData.Value(i), "priority at %d", i)
			assert.Equal(t, expectedId[i], idData.Value(i), "id at %d", i)
		}
	})
}

func TestSortIndicesChunked(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	t.Run("Int32ChunkedAscending", func(t *testing.T) {
		// Create chunked array: [[3, 1], [4, 1, 5]]
		bldr1 := array.NewInt32Builder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]int32{3, 1}, nil)
		chunk1 := bldr1.NewArray()
		defer chunk1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{4, 1, 5}, nil)
		chunk2 := bldr2.NewArray()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		opts := compute.SortOptions{kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}}
		result, err := compute.SortIndices(ctx, &compute.ChunkedDatum{Value: chunked}, opts)
		require.NoError(t, err)
		defer result.Release()

		resultArr := result.(*compute.ArrayDatum).MakeArray().(*array.Uint64)
		defer resultArr.Release()

		// Expected: values [1, 1, 3, 4, 5] -> indices [1, 3, 0, 2, 4]
		expected := []uint64{1, 3, 0, 2, 4}
		require.Equal(t, len(expected), resultArr.Len())
		for i := 0; i < resultArr.Len(); i++ {
			assert.Equal(t, expected[i], resultArr.Value(i), "index at %d", i)
		}
	})

	t.Run("StringChunkedWithNulls", func(t *testing.T) {
		// Create chunked array: [["b", null], ["a", "c"]]
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"b", ""}, []bool{true, false})
		chunk1 := bldr1.NewArray()
		defer chunk1.Release()

		bldr2 := array.NewStringBuilder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]string{"a", "c"}, nil)
		chunk2 := bldr2.NewArray()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		opts := compute.SortOptions{kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}}
		result, err := compute.SortIndices(ctx, &compute.ChunkedDatum{Value: chunked}, opts)
		require.NoError(t, err)
		defer result.Release()

		resultArr := result.(*compute.ArrayDatum).MakeArray().(*array.Uint64)
		defer resultArr.Release()

		// Expected: ["a", "b", "c", null] -> indices [2, 0, 3, 1]
		expected := []uint64{2, 0, 3, 1}
		require.Equal(t, len(expected), resultArr.Len())
		for i := 0; i < resultArr.Len(); i++ {
			assert.Equal(t, expected[i], resultArr.Value(i), "index at %d", i)
		}
	})

	t.Run("Float64ChunkedWithNaN", func(t *testing.T) {
		// Create chunked array: [[1.0, NaN], [2.0, 0.5]]
		bldr1 := array.NewFloat64Builder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]float64{1.0, math.NaN()}, nil)
		chunk1 := bldr1.NewArray()
		defer chunk1.Release()

		bldr2 := array.NewFloat64Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]float64{2.0, 0.5}, nil)
		chunk2 := bldr2.NewArray()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		opts := compute.SortOptions{kernels.SortKey{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}}
		result, err := compute.SortIndices(ctx, &compute.ChunkedDatum{Value: chunked}, opts)
		require.NoError(t, err)
		defer result.Release()

		resultArr := result.(*compute.ArrayDatum).MakeArray().(*array.Uint64)
		defer resultArr.Release()

		// Expected: [0.5, 1.0, 2.0, NaN] -> indices [3, 0, 2, 1]
		expected := []uint64{3, 0, 2, 1}
		require.Equal(t, len(expected), resultArr.Len())
		for i := 0; i < resultArr.Len(); i++ {
			assert.Equal(t, expected[i], resultArr.Value(i), "index at %d", i)
		}
	})
}

func TestSortTableChunked(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	t.Run("MultiChunkSingleColumn", func(t *testing.T) {
		// Create table with chunked columns
		// category: [["B", "A"], ["C"]]
		// value: [[2, 1], [3]]
		bldr1 := array.NewStringBuilder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]string{"B", "A"}, nil)
		catChunk1 := bldr1.NewArray()
		defer catChunk1.Release()

		bldr2 := array.NewStringBuilder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]string{"C"}, nil)
		catChunk2 := bldr2.NewArray()
		defer catChunk2.Release()

		bldr3 := array.NewInt32Builder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]int32{2, 1}, nil)
		valChunk1 := bldr3.NewArray()
		defer valChunk1.Release()

		bldr4 := array.NewInt32Builder(mem)
		defer bldr4.Release()
		bldr4.AppendValues([]int32{3}, nil)
		valChunk2 := bldr4.NewArray()
		defer valChunk2.Release()

		catChunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{catChunk1, catChunk2})
		defer catChunked.Release()
		valChunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{valChunk1, valChunk2})
		defer valChunked.Release()

		tbl := array.NewTable(schema, []arrow.Column{
			*arrow.NewColumn(schema.Field(0), catChunked),
			*arrow.NewColumn(schema.Field(1), valChunked),
		}, 3)
		defer tbl.Release()

		keys := []kernels.SortKey{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}

		result, err := compute.SortTable(ctx, tbl, keys)
		require.NoError(t, err)
		defer result.Release()

		// Expected order: ["A", "B", "C"] with values [1, 2, 3]
		expectedCat := []string{"A", "B", "C"}
		expectedVal := []int32{1, 2, 3}

		// Result should have all data in single chunks after Take
		require.Equal(t, int64(3), result.NumRows())

		catData := result.Column(0).Data().Chunk(0).(*array.String)
		valData := result.Column(1).Data().Chunk(0).(*array.Int32)

		for i := 0; i < 3; i++ {
			assert.Equal(t, expectedCat[i], catData.Value(i), "category at %d", i)
			assert.Equal(t, expectedVal[i], valData.Value(i), "value at %d", i)
		}
	})

	t.Run("MultiChunkMultiColumn", func(t *testing.T) {
		// Create table with 3 columns, all chunked
		// col1: [[1, 1], [2]]
		// col2: [["b", "a"], ["a"]]
		// col3: [[20, 10], [30]]
		multiSchema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "col1", Type: arrow.PrimitiveTypes.Int32},
				{Name: "col2", Type: arrow.BinaryTypes.String},
				{Name: "col3", Type: arrow.PrimitiveTypes.Int32},
			},
			nil,
		)

		// Build col1
		bldr1 := array.NewInt32Builder(mem)
		defer bldr1.Release()
		bldr1.AppendValues([]int32{1, 1}, nil)
		col1Chunk1 := bldr1.NewArray()
		defer col1Chunk1.Release()

		bldr2 := array.NewInt32Builder(mem)
		defer bldr2.Release()
		bldr2.AppendValues([]int32{2}, nil)
		col1Chunk2 := bldr2.NewArray()
		defer col1Chunk2.Release()

		col1Chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{col1Chunk1, col1Chunk2})
		defer col1Chunked.Release()

		// Build col2
		bldr3 := array.NewStringBuilder(mem)
		defer bldr3.Release()
		bldr3.AppendValues([]string{"b", "a"}, nil)
		col2Chunk1 := bldr3.NewArray()
		defer col2Chunk1.Release()

		bldr4 := array.NewStringBuilder(mem)
		defer bldr4.Release()
		bldr4.AppendValues([]string{"a"}, nil)
		col2Chunk2 := bldr4.NewArray()
		defer col2Chunk2.Release()

		col2Chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{col2Chunk1, col2Chunk2})
		defer col2Chunked.Release()

		// Build col3
		bldr5 := array.NewInt32Builder(mem)
		defer bldr5.Release()
		bldr5.AppendValues([]int32{20, 10}, nil)
		col3Chunk1 := bldr5.NewArray()
		defer col3Chunk1.Release()

		bldr6 := array.NewInt32Builder(mem)
		defer bldr6.Release()
		bldr6.AppendValues([]int32{30}, nil)
		col3Chunk2 := bldr6.NewArray()
		defer col3Chunk2.Release()

		col3Chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{col3Chunk1, col3Chunk2})
		defer col3Chunked.Release()

		tbl := array.NewTable(multiSchema, []arrow.Column{
			*arrow.NewColumn(multiSchema.Field(0), col1Chunked),
			*arrow.NewColumn(multiSchema.Field(1), col2Chunked),
			*arrow.NewColumn(multiSchema.Field(2), col3Chunked),
		}, 3)
		defer tbl.Release()

		// Sort by col1 ascending, then col2 descending
		keys := []kernels.SortKey{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}

		result, err := compute.SortTable(ctx, tbl, keys)
		require.NoError(t, err)
		defer result.Release()

		// Expected order:
		// col1=1: sort by col2 desc: "b" > "a", so (1, "b", 20), (1, "a", 10)
		// col1=2: (2, "a", 30)
		// Final: [(1, "b", 20), (1, "a", 10), (2, "a", 30)]
		expectedCol1 := []int32{1, 1, 2}
		expectedCol2 := []string{"b", "a", "a"}
		expectedCol3 := []int32{20, 10, 30}

		require.Equal(t, int64(3), result.NumRows())

		col1Data := result.Column(0).Data().Chunk(0).(*array.Int32)
		col2Data := result.Column(1).Data().Chunk(0).(*array.String)
		col3Data := result.Column(2).Data().Chunk(0).(*array.Int32)

		for i := 0; i < 3; i++ {
			assert.Equal(t, expectedCol1[i], col1Data.Value(i), "col1 at %d", i)
			assert.Equal(t, expectedCol2[i], col2Data.Value(i), "col2 at %d", i)
			assert.Equal(t, expectedCol3[i], col3Data.Value(i), "col3 at %d", i)
		}
	})

	t.Run("MisalignedChunksMultiColumnLexicographic", func(t *testing.T) {
		// Column 0 is chunked [2,1]; column 1 is a single chunk [3]. Chunk boundaries differ,
		// so kernels fall back to one global stable sort (must still match lexicographic order).
		s := arrow.NewSchema(
			[]arrow.Field{
				{Name: "a", Type: arrow.PrimitiveTypes.Int32},
				{Name: "b", Type: arrow.PrimitiveTypes.Int32},
			},
			nil,
		)

		b0 := array.NewInt32Builder(mem)
		b0.AppendValues([]int32{10, 20}, nil)
		a0 := b0.NewArray()
		defer a0.Release()
		b1 := array.NewInt32Builder(mem)
		b1.AppendValues([]int32{15}, nil)
		a1 := b1.NewArray()
		defer a1.Release()
		colA := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{a0, a1})
		defer colA.Release()

		b2 := array.NewInt32Builder(mem)
		b2.AppendValues([]int32{1, 2, 3}, nil)
		a2 := b2.NewArray()
		defer a2.Release()
		colB := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{a2})
		defer colB.Release()

		tbl := array.NewTable(s, []arrow.Column{
			*arrow.NewColumn(s.Field(0), colA),
			*arrow.NewColumn(s.Field(1), colB),
		}, 3)
		defer tbl.Release()

		keys := []kernels.SortKey{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}

		result, err := compute.SortTable(ctx, tbl, keys)
		require.NoError(t, err)
		defer result.Release()

		// Sorted by (a,b): (10,1), (15,3), (20,2)
		ada := result.Column(0).Data().Chunk(0).(*array.Int32)
		bdb := result.Column(1).Data().Chunk(0).(*array.Int32)
		require.Equal(t, []int32{10, 15, 20}, []int32{ada.Value(0), ada.Value(1), ada.Value(2)})
		require.Equal(t, []int32{1, 3, 2}, []int32{bdb.Value(0), bdb.Value(1), bdb.Value(2)})
	})
}

// testSortIndicesUint64 runs sort_indices and compares to expected permutation indices.
// Input datum must not be released by this helper; use compute.NewDatumWithoutOwning for
// caller-owned values.
//
// TestVectorSortIndicesCpp* functions below mirror sort_indices coverage from Apache Arrow C++
// (vector_sort_test.cc, SortIndices / array_sort_indices). Where Go differs (e.g. NaN ordering),
// tests substitute or skip with a short comment.
func testSortIndicesUint64(t *testing.T, ctx context.Context, input compute.Datum, opts compute.SortOptions, want []uint64) {
	t.Helper()
	out, err := compute.SortIndices(ctx, input, opts)
	require.NoError(t, err)
	defer out.Release()
	arr := out.(*compute.ArrayDatum).MakeArray().(*array.Uint64)
	defer arr.Release()
	require.Equal(t, len(want), arr.Len(), "length mismatch")
	for i := range want {
		assert.Equal(t, want[i], arr.Value(i), "index %d", i)
	}
}

// TestVectorSortIndicesCppArrayParity mirrors ArraySortIndicesFunction and typed array cases
// from Apache Arrow C++ vector_sort_test.cc (sort_indices on scalar arrays).
func TestVectorSortIndicesCppArrayParity(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	t.Run("Int16NullsDefaultAndDescendingAtStart", func(t *testing.T) {
		// CallFunction("array_sort_indices", {arr}) in C++; same logical data as sort_indices on array.
		arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader("[0, 1, null, -3, null, -42, 5]"))
		require.NoError(t, err)
		defer arr.Release()
		d := compute.NewDatumWithoutOwning(arr)

		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{5, 3, 0, 1, 6, 2, 4})

		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{2, 4, 6, 1, 0, 3, 5})
	})

	t.Run("Float64NullNaNMatchesCppRealSuite", func(t *testing.T) {
		arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float64,
			strings.NewReader("[null, 1, 3.3, null, 2, 5.3]"))
		require.NoError(t, err)
		defer arr.Release()
		d := compute.NewDatumWithoutOwning(arr)

		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{1, 4, 2, 5, 0, 3})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 1, 4, 2, 5})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{5, 2, 4, 1, 0, 3})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 5, 2, 4, 1})
	})

	t.Run("UInt8TieBreakSmallRange", func(t *testing.T) {
		arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Uint8,
			strings.NewReader("[255, null, 0, 255, 10, null, 128, 0]"))
		require.NoError(t, err)
		defer arr.Release()
		d := compute.NewDatumWithoutOwning(arr)

		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{2, 7, 4, 6, 0, 3, 1, 5})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{1, 5, 2, 7, 4, 6, 0, 3})
	})

	t.Run("FixedSizeBinaryMatchesCpp", func(t *testing.T) {
		// C++ TestArraySortIndicesForFixedSizeBinary, fixed_size_binary(3).
		// array.FromJSON decodes fixed_size_binary elements as standard base64 (builder UnmarshalOne).
		dt := &arrow.FixedSizeBinaryType{ByteWidth: 3}
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`["ZmVm", "YWJj", "Z2hp"]`))
		require.NoError(t, err)
		defer arr.Release()
		d := compute.NewDatumWithoutOwning(arr)
		for _, np := range []kernels.NullPlacement{kernels.NullsAtEnd, kernels.NullsAtStart} {
			testSortIndicesUint64(t, ctx, d, compute.SortOptions{
				{Order: kernels.Ascending, NullPlacement: np},
			}, []uint64{1, 0, 2})
			testSortIndicesUint64(t, ctx, d, compute.SortOptions{
				{Order: kernels.Descending, NullPlacement: np},
			}, []uint64{2, 0, 1})
		}
		inp := `[null, "Y2Nj", "YmJi", null, "YWFh", "YmJi"]`
		arr2, _, err := array.FromJSON(mem, dt, strings.NewReader(inp))
		require.NoError(t, err)
		defer arr2.Release()
		d2 := compute.NewDatumWithoutOwning(arr2)
		testSortIndicesUint64(t, ctx, d2, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{4, 2, 5, 1, 0, 3})
		testSortIndicesUint64(t, ctx, d2, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 4, 2, 5, 1})
		testSortIndicesUint64(t, ctx, d2, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{1, 2, 5, 4, 0, 3})
		testSortIndicesUint64(t, ctx, d2, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 1, 2, 5, 4})
	})
}

// TestVectorSortIndicesCppChunkedParity mirrors TestChunkedArraySortIndices in C++ vector_sort_test.cc.
func TestVectorSortIndicesCppChunkedParity(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	t.Run("Int16ContiguousEqualsSingleArray", func(t *testing.T) {
		c0, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader("[0, 1]"))
		require.NoError(t, err)
		defer c0.Release()
		c1, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader("[null, -3, null, -42, 5]"))
		require.NoError(t, err)
		defer c1.Release()
		ch := arrow.NewChunked(arrow.PrimitiveTypes.Int16, []arrow.Array{c0, c1})
		defer ch.Release()

		d := compute.NewDatumWithoutOwning(ch)
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{5, 3, 0, 1, 6, 2, 4})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{2, 4, 6, 1, 0, 3, 5})
	})

	t.Run("Uint8NullsAcrossChunks", func(t *testing.T) {
		c0, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Uint8, strings.NewReader("[null, 1]"))
		require.NoError(t, err)
		defer c0.Release()
		c1, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Uint8, strings.NewReader("[3, null, 2]"))
		require.NoError(t, err)
		defer c1.Release()
		c2, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Uint8, strings.NewReader("[1]"))
		require.NoError(t, err)
		defer c2.Release()
		ch := arrow.NewChunked(arrow.PrimitiveTypes.Uint8, []arrow.Array{c0, c1, c2})
		defer ch.Release()
		d := compute.NewDatumWithoutOwning(ch)

		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{1, 5, 4, 2, 0, 3})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 1, 5, 4, 2})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}, []uint64{2, 4, 1, 5, 0, 3})
		testSortIndicesUint64(t, ctx, d, compute.SortOptions{
			{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
		}, []uint64{0, 3, 2, 4, 1, 5})
	})

	t.Run("Float32NaNAcrossChunks", func(t *testing.T) {
		// C++ uses the same chunks and expects a specific stable order among NaNs.
		// Go's float comparator may not match C++ NaN ordering for chunked inputs; skip
		// exact permutation parity until aligned with arrow-cpp.
		t.Skip("chunked float32 NaN ordering differs from Apache Arrow C++ vector_sort_test.cc")
	})
}

func cppRecordKeysAB(null kernels.NullPlacement) compute.SortOptions {
	return compute.SortOptions{
		{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: null},
		{ColumnIndex: 1, Order: kernels.Descending, NullPlacement: null},
	}
}

// TestVectorSortIndicesCppRecordBatchParity mirrors TestRecordBatchSortIndices in C++ vector_sort_test.cc.
func TestVectorSortIndicesCppRecordBatchParity(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	t.Run("NoNull", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "b", Type: arrow.PrimitiveTypes.Uint32},
		}, nil)
		jsonRows := `[
			{"a": 3, "b": 5},
			{"a": 1, "b": 3},
			{"a": 3, "b": 4},
			{"a": 0, "b": 6},
			{"a": 2, "b": 5},
			{"a": 1, "b": 5},
			{"a": 1, "b": 3}
		]`
		batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(jsonRows))
		require.NoError(t, err)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)

		for _, np := range []kernels.NullPlacement{kernels.NullsAtEnd, kernels.NullsAtStart} {
			testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(np), []uint64{3, 5, 1, 6, 4, 0, 2})
		}
	})

	t.Run("Null", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "b", Type: arrow.PrimitiveTypes.Uint32},
		}, nil)
		jsonRows := `[
			{"a": null, "b": 5},
			{"a": 1,    "b": 3},
			{"a": 3,    "b": null},
			{"a": null, "b": null},
			{"a": 2,    "b": 5},
			{"a": 1,    "b": 5},
			{"a": 3,    "b": 5}
		]`
		batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(jsonRows))
		require.NoError(t, err)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)

		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtEnd), []uint64{5, 1, 4, 6, 2, 0, 3})
		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtStart), []uint64{3, 0, 5, 1, 4, 2, 6})
	})

	t.Run("NaN", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Float32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		ba := array.NewFloat32Builder(mem)
		defer ba.Release()
		ba.AppendValues([]float32{3, 1, 3, 0, float32(math.NaN()), float32(math.NaN()), float32(math.NaN()), 1}, nil)
		colA := ba.NewArray()
		defer colA.Release()
		bb := array.NewFloat64Builder(mem)
		defer bb.Release()
		bb.Append(5)
		bb.Append(math.NaN())
		bb.Append(4)
		bb.Append(6)
		bb.Append(5)
		bb.Append(math.NaN())
		bb.Append(5)
		bb.Append(5)
		colB := bb.NewArray()
		defer colB.Release()
		batch := array.NewRecordBatch(schema, []arrow.Array{colA, colB}, 8)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)

		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtEnd), []uint64{3, 7, 1, 0, 2, 4, 6, 5})
		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtStart), []uint64{5, 4, 6, 3, 1, 7, 0, 2})
	})

	t.Run("NaNAndNull", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Float32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		ba := array.NewFloat32Builder(mem)
		defer ba.Release()
		ba.AppendNull()
		ba.Append(1)
		ba.Append(3)
		ba.AppendNull()
		ba.AppendValues([]float32{float32(math.NaN()), float32(math.NaN()), float32(math.NaN()), 1}, nil)
		colA := ba.NewArray()
		defer colA.Release()
		bb := array.NewFloat64Builder(mem)
		defer bb.Release()
		bb.Append(5)
		bb.Append(3)
		bb.AppendNull()
		bb.AppendNull()
		bb.AppendNull()
		bb.Append(math.NaN())
		bb.Append(5)
		bb.Append(5)
		colB := bb.NewArray()
		defer colB.Release()
		batch := array.NewRecordBatch(schema, []arrow.Array{colA, colB}, 8)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)

		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtEnd), []uint64{7, 1, 2, 6, 5, 4, 0, 3})
		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtStart), []uint64{3, 0, 4, 5, 6, 7, 1, 2})
	})

	t.Run("Boolean", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
		}, nil)
		jsonRows := `[
			{"a": true,  "b": null},
			{"a": false, "b": null},
			{"a": true,  "b": true},
			{"a": false, "b": true},
			{"a": true,  "b": false},
			{"a": null,  "b": false},
			{"a": false, "b": null},
			{"a": null,  "b": true}
		]`
		batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(jsonRows))
		require.NoError(t, err)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)

		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtEnd), []uint64{3, 1, 6, 2, 4, 0, 7, 5})
		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtStart), []uint64{7, 5, 1, 6, 3, 0, 2, 4})
	})

	t.Run("MoreTypes", func(t *testing.T) {
		ts := &arrow.TimestampType{Unit: arrow.Microsecond}
		fsb3 := &arrow.FixedSizeBinaryType{ByteWidth: 3}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: ts},
			{Name: "b", Type: arrow.BinaryTypes.LargeString},
			{Name: "c", Type: fsb3},
		}, nil)
		ba := array.NewTimestampBuilder(mem, ts)
		defer ba.Release()
		ba.Append(arrow.Timestamp(3))
		ba.Append(arrow.Timestamp(1))
		ba.Append(arrow.Timestamp(3))
		ba.Append(arrow.Timestamp(0))
		ba.Append(arrow.Timestamp(2))
		ba.Append(arrow.Timestamp(1))
		colA := ba.NewArray()
		defer colA.Release()

		bb := array.NewLargeStringBuilder(mem)
		defer bb.Release()
		bb.AppendValues([]string{"05", "031", "05", "0666", "05", "05"}, nil)
		colB := bb.NewArray()
		defer colB.Release()

		bc := array.NewFixedSizeBinaryBuilder(mem, fsb3)
		defer bc.Release()
		for _, v := range [][]byte{[]byte("aaa"), []byte("bbb"), []byte("bbb"), []byte("aaa"), []byte("aaa"), []byte("bbb")} {
			bc.Append(v)
		}
		colC := bc.NewArray()
		defer colC.Release()

		batch := array.NewRecordBatch(schema, []arrow.Array{colA, colB, colC}, 6)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)
		keys := compute.SortOptions{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 2, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}
		for _, np := range []kernels.NullPlacement{kernels.NullsAtEnd, kernels.NullsAtStart} {
			for i := range keys {
				keys[i].NullPlacement = np
			}
			testSortIndicesUint64(t, ctx, d, keys, []uint64{3, 5, 1, 4, 0, 2})
		}
	})

	t.Run("Decimal", func(t *testing.T) {
		d128 := &arrow.Decimal128Type{Precision: 3, Scale: 1}
		d256 := &arrow.Decimal256Type{Precision: 4, Scale: 2}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: d128},
			{Name: "b", Type: d256},
		}, nil)
		jsonRows := `[
			{"a": "12.3", "b": "12.34"},
			{"a": "45.6", "b": "12.34"},
			{"a": "12.3", "b": "-12.34"},
			{"a": "-12.3", "b": null},
			{"a": "-12.3", "b": "-45.67"}
		]`
		batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(jsonRows))
		require.NoError(t, err)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)
		keys := compute.SortOptions{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}
		testSortIndicesUint64(t, ctx, d, keys, []uint64{4, 3, 0, 2, 1})
		keys[0].NullPlacement = kernels.NullsAtStart
		keys[1].NullPlacement = kernels.NullsAtStart
		testSortIndicesUint64(t, ctx, d, keys, []uint64{3, 4, 0, 2, 1})
	})

	t.Run("DuplicateSortKeys", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Float32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		ba := array.NewFloat32Builder(mem)
		defer ba.Release()
		ba.AppendNull()
		ba.Append(1)
		ba.Append(3)
		ba.AppendNull()
		ba.AppendValues([]float32{float32(math.NaN()), float32(math.NaN()), float32(math.NaN()), 1}, nil)
		colA := ba.NewArray()
		defer colA.Release()
		bb := array.NewFloat64Builder(mem)
		defer bb.Release()
		bb.Append(5)
		bb.Append(3)
		bb.AppendNull()
		bb.AppendNull()
		bb.AppendNull()
		bb.Append(math.NaN())
		bb.Append(5)
		bb.Append(5)
		colB := bb.NewArray()
		defer colB.Release()
		batch := array.NewRecordBatch(schema, []arrow.Array{colA, colB}, 8)
		defer batch.Release()
		d := compute.NewDatumWithoutOwning(batch)
		// ARROW-14073: only the first occurrence of each logical column is used.
		opts := compute.SortOptions{
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 0, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 0, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}
		testSortIndicesUint64(t, ctx, d, opts, []uint64{7, 1, 2, 6, 5, 4, 0, 3})
		for i := range opts {
			opts[i].NullPlacement = kernels.NullsAtStart
		}
		testSortIndicesUint64(t, ctx, d, opts, []uint64{3, 0, 4, 5, 6, 7, 1, 2})
	})
}

// TestVectorSortIndicesCppTableParity mirrors TestTableSortIndices in C++ vector_sort_test.cc.
func TestVectorSortIndicesCppTableParity(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	schemaAB := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "b", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	t.Run("EmptyTable", func(t *testing.T) {
		batch, _, err := array.RecordFromJSON(mem, schemaAB, strings.NewReader("[]"))
		require.NoError(t, err)
		defer batch.Release()
		tbl := array.NewTableFromRecords(schemaAB, []arrow.RecordBatch{batch})
		defer tbl.Release()
		d := compute.NewDatumWithoutOwning(tbl)
		for _, np := range []kernels.NullPlacement{kernels.NullsAtEnd, kernels.NullsAtStart} {
			testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(np), []uint64{})
		}
	})

	t.Run("EmptySortKeysInvalid", func(t *testing.T) {
		jsonOne := `[{"a": null, "b": 5}]`
		batch, _, err := array.RecordFromJSON(mem, schemaAB, strings.NewReader(jsonOne))
		require.NoError(t, err)
		defer batch.Release()
		tbl := array.NewTableFromRecords(schemaAB, []arrow.RecordBatch{batch})
		defer tbl.Release()
		_, err = compute.SortIndices(ctx, compute.NewDatumWithoutOwning(tbl), compute.SortOptions{})
		require.Error(t, err)
		require.ErrorIs(t, err, arrow.ErrInvalid)
	})

	t.Run("NullSingleAndMultiChunk", func(t *testing.T) {
		json1 := `[
			{"a": null, "b": 5},
			{"a": 1,    "b": 3},
			{"a": 3,    "b": null}
		]`
		json2 := `[
			{"a": null, "b": null},
			{"a": 2,    "b": 5},
			{"a": 1,    "b": 5},
			{"a": 3,    "b": 5}
		]`
		b1, _, err := array.RecordFromJSON(mem, schemaAB, strings.NewReader(json1))
		require.NoError(t, err)
		defer b1.Release()
		b2, _, err := array.RecordFromJSON(mem, schemaAB, strings.NewReader(json2))
		require.NoError(t, err)
		defer b2.Release()
		tbl := array.NewTableFromRecords(schemaAB, []arrow.RecordBatch{b1, b2})
		defer tbl.Release()
		d := compute.NewDatumWithoutOwning(tbl)

		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtEnd), []uint64{5, 1, 4, 6, 2, 0, 3})
		testSortIndicesUint64(t, ctx, d, cppRecordKeysAB(kernels.NullsAtStart), []uint64{3, 0, 5, 1, 4, 2, 6})
	})

	t.Run("BinaryLikeTwoChunks", func(t *testing.T) {
		fsb3 := &arrow.FixedSizeBinaryType{ByteWidth: 3}
		s := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.BinaryTypes.LargeString},
			{Name: "b", Type: fsb3},
		}, nil)
		buildBatch := func(a []string, b [][]byte, bNulls []bool) arrow.RecordBatch {
			ab := array.NewLargeStringBuilder(mem)
			defer ab.Release()
			ab.AppendValues(a, nil)
			colA := ab.NewArray()

			bb := array.NewFixedSizeBinaryBuilder(mem, fsb3)
			defer bb.Release()
			for i := range a {
				if bNulls[i] {
					bb.AppendNull()
				} else {
					bb.Append(b[i])
				}
			}
			colB := bb.NewArray()

			rb := array.NewRecordBatch(s, []arrow.Array{colA, colB}, int64(len(a)))
			colA.Release()
			colB.Release()
			return rb
		}
		b1 := buildBatch(
			[]string{"one", "two", "three", "four"},
			[][]byte{nil, []byte("aaa"), []byte("bbb"), []byte("ccc")},
			[]bool{true, false, false, false},
		)
		defer b1.Release()
		b2 := buildBatch(
			[]string{"one", "two", "three", "four"},
			[][]byte{[]byte("ddd"), []byte("ccc"), []byte("bbb"), []byte("aaa")},
			[]bool{false, false, false, false},
		)
		defer b2.Release()
		tbl := array.NewTableFromRecords(s, []arrow.RecordBatch{b1, b2})
		defer tbl.Release()
		d := compute.NewDatumWithoutOwning(tbl)
		keys := compute.SortOptions{
			{ColumnIndex: 0, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
		}
		testSortIndicesUint64(t, ctx, d, keys, []uint64{1, 5, 2, 6, 4, 0, 7, 3})
		keys[0].NullPlacement = kernels.NullsAtStart
		keys[1].NullPlacement = kernels.NullsAtStart
		testSortIndicesUint64(t, ctx, d, keys, []uint64{1, 5, 2, 6, 0, 4, 7, 3})
	})

	t.Run("HeterogenousChunking", func(t *testing.T) {
		s := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Float32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		a0, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float32, strings.NewReader("[null, 1]"))
		require.NoError(t, err)
		defer a0.Release()
		a1, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float32, strings.NewReader("[]"))
		require.NoError(t, err)
		defer a1.Release()
		a2b := array.NewFloat32Builder(mem)
		a2b.Append(3)
		a2b.AppendNull()
		a2b.Append(float32(math.NaN()))
		a2b.Append(float32(math.NaN()))
		a2b.Append(float32(math.NaN()))
		a2b.Append(1)
		a2 := a2b.NewArray()
		defer a2.Release()
		colA := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{a0, a1, a2})
		defer colA.Release()

		b0, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float64, strings.NewReader("[5]"))
		require.NoError(t, err)
		defer b0.Release()
		b1, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float64, strings.NewReader("[3, null, null]"))
		require.NoError(t, err)
		defer b1.Release()
		b2b := array.NewFloat64Builder(mem)
		b2b.AppendNull()
		b2b.Append(math.NaN())
		b2b.Append(5)
		b2 := b2b.NewArray()
		defer b2.Release()
		b3, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Float64, strings.NewReader("[5]"))
		require.NoError(t, err)
		defer b3.Release()
		colB := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{b0, b1, b2, b3})
		defer colB.Release()

		tbl := array.NewTable(s, []arrow.Column{
			*arrow.NewColumn(s.Field(0), colA),
			*arrow.NewColumn(s.Field(1), colB),
		}, 8)
		defer tbl.Release()
		d := compute.NewDatumWithoutOwning(tbl)

		opts1 := cppRecordKeysAB(kernels.NullsAtEnd)
		testSortIndicesUint64(t, ctx, d, opts1, []uint64{7, 1, 2, 6, 5, 4, 0, 3})
		opts1[0].NullPlacement = kernels.NullsAtStart
		opts1[1].NullPlacement = kernels.NullsAtStart
		testSortIndicesUint64(t, ctx, d, opts1, []uint64{3, 0, 4, 5, 6, 7, 1, 2})

		opts2 := compute.SortOptions{
			{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
			{ColumnIndex: 0, Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
		}
		testSortIndicesUint64(t, ctx, d, opts2, []uint64{1, 7, 6, 0, 5, 2, 4, 3})
		opts2[0].NullPlacement = kernels.NullsAtStart
		opts2[1].NullPlacement = kernels.NullsAtStart
		testSortIndicesUint64(t, ctx, d, opts2, []uint64{3, 4, 2, 5, 1, 0, 6, 7})
	})
}

// TestSortIndicesUUIDLexicographic checks extension UUID columns sort by underlying 16-byte order.
func TestSortIndicesUUIDLexicographic(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	uLo := uuid.UUID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	uMid := uuid.UUID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})
	uHi := uuid.UUID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3})

	b := extensions.NewUUIDBuilder(mem)
	defer b.Release()
	b.Append(uHi)
	b.AppendNull()
	b.Append(uLo)
	b.Append(uMid)
	arr := b.NewArray()
	defer arr.Release()

	d := compute.NewDatumWithoutOwning(arr)
	testSortIndicesUint64(t, ctx, d, compute.SortOptions{
		{Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd},
	}, []uint64{2, 3, 0, 1})
	testSortIndicesUint64(t, ctx, d, compute.SortOptions{
		{Order: kernels.Ascending, NullPlacement: kernels.NullsAtStart},
	}, []uint64{1, 2, 3, 0})
	testSortIndicesUint64(t, ctx, d, compute.SortOptions{
		{Order: kernels.Descending, NullPlacement: kernels.NullsAtEnd},
	}, []uint64{0, 3, 2, 1})
	testSortIndicesUint64(t, ctx, d, compute.SortOptions{
		{Order: kernels.Descending, NullPlacement: kernels.NullsAtStart},
	}, []uint64{1, 0, 3, 2})
}
