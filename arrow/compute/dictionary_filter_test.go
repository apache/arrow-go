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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
	"github.com/stretchr/testify/require"
)

func TestDictionaryFilterDirectIndices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	input, err := array.DictArrayFromJSON(mem, dictType, `[2, 0, 1, 0, 2, 1]`, `["a", null, "c"]`)
	require.NoError(t, err)
	defer input.Release()

	cases := []struct {
		name       string
		filter     string
		nullSelect compute.NullSelectionBehavior
		want       string
	}{
		{
			name:       "drop null filter values",
			filter:     `[true, true, false, null, true, false]`,
			nullSelect: compute.SelectionDropNulls,
			want:       `[2, 0, 2]`,
		},
		{
			name:       "emit null filter values",
			filter:     `[true, true, false, null, true, null]`,
			nullSelect: compute.SelectionEmitNulls,
			want:       `[2, 0, null, 2, null]`,
		},
		{
			name:       "all selected",
			filter:     `[true, true, true, true, true, true]`,
			nullSelect: compute.SelectionDropNulls,
			want:       `[2, 0, 1, 0, 2, 1]`,
		},
		{
			name:       "none selected",
			filter:     `[false, false, false, false, false, false]`,
			nullSelect: compute.SelectionDropNulls,
			want:       `[]`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(tc.filter))
			require.NoError(t, err)
			defer filter.Release()

			got, err := compute.FilterArray(ctx, input, filter, compute.FilterOptions{NullSelection: tc.nullSelect})
			require.NoError(t, err)
			defer got.Release()

			gotDict, ok := got.(*array.Dictionary)
			require.Truef(t, ok, "expected *array.Dictionary, got %T", got)
			require.True(t, arrow.TypeEqual(dictType, gotDict.DataType()))

			wantIndices, _, err := array.FromJSON(mem, dictType.IndexType, strings.NewReader(tc.want))
			require.NoError(t, err)
			defer wantIndices.Release()
			require.True(t, array.Equal(wantIndices, gotDict.Indices()))

			require.True(t, array.Equal(input.(*array.Dictionary).Dictionary(), gotDict.Dictionary()))
			for i, buf := range input.(*array.Dictionary).Dictionary().Data().Buffers() {
				require.Same(t, buf, gotDict.Dictionary().Data().Buffers()[i])
			}
			require.Equal(t, input.(*array.Dictionary).Dictionary().Len(), gotDict.Dictionary().Len())
		})
	}

	allSelected := mustBoolArray(t, mem, `[true, true, true, true, true, true]`)
	defer allSelected.Release()
	filtered, err := compute.FilterArray(ctx, input, allSelected, *compute.DefaultFilterOptions())
	require.NoError(t, err)
	defer filtered.Release()
	filteredDict := filtered.(*array.Dictionary)
	require.False(t, filteredDict.Indices().IsNull(2))
	require.True(t, filteredDict.Dictionary().IsNull(1))
}

func TestDictionaryFilterSlicedInputs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	baseInput, err := array.DictArrayFromJSON(mem, dictType, `[0, 1, 2, 0, 0, 1, 2, 0]`, `["a", null, "c"]`)
	require.NoError(t, err)
	defer baseInput.Release()
	input := array.NewSlice(baseInput, 1, 7).(*array.Dictionary)
	defer input.Release()

	baseFilter := mustBoolArray(t, mem, `[false, false, true, true, false, true, false, true, true, false]`)
	defer baseFilter.Release()
	filter := array.NewSlice(baseFilter, 2, 8)
	defer filter.Release()

	got, err := compute.FilterArray(ctx, input, filter, *compute.DefaultFilterOptions())
	require.NoError(t, err)
	defer got.Release()

	gotDict := got.(*array.Dictionary)
	wantIndices, _, err := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[1, 2, 0, 2]`))
	require.NoError(t, err)
	defer wantIndices.Release()
	require.True(t, array.Equal(wantIndices, gotDict.Indices()))
	require.True(t, array.Equal(input.Dictionary(), gotDict.Dictionary()))
	for i, buf := range input.Dictionary().Data().Buffers() {
		require.Same(t, buf, gotDict.Dictionary().Data().Buffers()[i])
	}
}

func TestDictionaryFilterRetainsNestedDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int16,
		ValueType: arrow.ListOf(arrow.PrimitiveTypes.Int32),
		Ordered:   true,
	}

	// Release the inputs before reading the result to check ownership of both
	// the sliced dictionary buffers and its nested values.
	var got arrow.Array
	func() {
		base, _, err := array.FromJSON(mem, dictType.ValueType, strings.NewReader(`[[-1], [10, 20], null, [], [30], [99]]`))
		require.NoError(t, err)
		defer base.Release()
		dictionary := array.NewSlice(base, 1, 5)
		defer dictionary.Release()
		indices, _, err := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[0, 1, 2, 3, 0]`))
		require.NoError(t, err)
		defer indices.Release()
		input := array.NewDictionaryArray(dictType, indices, dictionary)
		defer input.Release()
		filter := mustBoolArray(t, mem, `[false, true, true, null, true]`)
		defer filter.Release()

		got, err = compute.FilterArray(ctx, input, filter, compute.FilterOptions{NullSelection: compute.SelectionEmitNulls})
		require.NoError(t, err)
	}()
	defer got.Release()

	want, err := array.DictArrayFromJSON(mem, dictType, `[1, 2, null, 0]`, `[[10, 20], null, [], [30]]`)
	require.NoError(t, err)
	defer want.Release()
	require.True(t, array.Equal(want, got))
	require.Equal(t, 1, got.NullN())
	require.Equal(t, 1, got.(*array.Dictionary).Dictionary().Data().Offset())
}

func mustBoolArray(t *testing.T, mem memory.Allocator, json string) arrow.Array {
	t.Helper()
	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(json))
	require.NoError(t, err)
	return arr
}

func TestDictionaryFilterIndexTypesAndChunking(t *testing.T) {
	for _, indexType := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64} {
		t.Run(indexType.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			dt := &arrow.DictionaryType{IndexType: indexType, ValueType: arrow.BinaryTypes.String, Ordered: true}
			base, err := array.DictArrayFromJSON(mem, dt, `[0, 2, null, 1, 0, 2, 1, 0]`, `["a", null, "c"]`)
			require.NoError(t, err)
			defer base.Release()
			input := array.NewSlice(base, 1, 7)
			defer input.Release()
			baseFilter := mustBoolArray(t, mem, `[false, true, true, null, false, true, true, false]`)
			defer baseFilter.Release()
			filter := array.NewSlice(baseFilter, 1, 7)
			defer filter.Release()
			execCtx := compute.DefaultExecCtx()
			execCtx.ChunkSize = 2
			ctx := compute.SetExecCtx(compute.WithAllocator(context.Background(), mem), execCtx)
			for _, tc := range []struct {
				mode compute.NullSelectionBehavior
				want string
			}{
				{compute.SelectionDropNulls, `[2, null, 2, 1]`},
				{compute.SelectionEmitNulls, `[2, null, null, 2, 1]`},
			} {
				valuesDatum, filterDatum := compute.NewDatum(input), compute.NewDatum(filter)
				defer valuesDatum.Release()
				defer filterDatum.Release()
				result, err := compute.Filter(ctx, valuesDatum, filterDatum, compute.FilterOptions{NullSelection: tc.mode})
				require.NoError(t, err)
				defer result.Release()
				require.IsType(t, &compute.ChunkedDatum{}, result)
				got, err := array.Concatenate(result.(*compute.ChunkedDatum).Chunks(), mem)
				require.NoError(t, err)
				defer got.Release()
				dict := got.(*array.Dictionary)
				want, _, err := array.FromJSON(mem, indexType, strings.NewReader(tc.want))
				require.NoError(t, err)
				defer want.Release()
				require.True(t, array.Equal(want, dict.Indices()))
				require.True(t, arrow.TypeEqual(dt, dict.DataType()))
				require.True(t, array.Equal(base.(*array.Dictionary).Dictionary(), dict.Dictionary()))
			}
		})
	}
}
