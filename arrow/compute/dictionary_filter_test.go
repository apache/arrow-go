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
	input, err := array.DictArrayFromJSON(mem, dictType, `[2, null, 1, 0, 2, 1]`, `["a", null, "c"]`)
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
			want:       `[2, null, 2]`,
		},
		{
			name:       "emit null filter values",
			filter:     `[true, true, false, null, true, null]`,
			nullSelect: compute.SelectionEmitNulls,
			want:       `[2, null, null, 2, null]`,
		},
		{
			name:       "all selected",
			filter:     `[true, true, true, true, true, true]`,
			nullSelect: compute.SelectionDropNulls,
			want:       `[2, null, 1, 0, 2, 1]`,
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

			require.Same(t, input.Data().(*array.Data).Dictionary(), gotDict.Data().(*array.Data).Dictionary())
			require.Equal(t, input.Dictionary().Len(), gotDict.Dictionary().Len())
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
	baseInput, err := array.DictArrayFromJSON(mem, dictType, `[0, 1, 2, 0, null, 1, 2, 0]`, `["a", null, "c"]`)
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
	wantIndices, _, err := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[1, 2, null, 2]`))
	require.NoError(t, err)
	defer wantIndices.Release()
	require.True(t, array.Equal(wantIndices, gotDict.Indices()))
	require.Same(t, input.Data().(*array.Data).Dictionary(), gotDict.Data().(*array.Data).Dictionary())
}

func mustBoolArray(t *testing.T, mem memory.Allocator, json string) arrow.Array {
	t.Helper()
	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(json))
	require.NoError(t, err)
	return arr
}
