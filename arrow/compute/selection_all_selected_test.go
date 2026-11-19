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

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func newAllSelectedTestBatch(mem memory.Allocator) arrow.RecordBatch {
	ints := array.NewInt64Builder(mem)
	ints.AppendValues([]int64{10, 20, 30, 40}, nil)
	first := ints.NewInt64Array()
	ints.Release()

	strs := array.NewStringBuilder(mem)
	strs.Append("a")
	strs.AppendNull()
	strs.Append("c")
	strs.Append("d")
	second := strs.NewStringArray()
	strs.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	out := array.NewRecordBatch(schema, []arrow.Array{first, second}, 4)
	first.Release()
	second.Release()
	return out
}

func newAllSelectedTestMask(mem memory.Allocator, values ...bool) *array.Boolean {
	b := array.NewBooleanBuilder(mem)
	b.AppendValues(values, nil)
	out := b.NewBooleanArray()
	b.Release()
	return out
}

func TestFilterRecordBatchAllSelectedSliced(t *testing.T) {
	for _, behavior := range []struct {
		name  string
		value compute.NullSelectionBehavior
	}{
		{"drop", compute.SelectionDropNulls},
		{"emit", compute.SelectionEmitNulls},
	} {
		t.Run(behavior.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			batch := newAllSelectedTestBatch(mem)
			mask := newAllSelectedTestMask(mem, false, true, true, true, true, false)
			slice := array.NewSlice(mask, 1, 5)

			got, err := compute.FilterRecordBatch(compute.WithAllocator(context.Background(), mem), batch, slice, &compute.FilterOptions{NullSelection: behavior.value})
			require.NoError(t, err)
			require.Same(t, batch, got)
			batch.Release()
			require.EqualValues(t, 4, got.NumRows())
			require.True(t, got.Column(1).IsNull(1))
			require.Equal(t, "c", got.Column(1).(*array.String).Value(2))
			got.Release()
			slice.Release()
			mask.Release()
			mem.AssertSize(t, 0)
		})
	}
}

func TestFilterRecordBatchAllSelectedSliceWithNullsOutside(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	batch := newAllSelectedTestBatch(mem)
	builder := array.NewBooleanBuilder(mem)
	builder.AppendNull()
	for i := 0; i < 4; i++ {
		builder.Append(true)
	}
	builder.AppendNull()
	mask := builder.NewBooleanArray()
	builder.Release()
	slice := array.NewSlice(mask, 1, 5)

	got, err := compute.FilterRecordBatch(compute.WithAllocator(context.Background(), mem), batch, slice, compute.DefaultFilterOptions())
	require.NoError(t, err)
	require.Same(t, batch, got)
	batch.Release()
	require.EqualValues(t, 4, got.NumRows())
	require.EqualValues(t, 40, got.Column(0).(*array.Int64).Value(3))
	got.Release()
	slice.Release()
	mask.Release()
	mem.AssertSize(t, 0)
}

func TestFilterRecordBatchAllSelectedDoesNotIgnoreNulls(t *testing.T) {
	for _, behavior := range []struct {
		name  string
		value compute.NullSelectionBehavior
		rows  int64
	}{
		{"drop", compute.SelectionDropNulls, 3},
		{"emit", compute.SelectionEmitNulls, 4},
	} {
		t.Run(behavior.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			batch := newAllSelectedTestBatch(mem)
			builder := array.NewBooleanBuilder(mem)
			builder.Append(true)
			builder.AppendNull()
			builder.Append(true)
			builder.Append(true)
			mask := builder.NewBooleanArray()
			builder.Release()

			// A null slot can contain a set data bit. Validity still takes priority.
			bitutil.SetBit(mask.Data().Buffers()[1].Bytes(), 1)
			got, err := compute.FilterRecordBatch(compute.WithAllocator(context.Background(), mem), batch, mask, &compute.FilterOptions{NullSelection: behavior.value})
			require.NoError(t, err)
			require.NotSame(t, batch, got)
			require.Equal(t, behavior.rows, got.NumRows())
			values := got.Column(0).(*array.Int64)
			if behavior.value == compute.SelectionEmitNulls {
				require.True(t, values.IsNull(1))
				require.EqualValues(t, 30, values.Value(2))
			} else {
				require.EqualValues(t, 30, values.Value(1))
			}
			got.Release()
			batch.Release()
			mask.Release()
			mem.AssertSize(t, 0)
		})
	}
}

func TestFilterTableAllSelectedChunked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	batch := newAllSelectedTestBatch(mem)
	tbl := array.NewTableFromRecords(batch.Schema(), []arrow.RecordBatch{batch})
	batch.Release()

	mask := newAllSelectedTestMask(mem, false, true, true, true, true, false)
	first := array.NewSlice(mask, 1, 3)
	second := array.NewSlice(mask, 3, 5)
	chunked := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{first, second})
	selection := compute.NewDatum(chunked)
	first.Release()
	second.Release()
	chunked.Release()

	got, err := compute.FilterTable(compute.WithAllocator(context.Background(), mem), tbl, selection, compute.DefaultFilterOptions())
	require.NoError(t, err)
	require.Same(t, tbl, got)
	tbl.Release()
	require.EqualValues(t, 4, got.NumRows())
	require.EqualValues(t, 30, got.Column(0).Data().Chunk(0).(*array.Int64).Value(2))
	require.True(t, got.Column(1).Data().Chunk(0).IsNull(1))
	got.Release()
	selection.Release()
	mask.Release()
	mem.AssertSize(t, 0)
}
