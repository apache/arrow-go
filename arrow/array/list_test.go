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

package array_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListArray(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()

			for i := 0; i < 10; i++ {
				vb := lb.ValueBuilder().(*array.Int32Builder)
				vb.Reserve(len(vs))

				pos := 0
				for i, length := range lengths {
					lb.AppendWithSize(isValid[i], length)
					for j := 0; j < length; j++ {
						vb.Append(vs[pos])
						pos++
					}
				}

				arr := lb.NewArray().(array.ListLike)
				defer arr.Release()

				arr.Retain()
				arr.Release()

				if got, want := arr.DataType().ID(), tt.typeID; got != want {
					t.Fatalf("got=%v, want=%v", got, want)
				}

				if got, want := arr.Len(), len(isValid); got != want {
					t.Fatalf("got=%d, want=%d", got, want)
				}

				for i := range lengths {
					if got, want := arr.IsValid(i), isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
					if got, want := arr.IsNull(i), !isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
				}

				var gotOffsets, gotSizes interface{}
				switch tt.typeID {
				case arrow.LIST:
					arr := arr.(*array.List)
					gotOffsets = arr.Offsets()
				case arrow.LARGE_LIST:
					arr := arr.(*array.LargeList)
					gotOffsets = arr.Offsets()
				case arrow.LIST_VIEW:
					arr := arr.(*array.ListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				case arrow.LARGE_LIST_VIEW:
					arr := arr.(*array.LargeListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				}

				if !reflect.DeepEqual(gotOffsets, tt.offsets) {
					t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
				}

				if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
					if !reflect.DeepEqual(gotSizes, tt.sizes) {
						t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
					}
				}

				varr := arr.ListValues().(*array.Int32)
				if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%v, want=%v", got, want)
				}
			}
		})
	}
}

// Like the list-view tests in TestListArray, but with out-of-order offsets.
func TestListViewArray(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()

			for i := 0; i < 10; i++ {
				switch lvb := lb.(type) {
				case *array.ListViewBuilder:
					lvb.AppendDimensions(5, 3)
					lb.AppendNull()
					lvb.AppendDimensions(0, 0)
					lvb.AppendDimensions(1, 4)
				case *array.LargeListViewBuilder:
					lvb.AppendDimensions(5, 3)
					lb.AppendNull()
					lvb.AppendDimensions(0, 0)
					lvb.AppendDimensions(1, 4)
				}

				vb := lb.ValueBuilder().(*array.Int32Builder)
				vb.Reserve(len(vs))
				vb.AppendValues(vs, []bool{false, true, true, true, true, true, true, true})

				arr := lb.NewArray().(array.ListLike)
				defer arr.Release()

				arr.Retain()
				arr.Release()

				if got, want := arr.DataType().ID(), tt.typeID; got != want {
					t.Fatalf("got=%v, want=%v", got, want)
				}

				if got, want := arr.Len(), len(isValid); got != want {
					t.Fatalf("got=%d, want=%d", got, want)
				}

				for i := range lengths {
					if got, want := arr.IsValid(i), isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
					if got, want := arr.IsNull(i), !isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
				}

				var gotOffsets, gotSizes interface{}
				switch tt.typeID {
				case arrow.LIST_VIEW:
					arr := arr.(*array.ListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				case arrow.LARGE_LIST_VIEW:
					arr := arr.(*array.LargeListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				}

				if !reflect.DeepEqual(gotOffsets, tt.offsets) {
					t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
				}

				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}

				varr := arr.ListValues().(*array.Int32)
				if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%v, want=%v", got, want)
				}
			}
		})
	}
}

func TestListArrayEmpty(t *testing.T) {
	typ := []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
	}

	for _, dt := range typ {
		t.Run(dt.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			lb := array.NewBuilder(pool, dt)
			defer lb.Release()
			arr := lb.NewArray()
			defer arr.Release()
			if got, want := arr.Len(), 0; got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		})
	}
}

func TestListArrayBulkAppend(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				gotOffsets = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				gotOffsets = arr.Offsets()
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}
			if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestListViewArrayBulkAppend(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}
			if !reflect.DeepEqual(gotSizes, tt.sizes) {
				t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestListArraySlice(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3, 7}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3, 7}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				gotOffsets = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				gotOffsets = arr.Offsets()
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}

			if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.String(), `[[0 1 2] (null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
			assert.Equal(t, "[0,1,2]", arr.ValueStr(0))

			sub := array.NewSlice(arr, 1, 4).(array.ListLike)
			defer sub.Release()

			if got, want := sub.String(), `[(null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
		})
	}
}

func TestListViewArraySlice(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}

			if !reflect.DeepEqual(gotSizes, tt.sizes) {
				t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.String(), `[[0 1 2] (null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
			assert.Equal(t, "[0,1,2]", arr.ValueStr(0))

			sub := array.NewSlice(arr, 1, 4).(array.ListLike)
			defer sub.Release()

			if got, want := sub.String(), `[(null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
		})
	}
}

func TestVarLenListLikeStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builders := []array.VarLenListLikeBuilder{
		array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
	}

	builders1 := []array.VarLenListLikeBuilder{
		array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
	}

	for i, b := range builders {
		defer b.Release()

		vb := b.ValueBuilder().(*array.Int32Builder)

		var values = [][]int32{
			{0, 1, 2, 3, 4, 5, 6},
			{1, 2, 3, 4, 5, 6, 7},
			{2, 3, 4, 5, 6, 7, 8},
			{3, 4, 5, 6, 7, 8, 9},
		}
		for _, value := range values {
			b.AppendNull()
			b.AppendWithSize(true, 2*len(value))
			for _, el := range value {
				vb.Append(el)
				vb.AppendNull()
			}
			b.AppendWithSize(false, 0)
		}

		arr := b.NewArray()
		defer arr.Release()

		// 2. create array via AppendValueFromString
		b1 := builders1[i]
		defer b1.Release()

		for i := 0; i < arr.Len(); i++ {
			assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
		}

		arr1 := b1.NewArray()
		defer arr1.Release()

		assert.True(t, array.Equal(arr, arr1))
	}
}

// Test the string roun-trip for a list-view containing out-of-order offsets.
func TestListViewStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builders := []array.VarLenListLikeBuilder{
		array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
	}

	builders1 := []array.VarLenListLikeBuilder{
		array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
		array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32),
	}

	for i, b := range builders {
		defer b.Release()

		switch lvb := b.(type) {
		case *array.ListViewBuilder:
			lvb.AppendDimensions(5, 3)
			b.AppendNull()
			lvb.AppendDimensions(0, 0)
			lvb.AppendDimensions(1, 4)
		case *array.LargeListViewBuilder:
			lvb.AppendDimensions(5, 3)
			b.AppendNull()
			lvb.AppendDimensions(0, 0)
			lvb.AppendDimensions(1, 4)
		}

		vb := b.ValueBuilder().(*array.Int32Builder)

		vs := []int32{-1, 3, 4, 5, 6, 0, 1, 2}
		isValid := []bool{false, true, true, true, true, true, true, true}
		vb.Reserve(len(vs))
		vb.AppendValues(vs, isValid)

		arr := b.NewArray()
		defer arr.Release()

		// 2. create array via AppendValueFromString
		b1 := builders1[i]
		defer b1.Release()

		for i := 0; i < arr.Len(); i++ {
			assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
		}

		arr1 := b1.NewArray()
		defer arr1.Release()

		assert.True(t, array.Equal(arr, arr1))
	}
}

func TestRangeOfValuesUsed(t *testing.T) {
	tests := []struct {
		typeID arrow.Type
		dt     arrow.DataType
	}{
		{arrow.LIST, arrow.ListOf(arrow.PrimitiveTypes.Int16)},
		{arrow.LARGE_LIST, arrow.LargeListOf(arrow.PrimitiveTypes.Int16)},
		{arrow.LIST_VIEW, arrow.ListViewOf(arrow.PrimitiveTypes.Int16)},
		{arrow.LARGE_LIST_VIEW, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int16)},
	}
	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			isListView := tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW

			bldr := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer bldr.Release()

			var arr array.VarLenListLike

			// Empty array
			arr = bldr.NewArray().(array.VarLenListLike)
			defer arr.Release()
			offset, len := array.RangeOfValuesUsed(arr)
			assert.Equal(t, 0, offset)
			assert.Equal(t, 0, len)

			// List-like array with only nulls
			bldr.AppendNulls(3)
			arr = bldr.NewArray().(array.VarLenListLike)
			defer arr.Release()
			offset, len = array.RangeOfValuesUsed(arr)
			assert.Equal(t, 0, offset)
			assert.Equal(t, 0, len)

			// Array with nulls and non-nulls (starting at a non-zero offset)
			vb := bldr.ValueBuilder().(*array.Int16Builder)
			vb.Append(-2)
			vb.Append(-1)
			bldr.AppendWithSize(false, 0)
			bldr.AppendWithSize(true, 2)
			vb.Append(0)
			vb.Append(1)
			bldr.AppendWithSize(true, 3)
			vb.Append(2)
			vb.Append(3)
			vb.Append(4)
			if isListView {
				vb.Append(10)
				vb.Append(11)
			}
			arr = bldr.NewArray().(array.VarLenListLike)
			defer arr.Release()
			offset, len = array.RangeOfValuesUsed(arr)
			assert.Equal(t, 2, offset)
			assert.Equal(t, 5, len)

			// Overlapping list-views
			// [null, [0, 1, 2, 3, 4, 5], [1, 2], null, [4], null, null]
			vb = bldr.ValueBuilder().(*array.Int16Builder)
			vb.Append(-2)
			vb.Append(-1)
			bldr.AppendWithSize(false, 0)
			if isListView {
				bldr.AppendWithSize(true, 6)
				vb.Append(0)
				bldr.AppendWithSize(true, 2)
				vb.Append(1)
				vb.Append(2)
				vb.Append(3)
				bldr.AppendWithSize(false, 0)
				bldr.AppendWithSize(true, 1)
				vb.Append(4)
				vb.Append(5)
				// -- used range ends here --
				vb.Append(10)
				vb.Append(11)
			} else {
				bldr.AppendWithSize(true, 6)
				vb.Append(0)
				vb.Append(1)
				vb.Append(2)
				vb.Append(3)
				vb.Append(4)
				vb.Append(5)
				bldr.AppendWithSize(true, 2)
				vb.Append(1)
				vb.Append(2)
				bldr.AppendWithSize(false, 0)
				bldr.AppendWithSize(true, 1)
				vb.Append(4)
			}
			bldr.AppendNulls(2)
			arr = bldr.NewArray().(array.VarLenListLike)
			defer arr.Release()

			// Check the range
			offset, len = array.RangeOfValuesUsed(arr)
			assert.Equal(t, 2, offset)
			if isListView {
				assert.Equal(t, 6, len)
			} else {
				assert.Equal(t, 9, len)
			}
		})
	}
}

func TestListUnmarshalNonNullableElem(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	tests := []struct {
		name      string
		dt        arrow.DataType
		jsonInput string
		wantErr   string
		want      string
	}{
		{
			name:      "list",
			dt:        arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "large list",
			dt:        arrow.LargeListOfNonNullable(arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "list view",
			dt:        arrow.ListViewOfNonNullable(arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "large list view",
			dt:        arrow.LargeListViewOfNonNullable(arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "fixed size list",
			dt:        arrow.FixedSizeListOfNonNullable(2, arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "nested non-nullable elem",
			dt:        arrow.ListOf(arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32)),
			jsonInput: `[[[1, null]]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "non-nullable struct elem",
			dt:        arrow.ListOfNonNullable(arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true})),
			jsonInput: `[[null]]`,
			wantErr:   "field 'item' is non-nullable but got null",
		},
		{
			name:      "nullable elem accepts null",
			dt:        arrow.ListOf(arrow.PrimitiveTypes.Int32),
			jsonInput: `[[1, null]]`,
			want:      `[[1 (null)]]`,
		},
		{
			name:      "non-nullable elem accepts values",
			dt:        arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int64),
			jsonInput: `[[9007199254740993]]`,
			want:      `[[9007199254740993]]`,
		},
		{
			name:      "non-nullable elem accepts null list",
			dt:        arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32),
			jsonInput: `[null, [1]]`,
			want:      `[(null) [1]]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arr, _, err := array.FromJSON(pool, tc.dt, strings.NewReader(tc.jsonInput))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			defer arr.Release()

			require.NoError(t, array.ValidateFull(arr))
			require.Equal(t, tc.want, arr.String())
		})
	}
}

func TestListBuilderUnmarshalOneRollback(t *testing.T) {
	tests := []struct {
		name      string
		dt        arrow.DataType
		bad       string
		good      string
		wantErr   string
		want      string
		wantElems int
	}{
		{
			name:      "list null elem",
			dt:        arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32),
			bad:       `[1, null]`,
			good:      `[1, 2]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "list wrong elem type",
			dt:        arrow.ListOf(arrow.PrimitiveTypes.Int32),
			bad:       `[1, "nope"]`,
			good:      `[1, 2]`,
			wantErr:   "cannot unmarshal",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "large list null elem",
			dt:        arrow.LargeListOfNonNullable(arrow.PrimitiveTypes.Int32),
			bad:       `[1, null]`,
			good:      `[1, 2]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "list view null elem",
			dt:        arrow.ListViewOfNonNullable(arrow.PrimitiveTypes.Int32),
			bad:       `[1, null]`,
			good:      `[1, 2]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "large list view null elem",
			dt:        arrow.LargeListViewOfNonNullable(arrow.PrimitiveTypes.Int32),
			bad:       `[1, null]`,
			good:      `[1, 2]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "fixed size list null elem",
			dt:        arrow.FixedSizeListOfNonNullable(2, arrow.PrimitiveTypes.Int32),
			bad:       `[1, null]`,
			good:      `[1, 2]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[1 2]]`,
			wantElems: 2,
		},
		{
			name:      "nested list null elem",
			dt:        arrow.ListOf(arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32)),
			bad:       `[[1, null]]`,
			good:      `[[1, 2]]`,
			wantErr:   "field 'item' is non-nullable but got null",
			want:      `[[[1 2]]]`,
			wantElems: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			bldr := array.NewBuilder(pool, tc.dt).(array.ListLikeBuilder)
			defer bldr.Release()

			require.ErrorContains(t, bldr.UnmarshalOne(json.NewDecoder(strings.NewReader(tc.bad))), tc.wantErr)
			require.Zero(t, bldr.Len())
			require.Zero(t, bldr.ValueBuilder().Len())

			require.NoError(t, bldr.UnmarshalOne(json.NewDecoder(strings.NewReader(tc.good))))
			require.Equal(t, 1, bldr.Len())
			require.Equal(t, tc.wantElems, bldr.ValueBuilder().Len())

			arr := bldr.NewArray()
			defer arr.Release()

			require.NoError(t, array.ValidateFull(arr))
			require.Equal(t, tc.want, arr.String())
		})
	}
}
