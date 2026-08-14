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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFixedSizeListArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs      = []int32{0, 1, 2, 0, 0, 0, 3, 4, 5}
		isValid = []bool{true, false, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	for i := 0; i < 10; i++ {
		vb := lb.ValueBuilder().(*array.Int32Builder)
		vb.Reserve(len(vs))

		lb.Append(true)
		vb.AppendValues(vs[:3], nil)
		lb.AppendNull()
		lb.Append(true)
		vb.AppendValues(vs[6:], nil)

		arr := lb.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		arr.Retain()
		arr.Release()

		if got, want := arr.DataType().ID(), arrow.FIXED_SIZE_LIST; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}

		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		for i := range isValid {
			if got, want := arr.IsValid(i), isValid[i]; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
			if got, want := arr.IsNull(i), !isValid[i]; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		varr := arr.ListValues().(*array.Int32)
		if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}

func TestFixedSizeListArrayEmpty(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()
	if got, want := arr.Len(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestFixedSizeListArrayBulkAppend(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs      = []int32{0, 1, 2, 0, 0, 0, 3, 4, 5}
		isValid = []bool{true, false, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	lb.AppendValues(isValid)
	for _, v := range vs {
		vb.Append(v)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	if got, want := arr.DataType().ID(), arrow.FIXED_SIZE_LIST; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	if got, want := arr.Len(), len(isValid); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range isValid {
		if got, want := arr.IsValid(i), isValid[i]; got != want {
			t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
		}
		if got, want := arr.IsNull(i), !isValid[i]; got != want {
			t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
		}
	}

	varr := arr.ListValues().(*array.Int32)
	if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestFixedSizeListArrayBulkAppendNullsAndEmptyValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Int32Builder)

	lb.Append(true)
	vb.AppendValues([]int32{1, 2, 3}, nil)
	lb.AppendNulls(5)
	lb.AppendEmptyValues(4)
	lb.Append(true)
	vb.AppendValues([]int32{4, 5, 6}, nil)

	arr := lb.NewListArray()
	defer arr.Release()
	assert.Equal(t, 11, arr.Len())
	assert.Equal(t, 5, arr.NullN())
	valid := []bool{true, false, false, false, false, false, true, true, true, true, true}
	for i, want := range valid {
		assert.Equal(t, want, arr.IsValid(i), "list value %d", i)
	}

	values := arr.ListValues().(*array.Int32)
	assert.Equal(t, 33, values.Len())
	assert.Equal(t, 15, values.NullN())
	assert.Equal(t, []int32{1, 2, 3}, values.Int32Values()[:3])
	for i := 3; i < 18; i++ {
		assert.True(t, values.IsNull(i), "child value %d", i)
	}
	for i := 18; i < 30; i++ {
		assert.True(t, values.IsValid(i), "child value %d", i)
		assert.Zero(t, values.Value(i), "child value %d", i)
	}
	assert.Equal(t, []int32{4, 5, 6}, values.Int32Values()[30:])
}

func TestFixedSizeListArrayBulkAppendNested(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	lb := array.NewFixedSizeListBuilder(pool, 2, arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32))
	defer lb.Release()

	lb.AppendNulls(2)
	lb.AppendEmptyValues(2)

	arr := lb.NewListArray()
	defer arr.Release()
	assert.NoError(t, arr.ValidateFull())
	assert.Equal(t, 4, arr.Len())
	assert.Equal(t, 2, arr.NullN())

	children := arr.ListValues().(*array.FixedSizeList)
	assert.Equal(t, 8, children.Len())
	assert.Equal(t, 4, children.NullN())

	values := children.ListValues().(*array.Int32)
	assert.Equal(t, 24, values.Len())
	assert.Equal(t, 12, values.NullN())
	for i := 0; i < 12; i++ {
		assert.True(t, values.IsNull(i), "child value %d", i)
	}
	for i := 12; i < 24; i++ {
		assert.True(t, values.IsValid(i), "child value %d", i)
		assert.Zero(t, values.Value(i), "child value %d", i)
	}
}

func BenchmarkFixedSizeListBuilderBulkAppend(b *testing.B) {
	for _, rows := range []int{1, 8, 64, 1024, 65536} {
		for _, width := range []int32{1, 4, 16, 64} {
			name := fmt.Sprintf("rows=%d/width=%d", rows, width)
			b.Run("nulls/"+name, func(b *testing.B) {
				benchmarkFixedSizeListBuilderBulkAppend(b, rows, width, false)
			})
			b.Run("empty/"+name, func(b *testing.B) {
				benchmarkFixedSizeListBuilderBulkAppend(b, rows, width, true)
			})
		}
	}
}

func benchmarkFixedSizeListBuilderBulkAppend(b *testing.B, rows int, width int32, empty bool) {
	bldr := array.NewFixedSizeListBuilder(memory.DefaultAllocator, width, arrow.PrimitiveTypes.Int32)
	defer bldr.Release()
	b.ReportAllocs()
	b.StopTimer()

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		if empty {
			bldr.AppendEmptyValues(rows)
		} else {
			bldr.AppendNulls(rows)
		}
		b.StopTimer()

		arr := bldr.NewListArray()
		arr.Release()
	}
}

func TestFixedSizeListArrayStringer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const N = 3
	var (
		vs      = [][N]int32{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, -9, -8}}
		isValid = []bool{true, false, true, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	for i, v := range vs {
		lb.Append(isValid[i])
		vb.AppendValues(v[:], nil)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	arr.Retain()
	arr.Release()

	want := `[[0 1 2] (null) [6 7 8] [9 -9 -8]]`
	if got, want := arr.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
	assert.Equal(t, "[0,1,2]", arr.ValueStr(0))
	assert.Equal(t, array.NullValueStr, arr.ValueStr(1))
}

func TestFixedSizeListArraySlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const N = 3
	var (
		vs      = [][N]int32{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, -9, -8}}
		isValid = []bool{true, false, true, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	for i, v := range vs {
		lb.Append(isValid[i])
		vb.AppendValues(v[:], nil)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	arr.Retain()
	arr.Release()

	want := `[[0 1 2] (null) [6 7 8] [9 -9 -8]]`
	if got, want := arr.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	sub := array.NewSlice(arr, 1, 3).(*array.FixedSizeList)
	defer sub.Release()

	want = `[(null) [6 7 8]]`
	if got, want := sub.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestFixedSizeListBuilderRejectsMismatchedValueLength(t *testing.T) {
	for _, valueLen := range []int{1, 3} {
		t.Run(fmt.Sprintf("values_%d", valueLen), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			b := array.NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Int32)
			defer b.Release()
			b.Append(true)
			b.ValueBuilder().(*array.Int32Builder).AppendValues(make([]int32, valueLen), nil)

			assert.PanicsWithError(t,
				fmt.Sprintf("invalid: arrow/array: fixed-size list value count must equal list length times list size (values=%d, want=2)", valueLen),
				func() { b.NewListArray() })
		})
	}
}

func TestFixedSizeListStringRoundTrip(t *testing.T) {
	// 1. create array
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const N = 3
	var (
		values = [][N]int32{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, -9, -8}}
		valid  = []bool{true, false, true, true}
	)

	b := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer b.Release()

	vb := b.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(values))

	for i, v := range values {
		b.Append(valid[i])
		vb.AppendValues(v[:], nil)
	}

	arr := b.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.FixedSizeList)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}
