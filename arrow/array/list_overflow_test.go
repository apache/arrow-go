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

package array

import (
	"math"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var overInt32 int64 = math.MaxInt32 + 1

func TestListViewBuilderRejectsInt32Overflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := NewListViewBuilder(mem, arrow.Null)
	defer bldr.Release()

	bldr.AppendWithSize(true, 1)
	bldr.ValueBuilder().(*NullBuilder).AppendNull()

	assert.PanicsWithValue(t, "arrow/array: list-view size overflow", func() {
		bldr.AppendWithSize(true, -1)
	})
	assert.PanicsWithValue(t, "arrow/array: list-view offset and size overflow", func() {
		bldr.AppendWithSize(true, math.MaxInt32)
	})
	assert.PanicsWithValue(t, "arrow/array: list-view offset overflow", func() {
		bldr.AppendDimensions(-1, 0)
	})
	assert.PanicsWithValue(t, "arrow/array: list-view offset and size overflow", func() {
		bldr.AppendDimensions(math.MaxInt32, 1)
	})

	t.Run("64bit", func(t *testing.T) {
		if strconv.IntSize == 32 {
			t.Skip("requires 64-bit int")
		}

		assert.PanicsWithValue(t, "arrow/array: list-view size overflow", func() {
			bldr.AppendWithSize(true, int(overInt32))
		})
		assert.PanicsWithValue(t, "arrow/array: list-view offset overflow", func() {
			bldr.AppendDimensions(int(overInt32), 0)
		})
		assert.PanicsWithValue(t, "arrow/array: list-view size overflow", func() {
			bldr.AppendDimensions(0, int(overInt32))
		})
	})

	assert.Equal(t, 1, bldr.Len())
	arr := bldr.NewListViewArray()
	defer arr.Release()
	require.NoError(t, arr.ValidateFull())
	assert.Equal(t, 1, arr.Len())
}

func TestListViewBuilderBulkOverflowLeavesBuilderUnmutated(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("requires 64-bit int")
	}

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := NewListViewBuilder(mem, arrow.Null)
	defer bldr.Release()

	bldr.AppendWithSize(true, 1)
	vb := bldr.ValueBuilder().(*NullBuilder)
	vb.length = int(overInt32)
	vb.nulls = int(overInt32)

	assert.PanicsWithValue(t, "arrow/array: list-view offset overflow", func() {
		bldr.AppendNulls(2)
	})
	assert.Equal(t, 1, bldr.Len())
	assert.Equal(t, 0, bldr.NullN())
	assert.Equal(t, 1, bldr.offsets.Len())
	assert.Equal(t, 1, bldr.sizes.Len())

	assert.PanicsWithValue(t, "arrow/array: list-view offset overflow", func() {
		bldr.AppendEmptyValues(2)
	})
	assert.Equal(t, 1, bldr.Len())
	assert.Equal(t, 0, bldr.NullN())
	assert.Equal(t, 1, bldr.offsets.Len())
	assert.Equal(t, 1, bldr.sizes.Len())

	vb.length = 1
	vb.nulls = 1
	arr := bldr.NewListViewArray()
	defer arr.Release()
	require.NoError(t, arr.ValidateFull())
	assert.Equal(t, 1, arr.Len())
}

func TestListBuilderRejectsInt32OffsetOverflow(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("requires 64-bit int")
	}

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := NewListBuilder(mem, arrow.Null)
	defer bldr.Release()

	bldr.Append(true)
	vb := bldr.ValueBuilder().(*NullBuilder)
	vb.length = int(overInt32)
	vb.nulls = int(overInt32)

	assert.PanicsWithValue(t, "arrow/array: list offset overflow", func() {
		bldr.Append(true)
	})
	assert.PanicsWithValue(t, "arrow/array: list offset overflow", func() {
		bldr.AppendNull()
	})
	assert.PanicsWithValue(t, "arrow/array: list offset overflow", func() {
		bldr.AppendNulls(2)
	})
	assert.PanicsWithValue(t, "arrow/array: list offset overflow", func() {
		bldr.AppendEmptyValues(2)
	})
	assert.Equal(t, 1, bldr.Len())

	// Reset child length to a valid value so the builder can be cleanly finalized.
	vb.length = 1
	vb.nulls = 1
	arr := bldr.NewListArray()
	defer arr.Release()
	assert.Equal(t, 1, arr.Len())
}
