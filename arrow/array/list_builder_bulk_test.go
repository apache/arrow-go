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

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type listBuilderFactory func(memory.Allocator) array.VarLenListLikeBuilder

var listBuilderFactories = []struct {
	name string
	view bool
	new  listBuilderFactory
}{
	{"list", false, func(mem memory.Allocator) array.VarLenListLikeBuilder {
		return array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	}},
	{"large_list", false, func(mem memory.Allocator) array.VarLenListLikeBuilder {
		return array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
	}},
	{"list_view", true, func(mem memory.Allocator) array.VarLenListLikeBuilder {
		return array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	}},
	{"large_list_view", true, func(mem memory.Allocator) array.VarLenListLikeBuilder {
		return array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	}},
}

func TestListBuilderBulkAppendNullsAndEmptyValues(t *testing.T) {
	for _, tc := range listBuilderFactories {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			builder := tc.new(mem)
			defer builder.Release()
			values := builder.ValueBuilder().(*array.Int32Builder)

			builder.AppendWithSize(true, 2)
			values.AppendValues([]int32{10, 20}, nil)
			builder.AppendNulls(5)
			builder.AppendEmptyValues(4)
			builder.AppendWithSize(true, 1)
			values.Append(30)

			arr := builder.NewArray().(array.VarLenListLike)
			defer arr.Release()
			require.NoError(t, arr.(interface{ ValidateFull() error }).ValidateFull())

			assert.Equal(t, 11, arr.Len())
			assert.Equal(t, 5, arr.NullN())
			for i := range arr.Len() {
				assert.Equal(t, i == 0 || i >= 6, arr.IsValid(i), "list value %d", i)
				start, end := arr.ValueOffsets(i)
				switch i {
				case 0:
					assert.Equal(t, int64(0), start)
					assert.Equal(t, int64(2), end)
				case 10:
					assert.Equal(t, int64(2), start)
					assert.Equal(t, int64(3), end)
				default:
					if tc.view {
						assert.Equal(t, int64(0), start)
						assert.Equal(t, int64(0), end)
					} else {
						assert.Equal(t, int64(2), start)
						assert.Equal(t, int64(2), end)
					}
				}
			}
			switch arr := arr.(type) {
			case *array.ListView:
				assert.Equal(t, []int32{0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, arr.Offsets())
			case *array.LargeListView:
				assert.Equal(t, []int64{0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, arr.Offsets())
			}

			child := arr.ListValues().(*array.Int32)
			require.Equal(t, 3, child.Len())
			assert.Equal(t, []int32{10, 20, 30}, child.Int32Values())
		})
	}
}

func BenchmarkListBuilderBulkAppend(b *testing.B) {
	for _, tc := range listBuilderFactories {
		b.Run(tc.name, func(b *testing.B) {
			for _, rows := range []int{1, 16, 1024, 65536} {
				b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
					b.Run("nulls", func(b *testing.B) {
						benchmarkListBuilderBulkAppend(b, tc.new, rows, false)
					})
					b.Run("empty", func(b *testing.B) {
						benchmarkListBuilderBulkAppend(b, tc.new, rows, true)
					})
				})
			}
		})
	}
}

func benchmarkListBuilderBulkAppend(b *testing.B, factory listBuilderFactory, rows int, empty bool) {
	builder := factory(memory.DefaultAllocator)
	defer builder.Release()
	b.ReportAllocs()

	for b.Loop() {
		if empty {
			builder.AppendEmptyValues(rows)
		} else {
			builder.AppendNulls(rows)
		}

		arr := builder.NewArray()
		arr.Release()
	}
}
