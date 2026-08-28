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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparseUnionEqualRuns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	left := newSparseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0, 0, 1]`,
		`[10, null, 12, 13, 14, 15, 16, 17]`,
		`["a", "b", "c", "d", "e", "f", "g", "h"]`)
	defer left.Release()
	right := newSparseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0, 0, 1]`,
		`[10, null, 12, 13, 14, 15, 16, 17]`,
		`["a", "b", "c", "d", "e", "f", "g", "h"]`)
	defer right.Release()

	assert.True(t, array.Equal(left, right))
	assert.True(t, array.ApproxEqual(left, right))

	leftSlice := array.NewSlice(left, 1, 7)
	defer leftSlice.Release()
	rightSlice := array.NewSlice(right, 1, 7)
	defer rightSlice.Release()
	assert.True(t, array.Equal(leftSlice, rightSlice))
	assert.True(t, array.ApproxEqual(leftSlice, rightSlice))

	different := newSparseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0, 0, 1]`,
		`[10, 99, 12, 13, 14, 15, 16, 17]`,
		`["a", "b", "c", "d", "e", "f", "g", "h"]`)
	defer different.Release()
	assert.False(t, array.Equal(left, different))
	assert.False(t, array.ApproxEqual(left, different))
}

func TestDenseUnionEqualRuns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	left := newDenseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0]`,
		`[0, 2, 3, 0, 1, 4]`,
		`[10, 99, 20, null, 40]`,
		`["a", "b"]`)
	defer left.Release()
	right := newDenseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0]`,
		`[0, 2, 3, 0, 1, 4]`,
		`[10, 88, 20, null, 40]`,
		`["a", "b"]`)
	defer right.Release()

	assert.True(t, array.Equal(left, right))
	assert.True(t, array.ApproxEqual(left, right))

	leftSlice := array.NewSlice(left, 1, 5)
	defer leftSlice.Release()
	rightSlice := array.NewSlice(right, 1, 5)
	defer rightSlice.Release()
	assert.True(t, array.Equal(leftSlice, rightSlice))
	assert.True(t, array.ApproxEqual(leftSlice, rightSlice))

	leftWithDifferentOffsets := newDenseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 0]`,
		`[0, 1, 2, 3]`,
		`[10, 20, 30, 40]`,
		`[]`)
	defer leftWithDifferentOffsets.Release()
	rightWithDifferentOffsets := newDenseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 0]`,
		`[1, 3, 4, 5]`,
		`[99, 10, 88, 20, 30, 40]`,
		`[]`)
	defer rightWithDifferentOffsets.Release()
	assert.True(t, array.Equal(leftWithDifferentOffsets, rightWithDifferentOffsets))
	assert.True(t, array.ApproxEqual(leftWithDifferentOffsets, rightWithDifferentOffsets))

	different := newDenseUnionEqualTestArray(t, mem,
		`[0, 0, 0, 1, 1, 0]`,
		`[0, 2, 3, 0, 1, 4]`,
		`[10, 88, 21, null, 40]`,
		`["a", "b"]`)
	defer different.Release()
	assert.False(t, array.Equal(left, different))
	assert.False(t, array.ApproxEqual(left, different))
}

func newSparseUnionEqualTestArray(t *testing.T, mem memory.Allocator, typeIDs, ints, stringsJSON string) *array.SparseUnion {
	t.Helper()
	typeIDsArray := newUnionEqualTestArray(t, mem, arrow.PrimitiveTypes.Int8, typeIDs)
	defer typeIDsArray.Release()
	intArray := newUnionEqualTestArray(t, mem, arrow.PrimitiveTypes.Int32, ints)
	defer intArray.Release()
	stringArray := newUnionEqualTestArray(t, mem, arrow.BinaryTypes.String, stringsJSON)
	defer stringArray.Release()

	result, err := array.NewSparseUnionFromArrays(typeIDsArray, []arrow.Array{intArray, stringArray})
	require.NoError(t, err)
	return result
}

func newDenseUnionEqualTestArray(t *testing.T, mem memory.Allocator, typeIDs, offsets, ints, stringsJSON string) *array.DenseUnion {
	t.Helper()
	typeIDsArray := newUnionEqualTestArray(t, mem, arrow.PrimitiveTypes.Int8, typeIDs)
	defer typeIDsArray.Release()
	offsetsArray := newUnionEqualTestArray(t, mem, arrow.PrimitiveTypes.Int32, offsets)
	defer offsetsArray.Release()
	intArray := newUnionEqualTestArray(t, mem, arrow.PrimitiveTypes.Int32, ints)
	defer intArray.Release()
	stringArray := newUnionEqualTestArray(t, mem, arrow.BinaryTypes.String, stringsJSON)
	defer stringArray.Release()

	result, err := array.NewDenseUnionFromArrays(typeIDsArray, offsetsArray, []arrow.Array{intArray, stringArray})
	require.NoError(t, err)
	return result
}

func newUnionEqualTestArray(t *testing.T, mem memory.Allocator, dtype arrow.DataType, values string) arrow.Array {
	t.Helper()
	result, _, err := array.FromJSON(mem, dtype, strings.NewReader(values))
	require.NoError(t, err)
	return result
}
