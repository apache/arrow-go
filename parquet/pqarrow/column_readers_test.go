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

package pqarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunksToSingle(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("empty chunked array", func(t *testing.T) {
		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len())
	})

	t.Run("single chunk", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		bldr.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
		arr := bldr.NewInt32Array()
		defer arr.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len())
	})

	t.Run("multiple chunks", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues([]int32{1, 2, 3}, nil)
		chunk1 := bldr.NewInt32Array()
		defer chunk1.Release()

		bldr.AppendValues([]int32{4, 5, 6}, nil)
		chunk2 := bldr.NewInt32Array()
		defer chunk2.Release()

		bldr.AppendValues([]int32{7, 8, 9, 10}, nil)
		chunk3 := bldr.NewInt32Array()
		defer chunk3.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk1, chunk2, chunk3})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 10, result.Len())

		// Verify concatenated values
		resultArr := array.MakeFromData(result).(*array.Int32)
		defer resultArr.Release()
		for i, expected := range []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
			assert.Equal(t, expected, resultArr.Value(i))
		}
	})

	t.Run("multiple chunks with nulls", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues([]int32{1, 0, 3}, []bool{true, false, true})
		chunk1 := bldr.NewInt32Array()
		defer chunk1.Release()

		bldr.AppendValues([]int32{4, 0, 6}, []bool{true, false, true})
		chunk2 := bldr.NewInt32Array()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 6, result.Len())
		assert.Equal(t, 2, result.NullN())

		resultArr := array.MakeFromData(result).(*array.Int32)
		defer resultArr.Release()
		assert.False(t, resultArr.IsValid(1))
		assert.False(t, resultArr.IsValid(4))
		assert.Equal(t, int32(1), resultArr.Value(0))
		assert.Equal(t, int32(3), resultArr.Value(2))
	})

	t.Run("multiple chunks string type", func(t *testing.T) {
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues([]string{"hello", "world"}, nil)
		chunk1 := bldr.NewStringArray()
		defer chunk1.Release()

		bldr.AppendValues([]string{"arrow", "parquet"}, nil)
		chunk2 := bldr.NewStringArray()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 4, result.Len())

		resultArr := array.MakeFromData(result).(*array.String)
		defer resultArr.Release()
		assert.Equal(t, "hello", resultArr.Value(0))
		assert.Equal(t, "parquet", resultArr.Value(3))
	})
}
