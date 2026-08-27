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

package hashing_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/hashing"
	"github.com/stretchr/testify/assert"
)

func TestMemoTableTruncate(t *testing.T) {
	table := hashing.NewMemoTable[int32](0)

	idx, found, err := table.GetOrInsert(int32(10))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 0, idx)
	idx, found, err = table.GetOrInsert(int32(20))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 1, idx)
	idx, found, err = table.GetOrInsert(int32(30))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 2, idx)

	table.Truncate(2)
	assert.Equal(t, 2, table.Size())
	assertGetMemoValue(t, table, int32(10), 0)
	assertGetMemoValue(t, table, int32(20), 1)
	_, found = table.Get(int32(30))
	assert.False(t, found)

	idx, found, err = table.GetOrInsert(int32(30))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 2, idx)

	table.Truncate(0)
	assert.Equal(t, 0, table.Size())
	_, found = table.Get(int32(10))
	assert.False(t, found)

	idx, found, err = table.GetOrInsert(int32(40))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 0, idx)
}

func TestMemoTableTruncateNoOp(t *testing.T) {
	table := hashing.NewMemoTable[int32](0)

	idx, found, err := table.GetOrInsert(int32(7))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 0, idx)

	table.Truncate(1)
	table.Truncate(math.MaxInt)

	assert.Equal(t, 1, table.Size())
	assertGetMemoValue(t, table, int32(7), 0)
}

func assertGetMemoValue(t *testing.T, table *hashing.Table[int32], value int32, want int) {
	t.Helper()
	got, found := table.Get(value)
	assert.True(t, found)
	assert.Equal(t, want, got)
}

func TestBinaryMemoTableTruncate(t *testing.T) {
	t.Run("reinsert discarded values", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		table := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary))
		defer table.Release()

		insertBinaryMemoValue(t, table, "a", 0)
		insertBinaryMemoNull(t, table, 1)
		insertBinaryMemoValue(t, table, "discarded", 2)

		table.Truncate(2)
		assert.Equal(t, 2, table.Size())
		assertGetBinaryMemoValue(t, table, "a", 0)
		assertGetBinaryMemoNull(t, table, 1)
		_, found := table.Get("discarded")
		assert.False(t, found)

		insertBinaryMemoValue(t, table, "discarded", 2)
		assertGetBinaryMemoValue(t, table, "discarded", 2)
	})

	t.Run("retains or removes null at the truncation boundary", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		table := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary))
		defer table.Release()

		insertBinaryMemoValue(t, table, "kept", 0)
		insertBinaryMemoNull(t, table, 1)
		insertBinaryMemoValue(t, table, "removed", 2)

		table.Truncate(1)
		assert.Equal(t, 1, table.Size())
		_, found := table.GetNull()
		assert.False(t, found)
		insertBinaryMemoNull(t, table, 1)
		assertGetBinaryMemoNull(t, table, 1)

		table.Truncate(0)
		assert.Equal(t, 0, table.Size())
		_, found = table.GetNull()
		assert.False(t, found)
		insertBinaryMemoValue(t, table, "after zero", 0)
		assertGetBinaryMemoValue(t, table, "after zero", 0)
	})

	t.Run("rewinds binary data and offsets", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		table := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary))
		defer table.Release()

		insertBinaryMemoValue(t, table, "alpha", 0)
		insertBinaryMemoValue(t, table, "bravo", 1)
		insertBinaryMemoValue(t, table, "discarded", 2)

		table.Truncate(2)
		assert.Equal(t, len("alphabravo"), table.ValuesSize())
		assert.Equal(t, []int32{0, 5, 10}, binaryMemoOffsets(table))

		insertBinaryMemoValue(t, table, "charlie", 2)
		assert.Equal(t, len("alphabravocharlie"), table.ValuesSize())
		assert.Equal(t, []int32{0, 5, 10, 17}, binaryMemoOffsets(table))
		values := make([]byte, table.ValuesSize())
		table.CopyValues(values)
		assert.Equal(t, "alphabravocharlie", string(values))
	})

	t.Run("preserves hash probing", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		table := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary))
		defer table.Release()

		first, second := findHashBucketCollision()
		insertBinaryMemoValue(t, table, first, 0)
		insertBinaryMemoValue(t, table, second, 1)

		table.Truncate(1)
		assertGetBinaryMemoValue(t, table, first, 0)
		_, found := table.Get(second)
		assert.False(t, found)

		insertBinaryMemoValue(t, table, second, 1)
		assertGetBinaryMemoValue(t, table, second, 1)
	})
}

func findHashBucketCollision() (string, string) {
	seen := make(map[uint64]string)
	for i := 0; ; i++ {
		value := fmt.Sprintf("collision-%d", i)
		bucket := hashing.Hash([]byte(value), 0) & 31
		if previous, ok := seen[bucket]; ok {
			return previous, value
		}
		seen[bucket] = value
	}
}

func insertBinaryMemoValue(t *testing.T, table *hashing.BinaryMemoTable, value string, want int) {
	t.Helper()
	idx, found, err := table.GetOrInsert(value)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, want, idx)
}

func insertBinaryMemoNull(t *testing.T, table *hashing.BinaryMemoTable, want int) {
	t.Helper()
	idx, found := table.GetOrInsertNull()
	assert.False(t, found)
	assert.Equal(t, want, idx)
}

func assertGetBinaryMemoValue(t *testing.T, table *hashing.BinaryMemoTable, value string, want int) {
	t.Helper()
	idx, found := table.Get(value)
	assert.True(t, found)
	assert.Equal(t, want, idx)
}

func assertGetBinaryMemoNull(t *testing.T, table *hashing.BinaryMemoTable, want int) {
	t.Helper()
	idx, found := table.GetNull()
	assert.True(t, found)
	assert.Equal(t, want, idx)
}

func binaryMemoOffsets(table *hashing.BinaryMemoTable) []int32 {
	offsets := make([]int32, table.Size()+1)
	table.CopyOffsets(offsets)
	return offsets
}
