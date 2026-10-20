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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniqueChunkedChangingDictionaries(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int16,
		ValueType: arrow.BinaryTypes.String,
	}
	makeDict := func(indices, values string) *array.Dictionary {
		t.Helper()
		arr, err := array.DictArrayFromJSON(mem, dictType, indices, values)
		require.NoError(t, err)
		return arr
	}

	first := makeDict("[0, 1, 0]", `["a", "b"]`)
	defer first.Release()
	second := makeDict("[0, 1, 0]", `["b", "c"]`)
	defer second.Release()
	// Distinct arrays with equal dictionaries, after unification began.
	third := makeDict("[1, 0, 1]", `["b", "c"]`)
	defer third.Release()
	full := makeDict("[null, 0, 1, 2, null]", `["d", "a", "e"]`)
	defer full.Release()
	sliced := array.NewSlice(full, 1, 5)
	defer sliced.Release()
	require.Equal(t, 1, sliced.Data().Offset())
	revisit := makeDict("[1, 0]", `["a", "b"]`)
	defer revisit.Release()

	input := arrow.NewChunked(dictType, []arrow.Array{first, second, third, sliced, revisit})
	defer input.Release()
	out, err := compute.Unique(compute.WithAllocator(context.Background(), mem),
		&compute.ChunkedDatum{Value: input})
	require.NoError(t, err)
	defer out.Release()

	got := out.(*compute.ArrayDatum).MakeArray().(*array.Dictionary)
	defer got.Release()
	require.Equal(t, 6, got.Len()) // five distinct values plus the masked null
	require.Equal(t, 5, got.Dictionary().Len())
	for i, value := range []string{"a", "b", "c", "d", "e"} {
		assert.Equal(t, value, got.Dictionary().ValueStr(i))
		assert.Equal(t, i, got.GetValueIndex(i))
	}
	assert.True(t, got.IsNull(5))
}

func TestUniqueChunkedDictionaryValueNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int16,
		ValueType: arrow.BinaryTypes.String,
	}
	first, err := array.DictArrayFromJSON(mem, dictType, "[0, 1]", `["one", null]`)
	require.NoError(t, err)
	defer first.Release()
	second, err := array.DictArrayFromJSON(mem, dictType, "[0, 1, 2, null]",
		`[null, "one", "two"]`)
	require.NoError(t, err)
	defer second.Release()

	input := arrow.NewChunked(dictType, []arrow.Array{first, second})
	defer input.Release()
	out, err := compute.Unique(compute.WithAllocator(context.Background(), mem),
		&compute.ChunkedDatum{Value: input})
	require.NoError(t, err)
	defer out.Release()

	got := out.(*compute.ArrayDatum).MakeArray().(*array.Dictionary)
	defer got.Release()
	require.Equal(t, 4, got.Len()) // "one", dictionary null, "two", index null
	require.Equal(t, 3, got.Dictionary().Len())
	assert.Equal(t, "one", got.Dictionary().ValueStr(0))
	assert.True(t, got.Dictionary().IsNull(1))
	assert.Equal(t, "two", got.Dictionary().ValueStr(2))
	assert.True(t, got.IsValid(1)) // valid index pointing to a null dictionary value
	assert.True(t, got.IsNull(3))
}

func TestUniqueChunkedDictionaryIndexOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	chunks := make([]arrow.Array, 129)
	for i := range chunks {
		arr, err := array.DictArrayFromJSON(mem, dictType, "[0]",
			fmt.Sprintf(`["value-%d"]`, i))
		require.NoError(t, err)
		chunks[i] = arr
		defer arr.Release()
	}
	input := arrow.NewChunked(dictType, chunks)
	defer input.Release()

	_, err := compute.Unique(compute.WithAllocator(context.Background(), mem),
		&compute.ChunkedDatum{Value: input})
	require.ErrorIs(t, err, arrow.ErrInvalid)
}
