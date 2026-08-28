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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInt32DictionaryBuilderAppendEmptyValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.PrimitiveTypes.Int32}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	bldr.AppendEmptyValues(0)
	bldr.AppendEmptyValues(-1)
	assert.Equal(t, 0, bldr.Len())

	bldr.AppendEmptyValues(8)
	result := bldr.NewDictionaryArray()
	defer result.Release()

	dict := result.Dictionary().(*array.Int32)
	assert.Equal(t, 1, dict.Len())
	assert.Equal(t, int32(0), dict.Value(0))
	assert.Equal(t, 8, result.Len())
	assert.Equal(t, 0, result.NullN())
	for i := 0; i < result.Len(); i++ {
		assert.False(t, result.IsNull(i))
		assert.Equal(t, 0, result.GetValueIndex(i))
	}
}

func TestNullDictionaryBuilderAppendEmptyValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.Null}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	bldr.AppendEmptyValues(0)
	bldr.AppendEmptyValues(-1)
	assert.Equal(t, 0, bldr.Len())

	bldr.AppendEmptyValues(8)
	result := bldr.NewDictionaryArray()
	defer result.Release()

	require.Equal(t, 8, result.Len())
	assert.Equal(t, 8, result.NullN())
	assert.Equal(t, 0, result.Dictionary().Len())
	for i := 0; i < result.Len(); i++ {
		assert.True(t, result.IsNull(i))
	}
}
