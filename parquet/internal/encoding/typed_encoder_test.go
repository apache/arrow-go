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

package encoding

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertTypedDictEncoderPut[T int32 | int64 | float32 | float64](t *testing.T, values, expectedDict []T, expectedIndices []int32) {
	t.Helper()
	enc := &typedDictEncoder[T]{newDictEncoderBase(nil, NewDictionary[T](), memory.DefaultAllocator)}
	defer enc.Release()

	split := len(values) / 2
	enc.Put(values[:split])
	enc.Put(values[split:])

	assert.Equal(t, expectedIndices, enc.idxValues)
	actualDict := make([]T, len(expectedDict))
	enc.memo.CopyValues(actualDict)
	assert.Equal(t, expectedDict, actualDict)
	assert.Equal(t, len(expectedDict)*int(unsafe.Sizeof(T(0))), enc.DictEncodedSize())
}

func TestTypedDictEncoderPut(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		assertTypedDictEncoderPut(t,
			[]int32{1, 2, 1, 4, 2},
			[]int32{1, 2, 4},
			[]int32{0, 1, 0, 2, 1})
	})
	t.Run("int64", func(t *testing.T) {
		assertTypedDictEncoderPut(t,
			[]int64{-1, 2, -1, 3, 2},
			[]int64{-1, 2, 3},
			[]int32{0, 1, 0, 2, 1})
	})
	t.Run("float32", func(t *testing.T) {
		assertTypedDictEncoderPut(t,
			[]float32{1.5, 2.5, 1.5, 3.5, 2.5},
			[]float32{1.5, 2.5, 3.5},
			[]int32{0, 1, 0, 2, 1})
	})
	t.Run("float64", func(t *testing.T) {
		assertTypedDictEncoderPut(t,
			[]float64{1.5, 2.5, 1.5, 3.5, 2.5},
			[]float64{1.5, 2.5, 3.5},
			[]int32{0, 1, 0, 2, 1})
	})
}

func TestPutDictionary(t *testing.T) {
	exp := []int32{1, 2, 4, 8, 16}
	ad := array.NewData(
		arrow.PrimitiveTypes.Int32, len(exp),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(exp))},
		nil, 0, 0,
	)
	arr := array.NewInt32Data(ad)

	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(typ, 0, 0)
	enc := &typedDictEncoder[int32]{newDictEncoderBase(descr, NewDictionary[int32](), memory.DefaultAllocator)}

	err := enc.PutDictionary(arr)
	assert.NoError(t, err)
}

func TestDictionaryReferenceTracking(t *testing.T) {
	dictionary, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32,
		strings.NewReader(`[10, 20, 30]`))
	require.NoError(t, err)
	defer dictionary.Release()

	indices, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32,
		strings.NewReader(`[2, null, 2, 1]`))
	require.NoError(t, err)
	defer indices.Release()

	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(typ, 0, 0)
	enc := &typedDictEncoder[int32]{newDictEncoderBase(descr, NewDictionary[int32](), memory.DefaultAllocator)}
	defer enc.Release()
	enc.EnableDictionaryReferenceTracking()

	require.NoError(t, enc.PutDictionary(dictionary))
	require.NoError(t, enc.PutIndices(indices))
	assert.Equal(t, []int32{2, 1}, enc.ReferencedDictionaryIndices())
	assert.False(t, enc.DictionaryIndexReferenced(0))
	assert.True(t, enc.DictionaryIndexReferenced(1))
	assert.True(t, enc.DictionaryIndexReferenced(2))
}
