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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestPutIndicesRejectsOutOfBoundsIndices(t *testing.T) {
	dictionary, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32,
		strings.NewReader(`[10]`))
	require.NoError(t, err)
	defer dictionary.Release()

	tests := []struct {
		name       string
		newIndices func() arrow.Array
	}{
		{
			name: "negative signed index",
			newIndices: func() arrow.Array {
				builder := array.NewInt8Builder(memory.DefaultAllocator)
				defer builder.Release()
				builder.Append(-1)
				return builder.NewArray()
			},
		},
		{
			name: "index equal to dictionary length",
			newIndices: func() arrow.Array {
				builder := array.NewInt16Builder(memory.DefaultAllocator)
				defer builder.Release()
				builder.Append(0)
				builder.Append(1)
				return builder.NewArray()
			},
		},
		{
			name: "maximum int32 index",
			newIndices: func() arrow.Array {
				builder := array.NewInt32Builder(memory.DefaultAllocator)
				defer builder.Release()
				builder.Append(math.MaxInt32)
				return builder.NewArray()
			},
		},
		{
			name: "large unsigned index",
			newIndices: func() arrow.Array {
				builder := array.NewUint64Builder(memory.DefaultAllocator)
				defer builder.Release()
				builder.Append(math.MaxUint64)
				return builder.NewArray()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indices := tt.newIndices()
			defer indices.Release()

			typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
			descr := schema.NewColumn(typ, 0, 0)
			enc := &typedDictEncoder[int32]{newDictEncoderBase(descr, NewDictionary[int32](), memory.DefaultAllocator)}
			defer enc.Release()
			enc.EnableDictionaryReferenceTracking()
			require.NoError(t, enc.PutDictionary(dictionary))

			err := enc.PutIndices(indices)
			require.ErrorIs(t, err, arrow.ErrInvalid)
			assert.Empty(t, enc.idxValues)
			assert.Empty(t, enc.referencedBitmap)
			assert.Empty(t, enc.ReferencedDictionaryIndices())
		})
	}
}
