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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryViewBuilderBulkAppendNulls(t *testing.T) {
	starts := []int{0, 1, 7, 8, 9, 15, 16, 17}
	batchSizes := []int{-1, 0, 1, 2, 7, 8, 9, 16, 17}

	for _, start := range starts {
		for _, batchSize := range batchSizes {
			t.Run(fmt.Sprintf("start_%d_batch_%d", start, batchSize), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
				defer mem.AssertSize(t, 0)

				bulk := array.NewBinaryViewBuilder(mem)
				defer bulk.Release()
				scalar := array.NewBinaryViewBuilder(mem)
				defer scalar.Release()

				appendBinaryViewBuilderPrefix(bulk, start)
				appendBinaryViewBuilderPrefix(scalar, start)
				bulk.AppendNulls(batchSize)
				for i := 0; i < batchSize; i++ {
					scalar.AppendNull()
				}

				bulk.Append([]byte("tail"))
				scalar.Append([]byte("tail"))

				assertBinaryViewBuilderArrayParity(t, bulk, scalar)
			})
		}
	}
}

func TestBinaryViewBuilderAppendNullsRejectsLengthOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewBinaryViewBuilder(mem)
	defer builder.Release()
	builder.AppendEmptyValue()

	assert.PanicsWithValue(t, "arrow/array: builder length overflow", func() {
		builder.AppendNulls(math.MaxInt)
	})
	assert.Equal(t, 1, builder.Len())
	assert.Equal(t, 0, builder.NullN())

	arr := builder.NewArray().(*array.BinaryView)
	defer arr.Release()
	require.NoError(t, arr.ValidateFull())
	assert.Equal(t, 1, arr.Len())
	assert.Equal(t, 0, arr.NullN())
	assert.True(t, arr.IsValid(0))
}

func appendBinaryViewBuilderPrefix(builder *array.BinaryViewBuilder, n int) {
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			builder.Append([]byte(fmt.Sprintf("value-%d", i)))
		case 1:
			builder.AppendNull()
		case 2:
			builder.AppendEmptyValue()
		}
	}
}

func assertBinaryViewBuilderArrayParity(t *testing.T, bulk, scalar *array.BinaryViewBuilder) {
	t.Helper()

	assert.Equal(t, scalar.Len(), bulk.Len())
	assert.Equal(t, scalar.NullN(), bulk.NullN())

	bulkArray := bulk.NewArray()
	defer bulkArray.Release()
	scalarArray := scalar.NewArray()
	defer scalarArray.Release()

	require.NoError(t, bulkArray.(interface{ ValidateFull() error }).ValidateFull())
	require.NoError(t, scalarArray.(interface{ ValidateFull() error }).ValidateFull())
	assert.True(t, array.Equal(bulkArray, scalarArray))
}
