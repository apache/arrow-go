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

func TestConcatSameDictionarySlices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	backing, err := array.DictArrayFromJSON(mem, dictType, `[0, 1, null, 2, 1, 0]`, `["a", "b", "c"]`)
	require.NoError(t, err)
	defer backing.Release()

	inputs := []arrow.Array{
		array.NewSlice(backing, 1, 5),
		array.NewSlice(backing, 0, 2),
	}
	for _, input := range inputs {
		defer input.Release()
	}

	actual, err := array.Concatenate(inputs, mem)
	require.NoError(t, err)
	defer actual.Release()

	expected, err := array.DictArrayFromJSON(mem, dictType, `[1, null, 2, 1, 0, 1]`, `["a", "b", "c"]`)
	require.NoError(t, err)
	defer expected.Release()

	assert.True(t, array.Equal(expected, actual))
}
