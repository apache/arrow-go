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

func TestConcatenateBooleanBitmapSlices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := memory.NewResizableBuffer(mem)
	values.Resize(1)
	values.Bytes()[0] = 0b00010101
	allValid := array.NewBoolean(5, values, nil, 0)
	defer allValid.Release()
	values.Release()

	bldr := array.NewBooleanBuilder(mem)
	bldr.AppendValues(
		[]bool{true, false, true, false, true, false},
		[]bool{true, false, true, true, true, true},
	)
	nullable := bldr.NewBooleanArray()
	bldr.Release()
	defer nullable.Release()

	input := []arrow.Array{
		array.NewSlice(allValid, 0, 5),
		array.NewSlice(nullable, 1, 5),
	}
	for _, arr := range input {
		defer arr.Release()
	}

	actual, err := array.Concatenate(input, mem)
	require.NoError(t, err)
	defer actual.Release()

	expected, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean,
		strings.NewReader("[true, false, true, false, true, null, true, false, true]"))
	require.NoError(t, err)
	defer expected.Release()

	assert.True(t, array.Equal(expected, actual))
}
