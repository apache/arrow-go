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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrayEqualBooleanIgnoresNullValues(t *testing.T) {
	valid := []bool{true, false, true, false, true, true, false, true, true, false, true}
	leftValues := []bool{true, false, false, true, true, false, false, true, false, true, true}
	rightValues := append([]bool(nil), leftValues...)
	for i, isValid := range valid {
		if !isValid {
			rightValues[i] = !rightValues[i]
		}
	}

	left := makeBooleanEqualityArray(leftValues, valid)
	defer left.Release()
	right := makeBooleanEqualityArray(rightValues, valid)
	defer right.Release()

	assert.True(t, array.Equal(left, right))

	leftSlice := array.NewSlice(left, 1, int64(left.Len()-1))
	defer leftSlice.Release()
	rightSlice := array.NewSlice(right, 1, int64(right.Len()-1))
	defer rightSlice.Release()
	assert.True(t, array.Equal(leftSlice, rightSlice))

	rightValues[7] = !rightValues[7]
	different := makeBooleanEqualityArray(rightValues, valid)
	defer different.Release()
	differentSlice := array.NewSlice(different, 1, int64(different.Len()-1))
	defer differentSlice.Release()
	assert.False(t, array.Equal(leftSlice, differentSlice))
}

func TestArrayEqualBooleanWithDifferentOffsets(t *testing.T) {
	values := []bool{true, false, true, true, false, false, true, false, true, true, false, true}
	valid := []bool{true, true, false, true, true, false, true, true, true, false, true, true}

	leftBase := makeBooleanEqualityArray(append([]bool{false}, values...), append([]bool{true}, valid...))
	defer leftBase.Release()
	rightBase := makeBooleanEqualityArray(append([]bool{false, true, false}, values...), append([]bool{true, true, true}, valid...))
	defer rightBase.Release()

	left := array.NewSlice(leftBase, 1, int64(leftBase.Len()))
	defer left.Release()
	right := array.NewSlice(rightBase, 3, int64(rightBase.Len()))
	defer right.Release()

	assert.True(t, array.Equal(left, right))
}

func TestArrayEqualBooleanByValidRuns(t *testing.T) {
	const length = 130
	values := make([]bool, length)
	valid := make([]bool, length)
	for i := range values {
		values[i] = i%3 == 0
		valid[i] = i >= 5 && i < 125
	}
	rightValues := append([]bool(nil), values...)
	for i, isValid := range valid {
		if !isValid {
			rightValues[i] = !rightValues[i]
		}
	}

	left := makeBooleanEqualityArray(values, valid)
	defer left.Release()
	right := makeBooleanEqualityArray(rightValues, valid)
	defer right.Release()
	assert.True(t, array.Equal(left, right))

	rightValues[77] = !rightValues[77]
	different := makeBooleanEqualityArray(rightValues, valid)
	defer different.Release()
	assert.False(t, array.Equal(left, different))
}

func makeBooleanEqualityArray(values, valid []bool) *array.Boolean {
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return builder.NewBooleanArray()
}
