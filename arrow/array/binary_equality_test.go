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

func TestBinaryEqualityByValidRuns(t *testing.T) {
	types := []arrow.BinaryDataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.LargeString,
	}
	tests := []struct {
		name       string
		left       []string
		right      []string
		valid      []bool
		want       bool
		leftSlice  [2]int64
		rightSlice [2]int64
	}{
		{
			name:  "equal values",
			left:  []string{"alpha", "beta", "gamma"},
			right: []string{"alpha", "beta", "gamma"},
			valid: []bool{true, true, true},
			want:  true,
		},
		{
			name:  "different payload",
			left:  []string{"alpha", "beta", "gamma"},
			right: []string{"alpha", "zeta", "gamma"},
			valid: []bool{true, true, true},
		},
		{
			name:  "same payload with different boundaries",
			left:  []string{"ab", "c"},
			right: []string{"a", "bc"},
			valid: []bool{true, true},
		},
		{
			name:  "null payload ignored",
			left:  []string{"alpha", "ignored left", "gamma"},
			right: []string{"alpha", "ignored right payload", "gamma"},
			valid: []bool{true, false, true},
			want:  true,
		},
		{
			name:       "different physical offsets",
			left:       []string{"left prefix", "alpha", "beta", "left suffix"},
			right:      []string{"right prefix 1", "right prefix 2", "alpha", "beta"},
			valid:      []bool{true, true, true, true},
			want:       true,
			leftSlice:  [2]int64{1, 3},
			rightSlice: [2]int64{2, 4},
		},
	}

	for _, dtype := range types {
		t.Run(dtype.Name(), func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					left := makeBinaryEqualityArray(memory.DefaultAllocator, dtype, test.left, test.valid)
					defer left.Release()
					right := makeBinaryEqualityArray(memory.DefaultAllocator, dtype, test.right, test.valid)
					defer right.Release()

					if test.leftSlice != [2]int64{} {
						leftSlice := array.NewSlice(left, test.leftSlice[0], test.leftSlice[1])
						defer leftSlice.Release()
						rightSlice := array.NewSlice(right, test.rightSlice[0], test.rightSlice[1])
						defer rightSlice.Release()
						left, right = leftSlice, rightSlice
					}

					assert.Equal(t, test.want, array.Equal(left, right))
				})
			}
		})
	}
}

func TestBinaryEqualityWithFragmentedValidity(t *testing.T) {
	const length = 1024
	leftValues := makeBinaryEqualityValues(length, 16)
	rightValues := append([]string(nil), leftValues...)
	valid := make([]bool, length)
	for i := range valid {
		valid[i] = i%2 == 0
		if !valid[i] {
			rightValues[i] = "different ignored payload"
		}
	}

	left := makeBinaryEqualityArray(memory.DefaultAllocator, arrow.BinaryTypes.String, leftValues, valid)
	defer left.Release()
	right := makeBinaryEqualityArray(memory.DefaultAllocator, arrow.BinaryTypes.String, rightValues, valid)
	require.True(t, array.Equal(left, right))

	right.Release()
	rightValues[length-2] = "different valid payload"
	right = makeBinaryEqualityArray(memory.DefaultAllocator, arrow.BinaryTypes.String, rightValues, valid)
	defer right.Release()
	require.False(t, array.Equal(left, right))
}

func TestBinaryEqualityWithDeclaredNullsWithoutBitmap(t *testing.T) {
	makeArray := func() *array.Binary {
		offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8}))
		values := memory.NewBufferBytes([]byte("abcdefgh"))
		data := array.NewData(
			arrow.BinaryTypes.Binary,
			8,
			[]*memory.Buffer{nil, offsets, values},
			nil,
			1,
			0,
		)
		offsets.Release()
		values.Release()
		result := array.NewBinaryData(data)
		data.Release()
		return result
	}

	left := makeArray()
	defer left.Release()
	right := makeArray()
	defer right.Release()

	var equal bool
	assert.NotPanics(t, func() { equal = array.Equal(left, right) })
	assert.True(t, equal)
}

func makeBinaryEqualityArray(
	mem memory.Allocator, dtype arrow.BinaryDataType, values []string, valid []bool,
) arrow.Array {
	switch dtype.ID() {
	case arrow.BINARY:
		builder := array.NewBinaryBuilder(mem, dtype)
		defer builder.Release()
		builder.AppendStringValues(values, valid)
		return builder.NewBinaryArray()
	case arrow.STRING:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(values, valid)
		return builder.NewStringArray()
	case arrow.LARGE_BINARY:
		builder := array.NewBinaryBuilder(mem, dtype)
		defer builder.Release()
		builder.AppendStringValues(values, valid)
		return builder.NewLargeBinaryArray()
	case arrow.LARGE_STRING:
		builder := array.NewLargeStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(values, valid)
		return builder.NewLargeStringArray()
	default:
		panic("unsupported binary type")
	}
}
