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
	"github.com/apache/arrow-go/v18/arrow/bitutil"
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

func TestBinaryEqualityByValidRunsAcrossNulls(t *testing.T) {
	const length = 256

	types := []arrow.BinaryDataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.LargeString,
	}
	valid := makeLongValidRuns(length)
	tests := []struct {
		name  string
		build func(arrow.BinaryDataType) (arrow.Array, arrow.Array)
		want  bool
	}{
		{
			name: "equal arrays with long valid runs",
			build: func(dtype arrow.BinaryDataType) (arrow.Array, arrow.Array) {
				values := makeBinaryEqualityValues(length, 16)
				return makeBinaryEqualityArray(memory.DefaultAllocator, dtype, values, valid),
					makeBinaryEqualityArray(memory.DefaultAllocator, dtype, values, valid)
			},
			want: true,
		},
		{
			name: "different bytes under null slots are ignored",
			build: func(dtype arrow.BinaryDataType) (arrow.Array, arrow.Array) {
				leftValues := makeBinaryEqualityValues(length, 16)
				rightValues := append([]string(nil), leftValues...)
				for i, isValid := range valid {
					if !isValid {
						rightValues[i] = "different ignored payload"
					}
				}
				return makeRawBinaryEqualityArray(dtype, leftValues, valid),
					makeRawBinaryEqualityArray(dtype, rightValues, valid)
			},
			want: true,
		},
		{
			name: "mismatch in a later valid run",
			build: func(dtype arrow.BinaryDataType) (arrow.Array, arrow.Array) {
				leftValues := makeBinaryEqualityValues(length, 16)
				rightValues := append([]string(nil), leftValues...)
				value := []byte(rightValues[192])
				value[0]++
				rightValues[192] = string(value)
				return makeBinaryEqualityArray(memory.DefaultAllocator, dtype, leftValues, valid),
					makeBinaryEqualityArray(memory.DefaultAllocator, dtype, rightValues, valid)
			},
		},
		{
			name:  "different physical offsets with nulls",
			build: makeSlicedBinaryEqualityPair,
			want:  true,
		},
		{
			name: "shifted value boundary inside a valid run",
			build: func(dtype arrow.BinaryDataType) (arrow.Array, arrow.Array) {
				const length = 192
				valid := make([]bool, length)
				for i := range valid {
					valid[i] = i < 128
				}

				leftValues := makeBinaryEqualityValues(length, 16)
				rightValues := append([]string(nil), leftValues...)
				leftValues[64], leftValues[65] = "ab", "c"
				rightValues[64], rightValues[65] = "a", "bc"
				return makeBinaryEqualityArray(memory.DefaultAllocator, dtype, leftValues, valid),
					makeBinaryEqualityArray(memory.DefaultAllocator, dtype, rightValues, valid)
			},
		},
	}

	for _, dtype := range types {
		t.Run(dtype.Name(), func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					left, right := test.build(dtype)
					defer left.Release()
					defer right.Release()

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
		const length = 128
		offsets := make([]int32, length+1)
		values := make([]byte, length)
		for i := range values {
			offsets[i] = int32(i)
			values[i] = byte('a' + i%26)
		}
		offsets[length] = int32(length)

		offsetsBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
		valuesBuffer := memory.NewBufferBytes(values)
		data := array.NewData(
			arrow.BinaryTypes.Binary,
			length,
			[]*memory.Buffer{nil, offsetsBuffer, valuesBuffer},
			nil,
			1,
			0,
		)
		offsetsBuffer.Release()
		valuesBuffer.Release()
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

func makeLongValidRuns(length int) []bool {
	valid := make([]bool, length)
	for i := range valid {
		valid[i] = (i/8)%2 == 0
	}
	return valid
}

func makeSlicedBinaryEqualityPair(dtype arrow.BinaryDataType) (arrow.Array, arrow.Array) {
	const (
		length      = 256
		leftOffset  = 5
		rightOffset = 17
	)

	logicalValues := makeBinaryEqualityValues(length, 16)
	logicalValid := makeLongValidRuns(length)
	leftValues := makeBinaryEqualityValues(leftOffset+length+1, 16)
	rightValues := makeBinaryEqualityValues(rightOffset+length+1, 16)
	leftValid := make([]bool, len(leftValues))
	rightValid := make([]bool, len(rightValues))
	for i := range logicalValues {
		leftValues[leftOffset+i] = logicalValues[i]
		rightValues[rightOffset+i] = logicalValues[i]
		leftValid[leftOffset+i] = logicalValid[i]
		rightValid[rightOffset+i] = logicalValid[i]
	}

	leftBase := makeBinaryEqualityArray(memory.DefaultAllocator, dtype, leftValues, leftValid)
	left := array.NewSlice(leftBase, leftOffset, leftOffset+length)
	leftBase.Release()
	rightBase := makeBinaryEqualityArray(memory.DefaultAllocator, dtype, rightValues, rightValid)
	right := array.NewSlice(rightBase, rightOffset, rightOffset+length)
	rightBase.Release()
	return left, right
}

func makeRawBinaryEqualityArray(dtype arrow.BinaryDataType, values []string, valid []bool) arrow.Array {
	if len(values) != len(valid) {
		panic("len(values) != len(valid)")
	}

	valueBytes := make([]byte, 0)
	var offsetBytes []byte
	switch dtype.ID() {
	case arrow.BINARY, arrow.STRING:
		offsets := make([]int32, len(values)+1)
		for i, value := range values {
			offsets[i] = int32(len(valueBytes))
			valueBytes = append(valueBytes, value...)
		}
		offsets[len(values)] = int32(len(valueBytes))
		offsetBytes = arrow.Int32Traits.CastToBytes(offsets)
	case arrow.LARGE_BINARY, arrow.LARGE_STRING:
		offsets := make([]int64, len(values)+1)
		for i, value := range values {
			offsets[i] = int64(len(valueBytes))
			valueBytes = append(valueBytes, value...)
		}
		offsets[len(values)] = int64(len(valueBytes))
		offsetBytes = arrow.Int64Traits.CastToBytes(offsets)
	default:
		panic("unsupported binary type")
	}

	validity := make([]byte, (len(valid)+7)/8)
	nulls := 0
	for i, isValid := range valid {
		if isValid {
			bitutil.SetBit(validity, i)
		} else {
			nulls++
		}
	}

	validityBuffer := memory.NewBufferBytes(validity)
	offsetsBuffer := memory.NewBufferBytes(offsetBytes)
	valuesBuffer := memory.NewBufferBytes(valueBytes)
	data := array.NewData(
		dtype,
		len(values),
		[]*memory.Buffer{validityBuffer, offsetsBuffer, valuesBuffer},
		nil,
		nulls,
		0,
	)
	validityBuffer.Release()
	offsetsBuffer.Release()
	valuesBuffer.Release()
	result := array.MakeFromData(data)
	data.Release()
	return result
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
