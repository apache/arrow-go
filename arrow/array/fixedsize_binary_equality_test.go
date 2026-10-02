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
)

func TestFixedSizeBinaryEqualityByValidRuns(t *testing.T) {
	valid := []bool{true, true, false, true, false, false, true, true}
	leftNulls := []byte("aaaabbbbLEFTddddLEFTLEFTgggghhhh")
	rightNulls := []byte("aaaabbbbxxxxddddyyyyzzzzgggghhhh")

	leftOffsetValues := concatFixedSizeBinaryRows("LPRF", "one1", "left", "two2", "thre", "LSUF")
	rightOffsetValues := concatFixedSizeBinaryRows("RPRF", "free", "one1", "diff", "two2", "thre", "RSUF")
	leftOffsetValid := []bool{true, true, false, true, true, true}
	rightOffsetValid := []bool{true, true, true, false, true, true, true}

	allValid := make([]bool, 8)
	for i := range allValid {
		allValid[i] = true
	}

	tests := []struct {
		name                    string
		leftValues, rightValues []byte
		leftValid, rightValid   []bool
		length                  int
		leftOffset, rightOffset int
		leftNulls, rightNulls   int
		want                    bool
	}{
		{
			name:        "equal values",
			leftValues:  concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValues: concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			length:      8,
			want:        true,
		},
		{
			name:        "ignore bytes under null slots",
			leftValues:  leftNulls,
			rightValues: rightNulls,
			leftValid:   valid,
			rightValid:  valid,
			length:      len(valid),
			leftNulls:   3,
			rightNulls:  3,
			want:        true,
		},
		{
			name:        "detect mismatch in a later valid run",
			leftValues:  concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValues: concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "GGGG", "hhhh"),
			leftValid:   valid,
			rightValid:  valid,
			length:      8,
			leftNulls:   3,
			rightNulls:  3,
			want:        false,
		},
		{
			name:        "different physical offsets with nulls",
			leftValues:  leftOffsetValues,
			rightValues: rightOffsetValues,
			leftValid:   leftOffsetValid,
			rightValid:  rightOffsetValid,
			length:      4,
			leftOffset:  1,
			rightOffset: 2,
			leftNulls:   1,
			rightNulls:  1,
			want:        true,
		},
		{
			name:        "materialized bitmap remains authoritative with zero declared nulls",
			leftValues:  concatFixedSizeBinaryRows("aaaa", "LEFT", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValues: concatFixedSizeBinaryRows("aaaa", "DIFF", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			leftValid:   []bool{true, false, true, true, true, true, true, true},
			rightValid:  []bool{true, false, true, true, true, true, true, true},
			length:      8,
			want:        true,
		},
		{
			name:        "absent and all-valid materialized bitmaps",
			leftValues:  concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValues: concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValid:  allValid,
			length:      8,
			want:        true,
		},
		{
			name:        "different validity positions with the same null count",
			leftValues:  concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			rightValues: concatFixedSizeBinaryRows("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh"),
			leftValid:   []bool{true, false, true, true, true, true, true, true},
			rightValid:  []bool{true, true, false, true, true, true, true, true},
			length:      8,
			leftNulls:   1,
			rightNulls:  1,
			want:        false,
		},
		{
			name:        "all-null payloads are ignored",
			leftValues:  concatFixedSizeBinaryRows("LEFT", "LEFT", "LEFT", "LEFT", "LEFT", "LEFT", "LEFT", "LEFT"),
			rightValues: concatFixedSizeBinaryRows("DIFF", "DIFF", "DIFF", "DIFF", "DIFF", "DIFF", "DIFF", "DIFF"),
			leftValid:   []bool{false, false, false, false, false, false, false, false},
			rightValid:  []bool{false, false, false, false, false, false, false, false},
			length:      8,
			leftNulls:   8,
			rightNulls:  8,
			want:        true,
		},
		{
			name:   "empty arrays without buffers",
			length: 0,
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			left := makeFixedSizeBinaryEqualityArray(4, tc.leftValues, tc.leftValid, tc.length, tc.leftOffset, tc.leftNulls)
			defer left.Release()
			right := makeFixedSizeBinaryEqualityArray(4, tc.rightValues, tc.rightValid, tc.length, tc.rightOffset, tc.rightNulls)
			defer right.Release()

			assert.Equal(t, tc.want, array.Equal(left, right))
			assert.Equal(t, tc.want, array.Equal(right, left))
		})
	}
}

func TestFixedSizeBinaryEqualityLongValidRuns(t *testing.T) {
	const (
		width  = 4
		length = 256
	)

	valid := make([]bool, length)
	leftValues := make([]byte, length*width)
	rightValues := make([]byte, length*width)
	nulls := 0
	for i := range length {
		valid[i] = (i/8)%2 == 0
		if !valid[i] {
			nulls++
		}
		for j := range width {
			value := byte(i*7 + j*11)
			leftValues[i*width+j] = value
			rightValues[i*width+j] = value
		}
		if !valid[i] {
			rightValues[i*width]++
		}
	}

	left := makeFixedSizeBinaryEqualityArray(width, leftValues, valid, length, 0, nulls)
	defer left.Release()
	right := makeFixedSizeBinaryEqualityArray(width, rightValues, valid, length, 0, nulls)
	assert.True(t, array.Equal(left, right))
	right.Release()

	rightValues[240*width]++
	right = makeFixedSizeBinaryEqualityArray(width, rightValues, valid, length, 0, nulls)
	defer right.Release()
	assert.False(t, array.Equal(left, right))
}

func TestFixedSizeBinaryEqualityWithZeroDeclaredNullCount(t *testing.T) {
	const (
		width  = 4
		length = 128
	)

	valid := make([]bool, length)
	leftValues := make([]byte, length*width)
	rightValues := make([]byte, length*width)
	for i := range length {
		valid[i] = (i/8)%2 == 0
		for j := range width {
			value := byte(i*7 + j*11)
			leftValues[i*width+j] = value
			rightValues[i*width+j] = value
		}
		if !valid[i] {
			rightValues[i*width]++
		}
	}

	left := makeFixedSizeBinaryEqualityArray(width, leftValues, valid, length, 0, 0)
	defer left.Release()
	right := makeFixedSizeBinaryEqualityArray(width, rightValues, valid, length, 0, 0)
	defer right.Release()

	assert.True(t, array.Equal(left, right))
}

func TestFixedSizeBinaryEqualityWithDifferentLongOffsets(t *testing.T) {
	const (
		width       = 4
		length      = 128
		leftOffset  = 1
		rightOffset = 3
	)

	leftValues := make([]byte, (leftOffset+length)*width)
	rightValues := make([]byte, (rightOffset+length)*width)
	leftValid := make([]bool, leftOffset+length)
	rightValid := make([]bool, rightOffset+length)
	nulls := 0
	for i := range length {
		isValid := (i/8)%2 == 0
		leftValid[leftOffset+i] = isValid
		rightValid[rightOffset+i] = isValid
		if !isValid {
			nulls++
		}
		for j := range width {
			value := byte(i*7 + j*11)
			leftValues[(leftOffset+i)*width+j] = value
			rightValues[(rightOffset+i)*width+j] = value
		}
		if !isValid {
			rightValues[(rightOffset+i)*width]++
		}
	}

	left := makeFixedSizeBinaryEqualityArray(width, leftValues, leftValid, length, leftOffset, nulls)
	defer left.Release()
	right := makeFixedSizeBinaryEqualityArray(width, rightValues, rightValid, length, rightOffset, nulls)
	assert.True(t, array.Equal(left, right))
	right.Release()

	rightValues[(rightOffset+112)*width]++
	right = makeFixedSizeBinaryEqualityArray(width, rightValues, rightValid, length, rightOffset, nulls)
	defer right.Release()
	assert.False(t, array.Equal(left, right))
}

func TestFixedSizeBinaryEqualityWithFragmentedValidity(t *testing.T) {
	const (
		width  = 4
		length = 128
	)

	valid := make([]bool, length)
	leftValues := make([]byte, length*width)
	rightValues := make([]byte, length*width)
	nulls := 0
	for i := range length {
		valid[i] = i%2 == 0
		if !valid[i] {
			nulls++
		}
		for j := range width {
			value := byte(i*7 + j*11)
			leftValues[i*width+j] = value
			rightValues[i*width+j] = value
		}
		if !valid[i] {
			rightValues[i*width]++
		}
	}

	left := makeFixedSizeBinaryEqualityArray(width, leftValues, valid, length, 0, nulls)
	defer left.Release()
	right := makeFixedSizeBinaryEqualityArray(width, rightValues, valid, length, 0, nulls)
	assert.True(t, array.Equal(left, right))
	right.Release()

	rightValues[126*width]++
	right = makeFixedSizeBinaryEqualityArray(width, rightValues, valid, length, 0, nulls)
	defer right.Release()
	assert.False(t, array.Equal(left, right))
}

func makeFixedSizeBinaryEqualityArray(
	width int, values []byte, valid []bool, length, offset, nulls int,
) *array.FixedSizeBinary {
	var validityBuffer *memory.Buffer
	if valid != nil {
		validity := make([]byte, (len(valid)+7)/8)
		for i, isValid := range valid {
			if isValid {
				bitutil.SetBit(validity, i)
			}
		}
		validityBuffer = memory.NewBufferBytes(validity)
		defer validityBuffer.Release()
	}

	valuesBuffer := memory.NewBufferBytes(values)
	defer valuesBuffer.Release()

	data := array.NewData(
		&arrow.FixedSizeBinaryType{ByteWidth: width},
		length,
		[]*memory.Buffer{validityBuffer, valuesBuffer},
		nil,
		nulls,
		offset,
	)
	defer data.Release()

	return array.NewFixedSizeBinaryData(data)
}

func concatFixedSizeBinaryRows(rows ...string) []byte {
	var values []byte
	for _, row := range rows {
		values = append(values, row...)
	}
	return values
}
