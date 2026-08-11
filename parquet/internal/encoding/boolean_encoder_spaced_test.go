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

package encoding_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spacedBitmapEncoder interface {
	PutSpacedBitmap(bitmap []byte, bitmapOffset, numValues int64, validBits []byte, validBitsOffset int64) int64
}

func TestPlainBooleanEncoderPutSpacedBitmapOffsetsAndBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		length         int
		bitmapOffset   int
		validityOffset int
		valid          func(int) bool
	}{
		{name: "single value", length: 1, bitmapOffset: 3, validityOffset: 5, valid: func(int) bool { return true }},
		{name: "short runs", length: 257, bitmapOffset: 3, validityOffset: 5, valid: func(i int) bool { return i%10 != 0 }},
		{name: "alternating", length: 1025, bitmapOffset: 7, validityOffset: 3, valid: func(i int) bool { return i%2 == 0 }},
		{name: "long runs", length: 4097, bitmapOffset: 5, validityOffset: 7, valid: func(i int) bool { return i%256 >= 32 }},
		{name: "buffer minus one", length: 8191, bitmapOffset: 1, validityOffset: 2, valid: func(int) bool { return true }},
		{name: "exact buffer", length: 8192, bitmapOffset: 2, validityOffset: 3, valid: func(int) bool { return true }},
		{name: "buffer plus one", length: 8193, bitmapOffset: 3, validityOffset: 4, valid: func(int) bool { return true }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bitmap, validity, expected := makeSpacedBooleanInput(
				test.length, test.bitmapOffset, test.validityOffset, test.valid,
			)
			actual := encodeSpacedBooleanBitmap(
				t, bitmap, int64(test.bitmapOffset), int64(test.length),
				validity, int64(test.validityOffset),
			)
			assert.Equal(t, expected, actual)
		})
	}
}

func TestPlainBooleanEncoderPutSpacedBitmapMultipleCalls(t *testing.T) {
	encoder := encoding.NewEncoder(
		parquet.Types.Boolean, parquet.Encodings.Plain,
		false, nil, memory.DefaultAllocator,
	).(encoding.BooleanEncoder)
	spaced := encoder.(spacedBitmapEncoder)

	var expected []bool
	for call, length := range []int{31, 9000, 257} {
		bitmapOffset := call + 1
		validityOffset := call + 3
		bitmap, validity, values := makeSpacedBooleanInput(
			length, bitmapOffset, validityOffset,
			func(i int) bool { return (i+call)%5 != 0 },
		)
		expected = append(expected, values...)
		actualValid := spaced.PutSpacedBitmap(
			bitmap, int64(bitmapOffset), int64(length), validity, int64(validityOffset),
		)
		assert.Equal(t, int64(len(values)), actualValid)
	}

	buf, err := encoder.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	actual := decodeBooleanValues(t, buf.Bytes(), len(expected))
	assert.Equal(t, expected, actual)
}

func makeSpacedBooleanInput(
	length, bitmapOffset, validityOffset int, valid func(int) bool,
) (bitmap, validity []byte, expected []bool) {
	bitmap = make([]byte, bitutil.BytesForBits(int64(bitmapOffset+length)))
	validity = make([]byte, bitutil.BytesForBits(int64(validityOffset+length)))
	for i := range length {
		value := i%3 != 0
		if value {
			bitutil.SetBit(bitmap, bitmapOffset+i)
		}
		if valid(i) {
			bitutil.SetBit(validity, validityOffset+i)
			expected = append(expected, value)
		}
	}
	return
}

func encodeSpacedBooleanBitmap(
	t *testing.T, bitmap []byte, bitmapOffset, length int64, validity []byte, validityOffset int64,
) []bool {
	t.Helper()
	encoder := encoding.NewEncoder(
		parquet.Types.Boolean, parquet.Encodings.Plain,
		false, nil, memory.DefaultAllocator,
	).(encoding.BooleanEncoder)
	spaced := encoder.(spacedBitmapEncoder)

	numValid := spaced.PutSpacedBitmap(bitmap, bitmapOffset, length, validity, validityOffset)
	buf, err := encoder.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	return decodeBooleanValues(t, buf.Bytes(), int(numValid))
}

func decodeBooleanValues(t *testing.T, data []byte, length int) []bool {
	t.Helper()
	decoder := encoding.NewDecoder(
		parquet.Types.Boolean, parquet.Encodings.Plain, nil, memory.DefaultAllocator,
	).(encoding.BooleanDecoder)
	require.NoError(t, decoder.SetData(length, data))

	values := make([]bool, length)
	n, err := decoder.Decode(values)
	require.NoError(t, err)
	require.Equal(t, length, n)
	return values
}
