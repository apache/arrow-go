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
	"fmt"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestByteStreamSplitFixedLenByteArrayDecoderRejectsTruncatedRequiredData(t *testing.T) {
	const width = 4
	node := schema.NewFixedLenByteArrayNode("value", parquet.Repetitions.Required, width, -1)
	column := schema.NewColumn(node, 0, 0)
	decoder := NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.ByteStreamSplit, column, memory.DefaultAllocator)

	err := decoder.SetData(3, make([]byte, 2*width))
	require.EqualError(t, err, "BYTE_STREAM_SPLIT data contains 2 values, expected 3")
	require.Zero(t, decoder.ValuesLeft())
}

func TestByteStreamSplitFixedLenByteArrayDecoderAllowsNulls(t *testing.T) {
	const width = 4
	values := makeFixedLenByteArrayValues(2, width, 0)
	node := schema.NewFixedLenByteArrayNode("value", parquet.Repetitions.Optional, width, -1)
	column := schema.NewColumn(node, 1, 0)
	decoder := NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.ByteStreamSplit, column, memory.DefaultAllocator).(FixedLenByteArrayDecoder)

	require.NoError(t, decoder.SetData(3, encodeByteStreamSplitFixedLenByteArray(values, width)))
	out := make([]parquet.FixedLenByteArray, len(values))
	decoded, err := decoder.Decode(out)
	require.NoError(t, err)
	require.Equal(t, len(values), decoded)
	require.Equal(t, values, out)

	// An optional page can legitimately contain no physical values even when its
	// logical count would overflow when multiplied by the byte width.
	require.NoError(t, decoder.SetData(int(^uint(0)>>1), nil))
	require.Zero(t, decoder.ValuesLeft())
}

func TestByteStreamSplitFixedLenByteArrayDecoderContiguousOutput(t *testing.T) {
	for _, width := range []int{2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			values := makeFixedLenByteArrayValues(8, width, 0)
			decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)

			out := make([]parquet.FixedLenByteArray, len(values))
			decoded, err := decoder.Decode(out)
			require.NoError(t, err)
			require.Equal(t, len(values), decoded)
			require.Equal(t, values, out)

			for idx := range out {
				require.Equal(t, width, cap(out[idx]))
				if idx > 0 {
					previous := uintptr(unsafe.Pointer(&out[idx-1][0]))
					current := uintptr(unsafe.Pointer(&out[idx][0]))
					require.Equal(t, uintptr(width), current-previous)
				}
			}
		})
	}
}

// TestByteStreamSplitFixedLenByteArrayDecoderDoesNotWriteThroughProvidedOutput pins the
// narrowed reuse contract introduced for GH-1255.
//
// This decoder writes bytes through the slice headers it is handed, so it may only reuse
// a header backed by storage it allocated itself. Output buffers are shared across pages
// and encodings, and RLE_DICTIONARY and PLAIN both leave headers pointing at memory they
// own (dictionary entries and the page buffer respectively). Reusing those on capacity
// alone corrupted that memory, so capacity is no longer sufficient to claim a header.
func TestByteStreamSplitFixedLenByteArrayDecoderDoesNotWriteThroughProvidedOutput(t *testing.T) {
	const width = 16
	values := makeFixedLenByteArrayValues(4, width, 0)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)

	caller := make([]byte, width)
	out := []parquet.FixedLenByteArray{
		make([]byte, 0, width+4),
		nil,
		caller,
		nil,
	}
	firstPtr := unsafe.Pointer(unsafe.SliceData(out[0]))
	thirdPtr := unsafe.Pointer(unsafe.SliceData(out[2]))

	decoded, err := decoder.Decode(out)
	require.NoError(t, err)
	require.Equal(t, len(values), decoded)
	require.Equal(t, values, out)

	// The caller's buffers are re-pointed rather than written through, and are left
	// untouched.
	require.NotEqual(t, firstPtr, unsafe.Pointer(unsafe.SliceData(out[0])))
	require.NotEqual(t, thirdPtr, unsafe.Pointer(unsafe.SliceData(out[2])))
	require.Equal(t, make([]byte, width), caller, "decoding wrote through a caller buffer")

	// Every slot now comes from one contiguous block the decoder owns.
	for idx := 1; idx < len(out); idx++ {
		previous := uintptr(unsafe.Pointer(&out[idx-1][0]))
		current := uintptr(unsafe.Pointer(&out[idx][0]))
		require.Equal(t, uintptr(width), current-previous)
	}
}

// TestByteStreamSplitFixedLenByteArrayDecoderReusesOwnStorage checks that the narrowed
// contract still keeps repeated decodes into the same buffer allocation free, which is
// the case GH-1172 optimized.
func TestByteStreamSplitFixedLenByteArrayDecoderReusesOwnStorage(t *testing.T) {
	const width = 16
	values := makeFixedLenByteArrayValues(4, width, 0)
	data := encodeByteStreamSplitFixedLenByteArray(values, width)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)

	// First decode hands out the decoder's own block.
	out := make([]parquet.FixedLenByteArray, len(values))
	_, err := decoder.Decode(out)
	require.NoError(t, err)
	require.Equal(t, values, out)

	// Subsequent decodes into that same window recognize the block as their own.
	allocs := testing.AllocsPerRun(100, func() {
		require.NoError(t, decoder.SetData(len(values), data))
		_, err := decoder.Decode(out)
		require.NoError(t, err)
	})
	require.Zero(t, allocs)
	require.Equal(t, values, out)
}

func TestByteStreamSplitFixedLenByteArrayDecoderMixedOutputAllocations(t *testing.T) {
	const width = 16
	values := makeFixedLenByteArrayValues(4, width, 0)
	data := encodeByteStreamSplitFixedLenByteArray(values, width)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)

	out := []parquet.FixedLenByteArray{
		make([]byte, width),
		nil,
		make([]byte, width),
		nil,
	}
	allocs := testing.AllocsPerRun(100, func() {
		out[1] = nil
		out[3] = nil
		require.NoError(t, decoder.SetData(len(values), data))
		_, err := decoder.Decode(out)
		require.NoError(t, err)
	})
	require.Equal(t, float64(1), allocs)
}

func TestByteStreamSplitFixedLenByteArrayDecoderKeepsPreviousOutput(t *testing.T) {
	const width = 16
	firstValues := makeFixedLenByteArrayValues(4, width, 0)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, firstValues)

	firstOut := make([]parquet.FixedLenByteArray, len(firstValues))
	_, err := decoder.Decode(firstOut)
	require.NoError(t, err)

	secondValues := makeFixedLenByteArrayValues(4, width, 100)
	require.NoError(t, decoder.SetData(len(secondValues), encodeByteStreamSplitFixedLenByteArray(secondValues, width)))
	secondOut := make([]parquet.FixedLenByteArray, len(secondValues))
	_, err = decoder.Decode(secondOut)
	require.NoError(t, err)

	require.Equal(t, firstValues, firstOut)
	require.Equal(t, secondValues, secondOut)
}

func TestByteStreamSplitFixedLenByteArrayDecoderPartialOutput(t *testing.T) {
	const width = 8
	values := makeFixedLenByteArrayValues(3, width, 0)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)

	sentinel := parquet.FixedLenByteArray("sentinel")
	out := make([]parquet.FixedLenByteArray, 5)
	out[3] = sentinel
	out[4] = sentinel

	decoded, err := decoder.Decode(out)
	require.NoError(t, err)
	require.Equal(t, len(values), decoded)
	require.Equal(t, values, out[:decoded])
	require.Equal(t, unsafe.Pointer(&sentinel[0]), unsafe.Pointer(&out[3][0]))
	require.Equal(t, unsafe.Pointer(&sentinel[0]), unsafe.Pointer(&out[4][0]))
}

func TestByteStreamSplitFixedLenByteArrayDecoderSpacedOutput(t *testing.T) {
	const width = 8
	values := makeFixedLenByteArrayValues(3, width, 0)
	decoder := newByteStreamSplitFixedLenByteArrayDecoder(t, width, values)
	out := make([]parquet.FixedLenByteArray, 5)

	decoded, err := decoder.DecodeSpaced(out, 2, []byte{0b00010101}, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), decoded)
	require.Equal(t, values[0], out[0])
	require.Equal(t, values[1], out[2])
	require.Equal(t, values[2], out[4])

	first := uintptr(unsafe.Pointer(&out[0][0]))
	second := uintptr(unsafe.Pointer(&out[2][0]))
	third := uintptr(unsafe.Pointer(&out[4][0]))
	require.Equal(t, uintptr(width), second-first)
	require.Equal(t, uintptr(width), third-second)
}

func TestFixedLenByteArrayDecoderSpacedOutputAcrossEncodings(t *testing.T) {
	const width = 4
	validBits := []byte{0b00010101}
	firstValues := makeFixedLenByteArrayValues(3, width, 0)
	secondValues := makeFixedLenByteArrayValues(3, width, 100)

	node := schema.NewFixedLenByteArrayNode("value", parquet.Repetitions.Required, width, -1)
	column := schema.NewColumn(node, 0, 0)
	plain := NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.Plain, column, memory.DefaultAllocator).(FixedLenByteArrayDecoder)
	plainData := make([]byte, 0, len(firstValues)*width)
	for _, value := range firstValues {
		plainData = append(plainData, value...)
	}
	require.NoError(t, plain.SetData(len(firstValues), plainData))

	out := make([]parquet.FixedLenByteArray, 5)
	decoded, err := plain.DecodeSpaced(out, 2, validBits, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), decoded)

	byteStreamSplit := newByteStreamSplitFixedLenByteArrayDecoder(t, width, secondValues)
	decoded, err = byteStreamSplit.DecodeSpaced(out, 2, validBits, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), decoded)
	require.Equal(t, secondValues[0], out[0])
	require.Equal(t, secondValues[1], out[2])
	require.Equal(t, secondValues[2], out[4])
}

func newByteStreamSplitFixedLenByteArrayDecoder(t *testing.T, width int, values []parquet.FixedLenByteArray) FixedLenByteArrayDecoder {
	t.Helper()

	node := schema.NewFixedLenByteArrayNode("value", parquet.Repetitions.Required, int32(width), -1)
	column := schema.NewColumn(node, 0, 0)
	decoder := NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.ByteStreamSplit, column, memory.DefaultAllocator).(FixedLenByteArrayDecoder)
	require.NoError(t, decoder.SetData(len(values), encodeByteStreamSplitFixedLenByteArray(values, width)))
	return decoder
}

func makeFixedLenByteArrayValues(length, width int, offset byte) []parquet.FixedLenByteArray {
	values := make([]parquet.FixedLenByteArray, length)
	for valueIdx := range values {
		values[valueIdx] = make(parquet.FixedLenByteArray, width)
		for byteIdx := range values[valueIdx] {
			values[valueIdx][byteIdx] = offset + byte(valueIdx*width+byteIdx)
		}
	}
	return values
}

func encodeByteStreamSplitFixedLenByteArray(values []parquet.FixedLenByteArray, width int) []byte {
	data := make([]byte, len(values)*width)
	for valueIdx, value := range values {
		for byteIdx, valueByte := range value {
			data[byteIdx*len(values)+valueIdx] = valueByte
		}
	}
	return data
}
