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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodeRleBooleanValues(t testing.TB, values []bool) []byte {
	t.Helper()
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.RLE,
		false, nil, memory.DefaultAllocator).(encoding.BooleanEncoder)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	data := append([]byte(nil), buf.Bytes()...)
	buf.Release()
	return data
}

func newRleBooleanBitmapDecoder(t testing.TB, nvalues int, data []byte) encoding.BooleanBitmapDecoder {
	t.Helper()
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE,
		nil, memory.DefaultAllocator).(encoding.BooleanBitmapDecoder)
	require.NoError(t, dec.SetData(nvalues, data))
	return dec
}

func makeBooleanValues(length int, value func(int) bool) []bool {
	values := make([]bool, length)
	for i := range values {
		values[i] = value(i)
	}
	return values
}

func assertBitmapMatches(t *testing.T, bitmap []byte, offset int64, expected []bool) {
	t.Helper()
	for i, want := range expected {
		assert.Equal(t, want, bitutil.BitIsSet(bitmap, int(offset)+i), "bit %d", i)
	}
}

func TestRleBooleanDecoderDecodeToBitmap(t *testing.T) {
	patterns := []struct {
		name   string
		values []bool
	}{
		{name: "repeated_true", values: makeBooleanValues(2048, func(int) bool { return true })},
		{name: "repeated_false", values: makeBooleanValues(2048, func(int) bool { return false })},
		{name: "alternating", values: makeBooleanValues(2048, func(i int) bool { return i%2 == 0 })},
		{name: "short_runs", values: makeBooleanValues(2048, func(i int) bool { return (i/7)%2 == 0 })},
		{name: "mixed", values: makeBooleanValues(2048, func(i int) bool {
			switch {
			case i < 9:
				return true
			case i < 23:
				return false
			default:
				return i%5 == 0
			}
		})},
	}

	for _, tc := range patterns {
		t.Run(tc.name, func(t *testing.T) {
			data := encodeRleBooleanValues(t, tc.values)
			for _, outOffset := range []int64{0, 1, 7} {
				t.Run(fmt.Sprintf("offset_%d", outOffset), func(t *testing.T) {
					out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(outOffset+int64(len(tc.values)+8))))
					before := append([]byte(nil), out...)
					dec := newRleBooleanBitmapDecoder(t, len(tc.values), data)

					n, err := dec.DecodeToBitmap(out, outOffset, len(tc.values))
					require.NoError(t, err)
					require.Equal(t, len(tc.values), n)
					assertBitmapMatches(t, out, outOffset, tc.values)

					for i := int64(0); i < outOffset; i++ {
						assert.Equal(t, bitutil.BitIsSet(before, int(i)), bitutil.BitIsSet(out, int(i)), "prefix bit %d", i)
					}
					for i := outOffset + int64(len(tc.values)); i < int64(len(out))*8; i++ {
						assert.Equal(t, bitutil.BitIsSet(before, int(i)), bitutil.BitIsSet(out, int(i)), "suffix bit %d", i)
					}
				})
			}
		})
	}
}

func TestRleBooleanDecoderDecodeToBitmapConsecutiveCalls(t *testing.T) {
	values := makeBooleanValues(2048, func(i int) bool { return i%2 == 0 })
	data := encodeRleBooleanValues(t, values)
	dec := newRleBooleanBitmapDecoder(t, len(values), data)

	calls := []struct {
		outOffset int64
		length    int
	}{
		{outOffset: 7, length: 5},
		{outOffset: 1, length: 1000},
		{outOffset: 0, length: len(values) - 1005},
	}
	position := 0
	for i, call := range calls {
		out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(call.outOffset+int64(call.length+8))))
		n, err := dec.DecodeToBitmap(out, call.outOffset, call.length)
		require.NoError(t, err, "call %d", i)
		require.Equal(t, call.length, n, "call %d", i)
		assertBitmapMatches(t, out, call.outOffset, values[position:position+call.length])
		position += call.length
	}
	assert.Equal(t, len(values), position)
}

func TestRleBooleanDecoderDecodeToBitmapTruncatedLiteral(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payloadSize int
		err         error
	}{
		{name: "partial_group", payloadSize: 7, err: io.ErrUnexpectedEOF},
		{name: "group_boundary", payloadSize: 4, err: io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := append([]byte{17}, bytes.Repeat([]byte{0xff}, tc.payloadSize)...)
			data := make([]byte, 4+len(payload))
			binary.LittleEndian.PutUint32(data[:4], uint32(len(payload)))
			copy(data[4:], payload)

			out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(72)))
			before := append([]byte(nil), out...)
			dec := newRleBooleanBitmapDecoder(t, 64, data)
			n, err := dec.DecodeToBitmap(out, 7, 64)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, 32, n)
			for i := 0; i < n; i++ {
				assert.True(t, bitutil.BitIsSet(out, 7+i), "decoded bit %d", i)
			}
			for i := n; i < 64; i++ {
				assert.Equal(t, bitutil.BitIsSet(before, 7+i), bitutil.BitIsSet(out, 7+i), "unreturned bit %d", i)
			}
		})
	}
}
