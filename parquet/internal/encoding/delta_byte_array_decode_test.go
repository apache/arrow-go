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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func encodeDeltaByteArrayPage(t *testing.T, values []string) []byte {
	t.Helper()

	input := make([]parquet.ByteArray, len(values))
	for i, value := range values {
		input[i] = parquet.ByteArray(value)
	}

	enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer enc.Release()
	enc.Put(input)

	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func requireDecodedStrings(t *testing.T, got []parquet.ByteArray, want []string) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, value := range want {
		require.Equal(t, value, string(got[i]), "value %d", i)
	}
}

func TestDeltaByteArrayDecoderKeepsPartialDecodeResults(t *testing.T) {
	values := []string{
		"", "partition/000/value/000", "partition/000/value/001",
		"partition/001/value/000", "partition/001/value/001", "partition/001/value/001",
	}
	data := encodeDeltaByteArrayPage(t, values)
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		nil, memory.DefaultAllocator).(ByteArrayDecoder)
	require.NoError(t, dec.SetData(len(values), data))

	decoded := make([]parquet.ByteArray, len(values))
	for i := range decoded {
		n, err := dec.Decode(decoded[i : i+1])
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	requireDecodedStrings(t, decoded, values)
}

func TestDeltaByteArrayDecoderDecodesAllEmptyValues(t *testing.T) {
	values := []string{"", "", "", "", ""}
	data := encodeDeltaByteArrayPage(t, values)
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		nil, memory.DefaultAllocator).(ByteArrayDecoder)
	require.NoError(t, dec.SetData(len(values), data))

	decoded := make([]parquet.ByteArray, len(values))
	for i := range decoded {
		n, err := dec.Decode(decoded[i : i+1])
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	for i, value := range decoded {
		require.Empty(t, value, "value %d", i)
	}
}

func TestDeltaByteArrayDecoderKeepsResultsAcrossPages(t *testing.T) {
	firstValues := []string{"first/000", "first/001", "first/002"}
	secondValues := []string{"second/000", "second/001"}
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		nil, memory.DefaultAllocator).(ByteArrayDecoder)

	firstData := encodeDeltaByteArrayPage(t, firstValues)
	require.NoError(t, dec.SetData(len(firstValues), firstData))
	firstOut := make([]parquet.ByteArray, len(firstValues))
	decoded, err := dec.Decode(firstOut)
	require.NoError(t, err)
	require.Equal(t, len(firstValues), decoded)

	secondData := encodeDeltaByteArrayPage(t, secondValues)
	require.NoError(t, dec.SetData(len(secondValues), secondData))
	secondOut := make([]parquet.ByteArray, len(secondValues))
	decoded, err = dec.Decode(secondOut)
	require.NoError(t, err)
	require.Equal(t, len(secondValues), decoded)

	requireDecodedStrings(t, firstOut, firstValues)
	requireDecodedStrings(t, secondOut, secondValues)
}

func TestDeltaByteArrayDecoderDecodeSpaced(t *testing.T) {
	values := []string{"a/000", "a/001", "b/000", "b/001"}
	data := encodeDeltaByteArrayPage(t, values)
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		nil, memory.DefaultAllocator).(ByteArrayDecoder)
	require.NoError(t, dec.SetData(len(values), data))

	validBits := []byte{0b00101101}
	out := make([]parquet.ByteArray, 6)
	decoded, err := dec.DecodeSpaced(out, 2, validBits, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), decoded)
	for i, value := range map[int]string{0: "a/000", 2: "a/001", 3: "b/000", 5: "b/001"} {
		require.Equal(t, value, string(out[i]), "value %d", i)
	}
}

func TestDeltaByteArrayDecoderDecodedSizeRejectsInvalidState(t *testing.T) {
	tests := []struct {
		name          string
		lastVal       parquet.ByteArray
		prefixLengths []int32
		lengths       []int32
		want          string
	}{
		{
			name:          "nonzero first prefix",
			prefixLengths: []int32{1},
			lengths:       []int32{1},
			want:          "first delta byte array prefix length must be zero",
		},
		{
			name:          "prefix beyond previous value",
			lastVal:       parquet.ByteArray("abc"),
			prefixLengths: []int32{4},
			lengths:       []int32{1},
			want:          "invalid delta byte array prefix length 4",
		},
		{
			name:          "negative suffix",
			lastVal:       parquet.ByteArray("abc"),
			prefixLengths: []int32{1},
			lengths:       []int32{-1},
			want:          "negative delta byte array length -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := &DeltaByteArrayDecoder{
				DeltaLengthByteArrayDecoder: &DeltaLengthByteArrayDecoder{
					lengths: tt.lengths,
				},
				prefixLengths: tt.prefixLengths,
				lastVal:       tt.lastVal,
			}
			_, err := dec.decodedArenaSize(1)
			require.EqualError(t, err, fmt.Sprintf("parquet: %s", tt.want))
		})
	}
}
