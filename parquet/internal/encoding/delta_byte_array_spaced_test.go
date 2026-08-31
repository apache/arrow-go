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

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestDeltaByteArrayPutSpacedReusesScratch(t *testing.T) {
	const nvalues = deltaByteArrayBatchSize + 3

	values := make([]parquet.ByteArray, nvalues)
	validBits := make([]byte, bitutil.BytesForBits(nvalues))
	for i := range values {
		values[i] = parquet.ByteArray(fmt.Sprintf("value-%03d", i))
		bitutil.SetBit(validBits, i)
	}

	tests := []struct {
		name    string
		new     func() ByteArrayEncoder
		scratch func(ByteArrayEncoder) []parquet.ByteArray
	}{
		{
			name: "delta-length-byte-array",
			new: func() ByteArrayEncoder {
				return NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
			},
			scratch: func(enc ByteArrayEncoder) []parquet.ByteArray {
				return enc.(*DeltaLengthByteArrayEncoder).spacedScratch
			},
		},
		{
			name: "delta-byte-array",
			new: func() ByteArrayEncoder {
				return NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
					false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
			},
			scratch: func(enc ByteArrayEncoder) []parquet.ByteArray {
				return enc.(*DeltaByteArrayEncoder).spacedScratch
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := tt.new()
			defer enc.Release()

			enc.PutSpaced(values, validBits, 0)
			firstScratch := tt.scratch(enc)
			require.Len(t, firstScratch, nvalues)
			firstValue := &firstScratch[0]
			for i, value := range firstScratch {
				require.Nil(t, value, "scratch entry %d still references input data", i)
			}

			buf, err := enc.FlushValues()
			require.NoError(t, err)
			buf.Release()

			enc.PutSpaced(values[:1], validBits, 0)
			secondScratch := tt.scratch(enc)
			require.Len(t, secondScratch, 1)
			require.True(t, firstValue == &secondScratch[0], "scratch backing storage was not reused")
			for i, value := range secondScratch[:cap(secondScratch)] {
				require.Nil(t, value, "scratch entry %d still references input data", i)
			}

			buf, err = enc.FlushValues()
			require.NoError(t, err)
			buf.Release()
		})
	}
}

func TestDeltaByteArrayPutSpacedRoundTripWithOffset(t *testing.T) {
	const nvalues = deltaByteArrayBatchSize*2 + 7
	const validBitsOffset = int64(5)

	values := make([]parquet.ByteArray, nvalues)
	validBits := make([]byte, bitutil.BytesForBits(validBitsOffset+int64(nvalues)))
	want := make([]parquet.ByteArray, 0, nvalues)
	for i := range values {
		values[i] = parquet.ByteArray(fmt.Sprintf("partition-%02d/value-%03d", i/11, i))
		if i%7 != 2 {
			bitutil.SetBit(validBits, int(validBitsOffset)+i)
			want = append(want, values[i])
		}
	}

	for _, encoding := range []parquet.Encoding{
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	} {
		t.Run(encoding.String(), func(t *testing.T) {
			enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
			defer enc.Release()

			enc.PutSpaced(values, validBits, validBitsOffset)
			buf, err := enc.FlushValues()
			require.NoError(t, err)
			defer buf.Release()

			dec := NewDecoder(parquet.Types.ByteArray, encoding, nil, memory.DefaultAllocator).(ByteArrayDecoder)
			require.NoError(t, dec.SetData(len(want), buf.Bytes()))
			got := make([]parquet.ByteArray, len(want))
			decoded, err := dec.Decode(got)
			require.NoError(t, err)
			require.Equal(t, len(want), decoded)
			require.Equal(t, want, got)
		})
	}
}
