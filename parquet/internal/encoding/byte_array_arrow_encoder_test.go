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

type arrowByteArrayEncoder32Test interface {
	ByteArrayEncoder
	PutArrow([]byte, []int32)
	PutArrowSpaced([]byte, []int32, []byte, int64)
}

type arrowByteArrayEncoder64Test interface {
	ByteArrayEncoder
	PutArrow64([]byte, []int64)
	PutArrowSpaced64([]byte, []int64, []byte, int64)
}

func arrowByteArrayInput() ([]parquet.ByteArray, []byte, []int32, []int64) {
	values := []parquet.ByteArray{
		[]byte("prefix/000"),
		[]byte("prefix/001"),
		{},
		[]byte("prefix/003"),
		[]byte("other"),
		{},
		[]byte("other/longer"),
	}
	data := make([]byte, 0, 64)
	offsets32 := []int32{0}
	offsets64 := []int64{0}
	for _, value := range values {
		data = append(data, value...)
		offsets32 = append(offsets32, int32(len(data)))
		offsets64 = append(offsets64, int64(len(data)))
	}
	return values, data, offsets32, offsets64
}

func encodedByteArrays(t *testing.T, encoding parquet.Encoding, values []parquet.ByteArray, spaced bool, validBits []byte, validBitsOffset int64) []byte {
	t.Helper()
	enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer enc.Release()
	if spaced {
		enc.PutSpaced(values, validBits, validBitsOffset)
	} else {
		enc.Put(values)
	}
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func TestByteArrayArrowEncodersMatchByteArrayInput(t *testing.T) {
	values, data, offsets32, offsets64 := arrowByteArrayInput()
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values)+5)))
	validBitsOffset := int64(3)
	for _, index := range []int{0, 1, 3, 4, 6} {
		bitutil.SetBit(validBits, int(validBitsOffset)+index)
	}

	for _, encoding := range []parquet.Encoding{
		parquet.Encodings.Plain,
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	} {
		t.Run(encoding.String(), func(t *testing.T) {
			want := encodedByteArrays(t, encoding, values, false, nil, 0)
			wantSpaced := encodedByteArrays(t, encoding, values, true, validBits, validBitsOffset)

			for _, width := range []string{"int32", "int64"} {
				t.Run(width, func(t *testing.T) {
					enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
					defer enc.Release()
					if width == "int32" {
						direct := enc.(arrowByteArrayEncoder32Test)
						direct.PutArrow(data, offsets32)
					} else {
						direct := enc.(arrowByteArrayEncoder64Test)
						direct.PutArrow64(data, offsets64)
					}
					buf, err := enc.FlushValues()
					require.NoError(t, err)
					require.Equal(t, want, buf.Bytes())
					buf.Release()

					if width == "int32" {
						direct := enc.(arrowByteArrayEncoder32Test)
						direct.PutArrowSpaced(data, offsets32, validBits, validBitsOffset)
					} else {
						direct := enc.(arrowByteArrayEncoder64Test)
						direct.PutArrowSpaced64(data, offsets64, validBits, validBitsOffset)
					}
					buf, err = enc.FlushValues()
					require.NoError(t, err)
					require.Equal(t, wantSpaced, buf.Bytes())
					buf.Release()
				})
			}
		})
	}
}

func TestDeltaByteArrayArrowEncoderPreservesStateAcrossBatches(t *testing.T) {
	values, data, offsets32, offsets64 := arrowByteArrayInput()

	for _, width := range []string{"int32", "int64"} {
		t.Run(width, func(t *testing.T) {
			enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
			defer enc.Release()
			direct32, direct64 := enc.(arrowByteArrayEncoder32Test), enc.(arrowByteArrayEncoder64Test)
			if width == "int32" {
				direct32.PutArrow(data, offsets32[:3])
				direct32.PutArrow(data, offsets32[2:])
			} else {
				direct64.PutArrow64(data, offsets64[:3])
				direct64.PutArrow64(data, offsets64[2:])
			}
			got, err := enc.FlushValues()
			require.NoError(t, err)
			defer got.Release()
			want := encodedByteArrays(t, parquet.Encodings.DeltaByteArray, values, false, nil, 0)
			require.Equal(t, want, got.Bytes())
		})
	}
}

func TestDeltaArrowEncodersAcrossInternalBatches(t *testing.T) {
	const nvalues = deltaByteArrayBatchSize*2 + 17
	values := make([]parquet.ByteArray, nvalues)
	data := make([]byte, 0, nvalues*16)
	offsets32 := []int32{0}
	offsets64 := []int64{0}
	validBits := make([]byte, bitutil.BytesForBits(nvalues+1))
	validBitsOffset := int64(1)
	for i := range values {
		values[i] = parquet.ByteArray(fmt.Sprintf("partition/%03d/value", i))
		data = append(data, values[i]...)
		offsets32 = append(offsets32, int32(len(data)))
		offsets64 = append(offsets64, int64(len(data)))
		if i%5 != 0 {
			bitutil.SetBit(validBits, int(validBitsOffset)+i)
		}
	}

	for _, encoding := range []parquet.Encoding{
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	} {
		for _, width := range []string{"int32", "int64"} {
			t.Run(encoding.String()+"/"+width, func(t *testing.T) {
				want := encodedByteArrays(t, encoding, values, true, validBits, validBitsOffset)
				enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
				defer enc.Release()
				if width == "int32" {
					enc.(arrowByteArrayEncoder32Test).PutArrowSpaced(data, offsets32, validBits, validBitsOffset)
				} else {
					enc.(arrowByteArrayEncoder64Test).PutArrowSpaced64(data, offsets64, validBits, validBitsOffset)
				}
				got, err := enc.FlushValues()
				require.NoError(t, err)
				require.Equal(t, want, got.Bytes())
				got.Release()
			})
		}
	}
}
