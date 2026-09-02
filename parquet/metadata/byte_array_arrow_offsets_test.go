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

package metadata

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func byteArrayArrowMetadataInput() ([]parquet.ByteArray, []byte, []int32, []int64) {
	values := []parquet.ByteArray{
		[]byte("alpha"),
		[]byte("beta"),
		{},
		[]byte("gamma"),
		[]byte("delta"),
		{},
	}
	data := make([]byte, 0, 32)
	offsets32 := []int32{0}
	offsets64 := []int64{0}
	for _, value := range values {
		data = append(data, value...)
		offsets32 = append(offsets32, int32(len(data)))
		offsets64 = append(offsets64, int64(len(data)))
	}
	return values, data, offsets32, offsets64
}

func TestByteArrayStatisticsArrowOffsetsMatchByteArrayInput(t *testing.T) {
	values, data, offsets32, offsets64 := byteArrayArrowMetadataInput()
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values)+4)))
	validBitsOffset := int64(2)
	for _, index := range []int{0, 1, 3, 4} {
		bitutil.SetBit(validBits, int(validBitsOffset)+index)
	}

	column := schema.NewColumn(schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1), 1, 0)
	want := NewByteArrayStatistics(column, memory.DefaultAllocator)
	wantSpaced := NewByteArrayStatistics(column, memory.DefaultAllocator)
	want.Update(values, 0)
	wantSpaced.UpdateSpaced(values, validBits, validBitsOffset, 2)

	for _, width := range []string{"int32", "int64"} {
		t.Run(width, func(t *testing.T) {
			got := NewByteArrayStatistics(column, memory.DefaultAllocator)
			gotSpaced := NewByteArrayStatistics(column, memory.DefaultAllocator)
			if width == "int32" {
				got.UpdateFromArrowOffsets(data, offsets32, 0)
				gotSpaced.UpdateFromArrowOffsetsSpaced(data, offsets32, validBits, validBitsOffset, 2)
			} else {
				got.UpdateFromArrowOffsets64(data, offsets64, 0)
				gotSpaced.UpdateFromArrowOffsetsSpaced64(data, offsets64, validBits, validBitsOffset, 2)
			}
			require.Equal(t, want.Min(), got.Min())
			require.Equal(t, want.Max(), got.Max())
			require.Equal(t, want.NumValues(), got.NumValues())
			require.Equal(t, want.NullCount(), got.NullCount())
			require.Equal(t, wantSpaced.Min(), gotSpaced.Min())
			require.Equal(t, wantSpaced.Max(), gotSpaced.Max())
			require.Equal(t, wantSpaced.NumValues(), gotSpaced.NumValues())
			require.Equal(t, wantSpaced.NullCount(), gotSpaced.NullCount())
		})
	}
}

func TestByteArrayBloomHashesArrowOffsetsMatchByteArrayInput(t *testing.T) {
	values, data, offsets32, offsets64 := byteArrayArrowMetadataInput()
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values)+4)))
	validBitsOffset := int64(2)
	for _, index := range []int{0, 1, 3, 4} {
		bitutil.SetBit(validBits, int(validBitsOffset)+index)
	}

	want := newBatchRecordingBloomFilter(xxhasher{})
	InsertHashes(want, values)
	wantSpaced := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashes(wantSpaced, 4, values, validBits, validBitsOffset)

	for _, width := range []string{"int32", "int64"} {
		t.Run(width, func(t *testing.T) {
			got := newBatchRecordingBloomFilter(xxhasher{})
			gotSpaced := newBatchRecordingBloomFilter(xxhasher{})
			if width == "int32" {
				InsertArrowOffsetHashes(got, data, offsets32)
				InsertSpacedArrowOffsetHashes(gotSpaced, 4, data, offsets32, validBits, validBitsOffset)
			} else {
				InsertArrowOffsetHashes64(got, data, offsets64)
				InsertSpacedArrowOffsetHashes64(gotSpaced, 4, data, offsets64, validBits, validBitsOffset)
			}
			require.Equal(t, flattenHashBatches(want.batches), flattenHashBatches(got.batches))
			require.Equal(t, flattenHashBatches(wantSpaced.batches), flattenHashBatches(gotSpaced.batches))
		})
	}
}
