// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestDeltaBitPackDecoderValidatesHeader(t *testing.T) {
	tests := [][]byte{
		{0, 1, 1, 0},         // zero values per block
		{1, 1, 1, 0},         // block size is not a multiple of 128
		{128, 1, 0, 1, 0},    // zero miniblocks
		{128, 1, 8, 1, 0},    // 16 values per miniblock
		{128, 26, 101, 1, 0}, // miniblocks do not divide block size
		{128, 1, 4, 2, 0},    // encoded count exceeds page count
	}

	for _, data := range tests {
		dec := NewDecoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, nil, memory.DefaultAllocator)
		require.Error(t, dec.SetData(1, data))
	}
}

func TestDeltaBitPackInt32DecoderValidatesEncodedIntegerRange(t *testing.T) {
	// 2147483648 zigzag-encodes to the final five bytes in each payload.
	outOfRange := []byte{128, 128, 128, 128, 16}

	dec := NewDecoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, nil, memory.DefaultAllocator)
	require.Error(t, dec.SetData(1, append([]byte{128, 1, 4, 1}, outOfRange...)))

	data := append([]byte{128, 1, 4, 2, 0}, outOfRange...)
	require.NoError(t, dec.SetData(2, data))
	_, err := dec.Discard(2)
	require.Error(t, err)
}

func TestDeltaBitPackDecoderValidatesUsedBitWidth(t *testing.T) {
	// Header: 128 values/block, 4 miniblocks, 2 values, first value 0.
	// Block: min delta 0, then the supplied bit width for the first miniblock.
	for _, tc := range []struct {
		name     string
		typ      parquet.Type
		bitWidth byte
	}{
		{name: "int32", typ: parquet.Types.Int32, bitWidth: 33},
		{name: "int64", typ: parquet.Types.Int64, bitWidth: 65},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte{128, 1, 4, 2, 0, 0, tc.bitWidth, 0, 0, 0}
			dec := NewDecoder(tc.typ, parquet.Encodings.DeltaBinaryPacked, nil, memory.DefaultAllocator)
			require.NoError(t, dec.SetData(2, data))

			_, err := dec.Discard(2)
			require.Error(t, err)
		})
	}
}
