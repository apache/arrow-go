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

func TestDeltaLengthByteArrayDecoderRejectsInvalidLengths(t *testing.T) {
	tests := [][]byte{
		{128, 1, 4, 1, 1},  // one length of -1
		{128, 1, 4, 1, 10}, // one length of 5 with no payload
	}

	for _, data := range tests {
		dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, nil, memory.DefaultAllocator)
		require.Error(t, dec.SetData(1, data))
	}
}

func TestDeltaLengthByteArrayDecoderUsesEncodedLengthCount(t *testing.T) {
	// The page has two logical positions but only one physical value. This is
	// valid for an optional column whose other position is null.
	data := []byte{128, 1, 4, 1, 0}
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, nil, memory.DefaultAllocator)
	require.NoError(t, dec.SetData(2, data))
	require.Equal(t, 1, dec.ValuesLeft())

	values, err := dec.(ByteArrayDecoder).Decode(make([]parquet.ByteArray, 2))
	require.NoError(t, err)
	require.Equal(t, 1, values)
	require.Zero(t, dec.ValuesLeft())
}
