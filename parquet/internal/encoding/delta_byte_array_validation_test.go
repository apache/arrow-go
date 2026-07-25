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

func encodeDeltaPrefixes(t *testing.T, values []int32) []byte {
	t.Helper()
	enc := NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, false, nil, memory.DefaultAllocator)
	enc.(Int32Encoder).Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func encodeDeltaSuffixes(t *testing.T, values []parquet.ByteArray) []byte {
	t.Helper()
	enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, false, nil, memory.DefaultAllocator)
	enc.(ByteArrayEncoder).Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func deltaBytePage(t *testing.T, prefixes []int32, suffixes ...string) []byte {
	t.Helper()
	values := make([]parquet.ByteArray, len(suffixes))
	for i := range suffixes {
		values[i] = parquet.ByteArray(suffixes[i])
	}
	return append(encodeDeltaPrefixes(t, prefixes), encodeDeltaSuffixes(t, values)...)
}

func TestDeltaByteArrayDecoderResetsBetweenPages(t *testing.T) {
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, nil, memory.DefaultAllocator)
	require.NoError(t, dec.SetData(1, deltaBytePage(t, []int32{0}, "abc")))
	_, err := dec.(ByteArrayDecoder).Decode(make([]parquet.ByteArray, 1))
	require.NoError(t, err)

	require.NoError(t, dec.SetData(1, deltaBytePage(t, []int32{1}, "x")))
	_, err = dec.(ByteArrayDecoder).Decode(make([]parquet.ByteArray, 1))
	require.Error(t, err)
}

func TestDeltaByteArrayDecoderRejectsInvalidPrefixes(t *testing.T) {
	for _, prefixes := range [][]int32{{0, -1}, {0, 2}} {
		dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, nil, memory.DefaultAllocator)
		require.NoError(t, dec.SetData(2, deltaBytePage(t, prefixes, "a", "b")))
		_, err := dec.(ByteArrayDecoder).Decode(make([]parquet.ByteArray, 2))
		require.Error(t, err)
	}
}
