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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestDeltaByteArrayDecoderDiscardKeepsDecodedPrefixes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"repeated", "aa"},
		{"shorter-prefix", "a"},
		{"empty", ""},
		{"non-empty-suffix", "ab"},
	} {
		for _, nextPage := range []bool{false, true} {
			pageName := "same-page"
			if nextPage {
				pageName = "next-page"
			}
			t.Run(tc.name+"/"+pageName, func(t *testing.T) {
				values := []string{"aa", tc.value, "zz"}
				if nextPage {
					values = values[:2]
				}
				dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
					nil, memory.DefaultAllocator).(ByteArrayDecoder)
				require.NoError(t, dec.SetData(len(values), encodeDeltaByteArrayPage(t, values)))

				discarded, err := dec.Discard(1)
				require.NoError(t, err)
				require.Equal(t, 1, discarded)

				out := make([]parquet.ByteArray, 1)
				decoded, err := dec.Decode(out)
				require.NoError(t, err)
				require.Equal(t, 1, decoded)
				require.Equal(t, tc.value, string(out[0]))

				if nextPage {
					require.NoError(t, dec.SetData(1, encodeDeltaByteArrayPage(t, []string{"zz"})))
				}
				discarded, err = dec.Discard(1)
				require.NoError(t, err)
				require.Equal(t, 1, discarded)
				require.Equal(t, tc.value, string(out[0]))
			})
		}
	}
}

func TestDeltaByteArrayDecoderDiscardKeepsDecodedPrefixBatch(t *testing.T) {
	values := []string{"aa", "aa", "a", "zz"}
	dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		nil, memory.DefaultAllocator).(ByteArrayDecoder)
	require.NoError(t, dec.SetData(len(values), encodeDeltaByteArrayPage(t, values)))

	discarded, err := dec.Discard(1)
	require.NoError(t, err)
	require.Equal(t, 1, discarded)
	out := make([]parquet.ByteArray, 2)
	decoded, err := dec.Decode(out)
	require.NoError(t, err)
	require.Equal(t, 2, decoded)

	discarded, err = dec.Discard(1)
	require.NoError(t, err)
	require.Equal(t, 1, discarded)
	require.Equal(t, "aa", string(out[0]))
	require.Equal(t, "a", string(out[1]))
}
