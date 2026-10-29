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

package variant_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/require"
)

func TestMetadataOffsetTableMayEndAtInputBoundary(t *testing.T) {
	metadata, err := variant.NewMetadata([]byte{
		0x01, // version 1 with one-byte offsets
		0x01, // one dictionary key
		0x00, // first offset
		0x00, // final offset: the key is empty
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, metadata.DictionarySize())

	key, err := metadata.KeyAt(0)
	require.NoError(t, err)
	require.Empty(t, key)
}

// Multi-byte offset tables can cross byte 255 even for small dictionaries.
func TestMetadataOffsetTableCrossesByteBoundary(t *testing.T) {
	for _, tc := range []struct{ width, keys int }{
		{2, 125}, {2, 126}, {2, 127}, {2, 128}, {2, 256},
		{3, 82}, {3, 83}, {3, 84}, {3, 256},
		{4, 61}, {4, 62}, {4, 63}, {4, 256},
	} {
		t.Run(fmt.Sprintf("width_%d/keys_%d", tc.width, tc.keys), func(t *testing.T) {
			// Use explicitly encoded, valid metadata to exercise each offset width
			// independently of the builder's choice of the smallest representation.
			encoded := []byte{byte(1 | (tc.width-1)<<6)}
			appendOffset := func(value int) {
				for i := 0; i < tc.width; i++ {
					encoded = append(encoded, byte(value>>(8*i)))
				}
			}
			appendOffset(tc.keys)
			keys := make([]string, tc.keys)
			offset := 0
			for i := range keys {
				keys[i] = fmt.Sprintf("key_%04d", i)
				appendOffset(offset)
				offset += len(keys[i])
			}
			appendOffset(offset)
			for _, key := range keys {
				encoded = append(encoded, key...)
			}
			metadata, err := variant.NewMetadata(encoded)
			require.NoError(t, err)
			require.EqualValues(t, tc.keys, metadata.DictionarySize())
			for i, want := range keys {
				got, err := metadata.KeyAt(uint32(i))
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}
