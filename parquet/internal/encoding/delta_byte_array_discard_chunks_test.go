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

func TestDeltaByteArrayDecoderDiscardChunkBoundaries(t *testing.T) {
	values := []string{"aa", "aa", "a", "", "", "prefix/000", "prefix/001", "prefix/001", "z"}
	data := encodeDeltaByteArrayPage(t, values)
	for initial := 0; initial <= len(values); initial++ {
		for skip := 0; skip <= len(values)+1; skip++ {
			t.Run(fmt.Sprintf("decoded=%d/discard=%d", initial, skip), func(t *testing.T) {
				dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
					nil, memory.DefaultAllocator).(*DeltaByteArrayDecoder)
				require.NoError(t, dec.SetData(len(values), data))

				retained := make([]parquet.ByteArray, initial)
				decoded, err := dec.Decode(retained)
				require.NoError(t, err)
				require.Equal(t, initial, decoded)

				discarded, err := dec.Discard(skip)
				require.NoError(t, err)
				require.Equal(t, min(skip, len(values)-initial), discarded)
				next := initial + discarded
				require.Equal(t, len(values)-next, dec.nvals)
				require.Len(t, dec.lengths, len(values)-next)
				require.Len(t, dec.prefixLengths, len(values)-next)

				rest := make([]parquet.ByteArray, len(values)+1)
				decoded, err = dec.Decode(rest)
				require.NoError(t, err)
				require.Equal(t, len(values)-next, decoded)
				for i, value := range rest[:decoded] {
					require.Equal(t, values[next+i], string(value))
				}
				for i, value := range retained {
					require.Equal(t, values[i], string(value))
				}

				discarded, err = dec.Discard(1)
				require.NoError(t, err)
				require.Zero(t, discarded)
				require.Zero(t, dec.nvals)
				require.Empty(t, dec.lengths)
				require.Empty(t, dec.prefixLengths)
				require.Empty(t, dec.data)
			})
		}
	}
}
