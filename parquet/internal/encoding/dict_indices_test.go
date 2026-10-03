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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/stretchr/testify/require"
)

func makeDictIndicesEncoder(t testing.TB, cardinality int) dictEncoder {
	t.Helper()
	memo := NewInt32Dictionary()
	for i := range cardinality {
		_, _, err := memo.GetOrInsert(int32(i))
		require.NoError(t, err)
	}
	t.Cleanup(memo.Reset)
	return dictEncoder{memo: memo}
}

func TestDictEncoderWriteIndicesMatchesScalar(t *testing.T) {
	for _, tc := range []struct{ cardinality, width int }{
		{0, 0}, {1, 1}, {2, 1}, {256, 8}, {257, 9}, {65537, 17},
	} {
		t.Run(fmt.Sprint(tc.cardinality), func(t *testing.T) {
			enc := makeDictIndicesEncoder(t, tc.cardinality)
			var indices []int32
			if tc.cardinality > 0 {
				indices = make([]int32, 1031)
				for i := range indices {
					indices[i] = int32(tc.cardinality - 1)
					if i%31 < 9 {
						indices[i] = int32(i % tc.cardinality)
					}
				}
			}
			for range 2 {
				enc.idxValues = indices
				enc.rawDataSize = int64(len(indices) * 4)
				expected := make([]byte, enc.EstimatedDataEncodedSize())
				expected[0] = byte(tc.width)
				scalar := utils.NewRleEncoder(utils.NewWriterAtBuffer(expected[1:]), tc.width)
				for _, index := range indices {
					require.NoError(t, scalar.Put(uint64(index)))
				}
				expectedSize := scalar.Flush() + 1
				output := make([]byte, len(expected))
				n, err := enc.WriteIndices(output)
				require.NoError(t, err)
				require.Equal(t, expected[:expectedSize], output[:n])
				require.Empty(t, enc.idxValues)
				require.Zero(t, enc.rawDataSize)
				require.Equal(t, tc.cardinality, enc.NumEntries())
			}
		})
	}
}

func TestDictEncoderWriteIndicesPreservesStateOnError(t *testing.T) {
	enc := makeDictIndicesEncoder(t, 2)
	indices := []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	enc.idxValues = indices
	enc.rawDataSize = int64(len(indices) * 4)
	n, err := enc.WriteIndices(make([]byte, 1))
	require.Error(t, err)
	require.Equal(t, -1, n)
	require.Equal(t, indices, enc.idxValues)
	require.EqualValues(t, len(indices)*4, enc.rawDataSize)

	output := make([]byte, enc.EstimatedDataEncodedSize())
	n, err = enc.WriteIndices(output)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 18, 0, 2, 1}, output[:n])
	require.Empty(t, enc.idxValues)
	require.Zero(t, enc.rawDataSize)
}
