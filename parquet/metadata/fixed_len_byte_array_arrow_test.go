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

func TestFixedLenByteArrayStatisticsUpdateFromArrowFixedWidth(t *testing.T) {
	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Optional, parquet.Types.FixedLenByteArray, -1, 3)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)
	stats := NewStatistics(descr, memory.DefaultAllocator).(*FixedLenByteArrayStatistics)

	values := []byte("bbb" + "aaa" + "ccc" + "abc")
	stats.UpdateFromArrowFixedWidth(values, 3, 0)

	require.Equal(t, int64(4), stats.NumValues())
	require.Equal(t, int64(0), stats.NullCount())
	require.Equal(t, []byte("aaa"), []byte(stats.Min()))
	require.Equal(t, []byte("ccc"), []byte(stats.Max()))

	validBits := []byte{0b1101}
	stats.Reset()
	stats.UpdateFromArrowFixedWidthSpaced(values, 3, validBits, 0, 1)
	require.Equal(t, int64(3), stats.NumValues())
	require.Equal(t, int64(1), stats.NullCount())
	require.Equal(t, []byte("abc"), []byte(stats.Min()))
	require.Equal(t, []byte("ccc"), []byte(stats.Max()))
}

func TestInsertArrowFixedLenHashes(t *testing.T) {
	values := []byte("aaaa" + "bbbb" + "cccc" + "dddd")
	parquetValues := []parquet.FixedLenByteArray{
		values[0:4], values[4:8], values[8:12], values[12:16],
	}

	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertArrowFixedLenHashes(bloom, values, 4)
	require.Equal(t, GetHashes(xxhasher{}, parquetValues), flattenHashBatches(bloom.batches))

	validBits := make([]byte, bitutil.BytesForBits(6))
	bitutil.SetBit(validBits, 2)
	bitutil.SetBit(validBits, 4)
	valid := []parquet.FixedLenByteArray{parquetValues[1], parquetValues[3]}
	bloom = newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedArrowFixedLenHashes(bloom, 2, values, 4, validBits, 1)
	require.Equal(t, GetHashes(xxhasher{}, valid), flattenHashBatches(bloom.batches))
}
