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
	"runtime"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestDictionaryConverterBoundsGrowthByValuesLeft(t *testing.T) {
	decoder := NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	require.NoError(t, decoder.SetData(1, []byte{42, 0, 0, 0}))
	converter := NewDictConverter[int32](decoder).(*dictConverter[int32])

	require.Equal(t, 1, converter.dictLen)
	require.LessOrEqual(t, cap(converter.dict), 1)
	require.Error(t, converter.ensure(utils.IndexType(1<<30)))
	require.LessOrEqual(t, cap(converter.dict), 1)

	require.NoError(t, converter.ensure(0))
	require.Equal(t, []int32{42}, converter.dict)
}

func TestDictionaryConverterBoundsGrowthByData(t *testing.T) {
	decoder := NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	require.NoError(t, decoder.SetData(1<<30, []byte{42, 0, 0, 0}))
	converter := NewDictConverter[int32](decoder).(*dictConverter[int32])

	require.LessOrEqual(t, cap(converter.dict), 1)
	// an index the page's 4 bytes can't reach is rejected before the
	// dictionary grows toward it
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.Error(t, converter.ensure(utils.IndexType(1<<24)))
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
	require.LessOrEqual(t, cap(converter.dict), 1)

	require.NoError(t, converter.ensure(0))
	require.Equal(t, []int32{42}, converter.dict)
	require.Error(t, converter.ensure(1))
	require.LessOrEqual(t, cap(converter.dict), 1)
}

func TestDictionaryConverterPreallocNoLargerThanPage(t *testing.T) {
	// 4096 zero-length values: each is just a 4-byte length prefix, but
	// would cost a slice header in the dictionary
	const n = 4096
	data := make([]byte, 4*n)
	decoder := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	require.NoError(t, decoder.SetData(n, data))
	converter := NewDictConverter[parquet.ByteArray](decoder).(*dictConverter[parquet.ByteArray])

	require.LessOrEqual(t, cap(converter.dict)*int(unsafe.Sizeof(parquet.ByteArray(nil))), len(data))
	require.NoError(t, converter.ensure(n-1))
	require.Len(t, converter.dict, n)
}
