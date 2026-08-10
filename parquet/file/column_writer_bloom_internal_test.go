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

package file

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestDictionaryBloomHashesUsePlainEncoding(t *testing.T) {
	tests := []struct {
		name     string
		node     *schema.PrimitiveNode
		putValue func(encoding.TypedEncoder)
		expected []byte
	}{
		{
			name: "int32",
			node: schema.NewInt32Node("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.Int32Encoder).Put([]int32{0x01020304})
			},
			expected: []byte{0x04, 0x03, 0x02, 0x01},
		},
		{
			name: "int64",
			node: schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.Int64Encoder).Put([]int64{0x0102030405060708})
			},
			expected: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
		},
		{
			name: "float",
			node: schema.NewFloat32Node("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.Float32Encoder).Put([]float32{math.Float32frombits(0x01020304)})
			},
			expected: []byte{0x04, 0x03, 0x02, 0x01},
		},
		{
			name: "double",
			node: schema.NewFloat64Node("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.Float64Encoder).Put([]float64{math.Float64frombits(0x0102030405060708)})
			},
			expected: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
		},
		{
			name: "int96",
			node: schema.NewInt96Node("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.Int96Encoder).Put([]parquet.Int96{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}})
			},
			expected: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
		{
			name: "byte array",
			node: schema.NewByteArrayNode("value", parquet.Repetitions.Required, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.ByteArrayEncoder).Put([]parquet.ByteArray{[]byte("plain")})
			},
			expected: []byte("plain"),
		},
		{
			name: "fixed length byte array",
			node: schema.NewFixedLenByteArrayNode("value", parquet.Repetitions.Required, 5, -1),
			putValue: func(enc encoding.TypedEncoder) {
				enc.(encoding.FixedLenByteArrayEncoder).Put([]parquet.FixedLenByteArray{[]byte("fixed")})
			},
			expected: []byte("fixed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descr := schema.NewColumn(tt.node, 0, 0)
			enc := encoding.NewEncoder(descr.PhysicalType(), parquet.Encodings.PlainDict, true, descr, memory.DefaultAllocator)
			defer enc.Release()
			dictEnc := enc.(encoding.DictEncoder)
			dictEnc.EnableDictionaryReferenceTracking()
			tt.putValue(enc)

			dictionary := make([]byte, dictEnc.DictEncodedSize())
			dictEnc.WriteDict(dictionary)
			builder := metadata.NewBloomFilter(32, 32, memory.DefaultAllocator)
			writer := columnWriter{descr: descr, bloomFilter: builder}
			require.NoError(t, writer.populateBloomFilterFromDictionary(dictEnc, dictionary))

			filter, ok := builder.(metadata.BloomFilter)
			require.True(t, ok)
			require.True(t, filter.CheckHash(xxhash.Sum64(tt.expected)))
		})
	}
}
