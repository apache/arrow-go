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
)

func BenchmarkDeltaByteArrayEncoding(b *testing.B) {
	for _, test := range []struct {
		name  string
		value func(int) []byte
	}{
		{
			name: "prefix-heavy",
			value: func(i int) []byte {
				return []byte(fmt.Sprintf("partition/%06d", i))
			},
		},
		{
			name: "low-prefix",
			value: func(i int) []byte {
				return []byte(fmt.Sprintf("%c/%06d", byte(i%251), i))
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			const nvalues = 64 * 1024
			values := make([]parquet.ByteArray, nvalues)
			var inputBytes int64
			for i := range values {
				values[i] = test.value(i)
				inputBytes += int64(values[i].Len())
			}

			for _, putSize := range []int{1, 8, 32, deltaByteArrayBatchSize, nvalues} {
				putSize := putSize
				b.Run(fmt.Sprintf("put-%d", putSize), func(b *testing.B) {
					b.SetBytes(inputBytes)
					b.ReportAllocs()
					for b.Loop() {
						enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
							false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
						for offset := 0; offset < len(values); offset += putSize {
							end := min(offset+putSize, len(values))
							enc.Put(values[offset:end])
						}
						buf, err := enc.FlushValues()
						if err != nil {
							enc.Release()
							b.Fatal(err)
						}
						buf.Release()
						enc.Release()
					}
				})
			}
		})
	}
}
