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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func BenchmarkDeltaByteArrayDecoderDecode(b *testing.B) {
	repeatedValue := strings.Repeat("x", 1024)
	for _, test := range []struct {
		name  string
		value func(int) string
	}{
		{
			name: "identical",
			value: func(int) string {
				return repeatedValue
			},
		},
		{
			name: "prefix-heavy",
			value: func(i int) string {
				return fmt.Sprintf("tenant/%04d/partition/%04d/object", i/100, i)
			},
		},
		{
			name: "low-prefix",
			value: func(i int) string {
				return fmt.Sprintf("%08x/%08x", i, i*7919)
			},
		},
	} {
		for _, nvalues := range []int{1024, 65536} {
			test := test
			nvalues := nvalues
			b.Run(fmt.Sprintf("%s/%d", test.name, nvalues), func(b *testing.B) {
				values := make([]parquet.ByteArray, nvalues)
				inputBytes := 0
				for i := range values {
					values[i] = parquet.ByteArray(test.value(i))
					inputBytes += len(values[i])
				}
				encoded := encodeDeltaByteArrayValues(values)

				for _, batchSize := range []int{128, 1024, nvalues} {
					batchSize := batchSize
					b.Run(fmt.Sprintf("batch-%d", batchSize), func(b *testing.B) {
						output := make([]parquet.ByteArray, batchSize)
						dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
							nil, memory.DefaultAllocator).(ByteArrayDecoder)
						b.SetBytes(int64(inputBytes))
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							if err := dec.SetData(nvalues, encoded); err != nil {
								b.Fatal(err)
							}
							remaining := nvalues
							for remaining > 0 {
								count := min(batchSize, remaining)
								decoded, err := dec.Decode(output[:count])
								if err != nil {
									b.Fatal(err)
								}
								if decoded != count {
									b.Fatalf("decoded %d values, expected %d", decoded, count)
								}
								remaining -= count
							}
						}
					})
				}
			})
		}
	}
}

func encodeDeltaByteArrayValues(values []parquet.ByteArray) []byte {
	enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
		false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	if err != nil {
		panic(err)
	}
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}
