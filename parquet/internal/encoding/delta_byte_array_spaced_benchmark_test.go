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

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func BenchmarkDeltaLengthByteArrayPutSpaced(b *testing.B) {
	benchmarkDeltaByteArrayPutSpaced(b, parquet.Encodings.DeltaLengthByteArray)
}

func BenchmarkDeltaByteArrayPutSpaced(b *testing.B) {
	benchmarkDeltaByteArrayPutSpaced(b, parquet.Encodings.DeltaByteArray)
}

func benchmarkDeltaByteArrayPutSpaced(b *testing.B, encoding parquet.Encoding) {
	patterns := []struct {
		name  string
		valid func(int) bool
	}{
		{name: "all_valid", valid: func(int) bool { return true }},
		{name: "ten_percent_null", valid: func(i int) bool { return i%10 != 0 }},
		{name: "fifty_percent_null", valid: func(i int) bool { return i%2 != 0 }},
		{name: "ninety_percent_null", valid: func(i int) bool { return i%10 == 0 }},
	}

	for _, length := range []int{1024, 64 * 1024} {
		values := make([]parquet.ByteArray, length)
		for i := range values {
			values[i] = parquet.ByteArray(fmt.Sprintf("partition/%06d", i))
		}

		for _, pattern := range patterns {
			b.Run(fmt.Sprintf("length_%d/%s", length, pattern.name), func(b *testing.B) {
				validBits := make([]byte, bitutil.BytesForBits(int64(length)))
				for i := range length {
					if pattern.valid(i) {
						bitutil.SetBit(validBits, i)
					}
				}

				encoder := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
				defer encoder.Release()

				encode := func() {
					encoder.PutSpaced(values, validBits, 0)
					buf, err := encoder.FlushValues()
					if err != nil {
						b.Fatal(err)
					}
					buf.Release()
				}

				encode()
				b.SetBytes(int64(length * 16))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					encode()
				}
			})
		}
	}
}
