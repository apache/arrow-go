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

package encoding_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
)

func BenchmarkDeltaBinaryPackedPutSpacedInt32(b *testing.B) {
	benchmarkDeltaBinaryPackedPutSpaced[int32](b, parquet.Types.Int32, arrow.Int32SizeBytes)
}

func BenchmarkDeltaBinaryPackedPutSpacedInt64(b *testing.B) {
	benchmarkDeltaBinaryPackedPutSpaced[int64](b, parquet.Types.Int64, arrow.Int64SizeBytes)
}

func benchmarkDeltaBinaryPackedPutSpaced[T int32 | int64](b *testing.B, typ parquet.Type, bytesPerValue int) {
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
		b.Run(fmt.Sprintf("length_%d", length), func(b *testing.B) {
			values := make([]T, length)
			for i := range values {
				values[i] = T(i)
			}

			for _, pattern := range patterns {
				b.Run(pattern.name, func(b *testing.B) {
					validBits := make([]byte, bitutil.BytesForBits(int64(length)))
					for i := range length {
						if pattern.valid(i) {
							bitutil.SetBit(validBits, i)
						}
					}

					encoder := encoding.NewEncoder(
						typ, parquet.Encodings.DeltaBinaryPacked,
						false, nil, memory.DefaultAllocator,
					).(encoding.Encoder[T])
					defer encoder.Release()

					encoder.PutSpaced(values, validBits, 0)
					buf, err := encoder.FlushValues()
					if err != nil {
						b.Fatal(err)
					}
					buf.Release()

					b.ReportAllocs()
					b.SetBytes(int64(length * bytesPerValue))
					b.ResetTimer()
					for b.Loop() {
						encoder.PutSpaced(values, validBits, 0)
						buf, err := encoder.FlushValues()
						if err != nil {
							b.Fatal(err)
						}
						buf.Release()
					}
				})
			}
		})
	}
}
