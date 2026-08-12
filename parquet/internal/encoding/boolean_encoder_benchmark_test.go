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

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
)

func BenchmarkPlainBooleanEncoderPutSpacedBitmap(b *testing.B) {
	patterns := []struct {
		name         string
		bitmapOffset int
		valid        func(int) bool
	}{
		{name: "all_valid", valid: func(int) bool { return true }},
		{name: "one_percent_null", valid: func(i int) bool { return i%100 != 0 }},
		{name: "ten_percent_null", valid: func(i int) bool { return i%10 != 0 }},
		{name: "fifty_percent_null", valid: func(i int) bool { return i%2 != 0 }},
		{name: "ninety_percent_null", valid: func(i int) bool { return i%10 == 0 }},
		{name: "clustered", valid: func(i int) bool { return i%1024 >= 256 }},
		{
			name:  "eight_valid_eight_null_aligned",
			valid: func(i int) bool { return i%16 < 8 },
		},
		{
			name:         "eight_valid_eight_null_unaligned",
			bitmapOffset: 1,
			valid:        func(i int) bool { return i%16 < 8 },
		},
		{
			name:  "twenty_four_valid_eight_null_aligned",
			valid: func(i int) bool { return i%32 < 24 },
		},
		{
			name:         "twenty_four_valid_eight_null_unaligned",
			bitmapOffset: 1,
			valid:        func(i int) bool { return i%32 < 24 },
		},
	}

	for _, length := range []int{1024, 64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("length_%d", length), func(b *testing.B) {
			for _, pattern := range patterns {
				b.Run(pattern.name, func(b *testing.B) {
					bitmap := makeBooleanBitmap(length, pattern.bitmapOffset, func(i int) bool { return i%3 != 0 })
					validity := makeBooleanBitmap(length, 0, pattern.valid)
					expectedValid := int64(bitutil.CountSetBits(validity, 0, length))
					encoder := encoding.NewEncoder(
						parquet.Types.Boolean, parquet.Encodings.Plain,
						false, nil, memory.DefaultAllocator,
					).(encoding.BooleanEncoder)
					spaced := encoder.(spacedBitmapEncoder)

					b.ReportAllocs()
					b.SetBytes(int64(length))
					b.ResetTimer()
					for b.Loop() {
						if actual := spaced.PutSpacedBitmap(bitmap, int64(pattern.bitmapOffset), int64(length), validity, 0); actual != expectedValid {
							b.Fatalf("expected %d valid values, got %d", expectedValid, actual)
						}
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

func makeBooleanBitmap(length, offset int, value func(int) bool) []byte {
	bitmap := make([]byte, bitutil.BytesForBits(int64(length+offset)))
	for i := range length {
		if value(i) {
			bitutil.SetBit(bitmap, offset+i)
		}
	}
	return bitmap
}
