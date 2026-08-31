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

func BenchmarkRleBooleanDecoderDecodeToBitmap(b *testing.B) {
	patterns := []struct {
		name  string
		value func(int) bool
	}{
		{name: "all_true", value: func(int) bool { return true }},
		{name: "all_false", value: func(int) bool { return false }},
		{name: "alternating", value: func(i int) bool { return i%2 == 0 }},
		{name: "short_runs", value: func(i int) bool { return (i/7)%2 == 0 }},
	}

	for _, size := range []int{64 * 1024, 1024 * 1024} {
		for _, pattern := range patterns {
			b.Run(fmt.Sprintf("size_%d/%s", size, pattern.name), func(b *testing.B) {
				values := makeBooleanValues(size, pattern.value)
				data := encodeRleBooleanValues(b, values)
				out := make([]byte, bitutil.BytesForBits(int64(size)))
				dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE,
					nil, memory.DefaultAllocator).(encoding.BooleanBitmapDecoder)

				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for b.Loop() {
					if err := dec.SetData(size, data); err != nil {
						b.Fatal(err)
					}
					n, err := dec.DecodeToBitmap(out, 0, size)
					if err != nil {
						b.Fatal(err)
					}
					if n != size {
						b.Fatalf("expected %d values, got %d", size, n)
					}
				}
			})
		}
	}
}

func BenchmarkRleBooleanDecoderDecodeSpacedToBitmap(b *testing.B) {
	patterns := []struct {
		name      string
		nullEvery int
	}{
		{name: "all_valid"},
		{name: "nullable_10pct", nullEvery: 10},
		{name: "nullable_50pct", nullEvery: 2},
	}

	for _, size := range []int{64 * 1024, 1024 * 1024} {
		for _, pattern := range patterns {
			b.Run(fmt.Sprintf("size_%d/%s", size, pattern.name), func(b *testing.B) {
				logicalValues := makeBooleanValues(size, func(i int) bool { return i%2 == 0 })
				validity := make([]byte, bitutil.BytesForBits(int64(size)))
				physicalValues := make([]bool, 0, size)
				nullCount := 0
				for i, value := range logicalValues {
					if pattern.nullEvery > 0 && i%pattern.nullEvery == 0 {
						nullCount++
						continue
					}
					bitutil.SetBit(validity, i)
					physicalValues = append(physicalValues, value)
				}
				if pattern.nullEvery == 0 {
					for i := range logicalValues {
						bitutil.SetBit(validity, i)
					}
				}

				data := encodeRleBooleanValues(b, physicalValues)
				out := make([]byte, bitutil.BytesForBits(int64(size)))
				dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE,
					nil, memory.DefaultAllocator).(encoding.BooleanBitmapDecoder)

				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for b.Loop() {
					if err := dec.SetData(len(physicalValues), data); err != nil {
						b.Fatal(err)
					}
					n, err := dec.DecodeSpacedToBitmap(out, 0, size, nullCount, validity, 0)
					if err != nil {
						b.Fatal(err)
					}
					if n != size {
						b.Fatalf("expected %d values, got %d", size, n)
					}
				}
			})
		}
	}
}
