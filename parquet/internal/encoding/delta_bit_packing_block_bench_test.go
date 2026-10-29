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
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func benchmarkDeltaBitPackBlockFastPath[T int32 | int64](b *testing.B, typ parquet.Type, bytesPerValue int) {
	const length = 64 * 1024

	for _, pattern := range []string{"constant", "sparse", "last-outlier", "alternating", "random"} {
		b.Run(pattern, func(b *testing.B) {
			values := make([]T, length)
			rng := rand.New(rand.NewSource(42))
			for i := 1; i < length; i++ {
				switch pattern {
				case "constant":
					values[i] = values[i-1] + T(3)
				case "sparse":
					step := T(3)
					if i%128 == 90 {
						step = 7
					}
					values[i] = values[i-1] + step
				case "last-outlier":
					step := T(3)
					// Delta indices 128, 256, ... end each full block.
					if i%128 == 0 {
						step = -7
					}
					values[i] = values[i-1] + step
				case "alternating":
					values[i] = values[i-1] + T(i%2+1)
				case "random":
					values[i] = T(rng.Uint64())
				}
			}

			enc := NewEncoder(typ, parquet.Encodings.DeltaBinaryPacked, false, nil, memory.DefaultAllocator).(*deltaBitPackEncoder[T])
			defer enc.Release()

			b.SetBytes(int64(length * bytesPerValue))
			b.ReportAllocs()
			for b.Loop() {
				enc.Put(values)
				buf, err := enc.FlushValues()
				if err != nil {
					b.Fatal(err)
				}
				buf.Release()
			}
		})
	}
}

func BenchmarkDeltaBitPackBlockFastPathInt32(b *testing.B) {
	benchmarkDeltaBitPackBlockFastPath[int32](b, parquet.Types.Int32, 4)
}

func BenchmarkDeltaBitPackBlockFastPathInt64(b *testing.B) {
	benchmarkDeltaBitPackBlockFastPath[int64](b, parquet.Types.Int64, 8)
}
