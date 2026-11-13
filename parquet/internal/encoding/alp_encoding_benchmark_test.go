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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
)

// ALP performance depends on its exception rate. The benchmarks cover no
// exceptions, one exception per ten values, and all exceptions.
var alpBenchmarkCases = []struct {
	name              string
	exceptionInterval int
}{
	{"no exceptions", 0},
	{"10% exceptions", 10},
	{"all exceptions", 1},
}

// alpBenchmarkValues returns values at the requested exception rate. Every
// other value is computed the way ALP decodes k with exponent 2 and factor 0,
// in the value's own precision, so a scaling exists that stores it without an
// exception. The sampled search occasionally picks an equivalent scaling that
// misses a few, about 2 in 65536 doubles. NaN never encodes.
func alpBenchmarkValues[T float32 | float64](size, exceptionInterval int) []T {
	values := make([]T, size)
	for i := range values {
		values[i] = T(i%10000) * T(1e0) * T(1e-2)
		if exceptionInterval != 0 && i%exceptionInterval == 0 {
			values[i] = T(math.NaN())
		}
	}
	return values
}

func BenchmarkALPEncodingFloat32(b *testing.B) {
	for _, benchmark := range alpBenchmarkCases {
		b.Run(benchmark.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float32](sz, benchmark.exceptionInterval)
					encoder := encoding.NewEncoder(parquet.Types.Float, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float32Encoder)
					defer encoder.Release()
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(len(values) * arrow.Float32SizeBytes))
					for b.Loop() {
						encoder.Put(values)
						buf, _ := encoder.FlushValues()
						buf.Release()
					}
				})
			}
		})
	}
}

func BenchmarkALPEncodingFloat64(b *testing.B) {
	for _, benchmark := range alpBenchmarkCases {
		b.Run(benchmark.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float64](sz, benchmark.exceptionInterval)
					encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
					defer encoder.Release()
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(len(values) * arrow.Float64SizeBytes))
					for b.Loop() {
						encoder.Put(values)
						buf, _ := encoder.FlushValues()
						buf.Release()
					}
				})
			}
		})
	}
}

func BenchmarkALPDecodingFloat32(b *testing.B) {
	for _, benchmark := range alpBenchmarkCases {
		b.Run(benchmark.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float32](sz, benchmark.exceptionInterval)
					output := make([]float32, sz)
					encoder := encoding.NewEncoder(parquet.Types.Float, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float32Encoder)
					defer encoder.Release()
					encoder.Put(values)
					buf, _ := encoder.FlushValues()
					defer buf.Release()

					decoder := encoding.NewDecoder(parquet.Types.Float, parquet.Encodings.ALP,
						nil, memory.DefaultAllocator).(encoding.Float32Decoder)
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(len(values) * arrow.Float32SizeBytes))
					for b.Loop() {
						if err := decoder.SetData(sz, buf.Bytes()); err != nil {
							b.Fatal(err)
						}
						if _, err := decoder.Decode(output); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkALPDecodingFloat64(b *testing.B) {
	for _, benchmark := range alpBenchmarkCases {
		b.Run(benchmark.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float64](sz, benchmark.exceptionInterval)
					output := make([]float64, sz)
					encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
					defer encoder.Release()
					encoder.Put(values)
					buf, _ := encoder.FlushValues()
					defer buf.Release()

					decoder := encoding.NewDecoder(parquet.Types.Double, parquet.Encodings.ALP,
						nil, memory.DefaultAllocator).(encoding.Float64Decoder)
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(len(values) * arrow.Float64SizeBytes))
					for b.Loop() {
						if err := decoder.SetData(sz, buf.Bytes()); err != nil {
							b.Fatal(err)
						}
						if _, err := decoder.Decode(output); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// Batched decoding exercises calls that stop partway through a vector.
func BenchmarkALPDecodingFloat64Batched(b *testing.B) {
	const batchSize = 137
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := alpBenchmarkValues[float64](sz, 10)
			output := make([]float64, batchSize)
			encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
				false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
			defer encoder.Release()
			encoder.Put(values)
			buf, _ := encoder.FlushValues()
			defer buf.Release()

			decoder := encoding.NewDecoder(parquet.Types.Double, parquet.Encodings.ALP,
				nil, memory.DefaultAllocator).(encoding.Float64Decoder)
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Float64SizeBytes))
			for b.Loop() {
				if err := decoder.SetData(sz, buf.Bytes()); err != nil {
					b.Fatal(err)
				}
				for left := sz; left > 0; left -= batchSize {
					if _, err := decoder.Decode(output[:min(left, batchSize)]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// Whole numbers up to 2^50 pack at about 50 bits each with no exceptions,
// which is wider than the 32 bits the vectorized unpackers handle.
func BenchmarkALPDecodingFloat64Wide(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]float64, sz)
			for i := range values {
				values[i] = float64((uint64(i) * 0x9E3779B97F4A7C15) >> 14)
			}
			output := make([]float64, sz)
			encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
				false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
			defer encoder.Release()
			encoder.Put(values)
			buf, _ := encoder.FlushValues()
			defer buf.Release()

			decoder := encoding.NewDecoder(parquet.Types.Double, parquet.Encodings.ALP,
				nil, memory.DefaultAllocator).(encoding.Float64Decoder)
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Float64SizeBytes))
			for b.Loop() {
				if err := decoder.SetData(sz, buf.Bytes()); err != nil {
					b.Fatal(err)
				}
				if _, err := decoder.Decode(output); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
