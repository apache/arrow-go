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

// How ALP performs depends on the data far more than a fixed-width encoding
// does, so each benchmark runs over three shapes: values that are all
// representable as a small decimal, which is the case ALP is for; a tenth of
// them not representable, which is the exception path at a rate a real column
// reaches; and none of them representable, which is the worst case, where every
// value is stored as itself on top of a packed integer that is never read.
var alpBenchmarkData = []struct {
	name          string
	exceptionRate int
}{
	{"clean", 0},
	{"exceptions 10%", 10},
	{"exceptions 100%", 1},
}

func alpBenchmarkValues[T float32 | float64](size, exceptionRate int) []T {
	values := make([]T, size)
	for i := range values {
		values[i] = T(i%100000) / 1000
		if exceptionRate != 0 && i%exceptionRate == 0 {
			values[i] = T(math.Pi) * T(i+1)
		}
	}
	return values
}

func BenchmarkAlpEncodingFloat32(b *testing.B) {
	for _, data := range alpBenchmarkData {
		b.Run(data.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float32](sz, data.exceptionRate)
					encoder := encoding.NewEncoder(parquet.Types.Float, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float32Encoder)
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

func BenchmarkAlpEncodingFloat64(b *testing.B) {
	for _, data := range alpBenchmarkData {
		b.Run(data.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float64](sz, data.exceptionRate)
					encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
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

func BenchmarkAlpDecodingFloat32(b *testing.B) {
	for _, data := range alpBenchmarkData {
		b.Run(data.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float32](sz, data.exceptionRate)
					output := make([]float32, sz)
					encoder := encoding.NewEncoder(parquet.Types.Float, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float32Encoder)
					encoder.Put(values)
					buf, _ := encoder.FlushValues()
					defer buf.Release()

					decoder := encoding.NewDecoder(parquet.Types.Float, parquet.Encodings.ALP,
						nil, memory.DefaultAllocator).(encoding.Float32Decoder)
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

func BenchmarkAlpDecodingFloat64(b *testing.B) {
	for _, data := range alpBenchmarkData {
		b.Run(data.name, func(b *testing.B) {
			for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
				b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
					values := alpBenchmarkValues[float64](sz, data.exceptionRate)
					output := make([]float64, sz)
					encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
						false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
					encoder.Put(values)
					buf, _ := encoder.FlushValues()
					defer buf.Release()

					decoder := encoding.NewDecoder(parquet.Types.Double, parquet.Encodings.ALP,
						nil, memory.DefaultAllocator).(encoding.Float64Decoder)
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

// A batched decode is what a reader actually does, since a batch is rarely a
// whole page and almost never a whole vector: the decoder has to hold the vector
// it is part way through between calls.
func BenchmarkAlpDecodingFloat64Batched(b *testing.B) {
	const batchSize = 137
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := alpBenchmarkValues[float64](sz, 10)
			output := make([]float64, batchSize)
			encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP,
				false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
			encoder.Put(values)
			buf, _ := encoder.FlushValues()
			defer buf.Release()

			decoder := encoding.NewDecoder(parquet.Types.Double, parquet.Encodings.ALP,
				nil, memory.DefaultAllocator).(encoding.Float64Decoder)
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
