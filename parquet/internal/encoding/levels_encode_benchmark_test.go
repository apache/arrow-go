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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	parquetutils "github.com/apache/arrow-go/v18/parquet/internal/utils"
)

func BenchmarkLevelEncoder(b *testing.B) {
	patterns := []struct {
		name string
		fill func([]int16, int16)
	}{
		{"all_defined", func(levels []int16, maxLevel int16) {
			for i := range levels {
				levels[i] = maxLevel
			}
		}},
		{"mostly_defined", func(levels []int16, maxLevel int16) {
			for i := range levels {
				if i%20 != 0 {
					levels[i] = maxLevel
				}
			}
		}},
		{"alternating", func(levels []int16, maxLevel int16) {
			for i := range levels {
				if i%2 != 0 {
					levels[i] = maxLevel
				}
			}
		}},
	}

	for _, size := range []int{1024, 64 * 1024} {
		for _, maxLevel := range []int16{1, 3} {
			for _, pattern := range patterns {
				b.Run(fmt.Sprintf("%s/max_level=%d/levels=%d", pattern.name, maxLevel, size), func(b *testing.B) {
					levels := make([]int16, size)
					pattern.fill(levels, maxLevel)
					output := encoding.NewBufferWriter(encoding.LevelEncodingMaxBufferSize(parquet.Encodings.RLE, maxLevel, size), memory.DefaultAllocator)
					defer output.Release()

					var encoder encoding.LevelEncoder
					encoder.Init(parquet.Encodings.RLE, maxLevel, output)
					b.ReportAllocs()
					b.SetBytes(int64(len(levels) * arrow.Int16SizeBytes))
					b.ResetTimer()
					for b.Loop() {
						encoder.Reset(maxLevel)
						encoded, err := encoder.Encode(levels)
						if err != nil {
							b.Fatal(err)
						}
						if encoded != size {
							b.Fatalf("encoded %d levels, want %d", encoded, size)
						}
					}
				})
			}
		}
	}
}

func BenchmarkRleLevelEncoder(b *testing.B) {
	patterns := []struct {
		name string
		fill func([]int16)
	}{
		{"all_defined", func(levels []int16) {
			for i := range levels {
				levels[i] = 1
			}
		}},
		{"mostly_defined", func(levels []int16) {
			for i := range levels {
				if i%20 != 0 {
					levels[i] = 1
				}
			}
		}},
		{"alternating", func(levels []int16) {
			for i := range levels {
				levels[i] = int16(i % 2)
			}
		}},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			levels := make([]int16, 64*1024)
			pattern.fill(levels)
			for _, mode := range []string{"scalar", "batch"} {
				b.Run(mode, func(b *testing.B) {
					output := make([]byte, parquetutils.MaxRLEBufferSize(1, len(levels)))
					encoder := parquetutils.NewRleEncoder(parquetutils.NewWriterAtBuffer(output), 1)

					b.ReportAllocs()
					b.SetBytes(int64(len(levels) * arrow.Int16SizeBytes))
					for b.Loop() {
						encoder.Clear()
						if mode == "scalar" {
							for _, level := range levels {
								if err := encoder.Put(uint64(level)); err != nil {
									b.Fatal(err)
								}
							}
						} else if _, err := encoder.PutBatchLevels(levels); err != nil {
							b.Fatal(err)
						}
						encoder.Flush()
					}
				})
			}
		})
	}
}
