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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
)

func benchmarkLevelData(b *testing.B, levels []int16, maxLevel int16) []byte {
	buf := encoding.NewBufferWriter(2*len(levels), memory.DefaultAllocator)
	defer buf.Release()

	buf.SetOffset(arrow.Int32SizeBytes)
	var encoder encoding.LevelEncoder
	encoder.Init(parquet.Encodings.RLE, maxLevel, buf)
	encoded, err := encoder.Encode(levels)
	if err != nil {
		b.Fatal(err)
	}
	if encoded != len(levels) {
		b.Fatalf("encoded %d levels, want %d", encoded, len(levels))
	}

	buf.SetOffset(0)
	binary.LittleEndian.PutUint32(buf.Bytes(), uint32(encoder.Len()))
	return append([]byte(nil), buf.Bytes()...)
}

func BenchmarkLevelDecoder(b *testing.B) {
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
					data := benchmarkLevelData(b, levels, maxLevel)
					output := make([]int16, size)

					b.ReportAllocs()
					b.SetBytes(int64(len(output) * arrow.Int16SizeBytes))
					b.ResetTimer()
					for b.Loop() {
						var decoder encoding.LevelDecoder
						if _, err := decoder.SetData(parquet.Encodings.RLE, maxLevel, size, data); err != nil {
							b.Fatal(err)
						}
						decoded, _, err := decoder.Decode(output)
						if err != nil {
							b.Fatal(err)
						}
						if decoded != size {
							b.Fatalf("decoded %d levels, want %d", decoded, size)
						}
					}
				})
			}
		}
	}
}
