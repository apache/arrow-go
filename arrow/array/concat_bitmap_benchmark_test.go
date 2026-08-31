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

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkConcatenateBitmaps(b *testing.B) {
	const totalValues = 1 << 16

	buildBoolean := func(mem memory.Allocator) arrow.Array {
		values := make([]bool, totalValues)
		valid := make([]bool, totalValues)
		for i := range values {
			values[i] = i%3 == 0
			valid[i] = i%7 != 0
		}

		bldr := array.NewBooleanBuilder(mem)
		bldr.AppendValues(values, valid)
		result := bldr.NewBooleanArray()
		bldr.Release()
		return result
	}

	buildInt32 := func(mem memory.Allocator) arrow.Array {
		values := make([]int32, totalValues)
		valid := make([]bool, totalValues)
		for i := range values {
			values[i] = int32(i)
			valid[i] = i%7 != 0
		}

		bldr := array.NewInt32Builder(mem)
		bldr.AppendValues(values, valid)
		result := bldr.NewInt32Array()
		bldr.Release()
		return result
	}

	inputs := []struct {
		name  string
		build func(memory.Allocator) arrow.Array
	}{
		{"boolean-nullable", buildBoolean},
		{"int32-nullable", buildInt32},
	}

	for _, input := range inputs {
		input := input
		b.Run(input.name, func(b *testing.B) {
			mem := memory.NewGoAllocator()
			backing := input.build(mem)
			defer backing.Release()

			for _, chunkCount := range []int{64, 1024, 8192} {
				chunkCount := chunkCount
				b.Run(fmt.Sprintf("chunks-%d", chunkCount), func(b *testing.B) {
					chunkSize := totalValues / chunkCount
					chunks := make([]arrow.Array, chunkCount)
					for i := range chunks {
						begin := int64(i * chunkSize)
						chunks[i] = array.NewSlice(backing, begin, begin+int64(chunkSize))
					}
					defer func() {
						for _, chunk := range chunks {
							chunk.Release()
						}
					}()

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						result, err := array.Concatenate(chunks, mem)
						if err != nil {
							b.Fatal(err)
						}
						if result.Len() != totalValues {
							b.Fatalf("result length = %d, want %d", result.Len(), totalValues)
						}
						result.Release()
					}
				})
			}
		})
	}
}
