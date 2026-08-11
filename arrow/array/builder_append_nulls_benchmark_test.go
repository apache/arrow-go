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

func BenchmarkFixedWidthBuilderAppendNulls(b *testing.B) {
	builders := []struct {
		name string
		new  func(memory.Allocator) array.Builder
	}{
		{"int32", func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) }},
		{"int64", func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) }},
		{"decimal128", func(mem memory.Allocator) array.Builder {
			return array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 38})
		}},
		{"timestamp", func(mem memory.Allocator) array.Builder {
			return array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
		}},
		{"boolean", func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) }},
		{"fixed-size-binary", func(mem memory.Allocator) array.Builder {
			return array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 16})
		}},
	}

	for _, builder := range builders {
		b.Run(builder.name, func(b *testing.B) {
			for _, count := range []int{1, 8, 64, 1024, 65536} {
				b.Run(fmt.Sprintf("count-%d", count), func(b *testing.B) {
					mem := memory.NewGoAllocator()
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						instance := builder.new(mem)
						instance.AppendNulls(count)
						instance.Release()
					}
				})
			}
		})
	}
}
