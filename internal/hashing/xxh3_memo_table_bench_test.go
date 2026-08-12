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

package hashing_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/hashing"
	"github.com/stretchr/testify/assert"
)

var offsetSink int64

var offsetBenchmarkCases = []struct {
	name  string
	n     int
	empty bool
	null  bool
}{
	{name: "unique-1k", n: 1_000},
	{name: "unique-100k", n: 100_000},
	{name: "empty-and-null-1k", n: 1_000, empty: true, null: true},
	{name: "empty-and-null-100k", n: 100_000, empty: true, null: true},
}

func newOffsetBenchmarkTable(n int, empty, null bool) *hashing.BinaryMemoTable {
	table := hashing.NewBinaryMemoTable(n, n*16, array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary))
	if empty {
		_, _, _ = table.GetOrInsertBytes(nil)
	}
	for i := 0; i < n; i++ {
		_, _, _ = table.GetOrInsertBytes([]byte(fmt.Sprintf("value-%08d", i)))
	}
	if null {
		_, _ = table.GetOrInsertNull()
	}
	return table
}

func TestBinaryMemoTableCopyOffsets(t *testing.T) {
	table := newOffsetBenchmarkTable(4, true, true)
	defer table.Release()

	want32 := []int32{0, 0, 14, 28, 42, 56, 56}
	got32 := make([]int32, len(want32))
	table.CopyOffsets(got32)
	assert.Equal(t, want32, got32)

	got32 = make([]int32, len(want32)-2)
	table.CopyOffsetsSubset(2, got32)
	assert.Equal(t, []int32{0, 14, 28, 42, 42}, got32)

	want64 := []int64{0, 0, 14, 28, 42, 56, 56}
	got64 := make([]int64, len(want64))
	table.CopyLargeOffsets(got64)
	assert.Equal(t, want64, got64)

	got64 = make([]int64, len(want64)-2)
	table.CopyLargeOffsetsSubset(2, got64)
	assert.Equal(t, []int64{0, 14, 28, 42, 42}, got64)
}

func BenchmarkBinaryMemoTableCopyOffsets(b *testing.B) {
	benchmarkBinaryMemoTableCopyOffsets(b, false, false)
}

func BenchmarkBinaryMemoTableCopyOffsetsSubset(b *testing.B) {
	benchmarkBinaryMemoTableCopyOffsets(b, false, true)
}

func BenchmarkBinaryMemoTableCopyLargeOffsets(b *testing.B) {
	benchmarkBinaryMemoTableCopyOffsets(b, true, false)
}

func BenchmarkBinaryMemoTableCopyLargeOffsetsSubset(b *testing.B) {
	benchmarkBinaryMemoTableCopyOffsets(b, true, true)
}

func benchmarkBinaryMemoTableCopyOffsets(b *testing.B, large, subset bool) {
	for _, tt := range offsetBenchmarkCases {
		b.Run(tt.name, func(b *testing.B) {
			table := newOffsetBenchmarkTable(tt.n, tt.empty, tt.null)
			defer table.Release()
			start := 0
			if subset {
				start = table.Size() / 2
			}
			var (
				out32 []int32
				out64 []int64
			)
			if large {
				out64 = make([]int64, table.Size()-start+1)
			} else {
				out32 = make([]int32, table.Size()-start+1)
			}

			b.ReportAllocs()
			b.ResetTimer()
			if large {
				if subset {
					for i := 0; i < b.N; i++ {
						table.CopyLargeOffsetsSubset(start, out64)
						offsetSink += out64[len(out64)-1]
					}
				} else {
					for i := 0; i < b.N; i++ {
						table.CopyLargeOffsets(out64)
						offsetSink += out64[len(out64)-1]
					}
				}
			} else {
				if subset {
					for i := 0; i < b.N; i++ {
						table.CopyOffsetsSubset(start, out32)
						offsetSink += int64(out32[len(out32)-1])
					}
				} else {
					for i := 0; i < b.N; i++ {
						table.CopyOffsets(out32)
						offsetSink += int64(out32[len(out32)-1])
					}
				}
			}
		})
	}
}
