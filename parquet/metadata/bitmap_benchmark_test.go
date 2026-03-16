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

package metadata

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// BenchmarkBooleanStatisticsFromBitmap benchmarks statistics update using direct bitmap operations
func BenchmarkBooleanStatisticsFromBitmap(b *testing.B) {
	const numValues = 100000

	bitmap := make([]byte, bitutil.BytesForBits(int64(numValues)))
	for i := 0; i < numValues; i++ {
		if i%2 == 0 {
			bitutil.SetBit(bitmap, i)
		}
	}

	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Required, -1), 0, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stats := NewBooleanStatistics(descr, memory.DefaultAllocator)
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)
	}
}

// BenchmarkBloomFilterHashingFromBitmap benchmarks bloom filter hashing using direct bitmap operations
func BenchmarkBloomFilterHashingFromBitmap(b *testing.B) {
	const numValues = 100000

	bitmap := make([]byte, bitutil.BytesForBits(int64(numValues)))
	for i := 0; i < numValues; i++ {
		if i%2 == 0 {
			bitutil.SetBit(bitmap, i)
		}
	}

	bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hashes := GetHashesFromBitmap(bloom.Hasher(), bitmap, 0, numValues)
		_ = hashes
	}
}

// BenchmarkBooleanWritePathBitmap benchmarks the complete write path with bitmap operations
func BenchmarkBooleanWritePathBitmap(b *testing.B) {
	const numValues = 100000

	bitmap := make([]byte, bitutil.BytesForBits(int64(numValues)))
	for i := 0; i < numValues; i++ {
		if i%2 == 0 {
			bitutil.SetBit(bitmap, i)
		}
	}

	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Required, -1), 0, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Statistics update
		stats := NewBooleanStatistics(descr, memory.DefaultAllocator)
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)

		// Bloom filter update
		bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
		hashes := GetHashesFromBitmap(bloom.Hasher(), bitmap, 0, numValues)
		bloom.InsertBulk(hashes)
	}
}
