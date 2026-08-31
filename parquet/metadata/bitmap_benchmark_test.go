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
	"fmt"
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

// BenchmarkBloomFilterBooleanBitmap benchmarks dense and spaced boolean bloom filter paths
// with the built-in xxhash implementation.
func BenchmarkBloomFilterBooleanBitmap(b *testing.B) {
	for _, numValues := range []int{100_000, 1_000_000} {
		numValues := numValues
		for _, tc := range benchmarkBooleanBitmapCases(numValues) {
			tc := tc
			b.Run(fmt.Sprintf("hash/%d/%s", numValues, tc.name), func(b *testing.B) {
				bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = GetHashesFromBitmap(bloom.Hasher(), tc.bitmap, 0, int64(numValues))
				}
			})

			b.Run(fmt.Sprintf("insert/%d/%s", numValues, tc.name), func(b *testing.B) {
				bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					InsertHashesFromBitmap(bloom, tc.bitmap, 0, int64(numValues))
				}
			})
		}

		for _, tc := range benchmarkSpacedBooleanBitmapCases(numValues) {
			tc := tc
			b.Run(fmt.Sprintf("spaced-hash/%d/%s", numValues, tc.name), func(b *testing.B) {
				bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = GetSpacedHashesFromBitmap(bloom.Hasher(), tc.numValid, tc.bitmap, 0, int64(numValues), tc.validBits, 0)
				}
			})

			b.Run(fmt.Sprintf("spaced-insert/%d/%s", numValues, tc.name), func(b *testing.B) {
				bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					InsertSpacedHashesFromBitmap(bloom, tc.numValid, tc.bitmap, 0, int64(numValues), tc.validBits, 0)
				}
			})
		}
	}
}

type benchmarkBooleanBitmapCase struct {
	name      string
	bitmap    []byte
	validBits []byte
	numValid  int64
}

func benchmarkBooleanBitmapCases(numValues int) []benchmarkBooleanBitmapCase {
	patterns := []string{"all-false", "all-true", "alternating", "random"}
	cases := make([]benchmarkBooleanBitmapCase, 0, len(patterns))
	for _, pattern := range patterns {
		cases = append(cases, benchmarkBooleanBitmapCase{
			name:   pattern,
			bitmap: makeBenchmarkBooleanBitmap(numValues, pattern),
		})
	}
	return cases
}

func benchmarkSpacedBooleanBitmapCases(numValues int) []benchmarkBooleanBitmapCase {
	bitmap := makeBenchmarkBooleanBitmap(numValues, "alternating")
	cases := make([]benchmarkBooleanBitmapCase, 0, 2)
	for _, tc := range []struct {
		name      string
		nullEvery int
	}{
		{name: "10pct-null", nullEvery: 10},
		{name: "50pct-null", nullEvery: 2},
	} {
		validBits := make([]byte, bitutil.BytesForBits(int64(numValues)))
		var numValid int64
		for i := 0; i < numValues; i++ {
			if i%tc.nullEvery != 0 {
				bitutil.SetBit(validBits, i)
				numValid++
			}
		}
		cases = append(cases, benchmarkBooleanBitmapCase{
			name:      tc.name,
			bitmap:    bitmap,
			validBits: validBits,
			numValid:  numValid,
		})
	}
	return cases
}

func makeBenchmarkBooleanBitmap(numValues int, pattern string) []byte {
	bitmap := make([]byte, bitutil.BytesForBits(int64(numValues)))
	var state uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < numValues; i++ {
		set := false
		switch pattern {
		case "all-true":
			set = true
		case "alternating":
			set = i%2 == 0
		case "random":
			state ^= state << 7
			state ^= state >> 9
			set = state&1 != 0
		}
		if set {
			bitutil.SetBit(bitmap, i)
		}
	}
	return bitmap
}

// BenchmarkBloomFilterHashBatching compares the materialized and streaming
// paths used to update a bloom filter from a large fixed-width batch.
func BenchmarkBloomFilterHashBatching(b *testing.B) {
	const numValues = 100000

	values := make([]int32, numValues)
	for i := range values {
		values[i] = int32(i)
	}

	b.Run("materialized", func(b *testing.B) {
		bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
		b.SetBytes(int64(numValues * 4))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			bloom.InsertBulk(GetHashes(bloom.Hasher(), values))
		}
	})

	b.Run("bounded", func(b *testing.B) {
		bloom := NewBloomFilter(1024, 1024, memory.DefaultAllocator)
		b.SetBytes(int64(numValues * 4))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			InsertHashes(bloom, values)
		}
	})
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
		InsertHashesFromBitmap(bloom, bitmap, 0, numValues)
	}
}
