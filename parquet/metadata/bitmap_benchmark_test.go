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
