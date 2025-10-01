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
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestSplitBlockFilter(t *testing.T) {
	const N = 1000
	const S = 3
	const P = 0.01

	bf := blockSplitBloomFilter{
		bitset32: make([]uint32, optimalNumBytes(N, P)),
	}

	p := rand.New(rand.NewPCG(S, S))
	for i := 0; i < N; i++ {
		bf.InsertHash(p.Uint64())
	}

	falsePositives := 0
	p = rand.New(rand.NewPCG(S, S))
	for i := 0; i < N; i++ {
		x := p.Uint64()

		if !bf.CheckHash(x) {
			t.Fatalf("bloom filter block does not contain value #%d that was inserted %d", i, x)
		}

		if bf.CheckHash(^x) {
			falsePositives++
		}
	}

	if r := (float64(falsePositives) / N); r > P {
		t.Fatalf("false positive rate is too high: %f", r)
	}
}

func testHash[T parquet.ColumnTypes](t assert.TestingT, h Hasher, vals []T) {
	results := GetHashes(h, vals)
	assert.Len(t, results, len(vals))
	for i, v := range vals {
		assert.Equal(t, GetHash(h, v), results[i])
	}

	var (
		nvalid     = int64(len(vals))
		validBits  = make([]byte, bitutil.BytesForBits(2*nvalid))
		spacedVals = make([]T, 2*nvalid)
	)

	for i, v := range vals {
		spacedVals[i*2] = v
		bitutil.SetBit(validBits, i*2)

	}

	results = GetSpacedHashes(h, nvalid, spacedVals, validBits, 0)
	assert.Len(t, results, len(vals))
	for i, v := range vals {
		assert.Equal(t, GetHash(h, v), results[i])
	}
}

func TestGetHashes(t *testing.T) {
	var (
		h      xxhasher
		valsBA = []parquet.ByteArray{
			[]byte("hello"),
			[]byte("world"),
		}

		valsFLBA = []parquet.FixedLenByteArray{
			[]byte("hello"),
			[]byte("world"),
		}

		valsI32 = []int32{42, 43}
	)

	assert.Len(t, GetSpacedHashes[int32](h, 0, nil, nil, 0), 0)

	testHash(t, h, valsBA)
	testHash(t, h, valsFLBA)
	testHash(t, h, valsI32)
}

func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		ndv           uint32
		fpp           float64
		maxBytes      int64
		expectedBytes int64
	}{
		{1, 0.09, 0, 0},
		// cap at maximumBloomFilterBytes
		{1024 * 1024 * 128, 0.9, maximumBloomFilterBytes + 1, maximumBloomFilterBytes},
		// round to power of 2
		{1024 * 1024, 0.01, maximumBloomFilterBytes, 1 << 21},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("ndv=%d,fpp=%0.3f", tt.ndv, tt.fpp), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			{
				bf := NewBloomFilterFromNDVAndFPP(tt.ndv, tt.fpp, tt.maxBytes, mem)
				assert.EqualValues(t, tt.expectedBytes, bf.Size())
				runtime.GC()
			}
			runtime.GC() // force GC to run and do the cleanup routines
		})
	}
}

func BenchmarkFilterInsert(b *testing.B) {
	bf := blockSplitBloomFilter{bitset32: make([]uint32, 8)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.InsertHash(uint64(i))
	}
	b.SetBytes(bytesPerFilterBlock)
}

func BenchmarkFilterCheck(b *testing.B) {
	bf := blockSplitBloomFilter{bitset32: make([]uint32, 8)}
	bf.InsertHash(42)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.CheckHash(42)
	}
	b.SetBytes(bytesPerFilterBlock)
}

func BenchmarkFilterCheckBulk(b *testing.B) {
	bf := blockSplitBloomFilter{bitset32: make([]uint32, 99*bitsSetPerBlock)}
	x := make([]uint64, 16)
	r := rand.New(rand.NewPCG(0, 0))
	for i := range x {
		x[i] = r.Uint64()
	}

	bf.InsertBulk(x)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.CheckBulk(x)
	}
	b.SetBytes(bytesPerFilterBlock * int64(len(x)))
}

func BenchmarkFilterInsertBulk(b *testing.B) {
	bf := blockSplitBloomFilter{bitset32: make([]uint32, 99*bitsSetPerBlock)}
	x := make([]uint64, 16)
	r := rand.New(rand.NewPCG(0, 0))
	for i := range x {
		x[i] = r.Uint64()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.InsertBulk(x)
	}
	b.SetBytes(bytesPerFilterBlock * int64(len(x)))
}

func TestAdaptiveBloomFilterEdgeCases(t *testing.T) {
	mem := memory.DefaultAllocator

	// Create a simple column for testing
	col := schema.NewColumn(schema.NewByteArrayNode("test", parquet.Repetitions.Required, -1), 1, 1)

	t.Run("InsertBulk handles duplicate hashes correctly", func(t *testing.T) {
		bf := NewAdaptiveBlockSplitBloomFilter(1024, 3, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		// Insert the same hash multiple times
		duplicateHash := uint64(12345)
		hashes := []uint64{duplicateHash, duplicateHash, duplicateHash}

		initialDistinct := bf.numDistinct
		bf.InsertBulk(hashes)

		// numDistinct should only increase by 1, not 3
		// Currently this will fail because the bug causes it to increment by 3
		expectedDistinct := initialDistinct + 1
		if bf.numDistinct != expectedDistinct {
			t.Errorf("InsertBulk duplicate handling bug: expected numDistinct=%d, got %d",
				expectedDistinct, bf.numDistinct)
		}
	})

	t.Run("candidate selection uses correct comparison logic", func(t *testing.T) {
		// Use larger maxBytes and more candidates to ensure multiple candidates are created
		bf := NewAdaptiveBlockSplitBloomFilter(8192, 5, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		if len(bf.candidates) < 2 {
			t.Skip("Need at least 2 candidates to test comparison functions")
		}

		// Test MinFunc - should return the candidate with smallest size
		optimal := bf.optimalCandidate()
		minSize := optimal.bloomFilter.Size()

		for _, c := range bf.candidates {
			if c.bloomFilter.Size() < minSize {
				t.Errorf("optimalCandidate() sign error: found candidate with smaller size %d < %d",
					c.bloomFilter.Size(), minSize)
			}
		}

		// Test MaxFunc - largestCandidate should have the largest size
		maxSize := bf.largestCandidate.bloomFilter.Size()
		for _, c := range bf.candidates {
			if c.bloomFilter.Size() > maxSize {
				t.Errorf("largestCandidate sign error: found candidate with larger size %d > %d",
					c.bloomFilter.Size(), maxSize)
			}
		}
	})

	t.Run("bloom filter data survives garbage collection", func(t *testing.T) {
		bf := NewAdaptiveBlockSplitBloomFilter(1024, 1, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		// Insert some data
		hashes := []uint64{1, 2, 3, 4, 5}
		bf.InsertBulk(hashes)

		// Force garbage collection to trigger finalizers
		runtime.GC()
		runtime.GC()

		// The bloom filter should still work after GC
		// If the GC issue exists, this might cause a panic or incorrect results
		for _, h := range hashes {
			if !bf.CheckHash(h) {
				t.Errorf("Hash %d not found after GC - potential GC safety issue", h)
			}
		}
	})
}

func TestAdaptiveBloomFilterEndToEnd(t *testing.T) {
	// This test simulates the full workflow: write parquet with adaptive bloom filters,
	// then read it back and verify bloom filter functionality

	mem := memory.DefaultAllocator

	// Create test data
	testValues := []parquet.ByteArray{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("elderberry"),
		[]byte("fig"),
		[]byte("grape"),
		[]byte("honeydew"),
	}

	// Values that should NOT be in the bloom filter
	absentValues := []parquet.ByteArray{
		[]byte("absent1"),
		[]byte("absent2"),
		[]byte("absent3"),
	}

	col := schema.NewColumn(schema.NewByteArrayNode("test_column", parquet.Repetitions.Required, -1), 1, 1)

	t.Run("create adaptive bloom filter with test data", func(t *testing.T) {
		// Create adaptive bloom filter as would be done during parquet writing
		bf := NewAdaptiveBlockSplitBloomFilter(1024, 3, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)
		hasher := bf.Hasher()

		// Insert test values (this simulates what happens during parquet writing)
		var hashes []uint64
		for _, val := range testValues {
			hashes = append(hashes, GetHash(hasher, val))
		}

		// Test bulk insertion with some duplicates to verify our fix
		duplicatedHashes := append(hashes, hashes[0], hashes[1], hashes[0]) // Add some duplicates
		bf.InsertBulk(duplicatedHashes)

		// Verify all original values are found
		for _, val := range testValues {
			hash := GetHash(hasher, val)
			if !bf.CheckHash(hash) {
				t.Errorf("Value %q (hash %d) not found in bloom filter", val, hash)
			}
		}

		// Verify absent values are (most likely) not found
		falsePositives := 0
		for _, val := range absentValues {
			hash := GetHash(hasher, val)
			if bf.CheckHash(hash) {
				falsePositives++
			}
		}

		// With a 1% false positive rate and 3 absent values, we expect 0-1 false positives most of the time
		if falsePositives > 1 {
			t.Logf("Note: Got %d false positives out of %d absent values (this can happen with bloom filters)",
				falsePositives, len(absentValues))
		}

		t.Logf("Bloom filter stats: size=%d bytes, numDistinct=%d, candidates=%d",
			bf.Size(), bf.numDistinct, len(bf.candidates))
	})

	t.Run("verify duplicate handling in bulk operations", func(t *testing.T) {
		bf := NewAdaptiveBlockSplitBloomFilter(1024, 3, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)
		hasher := bf.Hasher()

		// Create a slice with many duplicates
		testHash := GetHash(hasher, testValues[0])
		duplicateHashes := make([]uint64, 100)
		for i := range duplicateHashes {
			duplicateHashes[i] = testHash
		}

		initialDistinct := bf.numDistinct
		bf.InsertBulk(duplicateHashes)

		// Should only increment numDistinct by 1, not 100
		expectedDistinct := initialDistinct + 1
		if bf.numDistinct != expectedDistinct {
			t.Errorf("Duplicate handling failed: expected numDistinct=%d, got %d",
				expectedDistinct, bf.numDistinct)
		}

		// The value should still be findable
		if !bf.CheckHash(testHash) {
			t.Error("Hash not found after bulk insert with duplicates")
		}
	})

	t.Run("verify optimal candidate selection", func(t *testing.T) {
		// Create bloom filter with multiple candidates
		bf := NewAdaptiveBlockSplitBloomFilter(4096, 4, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		if len(bf.candidates) < 2 {
			t.Skip("Need multiple candidates to test selection logic")
		}

		// Insert some data to trigger candidate elimination
		hasher := bf.Hasher()
		for i, val := range testValues {
			hash := GetHash(hasher, val)
			bf.InsertHash(hash)

			// Check optimal candidate selection is working
			optimal := bf.optimalCandidate()
			largest := bf.largestCandidate

			if optimal.bloomFilter.Size() > largest.bloomFilter.Size() {
				t.Errorf("Iteration %d: optimal candidate size (%d) > largest candidate size (%d)",
					i, optimal.bloomFilter.Size(), largest.bloomFilter.Size())
			}
		}

		t.Logf("Final candidates: %d, optimal size: %d bytes",
			len(bf.candidates), bf.optimalCandidate().bloomFilter.Size())
	})
}
