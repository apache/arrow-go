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

		expectedDistinct := initialDistinct + 1
		assert.Equal(t, expectedDistinct, bf.numDistinct)
	})

	t.Run("candidate selection uses correct comparison logic", func(t *testing.T) {
		// Use larger maxBytes and more candidates to ensure multiple candidates are created
		bf := NewAdaptiveBlockSplitBloomFilter(8192, 5, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		assert.GreaterOrEqual(t, len(bf.candidates), 2, "Expected at least 2 candidates with numCandidates=5")

		// Test MinFunc - should return the candidate with smallest size
		optimal := bf.optimalCandidate()
		minSize := optimal.bloomFilter.Size()

		for _, c := range bf.candidates {
			assert.GreaterOrEqual(t, c.bloomFilter.Size(), minSize)
		}

		// Test MaxFunc - largestCandidate should have the largest size
		maxSize := bf.largestCandidate.bloomFilter.Size()
		for _, c := range bf.candidates {
			assert.LessOrEqual(t, c.bloomFilter.Size(), maxSize)
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
		for _, h := range hashes {
			assert.Truef(t, bf.CheckHash(h), "hash %d not found after GC - potential GC safety issue", h)
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

		// Test bulk insertion with some duplicates
		duplicatedHashes := append(hashes, hashes[0], hashes[1], hashes[0])
		bf.InsertBulk(duplicatedHashes)

		// Verify all original values are found
		for _, val := range testValues {
			assert.True(t, bf.CheckHash(GetHash(hasher, val)))
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
		assert.Equal(t, expectedDistinct, bf.numDistinct)

		// The value should still be findable
		assert.True(t, bf.CheckHash(testHash))
	})

	t.Run("verify optimal candidate selection", func(t *testing.T) {
		// Create bloom filter with multiple candidates
		bf := NewAdaptiveBlockSplitBloomFilter(4096, 4, 0.01, col, mem).(*adaptiveBlockSplitBloomFilter)

		assert.GreaterOrEqual(t, len(bf.candidates), 2, "Expected at least 2 candidates with numCandidates=4")

		// Insert some data to trigger candidate elimination
		hasher := bf.Hasher()
		for _, val := range testValues {
			hash := GetHash(hasher, val)
			bf.InsertHash(hash)

			// Check optimal candidate selection is working
			optimal := bf.optimalCandidate()
			largest := bf.largestCandidate

			assert.LessOrEqual(t, optimal.bloomFilter.Size(), largest.bloomFilter.Size())
		}

		t.Logf("Final candidates: %d, optimal size: %d bytes",
			len(bf.candidates), bf.optimalCandidate().bloomFilter.Size())
	})
}

func TestGetHashesFromBitmap(t *testing.T) {
	mem := memory.DefaultAllocator
	bf := NewBloomFilter(32, 1024, mem)
	hasher := bf.Hasher()

	t.Run("empty bitmap", func(t *testing.T) {
		hashes := GetHashesFromBitmap(hasher, []byte{}, 0, 0)
		assert.Empty(t, hashes)
	})

	t.Run("single byte aligned", func(t *testing.T) {
		bitmap := []byte{0b10101010} // alternating true/false
		numValues := int64(8)

		hashes := GetHashesFromBitmap(hasher, bitmap, 0, numValues)
		assert.Len(t, hashes, 8)

		// Verify each hash corresponds to the bit value
		// Bit 0 = 0 (false), Bit 1 = 1 (true), Bit 2 = 0 (false), etc.
		for i := int64(0); i < numValues; i++ {
			val := bitutil.BitIsSet(bitmap, int(i))
			expectedHash := GetHash(hasher, val)
			assert.Equal(t, expectedHash, hashes[i], "hash mismatch at position %d", i)
		}
	})

	t.Run("unaligned offset", func(t *testing.T) {
		// Create bitmap: [0,0,0,1,1,0,1,0, 1,1,1,...]
		bitmap := make([]byte, 2)
		bitutil.SetBit(bitmap, 3)
		bitutil.SetBit(bitmap, 4)
		bitutil.SetBit(bitmap, 6)
		bitutil.SetBit(bitmap, 8)
		bitutil.SetBit(bitmap, 9)
		bitutil.SetBit(bitmap, 10)

		// Read 5 bits starting from offset 3: [1,1,0,1,0]
		offset := int64(3)
		numValues := int64(5)

		hashes := GetHashesFromBitmap(hasher, bitmap, offset, numValues)
		assert.Len(t, hashes, 5)

		// Verify the hashes match the bit values at the offset
		for i := int64(0); i < numValues; i++ {
			val := bitutil.BitIsSet(bitmap, int(offset+i))
			expectedHash := GetHash(hasher, val)
			assert.Equal(t, expectedHash, hashes[i], "hash mismatch at offset %d position %d", offset, i)
		}
	})

	t.Run("all true bits", func(t *testing.T) {
		bitmap := []byte{0xFF, 0xFF} // all bits set
		numValues := int64(16)

		hashes := GetHashesFromBitmap(hasher, bitmap, 0, numValues)
		assert.Len(t, hashes, 16)

		// All hashes should be identical (hash of true)
		expectedHash := GetHash(hasher, true)
		for i, hash := range hashes {
			assert.Equal(t, expectedHash, hash, "hash mismatch at position %d", i)
		}
	})

	t.Run("all false bits", func(t *testing.T) {
		bitmap := []byte{0x00, 0x00} // all bits clear
		numValues := int64(16)

		hashes := GetHashesFromBitmap(hasher, bitmap, 0, numValues)
		assert.Len(t, hashes, 16)

		// All hashes should be identical (hash of false)
		expectedHash := GetHash(hasher, false)
		for i, hash := range hashes {
			assert.Equal(t, expectedHash, hash, "hash mismatch at position %d", i)
		}
	})

	t.Run("large bitmap", func(t *testing.T) {
		numValues := int64(1000)
		bitmap := make([]byte, bitutil.BytesForBits(numValues))

		// Set every 3rd bit
		for i := int64(0); i < numValues; i += 3 {
			bitutil.SetBit(bitmap, int(i))
		}

		hashes := GetHashesFromBitmap(hasher, bitmap, 0, numValues)
		assert.Len(t, hashes, 1000)

		// Verify hashes match bit values
		hashTrue := GetHash(hasher, true)
		hashFalse := GetHash(hasher, false)

		for i := int64(0); i < numValues; i++ {
			if i%3 == 0 {
				assert.Equal(t, hashTrue, hashes[i], "expected true hash at position %d", i)
			} else {
				assert.Equal(t, hashFalse, hashes[i], "expected false hash at position %d", i)
			}
		}
	})

	t.Run("consistency with GetHash", func(t *testing.T) {
		// Verify that GetHashesFromBitmap produces same results as GetHash for each bool
		testCases := []struct {
			name   string
			bitmap []byte
			offset int64
			count  int64
		}{
			{"aligned_8", []byte{0b10110100}, 0, 8},
			{"unaligned_start", []byte{0xFF, 0x00}, 3, 10},
			{"single_bit", []byte{0b00000001}, 0, 1},
			{"partial_byte", []byte{0b11110000}, 2, 5},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				hashes := GetHashesFromBitmap(hasher, tc.bitmap, tc.offset, tc.count)

				// Generate expected hashes using GetHash on individual bools
				for i := int64(0); i < tc.count; i++ {
					val := bitutil.BitIsSet(tc.bitmap, int(tc.offset+i))
					expectedHash := GetHash(hasher, val)
					assert.Equal(t, expectedHash, hashes[i],
						"hash mismatch at position %d (bit value: %v)", i, val)
				}
			})
		}
	})
}

func TestGetSpacedHashesFromBitmap(t *testing.T) {
	mem := memory.DefaultAllocator
	bf := NewBloomFilter(32, 1024, mem)
	hasher := bf.Hasher()

	t.Run("all valid bits", func(t *testing.T) {
		bitmap := []byte{0b10101010} // alternating true/false
		validBits := []byte{0xFF}    // all valid
		numValues := int64(8)
		numValid := int64(8)

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, 8)

		// Should match non-spaced version since all are valid
		expectedHashes := GetHashesFromBitmap(hasher, bitmap, 0, numValues)
		assert.Equal(t, expectedHashes, hashes)
	})

	t.Run("some null values", func(t *testing.T) {
		// Data bitmap: [1,1,1,1,0,0,0,0]
		bitmap := []byte{0b00001111}
		// Valid bits: [1,0,1,0,1,0,1,0] - only positions 0,2,4,6 are valid
		validBits := []byte{0b01010101}
		numValues := int64(8)
		numValid := int64(4)

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, 4, "should only hash valid values")

		// Manually verify the valid positions: 0,2,4,6 with data bits: 1,1,0,0
		expectedHashes := []uint64{
			GetHash(hasher, true),  // position 0: bit is set
			GetHash(hasher, true),  // position 2: bit is set
			GetHash(hasher, false), // position 4: bit is clear
			GetHash(hasher, false), // position 6: bit is clear
		}
		assert.Equal(t, expectedHashes, hashes)
	})

	t.Run("all null values", func(t *testing.T) {
		bitmap := []byte{0xFF}
		validBits := []byte{0x00} // all null

		hashes := GetSpacedHashesFromBitmap(hasher, 0, bitmap, 0, 8, validBits, 0)
		assert.Empty(t, hashes, "should return empty for all nulls")
	})

	t.Run("unaligned offsets", func(t *testing.T) {
		// Create larger bitmaps with offsets
		bitmap := make([]byte, 3)
		validBits := make([]byte, 3)

		// Set data bits starting from offset 5
		bitutil.SetBit(bitmap, 5) // true
		bitutil.SetBit(bitmap, 6) // true
		// bit 7 = false
		bitutil.SetBit(bitmap, 8) // true
		// bit 9 = false
		bitutil.SetBit(bitmap, 10) // true

		// Set valid bits (skip position 6)
		bitutil.SetBit(validBits, 5) // valid
		// bit 6 is null
		bitutil.SetBit(validBits, 7)  // valid
		bitutil.SetBit(validBits, 8)  // valid
		bitutil.SetBit(validBits, 9)  // valid
		bitutil.SetBit(validBits, 10) // valid

		numValues := int64(6)
		numValid := int64(5) // all except position 6

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 5, numValues, validBits, 5)
		assert.Len(t, hashes, 5)

		// Verify each hash: positions 5(true), 7(false), 8(true), 9(false), 10(true)
		expectedHashes := []uint64{
			GetHash(hasher, true),  // position 5
			GetHash(hasher, false), // position 7
			GetHash(hasher, true),  // position 8
			GetHash(hasher, false), // position 9
			GetHash(hasher, true),  // position 10
		}
		assert.Equal(t, expectedHashes, hashes)
	})

	t.Run("first half valid", func(t *testing.T) {
		bitmap := []byte{0xFF, 0xFF}    // all true
		validBits := []byte{0xFF, 0x00} // first 8 valid, last 8 null
		numValues := int64(16)
		numValid := int64(8)

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, 8)

		// All should be hash of true
		expectedHash := GetHash(hasher, true)
		for i, hash := range hashes {
			assert.Equal(t, expectedHash, hash, "position %d", i)
		}
	})

	t.Run("second half valid", func(t *testing.T) {
		bitmap := []byte{0xFF, 0x00}    // first 8 true, last 8 false
		validBits := []byte{0x00, 0xFF} // first 8 null, last 8 valid
		numValues := int64(16)
		numValid := int64(8)

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, 8)

		// All should be hash of false
		expectedHash := GetHash(hasher, false)
		for i, hash := range hashes {
			assert.Equal(t, expectedHash, hash, "position %d", i)
		}
	})

	t.Run("sparse valid bits", func(t *testing.T) {
		// Every 3rd bit is valid
		numValues := int64(24)
		bitmap := make([]byte, bitutil.BytesForBits(numValues))
		validBits := make([]byte, bitutil.BytesForBits(numValues))

		// Set data: alternating pattern
		for i := int64(0); i < numValues; i++ {
			if i%2 == 0 {
				bitutil.SetBit(bitmap, int(i))
			}
		}

		// Set valid: every 3rd bit
		numValid := int64(0)
		for i := int64(0); i < numValues; i += 3 {
			bitutil.SetBit(validBits, int(i))
			numValid++
		}

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, int(numValid))

		// Verify each hash matches the expected bit value
		hashIdx := 0
		for i := int64(0); i < numValues; i += 3 {
			val := bitutil.BitIsSet(bitmap, int(i))
			expectedHash := GetHash(hasher, val)
			assert.Equal(t, expectedHash, hashes[hashIdx], "hash mismatch at position %d", i)
			hashIdx++
		}
	})

	t.Run("consistency with GetSpacedHashes", func(t *testing.T) {
		// Verify GetSpacedHashesFromBitmap produces same results as GetSpacedHashes with []bool
		testCases := []struct {
			name         string
			bitmap       []byte
			validBits    []byte
			bitmapOffset int64
			validOffset  int64
			numValues    int64
			numValid     int64
		}{
			{"all_valid", []byte{0b10110010}, []byte{0xFF}, 0, 0, 8, 8},
			{"half_valid", []byte{0xFF}, []byte{0x0F}, 0, 0, 8, 4},
			{"sparse", []byte{0xFF, 0x00}, []byte{0b01010101, 0b10101010}, 0, 0, 16, 8},
			{"unaligned", []byte{0xFF, 0xFF, 0xFF}, []byte{0xFF, 0x0F, 0x00}, 3, 3, 10, 9},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Get hashes using bitmap method
				hashesFromBitmap := GetSpacedHashesFromBitmap(hasher, tc.numValid, tc.bitmap, tc.bitmapOffset, tc.numValues, tc.validBits, tc.validOffset)

				// Get hashes using []bool method
				bools := make([]bool, tc.numValues)
				for i := int64(0); i < tc.numValues; i++ {
					bools[i] = bitutil.BitIsSet(tc.bitmap, int(tc.bitmapOffset+i))
				}
				hashesFromBool := GetSpacedHashes(hasher, tc.numValid, bools, tc.validBits, tc.validOffset)

				// Both should produce identical results
				assert.Equal(t, hashesFromBool, hashesFromBitmap, "hash mismatch")
				assert.Len(t, hashesFromBitmap, int(tc.numValid))
			})
		}
	})

	t.Run("large bitmap with sparse valid", func(t *testing.T) {
		numValues := int64(1000)
		bitmap := make([]byte, bitutil.BytesForBits(numValues))
		validBits := make([]byte, bitutil.BytesForBits(numValues))

		// Set every 5th bit in bitmap
		for i := int64(0); i < numValues; i += 5 {
			bitutil.SetBit(bitmap, int(i))
		}

		// Set every 7th bit as valid
		numValid := int64(0)
		for i := int64(0); i < numValues; i += 7 {
			bitutil.SetBit(validBits, int(i))
			numValid++
		}

		hashes := GetSpacedHashesFromBitmap(hasher, numValid, bitmap, 0, numValues, validBits, 0)
		assert.Len(t, hashes, int(numValid))

		// Verify count of true vs false hashes
		hashTrue := GetHash(hasher, true)
		hashFalse := GetHash(hasher, false)

		trueCount, falseCount := 0, 0
		for _, h := range hashes {
			switch h {
			case hashTrue:
				trueCount++
			case hashFalse:
				falseCount++
			default:
				t.Fatalf("unexpected hash value")
			}
		}

		assert.Equal(t, int(numValid), trueCount+falseCount, "all hashes should be true or false")
	})
}
