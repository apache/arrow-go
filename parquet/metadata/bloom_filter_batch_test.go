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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchRecordingBloomFilter struct {
	BloomFilterBuilder
	batches [][]uint64
}

func (b *batchRecordingBloomFilter) InsertBulk(hashes []uint64) {
	b.batches = append(b.batches, append([]uint64(nil), hashes...))
	b.BloomFilterBuilder.InsertBulk(hashes)
}

func newBatchRecordingBloomFilter(h Hasher) *batchRecordingBloomFilter {
	bloom := NewBloomFilter(minimumBloomFilterBytes, minimumBloomFilterBytes, memory.DefaultAllocator)
	bloom.(*blockSplitBloomFilter).hasher = h
	return &batchRecordingBloomFilter{BloomFilterBuilder: bloom}
}

func flattenHashBatches(batches [][]uint64) []uint64 {
	var hashes []uint64
	for _, batch := range batches {
		hashes = append(hashes, batch...)
	}
	return hashes
}

func assertHashBatchesAreBounded(t *testing.T, batches [][]uint64) {
	t.Helper()
	for _, batch := range batches {
		assert.NotEmpty(t, batch)
		assert.LessOrEqual(t, len(batch), bloomFilterHashBatchSize)
	}
}

func scalarHashes[T parquet.ColumnTypes](values []T) []uint64 {
	hashes := make([]uint64, len(values))
	for i, value := range values {
		hashes[i] = GetHash(xxhasher{}, value)
	}
	return hashes
}

func TestInsertHashesBatchesValues(t *testing.T) {
	const numValues = 2*bloomFilterHashBatchSize + 3

	t.Run("fixed width", func(t *testing.T) {
		values := make([]int32, numValues)
		for i := range values {
			values[i] = int32(i*17 - 4)
		}

		bloom := newBatchRecordingBloomFilter(xxhasher{})
		InsertHashes(bloom, values)

		assertHashBatchesAreBounded(t, bloom.batches)
		assert.Equal(t, scalarHashes(values), flattenHashBatches(bloom.batches))
		assert.Equal(t, scalarHashes(values), GetHashes(xxhasher{}, values))
		assert.Equal(t, []int{bloomFilterHashBatchSize, bloomFilterHashBatchSize, 3}, batchLengths(bloom.batches))
	})

	t.Run("byte array", func(t *testing.T) {
		values := make([]parquet.ByteArray, numValues)
		for i := range values {
			values[i] = parquet.ByteArray{byte(i), byte(i >> 8), byte(i >> 16)}
		}

		bloom := newBatchRecordingBloomFilter(xxhasher{})
		InsertHashes(bloom, values)

		assertHashBatchesAreBounded(t, bloom.batches)
		assert.Equal(t, scalarHashes(values), flattenHashBatches(bloom.batches))
		assert.Equal(t, scalarHashes(values), GetHashes(xxhasher{}, values))
	})
}

func TestInsertHashesSupportsHasherWithoutIntoMethod(t *testing.T) {
	const numValues = bloomFilterHashBatchSize + 1
	values := make([]int64, numValues)
	for i := range values {
		values[i] = int64(i) * 31
	}

	hasher := &recordingHasher{}
	bloom := newBatchRecordingBloomFilter(hasher)
	InsertHashes(bloom, values)

	assertHashBatchesAreBounded(t, bloom.batches)
	assert.Equal(t, scalarHashes(values), flattenHashBatches(bloom.batches))
	assert.Len(t, hasher.inputs, numValues)
}

func TestInsertHashesHandlesEmptyInput(t *testing.T) {
	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertHashes[int32](bloom, nil)
	assert.Empty(t, bloom.batches)
}

func TestInsertSpacedHashesBatchesValidValues(t *testing.T) {
	const (
		numValues   = 2*bloomFilterHashBatchSize + 29
		validOffset = int64(5)
	)

	values := make([]int32, numValues)
	validBits := make([]byte, bitutil.BytesForBits(validOffset+numValues))
	var numValid int64
	for i := range values {
		values[i] = int32(i*13 + 7)
		if i < bloomFilterHashBatchSize+37 || i%7 != 2 {
			bitutil.SetBit(validBits, int(validOffset)+i)
			numValid++
		}
	}

	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashes(bloom, numValid, values, validBits, validOffset)

	assertHashBatchesAreBounded(t, bloom.batches)
	assert.Equal(t, GetSpacedHashes(xxhasher{}, numValid, values, validBits, validOffset), flattenHashBatches(bloom.batches))

	empty := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashes[int32](empty, 0, nil, nil, 0)
	assert.Empty(t, empty.batches)

	allNull := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashes(allNull, 0, values, make([]byte, len(validBits)), validOffset)
	assert.Empty(t, allNull.batches)
}

func TestInsertHashesFromBitmapBatchesValues(t *testing.T) {
	const (
		numValues    = 2*bloomFilterHashBatchSize + 11
		bitmapOffset = int64(3)
	)

	bitmap := make([]byte, bitutil.BytesForBits(bitmapOffset+numValues))
	for i := 0; i < numValues; i++ {
		if i%3 == 0 {
			bitutil.SetBit(bitmap, int(bitmapOffset)+i)
		}
	}

	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertHashesFromBitmap(bloom, bitmap, bitmapOffset, numValues)

	assertHashBatchesAreBounded(t, bloom.batches)
	assert.Equal(t, GetHashesFromBitmap(xxhasher{}, bitmap, bitmapOffset, numValues), flattenHashBatches(bloom.batches))

	empty := newBatchRecordingBloomFilter(xxhasher{})
	InsertHashesFromBitmap(empty, nil, 0, 0)
	assert.Empty(t, empty.batches)
}

func TestBitmapBloomHashingPreservesCustomHasher(t *testing.T) {
	const (
		numValues    = bloomFilterHashBatchSize + 7
		bitmapOffset = int64(3)
		validOffset  = int64(5)
	)

	bitmap := make([]byte, bitutil.BytesForBits(bitmapOffset+numValues))
	validBits := make([]byte, bitutil.BytesForBits(validOffset+numValues))
	var numValid int64
	for i := 0; i < numValues; i++ {
		if i%3 == 0 {
			bitutil.SetBit(bitmap, int(bitmapOffset)+i)
		}
		if i%4 != 0 {
			bitutil.SetBit(validBits, int(validOffset)+i)
			numValid++
		}
	}

	expectedDense := GetHashesFromBitmap(xxhasher{}, bitmap, bitmapOffset, numValues)
	hasher := &recordingHasher{}
	assert.Equal(t, expectedDense, GetHashesFromBitmap(hasher, bitmap, bitmapOffset, numValues))
	assert.Len(t, hasher.inputs, numValues)

	expectedSpaced := GetSpacedHashesFromBitmap(xxhasher{}, numValid, bitmap, bitmapOffset, numValues, validBits, validOffset)
	hasher = &recordingHasher{}
	assert.Equal(t, expectedSpaced, GetSpacedHashesFromBitmap(hasher, numValid, bitmap, bitmapOffset, numValues, validBits, validOffset))
	assert.Len(t, hasher.inputs, int(numValid))

	hasher = &recordingHasher{}
	bloom := newBatchRecordingBloomFilter(hasher)
	InsertHashesFromBitmap(bloom, bitmap, bitmapOffset, numValues)
	assert.Equal(t, expectedDense, flattenHashBatches(bloom.batches))
	assert.Len(t, hasher.inputs, numValues)

	hasher = &recordingHasher{}
	bloom = newBatchRecordingBloomFilter(hasher)
	InsertSpacedHashesFromBitmap(bloom, numValid, bitmap, bitmapOffset, numValues, validBits, validOffset)
	assert.Equal(t, expectedSpaced, flattenHashBatches(bloom.batches))
	assert.Len(t, hasher.inputs, int(numValid))
}

func TestInsertSpacedHashesFromBitmapBatchesValidValues(t *testing.T) {
	const (
		numValues    = 2*bloomFilterHashBatchSize + 19
		bitmapOffset = int64(7)
		validOffset  = int64(2)
	)

	bitmap := make([]byte, bitutil.BytesForBits(bitmapOffset+numValues))
	validBits := make([]byte, bitutil.BytesForBits(validOffset+numValues))
	var numValid int64
	for i := 0; i < numValues; i++ {
		if i%2 == 0 {
			bitutil.SetBit(bitmap, int(bitmapOffset)+i)
		}
		if i < bloomFilterHashBatchSize+23 || i%5 != 1 {
			bitutil.SetBit(validBits, int(validOffset)+i)
			numValid++
		}
	}

	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashesFromBitmap(bloom, numValid, bitmap, bitmapOffset, numValues, validBits, validOffset)

	assertHashBatchesAreBounded(t, bloom.batches)
	assert.Equal(t, GetSpacedHashesFromBitmap(xxhasher{}, numValid, bitmap, bitmapOffset, numValues, validBits, validOffset), flattenHashBatches(bloom.batches))

	empty := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashesFromBitmap(empty, 0, nil, 0, 0, nil, 0)
	assert.Empty(t, empty.batches)

	allNull := newBatchRecordingBloomFilter(xxhasher{})
	InsertSpacedHashesFromBitmap(allNull, 0, bitmap, bitmapOffset, numValues, make([]byte, len(validBits)), validOffset)
	assert.Empty(t, allNull.batches)
}

func batchLengths(batches [][]uint64) []int {
	lengths := make([]int, len(batches))
	for i, batch := range batches {
		lengths[i] = len(batch)
	}
	return lengths
}

func TestInsertHashesUsesAllValuesWhenBatchSizeIsExact(t *testing.T) {
	values := make([]parquet.FixedLenByteArray, bloomFilterHashBatchSize)
	for i := range values {
		values[i] = parquet.FixedLenByteArray{byte(i), byte(i >> 8)}
	}

	bloom := newBatchRecordingBloomFilter(xxhasher{})
	InsertHashes(bloom, values)

	require.Len(t, bloom.batches, 1)
	assert.Equal(t, scalarHashes(values), bloom.batches[0])
}
