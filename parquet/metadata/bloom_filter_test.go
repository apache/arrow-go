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

			bf := NewBloomFilterFromNDVAndFPP(tt.ndv, tt.fpp, tt.maxBytes, mem)
			defer runtime.GC()

			assert.EqualValues(t, tt.expectedBytes, bf.Size())
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
