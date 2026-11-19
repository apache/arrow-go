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

//go:build go1.18

package exec_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
)

func makeResolverChunks(lengths []int) []arrow.Array {
	chunks := make([]arrow.Array, len(lengths))
	for i, n := range lengths {
		chunks[i] = array.NewNull(n)
	}
	return chunks
}

func releaseResolverChunks(chunks []arrow.Array) {
	for _, chunk := range chunks {
		chunk.Release()
	}
}

func TestChunkResolverAdjacentAndEmptyChunks(t *testing.T) {
	tests := []struct {
		name    string
		lengths []int
	}{
		{"single", []int{7}},
		{"short", []int{1, 1, 1, 1, 1, 1, 1, 1}},
		{"mixed", []int{1, 4, 8, 2, 3}},
		{"empty", []int{0, 2, 0, 0, 1, 4, 0, 0, 3, 0}},
		{"sparse", []int{0, 0, 1, 0, 0, 1, 0, 0, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunks := makeResolverChunks(tc.lengths)
			defer releaseResolverChunks(chunks)
			resolver := exec.NewChunkResolver(chunks)

			var expectedChunk, expectedOffset []int64
			for i, n := range tc.lengths {
				for j := 0; j < n; j++ {
					expectedChunk = append(expectedChunk, int64(i))
					expectedOffset = append(expectedOffset, int64(j))
				}
			}

			n := len(expectedChunk)
			forward := make([]int64, n)
			reverse := make([]int64, n)
			zigzag := make([]int64, 0, 2*n)
			for i := 0; i < n; i++ {
				forward[i] = int64(i)
				reverse[i] = int64(n - 1 - i)
				zigzag = append(zigzag, int64(i), int64(n-1-i))
			}

			shuffled := make([]int64, n)
			for i, idx := range rand.New(rand.NewSource(42)).Perm(n) {
				shuffled[i] = int64(idx)
			}
			orders := []struct {
				name    string
				indices []int64
			}{
				{"forward", forward},
				{"reverse", reverse},
				{"zigzag", zigzag},
				{"shuffled", shuffled},
			}
			for _, order := range orders {
				t.Run(order.name, func(t *testing.T) {
					for _, idx := range order.indices {
						gotChunk, gotOffset := resolver.Resolve(idx)
						if gotChunk != expectedChunk[idx] || gotOffset != expectedOffset[idx] {
							t.Fatalf("Resolve(%d) = (%d, %d), want (%d, %d)",
								idx, gotChunk, gotOffset, expectedChunk[idx], expectedOffset[idx])
						}
					}
				})
			}
		})
	}
}

func TestChunkResolverConcurrent(t *testing.T) {
	lengths := []int{0, 3, 0, 17, 1, 0, 8, 2, 0, 19, 0}
	chunks := makeResolverChunks(lengths)
	defer releaseResolverChunks(chunks)
	resolver := exec.NewChunkResolver(chunks)

	var wantChunks, wantOffsets []int64
	for i, n := range lengths {
		for j := 0; j < n; j++ {
			wantChunks = append(wantChunks, int64(i))
			wantOffsets = append(wantOffsets, int64(j))
		}
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				seed = seed*6364136223846793005 + 1442695040888963407
				idx := int64(seed % uint64(len(wantChunks)))
				gotChunk, gotOffset := resolver.Resolve(idx)
				if gotChunk != wantChunks[idx] || gotOffset != wantOffsets[idx] {
					t.Errorf("Resolve(%d) = (%d, %d), want (%d, %d)",
						idx, gotChunk, gotOffset, wantChunks[idx], wantOffsets[idx])
					return
				}
			}
		}(uint64(worker + 1))
	}
	wg.Wait()
}

var chunkResolverBenchmarkSink int64

func BenchmarkChunkResolver(b *testing.B) {
	const (
		chunkCount = 8192
		indexCount = 16384
	)
	for _, rowsPerChunk := range []int{1, 64, 4096} {
		b.Run(fmt.Sprintf("rowsPerChunk=%d", rowsPerChunk), func(b *testing.B) {
			lengths := make([]int, chunkCount)
			for i := range lengths {
				lengths[i] = rowsPerChunk
			}
			chunks := makeResolverChunks(lengths)
			defer releaseResolverChunks(chunks)
			total := int64(chunkCount * rowsPerChunk)

			forward := make([]int64, indexCount)
			reverse := make([]int64, indexCount)
			random := make([]int64, indexCount)
			rng := rand.New(rand.NewSource(42))
			for i := range forward {
				forward[i] = int64(i) % total
				reverse[i] = total - 1 - int64(i)%total
				random[i] = rng.Int63n(total)
			}

			for _, pattern := range []struct {
				name    string
				indices []int64
			}{
				{"forward", forward},
				{"reverse", reverse},
				{"random", random},
			} {
				b.Run(pattern.name, func(b *testing.B) {
					resolver := exec.NewChunkResolver(chunks)
					var checksum int64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						chunk, offset := resolver.Resolve(pattern.indices[i&(indexCount-1)])
						checksum += chunk + offset
					}
					chunkResolverBenchmarkSink = checksum
				})
			}
		})
	}
}
