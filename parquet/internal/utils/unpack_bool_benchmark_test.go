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

package utils_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
)

func TestBytesToBoolsCorrectness(t *testing.T) {
	rng := rand.New(rand.NewPCG(12345, 12345))

	for _, nBytes := range []int{1, 2, 3, 7, 8, 15, 16, 31, 32, 63, 64, 100, 256, 1024} {
		t.Run(fmt.Sprintf("bytes=%d", nBytes), func(t *testing.T) {
			in := make([]byte, nBytes)
			for i := range in {
				in[i] = byte(rng.IntN(256))
			}

			outlen := nBytes * 8
			got := make([]bool, outlen)
			want := make([]bool, outlen)

			for i, b := range in {
				for j := 0; j < 8; j++ {
					want[8*i+j] = (b & (1 << j)) != 0
				}
			}

			utils.BytesToBools(in, got)

			for i := 0; i < outlen; i++ {
				if got[i] != want[i] {
					byteIdx := i / 8
					bitIdx := i % 8
					t.Fatalf("mismatch at index %d (byte %d, bit %d): got %v, want %v (input byte = 0x%02x)",
						i, byteIdx, bitIdx, got[i], want[i], in[byteIdx])
				}
			}
		})
	}
}

func BenchmarkBytesToBools(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096, 16384} {
		in := make([]byte, size)
		for i := range in {
			in[i] = byte(rand.IntN(256))
		}
		out := make([]bool, size*8)

		b.Run("bytes="+bToStr(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				utils.BytesToBools(in, out)
			}
		})
	}
}

func TestBytesToBoolsOutlenSmaller(t *testing.T) {
	in := []byte{0xFF, 0xAA, 0x55}
	for outlen := 1; outlen <= 24; outlen++ {
		t.Run(fmt.Sprintf("outlen=%d", outlen), func(t *testing.T) {
			got := make([]bool, outlen)
			want := make([]bool, outlen)

			for i, b := range in {
				for j := 0; j < 8; j++ {
					idx := 8*i + j
					if idx >= outlen {
						break
					}
					want[idx] = (b & (1 << j)) != 0
				}
			}

			utils.BytesToBools(in, got)

			for i := 0; i < outlen; i++ {
				if got[i] != want[i] {
					t.Fatalf("outlen=%d: mismatch at index %d: got %v, want %v", outlen, i, got[i], want[i])
				}
			}
		})
	}
}

func bToStr(n int) string {
	switch {
	case n >= 16384:
		return "16K"
	case n >= 4096:
		return "4K"
	case n >= 1024:
		return "1K"
	case n >= 256:
		return "256"
	default:
		return "64"
	}
}
