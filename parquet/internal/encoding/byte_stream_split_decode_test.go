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

package encoding

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/sys/cpu"
)

// TestDecodeByteStreamSplitWidth4 validates correctness of all width-4 decoding implementations
func TestDecodeByteStreamSplitWidth4(t *testing.T) {
	const width = 4

	// Test various sizes including edge cases
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			// Setup encoded data (byte stream split format)
			stride := nValues
			data := make([]byte, width*nValues)

			// Initialize with predictable pattern
			for i := 0; i < nValues; i++ {
				data[i] = byte(i % 256)                // stream 0
				data[stride+i] = byte((i + 1) % 256)   // stream 1
				data[2*stride+i] = byte((i + 2) % 256) // stream 2
				data[3*stride+i] = byte((i + 3) % 256) // stream 3
			}

			// Expected output: interleaved bytes
			expected := make([]byte, width*nValues)
			for i := 0; i < nValues; i++ {
				expected[i*4] = byte(i % 256)
				expected[i*4+1] = byte((i + 1) % 256)
				expected[i*4+2] = byte((i + 2) % 256)
				expected[i*4+3] = byte((i + 3) % 256)
			}

			// Test reference implementation
			t.Run("Reference", func(t *testing.T) {
				out := make([]byte, width*nValues)
				decodeByteStreamSplitBatchWidth4InByteOrder(data, nValues, stride, out)
				if !bytes.Equal(out, expected) {
					t.Errorf("Reference implementation produced incorrect output")
					for i := 0; i < len(expected) && i < 64; i++ {
						if out[i] != expected[i] {
							t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
							break
						}
					}
				}
			})

			// Test V2 implementation
			t.Run("V2", func(t *testing.T) {
				out := make([]byte, width*nValues)
				decodeByteStreamSplitBatchWidth4InByteOrderV2(data, nValues, stride, out)
				if !bytes.Equal(out, expected) {
					t.Errorf("V2 implementation produced incorrect output")
					for i := 0; i < len(expected) && i < 64; i++ {
						if out[i] != expected[i] {
							t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
							break
						}
					}
				}
			})
		})
	}
}

// BenchmarkDecodeByteStreamSplitBatchWidth4 benchmarks all width-4 decoding implementations with sub-benchmarks.
func BenchmarkDecodeByteStreamSplitBatchWidth4(b *testing.B) {
	const width = 4
	sizes := []int{8, 64, 512, 4096, 32768, 262144}

	for _, nValues := range sizes {
		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			stride := nValues
			data := make([]byte, width*nValues)
			for i := 0; i < nValues; i++ {
				data[i] = byte(i % 256)
				data[stride+i] = byte((i + 1) % 256)
				data[2*stride+i] = byte((i + 2) % 256)
				data[3*stride+i] = byte((i + 3) % 256)
			}
			out := make([]byte, width*nValues)

			b.SetBytes(int64(width * nValues))

			b.Run("Reference", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					decodeByteStreamSplitBatchWidth4InByteOrder(data, nValues, stride, out)
				}
			})

			b.Run("V2", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					decodeByteStreamSplitBatchWidth4InByteOrderV2(data, nValues, stride, out)
				}
			})

			if decodeByteStreamSplitBatchWidth4SIMD != nil {
				if cpu.X86.HasAVX2 {
					b.Run("AVX2", func(b *testing.B) {
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
						}
					})
				}

				if cpu.ARM64.HasASIMD {
					b.Run("NEON", func(b *testing.B) {
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
						}
					})
				}
			}
		})
	}
}
