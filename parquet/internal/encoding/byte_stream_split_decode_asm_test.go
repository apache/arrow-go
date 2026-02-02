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

//go:build !noasm
// +build !noasm

package encoding

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/sys/cpu"
)

// TestDecodeByteStreamSplitWidth4 validates correctness of all width-4 decoding implementations
func TestDecodeByteStreamSplitWidth4SIMD(t *testing.T) {
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

			if cpu.X86.HasAVX2 {
				// Test AVX2 assembly implementation
				t.Run("AVX2", func(t *testing.T) {
					out := make([]byte, width*nValues)
					decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
					if !bytes.Equal(out, expected) {
						t.Errorf("AVX2 implementation produced incorrect output")
						for i := 0; i < len(expected) && i < 64; i++ {
							if out[i] != expected[i] {
								t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
								break
							}
						}
					}
				})
			}

			if cpu.ARM64.HasASIMD {
				// Test NEON assembly implementation
				t.Run("NEON", func(t *testing.T) {
					out := make([]byte, width*nValues)
					// _decodeByteStreamSplitWidth4NEON(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
					decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
					if !bytes.Equal(out, expected) {
						t.Errorf("NEON implementation produced incorrect output")
						for i := 0; i < len(expected) && i < 64; i++ {
							if out[i] != expected[i] {
								t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
								break
							}
						}
					}
				})
			}
		})
	}
}

// TestDecodeByteStreamSplitWidth4EdgeCases tests edge cases and boundary conditions
func TestDecodeByteStreamSplitWidth4EdgeCasesSIMD(t *testing.T) {
	const width = 4

	t.Run("ExactBlockBoundary32", func(t *testing.T) {
		// Test exact block boundary (32 values = 1 complete AVX2 block)
		nValues := 32
		stride := nValues
		data := make([]byte, width*nValues)

		for i := 0; i < nValues; i++ {
			data[i] = byte(i)
			data[stride+i] = byte(i + 64)
			data[2*stride+i] = byte(i + 128)
			data[3*stride+i] = byte(i + 192)
		}

		expected := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			expected[i*4] = byte(i)
			expected[i*4+1] = byte(i + 64)
			expected[i*4+2] = byte(i + 128)
			expected[i*4+3] = byte(i + 192)
		}

		out := make([]byte, width*nValues)
		decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
		if !bytes.Equal(out, expected) {
			t.Errorf("Failed on exact 32-value block boundary")
		}
	})

	t.Run("SingleValue", func(t *testing.T) {
		// Test single value (all suffix, no vectorization)
		nValues := 1
		stride := nValues
		data := []byte{0xAA, 0xBB, 0xCC, 0xDD}
		expected := []byte{0xAA, 0xBB, 0xCC, 0xDD}

		out := make([]byte, width*nValues)
		decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
		if !bytes.Equal(out, expected) {
			t.Errorf("Failed on single value: got %v, want %v", out, expected)
		}
	})

	t.Run("AllZeros", func(t *testing.T) {
		// Test all zeros
		nValues := 100
		stride := nValues
		data := make([]byte, width*nValues)
		expected := make([]byte, width*nValues)

		out := make([]byte, width*nValues)
		decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
		if !bytes.Equal(out, expected) {
			t.Errorf("Failed on all-zero data")
		}
	})

	t.Run("AllOnes", func(t *testing.T) {
		// Test all 0xFF
		nValues := 100
		stride := nValues
		data := make([]byte, width*nValues)
		for i := range data {
			data[i] = 0xFF
		}
		expected := make([]byte, width*nValues)
		for i := range expected {
			expected[i] = 0xFF
		}

		out := make([]byte, width*nValues)
		decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
		if !bytes.Equal(out, expected) {
			t.Errorf("Failed on all-0xFF data")
		}
	})
}
