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

			if cpu.X86.HasAVX2 {
				// Test AVX2 assembly implementation
				t.Run("AVX2", func(t *testing.T) {
					out := make([]byte, width*nValues)
					// _decodeByteStreamSplitWidth4AVX2(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
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
func TestDecodeByteStreamSplitWidth4EdgeCases(t *testing.T) {
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
		// _decodeByteStreamSplitWidth4AVX2(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
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
		// _decodeByteStreamSplitWidth4AVX2(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
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
		// _decodeByteStreamSplitWidth4AVX2(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
		decodeByteStreamSplitBatchWidth4SIMD(data, nValues, stride, out)
		if !bytes.Equal(out, expected) {
			t.Errorf("Failed on all-0xFF data")
		}
	})
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
		})
	}
}
