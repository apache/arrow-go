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

	"github.com/apache/arrow-go/v18/parquet"
)

func TestDecodeByteStreamSplitWidth4(t *testing.T) {
	const width = 4

	// Test various sizes including edge cases
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
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

		out := make([]byte, width*nValues)
		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			decodeByteStreamSplitBatchWidth4InByteOrder(data, nValues, stride, out)
			if !bytes.Equal(out, expected) {
				for i := 0; i < len(expected); i++ {
					if out[i] != expected[i] {
						t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
						break
					}
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchWidth4(b *testing.B) {
	const width = 4
	sizes := []int{8, 64, 512, 4096, 32768, 2097152, 16777216}

	for _, nValues := range sizes {
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

		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decodeByteStreamSplitBatchWidth4InByteOrder(data, nValues, stride, out)
			}
		})
	}
}

func TestDecodeByteStreamSplitWidth8(t *testing.T) {
	const width = 8

	// Test various sizes including edge cases
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
		// Setup encoded data (byte stream split format)
		stride := nValues
		data := make([]byte, width*nValues)

		// Initialize with predictable pattern
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)                // stream 0
			data[stride+i] = byte((i + 1) % 256)   // stream 1
			data[2*stride+i] = byte((i + 2) % 256) // stream 2
			data[3*stride+i] = byte((i + 3) % 256) // stream 3
			data[4*stride+i] = byte((i + 4) % 256) // stream 4
			data[5*stride+i] = byte((i + 5) % 256) // stream 5
			data[6*stride+i] = byte((i + 6) % 256) // stream 6
			data[7*stride+i] = byte((i + 7) % 256) // stream 7
		}

		// Expected output: interleaved bytes
		expected := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			expected[i*8] = byte(i % 256)
			expected[i*8+1] = byte((i + 1) % 256)
			expected[i*8+2] = byte((i + 2) % 256)
			expected[i*8+3] = byte((i + 3) % 256)
			expected[i*8+4] = byte((i + 4) % 256)
			expected[i*8+5] = byte((i + 5) % 256)
			expected[i*8+6] = byte((i + 6) % 256)
			expected[i*8+7] = byte((i + 7) % 256)
		}

		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			out := make([]byte, width*nValues)
			decodeByteStreamSplitBatchWidth8InByteOrder(data, nValues, stride, out)
			if !bytes.Equal(out, expected) {
				t.Errorf("Reference implementation produced incorrect output")
				for i := 0; i < len(expected); i++ {
					if out[i] != expected[i] {
						t.Errorf("First mismatch at index %d: got %d, want %d", i, out[i], expected[i])
						break
					}
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchWidth8(b *testing.B) {
	const width = 8
	sizes := []int{8, 64, 512, 4096, 32768, 2097152, 16777216}

	for _, nValues := range sizes {
		stride := nValues
		data := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)
			data[stride+i] = byte((i + 1) % 256)
			data[2*stride+i] = byte((i + 2) % 256)
			data[3*stride+i] = byte((i + 3) % 256)
			data[4*stride+i] = byte((i + 4) % 256)
			data[5*stride+i] = byte((i + 5) % 256)
			data[6*stride+i] = byte((i + 6) % 256)
			data[7*stride+i] = byte((i + 7) % 256)
		}
		out := make([]byte, width*nValues)
		b.SetBytes(int64(width * nValues))

		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decodeByteStreamSplitBatchWidth8InByteOrder(data, nValues, stride, out)
			}
		})
	}
}

func TestDecodeByteStreamSplitFLBAWidth2(t *testing.T) {
	const width = 2

	// Test various sizes including edge cases
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
		// Setup encoded data (byte stream split format)
		stride := nValues
		data := make([]byte, width*nValues)

		// Initialize with predictable pattern
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)              // stream 0
			data[stride+i] = byte((i + 1) % 256) // stream 1
		}

		// Expected output: FixedLenByteArray slices with interleaved bytes
		expected := make([]parquet.FixedLenByteArray, nValues)
		for i := 0; i < nValues; i++ {
			expected[i] = make(parquet.FixedLenByteArray, width)
			expected[i][0] = byte(i % 256)
			expected[i][1] = byte((i + 1) % 256)
		}

		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			out := make([]parquet.FixedLenByteArray, nValues)
			for i := range out {
				out[i] = make(parquet.FixedLenByteArray, width)
			}
			decodeByteStreamSplitBatchFLBAWidth2(data, nValues, stride, out)
			for i := 0; i < nValues; i++ {
				if !bytes.Equal(out[i], expected[i]) {
					t.Errorf("Reference implementation mismatch at index %d: got %v, want %v", i, out[i], expected[i])
					break
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchFLBAWidth2(b *testing.B) {
	const width = 2
	sizes := []int{8, 64, 512, 4096, 32768, 262144, 2097152, 16777216}

	for _, nValues := range sizes {
		stride := nValues
		data := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)
			data[stride+i] = byte((i + 1) % 256)
		}
		out := make([]parquet.FixedLenByteArray, nValues)
		for i := range out {
			out[i] = make(parquet.FixedLenByteArray, width)
		}
		b.SetBytes(int64(width * nValues))

		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decodeByteStreamSplitBatchFLBAWidth2(data, nValues, stride, out)
			}
		})
	}
}

func TestDecodeByteStreamSplitFLBAWidth4(t *testing.T) {
	const width = 4
	// Test various sizes including edge cases and block boundaries
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
		// Setup encoded data (byte stream split format)
		stride := nValues
		data := make([]byte, width*nValues)

		// Initialize with predictable pattern
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)                // stream 0
			data[stride+i] = byte((i + 1) % 256)   // stream 1
			data[stride*2+i] = byte((i + 2) % 256) // stream 2
			data[stride*3+i] = byte((i + 3) % 256) // stream 3
		}

		// Expected output: FixedLenByteArray slices with interleaved bytes
		expected := make([]parquet.FixedLenByteArray, nValues)
		for i := 0; i < nValues; i++ {
			expected[i] = make(parquet.FixedLenByteArray, width)
			expected[i][0] = byte(i % 256)
			expected[i][1] = byte((i + 1) % 256)
			expected[i][2] = byte((i + 2) % 256)
			expected[i][3] = byte((i + 3) % 256)
		}

		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			out := make([]parquet.FixedLenByteArray, nValues)
			for i := range out {
				out[i] = make(parquet.FixedLenByteArray, width)
			}
			decodeByteStreamSplitBatchFLBAWidth4(data, nValues, stride, out)
			for i := 0; i < nValues; i++ {
				if !bytes.Equal(out[i], expected[i]) {
					t.Errorf("Reference implementation mismatch at index %d: got %v, want %v", i, out[i], expected[i])
					break
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchFLBAWidth4(b *testing.B) {
	const width = 4
	sizes := []int{8, 64, 512, 4096, 32768, 262144, 2097152, 16777216}

	for _, nValues := range sizes {
		stride := nValues
		data := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)
			data[stride+i] = byte((i + 1) % 256)
			data[stride*2+i] = byte((i + 2) % 256)
			data[stride*3+i] = byte((i + 3) % 256)
		}
		out := make([]parquet.FixedLenByteArray, nValues)
		for i := range out {
			out[i] = make(parquet.FixedLenByteArray, width)
		}
		b.SetBytes(int64(width * nValues))

		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decodeByteStreamSplitBatchFLBAWidth4(data, nValues, stride, out)
			}
		})
	}
}

func TestDecodeByteStreamSplitFLBAWidth8(t *testing.T) {
	const width = 8
	// Test various sizes including edge cases and block boundaries
	sizes := []int{1, 2, 7, 8, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024}

	for _, nValues := range sizes {
		// Setup encoded data (byte stream split format)
		stride := nValues
		data := make([]byte, width*nValues)
		// Initialize with predictable pattern
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)                // stream 0
			data[stride+i] = byte((i + 1) % 256)   // stream 1
			data[stride*2+i] = byte((i + 2) % 256) // stream 2
			data[stride*3+i] = byte((i + 3) % 256) // stream 3
			data[stride*4+i] = byte((i + 4) % 256) // stream 4
			data[stride*5+i] = byte((i + 5) % 256) // stream 5
			data[stride*6+i] = byte((i + 6) % 256) // stream 6
			data[stride*7+i] = byte((i + 7) % 256) // stream 7
		}
		// Expected output: FixedLenByteArray slices with interleaved bytes
		expected := make([]parquet.FixedLenByteArray, nValues)
		for i := 0; i < nValues; i++ {
			expected[i] = make(parquet.FixedLenByteArray, width)
			expected[i][0] = byte(i % 256)
			expected[i][1] = byte((i + 1) % 256)
			expected[i][2] = byte((i + 2) % 256)
			expected[i][3] = byte((i + 3) % 256)
			expected[i][4] = byte((i + 4) % 256)
			expected[i][5] = byte((i + 5) % 256)
			expected[i][6] = byte((i + 6) % 256)
			expected[i][7] = byte((i + 7) % 256)
		}

		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			out := make([]parquet.FixedLenByteArray, nValues)
			for i := range out {
				out[i] = make(parquet.FixedLenByteArray, width)
			}
			decodeByteStreamSplitBatchFLBAWidth8(data, nValues, stride, out)
			for i := 0; i < nValues; i++ {
				if !bytes.Equal(out[i], expected[i]) {
					t.Errorf("Reference implementation mismatch at index %d: got %v, want %v", i, out[i], expected[i])
					break
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchFLBAWidth8(b *testing.B) {
	const width = 8
	sizes := []int{8, 64, 512, 4096, 32768, 262144, 2097152, 16777216}

	for _, nValues := range sizes {
		stride := nValues
		data := make([]byte, width*nValues)
		for i := 0; i < nValues; i++ {
			data[i] = byte(i % 256)
			data[stride+i] = byte((i + 1) % 256)
			data[stride*2+i] = byte((i + 2) % 256)
			data[stride*3+i] = byte((i + 3) % 256)
			data[stride*4+i] = byte((i + 4) % 256)
			data[stride*5+i] = byte((i + 5) % 256)
			data[stride*6+i] = byte((i + 6) % 256)
			data[stride*7+i] = byte((i + 7) % 256)
		}
		out := make([]parquet.FixedLenByteArray, nValues)
		for i := range out {
			out[i] = make(parquet.FixedLenByteArray, width)
		}
		b.SetBytes(int64(width * nValues))

		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decodeByteStreamSplitBatchFLBAWidth8(data, nValues, stride, out)
			}
		})
	}
}
