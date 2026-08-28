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
	"math/rand"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
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
			for i := 0; i < nValues; i++ {
				got := utils.ToLE(*(*uint32)(unsafe.Pointer(&out[i*4])))
				want := *(*uint32)(unsafe.Pointer(&expected[i*4]))
				if got != want {
					t.Errorf("Mismatch at index %d: got %08x, want %08x", i, got, want)
					break
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchWidth4(b *testing.B) {
	const width = 4
	sizes := []int{8, 10, 64, 100, 512, 1000, 4096, 10000, 32768, 100000, 2097152, 10000000, 16777216}

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
			for i := 0; i < nValues; i++ {
				got := utils.ToLE(*(*uint64)(unsafe.Pointer(&out[i*8])))
				want := *(*uint64)(unsafe.Pointer(&expected[i*8]))
				if got != want {
					t.Errorf("Mismatch at index %d: got %016x, want %016x", i, got, want)
					break
				}
			}
		})
	}
}

func BenchmarkDecodeByteStreamSplitBatchWidth8(b *testing.B) {
	const width = 8
	sizes := []int{8, 10, 64, 100, 512, 1000, 4096, 10000, 32768, 100000, 2097152, 10000000, 16777216}

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
	sizes := []int{8, 10, 64, 100, 512, 1000, 4096, 10000, 32768, 100000, 2097152, 10000000, 16777216}

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
	sizes := []int{8, 10, 64, 100, 512, 1000, 4096, 10000, 32768, 100000, 2097152, 10000000, 16777216}

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
	sizes := []int{8, 10, 64, 100, 512, 1000, 4096, 10000, 32768, 100000, 2097152, 10000000, 16777216}

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

// TestByteStreamSplitFLBADecodeSpacedReusedBuffer guards the aliasing bug where decoding
// a second page into a buffer previously expanded by DecodeSpaced silently corrupted
// values: spacedExpand moves slice headers with copy, leaving duplicate headers behind in
// the null slots, and because this decoder writes through the caller's slices rather than
// replacing them, two output slots shared one backing array and clobbered each other.
func TestByteStreamSplitFLBADecodeSpacedReusedBuffer(t *testing.T) {
	for _, width := range []int{2, 3, 4, 7, 8, 16} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			// 5 slots, 2 nulls: slots 0, 2 and 4 are valid.
			validBits := []byte{0b00010101}
			const nullCount = 2

			col := schema.NewColumn(schema.NewFixedLenByteArrayNode("v", parquet.Repetitions.Optional, int32(width), -1), 1, 0)
			dec := NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.ByteStreamSplit,
				col, memory.DefaultAllocator).(FixedLenByteArrayDecoder)

			// A single output buffer reused across both pages, as the record reader does.
			out := make([]parquet.FixedLenByteArray, 5)

			for page, offset := range []byte{0, 100} {
				values := make([]parquet.FixedLenByteArray, 3)
				for i := range values {
					values[i] = make(parquet.FixedLenByteArray, width)
					for j := range values[i] {
						values[i][j] = offset + byte(i*width+j)
					}
				}

				data := make([]byte, len(values)*width)
				for vi, v := range values {
					for bi, b := range v {
						data[bi*len(values)+vi] = b
					}
				}

				require.NoError(t, dec.SetData(len(values), data))
				n, err := dec.DecodeSpaced(out, nullCount, validBits, 0)
				require.NoError(t, err)
				require.Equal(t, len(out), n)

				require.Equal(t, values[0], out[0], "page %d slot 0", page)
				require.Equal(t, values[1], out[2], "page %d slot 2", page)
				require.Equal(t, values[2], out[4], "page %d slot 4", page)
			}
		})
	}
}

// TestSpacedExpandSwapMatchesSpacedExpand checks that swapping places values in exactly
// the same slots as copying, and additionally never leaves duplicate entries behind.
func TestSpacedExpandSwapMatchesSpacedExpand(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 5000; iter++ {
		n := 1 + rng.Intn(200)
		validBits := make([]byte, bitutil.BytesForBits(int64(n)))
		nullCount, density := 0, rng.Float64()
		for i := 0; i < n; i++ {
			if rng.Float64() < density {
				bitutil.ClearBit(validBits, i)
				nullCount++
			} else {
				bitutil.SetBit(validBits, i)
			}
		}

		// distinct sentinels so duplicates are detectable
		copied, swapped := make([]int64, n), make([]int64, n)
		for i := range copied {
			copied[i], swapped[i] = int64(i+1), int64(i+1)
		}

		spacedExpand(copied, nullCount, validBits, 0)
		spacedExpandSwap(swapped, nullCount, validBits, 0)

		for i := 0; i < n; i++ {
			if bitutil.BitIsSet(validBits, i) {
				require.Equalf(t, copied[i], swapped[i],
					"iter %d n=%d nulls=%d: valid slot %d differs", iter, n, nullCount, i)
			}
		}

		seen := make(map[int64]struct{}, n)
		for _, v := range swapped {
			seen[v] = struct{}{}
		}
		require.Lenf(t, seen, n,
			"iter %d n=%d nulls=%d: swap left duplicate entries", iter, n, nullCount)
	}
}
