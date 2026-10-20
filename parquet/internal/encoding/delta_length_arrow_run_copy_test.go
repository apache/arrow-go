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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

// Exercise direct Arrow buffer writes with non-zero offsets, batches that
// straddle a DELTA miniblock, and runs separated by non-empty null payloads.
func TestDeltaLengthArrowContiguousRuns(t *testing.T) {
	t.Run("int32", testDeltaLengthArrowContiguousRuns[int32])
	t.Run("int64", testDeltaLengthArrowContiguousRuns[int64])
}

func testDeltaLengthArrowContiguousRuns[T arrowByteArrayOffset](t *testing.T) {
	const n = 2*deltaByteArrayBatchSize + 17
	data := []byte("discard before first value")
	offsets := make([]T, n+1)
	values := make([]parquet.ByteArray, n)
	for i := range values {
		offsets[i] = T(len(data))
		values[i] = make([]byte, (i*17+i/13)%19)
		for j := range values[i] {
			values[i][j] = byte(i*7 + j)
		}
		data = append(data, values[i]...)
	}
	offsets[n] = T(len(data))

	for _, span := range []struct {
		name       string
		begin, end int
	}{
		{"whole", 0, n},
		{"sliced", 11, n - 9},
		{"single", 47, 48},
		{"empty", 47, 47},
	} {
		for _, pattern := range []string{"all", "all-valid-bitmap", "none", "alternating", "long-runs"} {
			t.Run(span.name+"/"+pattern, func(t *testing.T) {
				var bits []byte
				if pattern != "all" {
					bits = make([]byte, bitutil.BytesForBits(int64(span.end-span.begin+3)))
				}
				want := []parquet.ByteArray{[]byte("before")}
				for i := span.begin; i < span.end; i++ {
					valid := pattern == "all" || pattern == "all-valid-bitmap" ||
						(pattern == "alternating" && i%2 == 0) ||
						(pattern == "long-runs" && (i/31)%2 == 0)
					if valid {
						want = append(want, values[i])
						if bits != nil {
							bitutil.SetBit(bits, 3+i-span.begin)
						}
					}
				}
				want = append(want, []byte("after"))

				enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					false, nil, memory.DefaultAllocator).(*DeltaLengthByteArrayEncoder)
				defer enc.Release()
				enc.Put([]parquet.ByteArray{[]byte("before")})
				middle := span.begin + (span.end-span.begin)/2
				for _, part := range [][2]int{{span.begin, middle}, {middle, span.end}} {
					putArrowDeltaLength(enc, data, offsets[part[0]:part[1]+1], bits,
						int64(3+part[0]-span.begin))
				}
				enc.Put([]parquet.ByteArray{[]byte("after")})
				got, err := enc.FlushValues()
				require.NoError(t, err)
				defer got.Release()
				require.Equal(t, encodedByteArrays(t, parquet.Encodings.DeltaLengthByteArray,
					want, false, nil, 0), got.Bytes())
			})
		}
	}
}

func BenchmarkDeltaLengthArrowContiguousRuns(b *testing.B) {
	const n = 64 * 1024
	for _, width := range []int{0, 4, 16, 64} {
		for _, pattern := range []string{"all", "long-runs", "alternating"} {
			b.Run(fmt.Sprintf("bytes=%d/%s", width, pattern), func(b *testing.B) {
				data := make([]byte, 7+n*width)
				offsets := make([]int32, n+1)
				for i := range offsets {
					offsets[i] = int32(7 + i*width)
				}
				var bits []byte
				if pattern != "all" {
					bits = make([]byte, bitutil.BytesForBits(n+3))
					for i := 0; i < n; i++ {
						valid := pattern == "alternating" && i%2 != 0 ||
							pattern == "long-runs" && (i/128)%2 == 0
						if valid {
							bitutil.SetBit(bits, i+3)
						}
					}
				}

				enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					false, nil, memory.DefaultAllocator).(*DeltaLengthByteArrayEncoder)
				defer enc.Release()
				b.SetBytes(int64(n * width))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if bits == nil {
						enc.PutArrow(data, offsets)
					} else {
						enc.PutArrowSpaced(data, offsets, bits, 3)
					}
					buf, err := enc.FlushValues()
					if err != nil {
						b.Fatal(err)
					}
					buf.Release()
				}
			})
		}
	}
}
