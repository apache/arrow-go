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

package bitutil_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func TestCountSetBitsOffsetByteBoundaries(t *testing.T) {
	for _, size := range []int{1, 2, 9, 65, 129} {
		for storageOffset := 0; storageOffset < 8; storageOffset++ {
			storage := make([]byte, storageOffset+size)
			bitmap := storage[storageOffset:]
			for i := range bitmap {
				bitmap[i] = byte(i*73 + 31)
			}

			for offset := 0; offset <= len(bitmap)*8; offset++ {
				available := len(bitmap)*8 - offset
				for _, n := range []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 31, 32, 63, 64, 65, 127, available} {
					if n > available {
						continue
					}
					want := 0
					for i := offset; i < offset+n; i++ {
						if bitutil.BitIsSet(bitmap, i) {
							want++
						}
					}
					if got := bitutil.CountSetBits(bitmap, offset, n); got != want {
						t.Fatalf("size=%d storageOffset=%d offset=%d n=%d: got=%d want=%d",
							size, storageOffset, offset, n, got, want)
					}
				}
			}
		}
	}
}

func TestCountSetBitsZeroLength(t *testing.T) {
	for _, tc := range []struct {
		name string
		buf  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"nonempty", []byte{0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, offset := range []int{0, 1, 8, 64, math.MaxInt} {
				if got := bitutil.CountSetBits(tc.buf, offset, 0); got != 0 {
					t.Fatalf("offset=%d: got=%d, want=0", offset, got)
				}
			}
		})
	}
}

var countSetBitsOffsetResult int

func BenchmarkCountSetBitsOffsetByteMask(b *testing.B) {
	for _, tc := range []struct {
		name   string
		offset int
		n      int
	}{
		{"2-bits-offset-7", 7, 2},
		{"1-bit-offset-8", 8, 1},
		{"2-bits-offset-63", 63, 2},
		{"5-bits-offset-3", 3, 5},
		{"32-bits-offset-3", 3, 32},
		{"32-bits-offset-8", 8, 32},
		{"128-bits-offset-37", 37, 128},
		{"8192-bits-offset-3", 3, 8192},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bitmap := make([]byte, (tc.offset+tc.n+7)/8)
			for i := range bitmap {
				bitmap[i] = byte(i*73 + 31)
			}
			b.ReportAllocs()
			var result int
			for b.Loop() {
				result = bitutil.CountSetBits(bitmap, tc.offset, tc.n)
			}
			countSetBitsOffsetResult = result
		})
	}
}
