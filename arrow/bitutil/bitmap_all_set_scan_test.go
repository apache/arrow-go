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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func TestBitmapAllSetWordBoundaries(t *testing.T) {
	for storageOffset := 0; storageOffset < 8; storageOffset++ {
		for offset := 0; offset < 64; offset++ {
			for _, n := range []int{1, 7, 8, 63, 64, 65, 511, 512, 513, 1023, 1024, 1025} {
				storage := make([]byte, storageOffset+(offset+n+7)/8)
				bitmap := storage[storageOffset:]
				// Keep all padding and neighboring bits clear.
				for i := offset; i < offset+n; i++ {
					bitutil.SetBit(bitmap, i)
				}
				if !bitutil.BitmapAllSet(bitmap, offset, n) {
					t.Fatalf("all-set range rejected: storage=%d offset=%d length=%d", storageOffset, offset, n)
				}
				for i := offset; i < offset+n; i++ {
					bitutil.ClearBit(bitmap, i)
					if bitutil.BitmapAllSet(bitmap, offset, n) {
						t.Fatalf("clear bit %d missed: storage=%d offset=%d length=%d", i, storageOffset, offset, n)
					}
					bitutil.SetBit(bitmap, i)
				}
			}
		}
	}
}

func BenchmarkBitmapAllSetScan(b *testing.B) {
	for _, n := range []int{64, 1024, 65536, 1048576} {
		for _, offset := range []int{0, 3} {
			for _, pattern := range []string{"all", "first-null", "last-null"} {
				b.Run(fmt.Sprintf("bits=%d/offset=%d/%s", n, offset, pattern), func(b *testing.B) {
					bitmap := make([]byte, (offset+n+7)/8)
					for i := range bitmap {
						bitmap[i] = 0xff
					}
					switch pattern {
					case "first-null":
						bitutil.ClearBit(bitmap, offset)
					case "last-null":
						bitutil.ClearBit(bitmap, offset+n-1)
					}
					want := pattern == "all"
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if bitutil.BitmapAllSet(bitmap, offset, n) != want {
							b.Fatal("unexpected bitmap result")
						}
					}
				})
			}
		}
	}
}
