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

//go:build linux || darwin

package bitutils_test

import (
	"fmt"
	"math/rand/v2"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/internal/bitutils"
)

func TestFourWordsUnalignedGuardPage(t *testing.T) {
	pageSize := syscall.Getpagesize()
	for offset := int64(1); offset < 8; offset++ {
		for _, length := range []int64{
			255, 256, 257,
			320 - offset - 1, 320 - offset, 320 - offset + 1,
			512, 1024, 1280 - offset - 1, 1280 - offset,
		} {
			t.Run(fmt.Sprintf("offset%d/length%d", offset, length), func(t *testing.T) {
				mapped, err := syscall.Mmap(-1, 0, 2*pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
				if err != nil {
					t.Fatalf("mmap: %v", err)
				}
				t.Cleanup(func() {
					if err := syscall.Munmap(mapped); err != nil {
						t.Errorf("munmap: %v", err)
					}
				})
				if err := syscall.Mprotect(mapped[pageSize:], syscall.PROT_NONE); err != nil {
					t.Fatalf("mprotect: %v", err)
				}

				// An unsafe read beyond the last required byte reaches the protected page.
				numBytes := int(bitutil.BytesForBits(offset + length))
				bitmap := mapped[pageSize-numBytes : pageSize]
				rng := &rand.ChaCha8{}
				_, _ = rng.Read(bitmap)

				counter := bitutils.NewBitBlockCounter(bitmap, offset, length)
				for pos := int64(0); pos < length; {
					block := counter.NextFourWords()
					wantLen := min(int64(256), length-pos)
					if int64(block.Len) != wantLen {
						t.Fatalf("offset=%d length=%d pos=%d: block length=%d, want %d", offset, length, pos, block.Len, wantLen)
					}
					wantPopcnt := bitutil.CountSetBits(bitmap, int(offset+pos), int(block.Len))
					if int(block.Popcnt) != wantPopcnt {
						t.Fatalf("offset=%d length=%d pos=%d: popcount=%d, want %d", offset, length, pos, block.Popcnt, wantPopcnt)
					}
					pos += int64(block.Len)
				}
				if got := counter.NextFourWords().Len; got != 0 {
					t.Fatalf("offset=%d length=%d: final block length=%d, want 0", offset, length, got)
				}
			})
		}
	}
}
