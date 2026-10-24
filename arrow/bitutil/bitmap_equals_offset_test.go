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

func TestBitmapEqualsOffsetParity(t *testing.T) {
	offsets := []int{0, 1, 2, 3, 7, 8, 11, 15, 16, 19, 31, 64, 71}
	lengths := []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 66, 127, 128, 129, 255, 256, 257, 511, 512, 513}
	for _, length := range lengths {
		for _, lo := range offsets {
			for _, ro := range offsets {
				left := make([]byte, bitutil.BytesForBits(int64(lo+length)))
				right := make([]byte, bitutil.BytesForBits(int64(ro+length)))
				for i := range left {
					left[i] = 0xa5
				}
				for i := range right {
					right[i] = 0x5a
				}
				for i := 0; i < length; i++ {
					val := (i*31+i/7)%11 < 5
					bitutil.SetBitTo(left, lo+i, val)
					bitutil.SetBitTo(right, ro+i, val)
				}
				if !bitutil.BitmapEquals(left, right, int64(lo), int64(ro), int64(length)) {
					t.Fatalf("equal ranges: len=%d left=%d right=%d", length, lo, ro)
				}
				for _, pos := range []int{0, length / 2, length - 1} {
					if pos < 0 || pos >= length {
						continue
					}
					index := ro + pos
					right[index/8] ^= byte(1) << uint(index%8)
					if bitutil.BitmapEquals(left, right, int64(lo), int64(ro), int64(length)) {
						t.Fatalf("missed difference: len=%d left=%d right=%d pos=%d", length, lo, ro, pos)
					}
					right[index/8] ^= byte(1) << uint(index%8)
				}
			}
		}
	}
}

func TestBitmapEqualsLargeOffsetParity(t *testing.T) {
	const length = 1 << 20
	for _, pair := range []struct{ left, right int }{{0, 0}, {3, 11}, {7, 15}, {3, 6}, {0, 7}, {15, 31}} {
		t.Run(fmt.Sprintf("%d_%d", pair.left, pair.right), func(t *testing.T) {
			left := make([]byte, bitutil.BytesForBits(length+int64(pair.left)))
			right := make([]byte, bitutil.BytesForBits(length+int64(pair.right)))
			for i := 0; i < length; i++ {
				v := (i*7+i/29)%11 < 5
				bitutil.SetBitTo(left, pair.left+i, v)
				bitutil.SetBitTo(right, pair.right+i, v)
			}
			if !bitutil.BitmapEquals(left, right, int64(pair.left), int64(pair.right), length) {
				t.Fatal("large equal bitmap ranges differ")
			}
			index := pair.right + length - 1
			right[index/8] ^= 1 << uint(index%8)
			if bitutil.BitmapEquals(left, right, int64(pair.left), int64(pair.right), length) {
				t.Fatal("missed difference at end of large range")
			}
		})
	}
}

func TestBitmapEqualsUnalignedBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		lo, ro, length int64
	}{
		{name: "same offset", lo: 3, ro: 11, length: 128},
		{name: "different offset", lo: 3, ro: 6, length: 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected out-of-bounds range to panic")
				}
			}()
			bitutil.BitmapEquals([]byte{0xff}, []byte{0}, tc.lo, tc.ro, tc.length)
		})
	}
}
