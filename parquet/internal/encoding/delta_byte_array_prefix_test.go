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
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
)

func TestCommonPrefixLength(t *testing.T) {
	if got := commonPrefixLength(nil, nil); got != 0 {
		t.Fatalf("nil: got %d, want 0", got)
	}
	if got := commonPrefixLength(nil, parquet.ByteArray("x")); got != 0 {
		t.Fatalf("nil versus value: got %d, want 0", got)
	}

	for length := 0; length <= 257; length++ {
		// Shift the starting addresses to cover unaligned slices.
		left := make([]byte, length+1)
		right := make([]byte, length+2)
		for i := 0; i < length; i++ {
			left[i+1] = byte(i*17 + 0x81)
			right[i+2] = left[i+1]
		}
		a, b := parquet.ByteArray(left[1:]), parquet.ByteArray(right[2:])
		for mismatch := 0; mismatch <= length; mismatch++ {
			if mismatch < length {
				b[mismatch] ^= 0x80
			}
			if got := commonPrefixLength(a, b); got != mismatch {
				t.Fatalf("length=%d mismatch=%d: got %d", length, mismatch, got)
			}
			if mismatch < length {
				b[mismatch] ^= 0x80
			}
		}
		if length > 0 {
			if got := commonPrefixLength(a, b[:length-1]); got != length-1 {
				t.Fatalf("right shorter, length=%d: got %d", length, got)
			}
			if got := commonPrefixLength(a[:length-1], b); got != length-1 {
				t.Fatalf("left shorter, length=%d: got %d", length, got)
			}
		}
	}
}

func BenchmarkCommonPrefixLength(b *testing.B) {
	for _, tc := range []struct {
		name           string
		length, prefix int
	}{
		{"different-first", 256, 0},
		{"prefix-3", 8, 3},
		{"prefix-7", 16, 7},
		{"prefix-32", 64, 32},
		{"prefix-128", 256, 128},
		{"prefix-255", 256, 255},
		{"equal-256", 256, 256},
		{"prefix-900", 1024, 900},
	} {
		b.Run(tc.name, func(b *testing.B) {
			left := make(parquet.ByteArray, tc.length)
			right := make(parquet.ByteArray, tc.length)
			for i := range left {
				left[i] = byte(i*17 + 0x81)
				right[i] = left[i]
			}
			if tc.prefix < tc.length {
				right[tc.prefix] ^= 0x80
			}
			b.ReportAllocs()
			for b.Loop() {
				commonPrefixLength(left, right)
			}
		})
	}
}
