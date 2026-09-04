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

//go:build go1.18 && arm64 && !noasm && !appengine

package kernels

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func TestFilterUint32NeonAllMasks(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	const (
		bytes  = 256
		length = bytes * 8
	)
	values := make([]uint32, length)
	filter := make([]byte, bytes)
	for i := range values {
		values[i] = uint32(i*17 + 3)
	}
	for i := range filter {
		filter[i] = byte(i)
	}

	expected := make([]uint32, 0, length)
	for i, mask := range filter {
		for lane := 0; lane < 8; lane++ {
			if mask&(1<<uint(lane)) != 0 {
				expected = append(expected, values[i*8+lane])
			}
		}
	}

	backing := make([]uint32, len(expected)+1)
	backing[len(expected)] = 0xdeadbeef
	if !filterUint32Neon(values, backing[:len(expected)], filter, 0, length) {
		t.Fatal("filterUint32Neon did not select the mixed-mask path")
	}
	for i, want := range expected {
		if got := backing[i]; got != want {
			t.Fatalf("output[%d] = %d, want %d", i, got, want)
		}
	}
	if got := backing[len(expected)]; got != 0xdeadbeef {
		t.Fatalf("sentinel = %#x, want %#x", got, uint32(0xdeadbeef))
	}
}

func TestFilterUint32NeonAlignedOffset(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	const (
		offset = 8
		length = 64
	)
	values := make([]uint32, length)
	for i := range values {
		values[i] = uint32(i)
	}
	filter := make([]byte, offset/8+length/8)
	for i := range filter[offset/8:] {
		filter[offset/8+i] = 0x55
	}

	output := make([]uint32, length/2)
	if !filterUint32Neon(values, output, filter, offset, length) {
		t.Fatal("filterUint32Neon did not select the aligned-offset path")
	}
	for i, got := range output {
		want := uint32(2 * i)
		if got != want {
			t.Fatalf("output[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestFilterUint32NeonGuards(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	values := make([]uint32, 128)
	mixed := make([]byte, 16)
	for i := range mixed {
		mixed[i] = 0x55
	}
	output := make([]uint32, 64)

	tests := []struct {
		name   string
		filter []byte
		offset int64
		length int64
		want   bool
	}{
		{name: "short", filter: mixed, length: 56},
		{name: "non-multiple-of-eight", filter: mixed, length: 72 - 1},
		{name: "unaligned-filter-offset", filter: mixed, offset: 3, length: 64},
		{name: "all-false", filter: make([]byte, 8), length: 64},
		{name: "all-true", filter: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, length: 64},
		{name: "too-few-mixed-bytes", filter: []byte{0x55, 0x55, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00}, length: 64},
		{name: "out-of-bounds-filter", filter: mixed[:4], length: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterUint32Neon(values, output, tt.filter, tt.offset, tt.length); got != tt.want {
				t.Fatalf("filterUint32Neon() = %t, want %t", got, tt.want)
			}
		})
	}
}
