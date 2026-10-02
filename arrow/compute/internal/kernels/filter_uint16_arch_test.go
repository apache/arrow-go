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

//go:build go1.18 && (amd64 || arm64) && !noasm && !appengine

package kernels

import "testing"

type filterUint16Func func(values []uint16, output []uint16, filter []byte, filterOffset, length int64) bool

func testFilterUint16AllMasks(t *testing.T, filter filterUint16Func) {
	t.Helper()

	const (
		filterBytes = 256
		length      = filterBytes * 8
	)
	values := make([]uint16, length)
	filterData := make([]byte, filterBytes)
	for i := range values {
		values[i] = uint16(i*977 + 0x8011)
	}
	for i := range filterData {
		filterData[i] = byte(i)
	}

	expected := make([]uint16, 0, length)
	for i, mask := range filterData {
		for lane := 0; lane < 8; lane++ {
			if mask&(1<<uint(lane)) != 0 {
				expected = append(expected, values[i*8+lane])
			}
		}
	}

	backing := make([]uint16, len(expected)+1)
	backing[len(expected)] = 0xdead
	if !filter(values, backing[:len(expected)], filterData, 0, length) {
		t.Fatal("filterUint16 did not select the mixed-mask path")
	}
	for i, want := range expected {
		if got := backing[i]; got != want {
			t.Fatalf("output[%d] = %#x, want %#x", i, got, want)
		}
	}
	if got := backing[len(expected)]; got != 0xdead {
		t.Fatalf("sentinel = %#x, want %#x", got, uint16(0xdead))
	}
}

func testFilterUint16AlignedOffset(t *testing.T, filter filterUint16Func) {
	t.Helper()

	const (
		offset = 8
		length = 64
	)
	values := make([]uint16, length)
	for i := range values {
		values[i] = uint16(i*31 + 0x8000)
	}
	filterData := make([]byte, offset/8+length/8)
	for i := range filterData[offset/8:] {
		filterData[offset/8+i] = 0x55
	}

	output := make([]uint16, length/2)
	if !filter(values, output, filterData, offset, length) {
		t.Fatal("filterUint16 did not select the aligned-offset path")
	}
	for i, got := range output {
		want := values[2*i]
		if got != want {
			t.Fatalf("output[%d] = %#x, want %#x", i, got, want)
		}
	}
}

func testFilterUint16UnalignedValues(t *testing.T, filter filterUint16Func) {
	t.Helper()

	const length = 64
	valuesBacking := make([]uint16, length+3)
	values := valuesBacking[3:]
	for i := range values {
		values[i] = uint16(i*31 + 0x8000)
	}
	filterData := make([]byte, length/8)
	for i := range filterData {
		filterData[i] = 0x55
	}

	output := make([]uint16, length/2)
	if !filter(values, output, filterData, 0, length) {
		t.Fatal("filterUint16 did not select the unaligned-values path")
	}
	for i, got := range output {
		want := values[2*i]
		if got != want {
			t.Fatalf("output[%d] = %#x, want %#x", i, got, want)
		}
	}
}

func testFilterUint16Guards(t *testing.T, filter filterUint16Func) {
	t.Helper()

	values := make([]uint16, 128)
	mixed := make([]byte, 16)
	for i := range mixed {
		mixed[i] = 0x55
	}
	output := make([]uint16, 64)

	tests := []struct {
		name   string
		filter []byte
		offset int64
		length int64
		want   bool
	}{
		{name: "short", filter: mixed, length: 56},
		{name: "non-multiple-of-eight", filter: mixed, length: 71},
		{name: "unaligned-filter-offset", filter: mixed, offset: 3, length: 64},
		{name: "negative-filter-offset", filter: mixed, offset: -8, length: 64},
		{name: "all-false", filter: make([]byte, 8), length: 64},
		{name: "all-true", filter: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, length: 64},
		{name: "too-few-mixed-bytes", filter: []byte{0x55, 0x55, 0x55, 0, 0, 0, 0, 0}, length: 64},
		{name: "out-of-bounds-filter", filter: mixed[:4], length: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter(values, output, tt.filter, tt.offset, tt.length); got != tt.want {
				t.Fatalf("filterUint16() = %t, want %t", got, tt.want)
			}
		})
	}
}
