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

package variants

import (
	"testing"
	"time"
)

func TestReadUint(t *testing.T) {
	cases := []struct {
		name    string
		raw     []byte
		offset  int
		size    int
		want    uint64
		wantErr bool
	}{
		{
			name:   "Read uint8, offset=1",
			raw:    []byte{0x00, 0x05},
			offset: 1,
			size:   1,
			want:   5,
		},
		{
			name:   "Read uint16, offset=1",
			raw:    []byte{0x00, 0x00, 0x01}, // 256
			offset: 1,
			size:   2,
			want:   256,
		},
		{
			name:   "Read uint32, offset=1",
			raw:    []byte{0x00, 0x00, 0x00, 0x01, 0x00}, // 65536
			offset: 1,
			size:   4,
			want:   65536,
		},
		{
			name:   "Read uint64, offset=1",
			raw:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}, // 4294967293
			offset: 1,
			size:   8,
			want:   4294967296,
		},
		{
			name:    "Empty raw buffer",
			offset:  0,
			size:    1,
			wantErr: true,
		},
		{
			name:    "Not enough bytes for offset",
			raw:     []byte{0x00},
			offset:  1,
			size:    1,
			wantErr: true,
		},
		{
			name:    "Not enough bytes to read",
			raw:     []byte{0x00},
			offset:  0,
			size:    2,
			wantErr: true,
		},
		{
			name:    "Invalid size 0",
			raw:     []byte{0x00},
			offset:  0,
			size:    0,
			wantErr: true,
		},
		{
			name:    "Invalid size too big",
			raw:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset:  0,
			size:    9,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readUint(c.raw, c.offset, c.size)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
			if got != c.want {
				t.Fatalf("Incorrect value returned. Got %d, want %d", got, c.want)
			}
		})
	}
}

func TestKindFromValue(t *testing.T) {
	cases := []struct {
		name string
		val  any
		want BasicType
	}{
		{
			name: "Int",
			val:  123,
			want: BasicPrimitive,
		},
		{
			name: "Int pointer",
			val: func() *int {
				a := 123
				return &a
			}(),
			want: BasicPrimitive,
		},
		{
			name: "Bool",
			val:  false,
			want: BasicPrimitive,
		},
		{
			name: "Byte slice is primitive",
			val:  []byte{'a', 'b', 'c'},
			want: BasicPrimitive,
		},
		{
			name: "Time",
			val:  time.Unix(100, 100),
			want: BasicPrimitive,
		},
		{
			name: "Struct",
			val:  struct{ a int }{1},
			want: BasicObject,
		},
		{
			name: "Struct pointer",
			val:  &struct{ a int }{1},
			want: BasicObject,
		},
		{
			name: "Slice is an array",
			val:  []int{1, 2, 3},
			want: BasicArray,
		},
		{
			name: "Map with string keys is an object",
			val:  map[string]bool{"a": true},
			want: BasicObject,
		},
		{
			name: "Map with non string keys is not supported",
			val:  map[int]string{1: "a"},
			want: BasicUndefined,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := kindFromValue(c.val); got != c.want {
				t.Fatalf("Incorrect kind. Got %s, want %s", got, c.want)
			}
		})
	}
}

func TestReadNthItem(t *testing.T) {
	cases := []struct {
		name        string
		raw         []byte
		offset      int
		item        int
		offsetSize  int
		numElements int
		want        []byte
		wantErr     bool
	}{
		{
			name:        "Third item, offset=1, offsetSize=1, width=1",
			raw:         []byte{0x00, 0x00, 0x01, 0x02, 0x03, 0xAA, 0xBB, 0xCC},
			offset:      1,
			item:        2,
			offsetSize:  1,
			numElements: 3,
			want:        []byte{0xCC},
		},
		{
			name:        "Second item, offset=1, offsetSize=2, width=1",
			raw:         []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0xAA, 0xBB, 0xCC},
			offset:      1,
			item:        1,
			offsetSize:  2,
			numElements: 3,
			want:        []byte{0xBB},
		},
		{
			name: "First item, offset=1, offsetSize=2, width=2",
			raw: []byte{
				0x00, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x06, 0x00,
				0xAA, 0xAA, 0xBB, 0xBB, 0xCC, 0xCC},
			offset:      1,
			item:        0,
			offsetSize:  2,
			numElements: 3,
			want:        []byte{0xAA, 0xAA},
		},
		{
			name: "Second item, offset=1, offsetSize=2, width=2",
			raw: []byte{
				0x00, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x06, 0x00,
				0xAA, 0xAA, 0xBB, 0xBB, 0xCC, 0xCC},
			offset:      1,
			item:        1,
			offsetSize:  2,
			numElements: 3,
			want:        []byte{0xBB, 0xBB},
		},
		{
			name:    "Offset out of bounds",
			raw:     []byte{0x00},
			offset:  1,
			wantErr: true,
		},
		{
			name:        "Item is greater than numElements",
			raw:         []byte{0x00, 0x00, 0x01, 0x02, 0x03, 0xAA, 0xBB, 0xCC},
			offset:      1,
			item:        4,
			offsetSize:  1,
			numElements: 3,
			wantErr:     true,
		},
		{
			name:        "Item is out of bounds",
			raw:         []byte{0x00, 0x01, 0x02, 0x04, 0xAA, 0xBB, 0xCC},
			offset:      0,
			item:        2,
			offsetSize:  1,
			numElements: 3,
			wantErr:     true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readNthItem(c.raw, c.offset, c.item, c.offsetSize, c.numElements)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("readNthItem(): %v", err)
			} else if c.wantErr {
				t.Fatalf("readNthItem(): wanted error, got none")
			}
			diffByteArrays(t, got, c.want)
		})
	}
}

func TestCheckBounds(t *testing.T) {
	cases := []struct {
		name      string
		raw       []byte
		low, high int
		wantErr   bool
	}{
		{
			name: "In bounds",
			raw:  make([]byte, 10),
			low:  1,
			high: 9,
		},
		{
			name: "low == high",
			raw:  make([]byte, 10),
			low:  1,
			high: 1,
		},
		{
			name:    "Out of bounds (idx == len(raw))",
			raw:     make([]byte, 10),
			low:     10,
			high:    10,
			wantErr: true,
		},
		{
			name:    "high < low",
			raw:     make([]byte, 10),
			low:     5,
			high:    1,
			wantErr: true,
		},
		{
			name:    "Negative index",
			raw:     make([]byte, 10),
			low:     -1,
			high:    1,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkBounds(c.raw, c.low, c.high)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
		})
	}
}
