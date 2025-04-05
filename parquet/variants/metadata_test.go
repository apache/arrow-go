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
	"bytes"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBuildMetadata(t *testing.T) {
	cases := []struct {
		name        string
		keys        []string
		wantKeys    []string
		wantEncoded []byte
	}{
		{
			name: "No keys added",
			wantEncoded: []byte{
				0b00000001, // Header: offset_size = 1, not sorted, version = 1
				0x00,       // Dictionary size of 0
				0x00,       // First (and last) index of elements in the empty dictionary
			},
		},
		{
			name:     "Small number of items",
			keys:     []string{"b", "c", "a"},
			wantKeys: []string{"b", "c", "a"},
			wantEncoded: []byte{
				0b00000001, // Header: offset_size = 1, not sorted, version = 1
				0x03,       // Dictionary size of 3
				0x00,       // Index of first item "b"
				0x01,       // Index of second item "c"
				0x02,       // Index of third item "a"
				0x03,       // First index after the last item
				'b',
				'c',
				'a',
			},
		},
		{
			name:     "Dedupe similar keys",
			keys:     []string{"b", "c", "a", "a", "a", "a", "b", "b", "c", "c", "c"},
			wantKeys: []string{"b", "c", "a"},
			wantEncoded: []byte{
				0b00000001, // Header: offset_size = 1, not sorted, version = 1
				0x03,       // Dictionary size of 3
				0x00,       // Index of first item "b"
				0x01,       // Index of second item "c"
				0x02,       // Index of third item "a"
				0x03,       // First index after the last item
				'b',
				'c',
				'a',
			},
		},
		{
			name: "Large number of keys (encoded in more than one byte)",
			keys: func() []string {
				keys := make([]string, 26*26)
				idx := 0
				for i := range 26 {
					for j := range 26 {
						keys[idx] = string([]byte{byte('a' + i), byte('a' + j)})
						idx++
					}
				}
				return keys
			}(),
			wantKeys:    largeKeysString(),
			wantEncoded: largeEncodedMetadata(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mdb := newMetadataBuilder()
			for _, k := range c.keys {
				mdb.Add(k)
			}
			md := mdb.Build()

			diffByteArrays(t, md, c.wantEncoded)

			// Decode and check keys. This does a bit of double duty and a lot of these cases are
			// covered in TestDecodeMetadata below, but it is useful to prove that anything that
			// can be encoded can also be decoded.
			decoded, err := decodeMetadata(md)
			if err != nil {
				t.Fatalf("decodeMetadata(): %v", err)
			}
			if diff := cmp.Diff(decoded.keys, c.wantKeys); diff != "" {
				t.Fatalf("Received incorrect keys. Diff (-want +got):\n%s", diff)
			}
		})
	}
}

// Helpers to create a Metadata struct that has a large list of keys (26^2) that will
// take more than one byte to encode.
func largeListKeyBytes() [][]byte {
	keys := make([][]byte, 26*26)
	idx := 0
	for i := range 26 {
		for j := range 26 {
			keys[idx] = []byte{byte('a' + i), byte('a' + j)}
			idx++
		}
	}
	return keys
}

func largeEncodedMetadata() []byte {
	// Offset size = 2
	// Total size of encoded metadata is:
	//   * Header: 1
	//   * Number of elements: 2
	//   * Offset table: (26*26 + 1)*2
	//   * Elements: 26*26*2
	buf := bytes.NewBuffer(make([]byte, 0, 1+2+(26*26+1)*2+(26*26*2)))
	buf.WriteByte(0b01000001) // offset_size_minus_one = 1, is_sorted = false, version = 1

	encodeNumber(676, 2, buf) // Encode the number of elements

	// Encode the offsets. NB: each key is 2 bytes.
	for i := range 676 + 1 {
		encodeNumber(int64(i*2), 2, buf)
	}

	for _, k := range largeListKeyBytes() {
		buf.Write(k)
	}

	return buf.Bytes()
}

func largeKeysString() []string {
	rawKeys := largeListKeyBytes()
	keys := make([]string, len(rawKeys))
	for i, k := range rawKeys {
		keys[i] = string(k)
	}
	return keys
}

// This test does duplicate some coverage provided in TestBuildMetadata, but is specifically
// focused on the decoding side of things.
func TestDecodeMetadata(t *testing.T) {
	cases := []struct {
		name    string
		raw     []byte
		want    []string
		wantErr bool
	}{
		{
			name: "Valid metadata with no elements",
			raw: []byte{
				0x01, // Base header, version = 1,
				0x00, // Zero length
				0x00, // First and last element
			},
		},
		{
			name: "Valid metadata, large number of elements",
			raw:  largeEncodedMetadata(),
			want: largeKeysString(),
		},
		{
			name:    "Zero length metadata",
			wantErr: true,
		},
		{
			name:    "Invalid version: 0",
			raw:     []byte{0x00, 0x00},
			wantErr: true,
		},
		{
			name:    "Invalid version: 2",
			raw:     []byte{0x02, 0x00},
			wantErr: true,
		},
		{
			name:    "Bad number of elements",
			raw:     []byte{0b11000001, 0x00}, // Offset size = 4, should be out of bounds read
			wantErr: true,
		},
		{
			name:    "Missing elements",
			raw:     []byte{0x01, 0x02, 0x00, 0x01, 0x02},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeMetadata(c.raw)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatal("Got no error when one was expected")
			}
			if diff := cmp.Diff(got.keys, c.want); diff != "" {
				t.Fatalf("Received incorrect keys. Diff (-want +got):\n%s", diff)
			}
		})
	}
}

func buildRandomKey(l int) string {
	buf := make([]byte, l)
	// Create a random ascii character between decimal 33 (!) and decimal 126 (~)
	for i := range l {
		randChar := byte(rand.Intn(94)) + 33
		buf[i] = randChar
	}
	return string(buf)
}

func TestOffsetCalculation(t *testing.T) {
	cases := []struct {
		name    string
		keyLen  int
		numKeys int
		wantHdr byte
	}{
		{
			name:    "Offset length 1",
			keyLen:  1,
			numKeys: 1,
			wantHdr: 0b00000001,
		},
		{
			name:    "Offset length 2",
			keyLen:  4,
			numKeys: 256,
			wantHdr: 0b01000001,
		},
		{
			name:    "Offset length 3",
			keyLen:  1<<16 + 1,
			numKeys: 1,
			wantHdr: 0b10000001,
		},
		{
			name:    "Offset length 4",
			keyLen:  1<<24 + 1,
			numKeys: 1,
			wantHdr: 0b11000001,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mdb := newMetadataBuilder()
			for range c.numKeys {
				mdb.Add(buildRandomKey(c.keyLen))
			}
			md := mdb.Build()
			if got, want := md[0], c.wantHdr; got != want {
				t.Fatalf("Incorrect header: got %x, want %x", got, want)
			}
		})
	}
}
