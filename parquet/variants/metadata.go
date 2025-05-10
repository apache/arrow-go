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
	"fmt"
	"strings"
)

const (
	versionMask = 0x0F
	sortedMask  = 0x10
	offsetMask  = 0xC0

	version = 0x01
)

type decodedMetadata struct {
	keys []string
}

func (d *decodedMetadata) At(i int) (string, bool) {
	if i >= len(d.keys) {
		return "", false
	}
	return d.keys[i], true
}

func decodeMetadata(raw []byte) (*decodedMetadata, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("invalid metadata")
	}

	// Ensure the version is something recognizable.
	if ver := raw[0] & versionMask; ver != version {
		return nil, fmt.Errorf("invalid version (got %d, want %d)", ver, version)
	}

	// Get the offset size.
	offsetSize := int((raw[0] >> 6) + 1)

	// Get the number of elements in the dictionary.
	elems, err := readUint(raw, 1, offsetSize)
	if err != nil {
		return nil, err
	}

	var keys []string
	if elems > 0 {
		keys = make([]string, int(elems))
		for i := range int(elems) {
			// Offset here is the first index of the offset list, which is the
			// first element after the header and the size.
			offset := offsetSize + 1
			raw, err := readNthItem(raw, offset, i, offsetSize, int(elems))
			if err != nil {
				return nil, err
			}
			keys[i] = string(raw)
		}
	}
	return &decodedMetadata{keys: keys}, err
}

type metadataBuilder struct {
	keyToIdx map[string]int
	utf8Keys [][]byte
	keyBytes int
}

func newMetadataBuilder() *metadataBuilder {
	return &metadataBuilder{
		keyToIdx: make(map[string]int),
	}
}

func (m *metadataBuilder) Build() []byte {
	// Build the header.
	hdr := byte(version)
	offsetSize := m.calculateOffsetBytes()
	hdr |= byte(offsetSize-1) << 6

	mdSize := 1 + offsetSize*(len(m.utf8Keys)+1) + m.keyBytes

	buf := bytes.NewBuffer(make([]byte, 0, mdSize))
	buf.WriteByte(hdr)

	// Write the number of elements in the dictionary.
	encodeNumber(int64(len(m.utf8Keys)), offsetSize, buf)

	// Write all of the offsets.
	var currOffset int64
	for _, k := range m.utf8Keys {
		encodeNumber(currOffset, offsetSize, buf)
		currOffset += int64(len(k))
	}
	encodeNumber(currOffset, offsetSize, buf)

	// Write all of the keys.
	for _, k := range m.utf8Keys {
		buf.Write(k)
	}

	return buf.Bytes()
}

func (m *metadataBuilder) calculateOffsetBytes() int {
	maxNum := m.keyBytes + 1
	if dictLen := len(m.utf8Keys); dictLen > maxNum {
		maxNum = dictLen
	}
	return fieldOffsetSize(int32(maxNum))
}

// Add adds a key to the metadata dictionary if not already present, and returns the index
// that the key is present.
func (m *metadataBuilder) Add(key string) int {
	// Key already present, nothing to do.
	if idx, ok := m.keyToIdx[key]; ok {
		return idx
	}

	// Ensure the passed in string is in UTF8 form (replacing invalid sequences with
	// a replacement character), and append to the key slice.
	keyBytes := []byte(strings.ToValidUTF8(key, "\uFFFD"))
	idx := len(m.utf8Keys)
	m.keyToIdx[key] = idx
	m.utf8Keys = append(m.utf8Keys, keyBytes)
	m.keyBytes += len(keyBytes)

	return idx
}

func (m *metadataBuilder) KeyID(key string) (int, bool) {
	id, ok := m.keyToIdx[key]
	return id, ok
}
