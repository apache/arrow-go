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

package file

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/require"
)

func TestPageReaderRejectsOversizedPages(t *testing.T) {
	p := &serializedPageReader{
		maxCompressedPageSize:   64,
		maxUncompressedPageSize: 128,
	}

	tests := []struct {
		name         string
		compressed   int32
		uncompressed int32
		match        string
	}{
		{"compressed", 65, 128, "compressed page size 65 exceeds configured limit 64"},
		{"uncompressed", 1, 129, "uncompressed page size 129 exceeds configured limit 128"},
		{"maximum int32", math.MaxInt32, math.MaxInt32, "exceeds configured limit"},
		{"negative", -1, 1, "negative page size"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := format.NewPageHeader()
			hdr.CompressedPageSize = tt.compressed
			hdr.UncompressedPageSize = tt.uncompressed

			_, _, err := p.validatePageSizes(hdr)
			require.ErrorContains(t, err, tt.match)
		})
	}
}

func TestReadLevelDataRLELengthBound(t *testing.T) {
	p := &serializedPageReader{maxDefLevel: 1, mem: memory.DefaultAllocator}

	// a complete 8-byte RLE level region, so the reject below is the bound, not EOF
	data := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(data[:4], 8)

	_, err := p.readLevelData(bytes.NewReader(data), format.Encoding_RLE, format.Encoding_RLE, 4, 8)
	require.Error(t, err)

	out, err := p.readLevelData(bytes.NewReader(data), format.Encoding_RLE, format.Encoding_RLE, 4, 1024)
	require.NoError(t, err)
	require.Equal(t, 12, out.Len())
	out.Release()
}

func TestReadLevelDataBitPacked(t *testing.T) {
	p := &serializedPageReader{maxDefLevel: 1, mem: memory.DefaultAllocator}

	// maxDef 1 -> bitWidth 1; 8 values -> BytesForBits(8) = 1 byte, no length prefix
	data := []byte{0b10110100}
	out, err := p.readLevelData(bytes.NewReader(data), format.Encoding_RLE, format.Encoding_BIT_PACKED, 8, 1024)
	require.NoError(t, err)
	require.Equal(t, data, out.Bytes())
	out.Release()
}
