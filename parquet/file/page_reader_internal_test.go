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
	"context"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
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

// parquet-mr zero-pads every encrypted thrift module out to its buffer size, so a
// decrypted page header is the thrift struct followed by zero bytes. The reader has
// to move past the whole encrypted module, not just the thrift it parsed.
func TestReadEncryptedPageHeaderWithTrailingPadding(t *testing.T) {
	const key = "0123456789012345"
	encProps := parquet.NewFileEncryptionProperties(key)
	enc := encryption.NewFileEncryptor(encProps, memory.DefaultAllocator).GetFooterEncryptor()
	enc.UpdateAad(encryption.CreateModuleAad(encProps.FileAad(), encryption.DictPageHeaderModule, 0, 0, -1))

	hdr := format.NewPageHeader()
	hdr.Type = format.PageType_DICTIONARY_PAGE
	hdr.CompressedPageSize, hdr.UncompressedPageSize = 10, 10
	hdr.DictionaryPageHeader = format.NewDictionaryPageHeader()
	thriftBytes, err := thrift.NewThriftSerializer().Write(context.Background(), hdr)
	require.NoError(t, err)

	var stream bytes.Buffer
	enc.Encrypt(&stream, append(thriftBytes, make([]byte, 76)...))
	stream.WriteString("page data!")

	dec := encryption.NewFileDecryptor(parquet.NewFileDecryptionProperties(parquet.WithFooterKey(key)),
		encProps.FileAad(), parquet.AesGcm, "", memory.DefaultAllocator).GetFooterDecryptor()
	rd, err := parquet.NewReaderProperties(memory.DefaultAllocator).
		GetStream(bytes.NewReader(stream.Bytes()), 0, int64(stream.Len()))
	require.NoError(t, err)

	p := &serializedPageReader{
		maxPageHeaderSize: defaultMaxPageHeaderSize,
		cryptoCtx:         CryptoContext{StartDecryptWithDictionaryPage: true, MetaDecryptor: dec},
	}
	got := format.NewPageHeader()
	require.NoError(t, p.readPageHeader(rd, got))
	require.Equal(t, format.PageType_DICTIONARY_PAGE, got.Type)

	rest, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "page data!", string(rest))
}
