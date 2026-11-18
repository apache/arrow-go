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
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/schema"
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

	// parquet-mr serializes each encrypted module into a TMemoryBuffer that starts at
	// INIT_MEM_ALLOC_ENCR_BUFFER (100) bytes and encrypts the whole backing array, so a
	// small header is followed by zeros up to 100 bytes. Pad the same way.
	const parquetMrInitBuffer = 100
	padding := make([]byte, parquetMrInitBuffer-len(thriftBytes))

	var stream bytes.Buffer
	enc.Encrypt(&stream, append(thriftBytes, padding...))
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

type pageSpec struct {
	hdr  *format.PageHeader
	body []byte
}

// encryptedChunk encrypts pages with the AADs a writer uses, numbering data
// pages from 0, and returns a page reader over them.
func encryptedChunk(t *testing.T, startWithDictionaryPage bool, nrows int64, pages ...pageSpec) *serializedPageReader {
	t.Helper()
	const key = "0123456789012345"
	encProps := parquet.NewFileEncryptionProperties(key)
	enc := encryption.NewFileEncryptor(encProps, memory.DefaultAllocator).GetFooterEncryptor()

	var stream bytes.Buffer
	var ord int16
	for _, pg := range pages {
		hdrModule, bodyModule, pageOrd := encryption.DataPageHeaderModule, encryption.DataPageModule, ord
		if pg.hdr.Type == format.PageType_DICTIONARY_PAGE {
			hdrModule, bodyModule, pageOrd = encryption.DictPageHeaderModule, encryption.DictPageModule, -1
		} else {
			ord++
		}

		var body bytes.Buffer
		enc.UpdateAad(encryption.CreateModuleAad(encProps.FileAad(), bodyModule, 0, 0, pageOrd))
		enc.Encrypt(&body, pg.body)
		pg.hdr.CompressedPageSize, pg.hdr.UncompressedPageSize = int32(body.Len()), int32(len(pg.body))
		hdrBytes, err := thrift.NewThriftSerializer().Write(context.Background(), pg.hdr)
		require.NoError(t, err)

		enc.UpdateAad(encryption.CreateModuleAad(encProps.FileAad(), hdrModule, 0, 0, pageOrd))
		enc.Encrypt(&stream, hdrBytes)
		stream.Write(body.Bytes())
	}

	fdec := encryption.NewFileDecryptor(parquet.NewFileDecryptionProperties(parquet.WithFooterKey(key)),
		encProps.FileAad(), parquet.AesGcm, "", memory.DefaultAllocator)
	rd, err := parquet.NewReaderProperties(memory.DefaultAllocator).
		GetStream(bytes.NewReader(stream.Bytes()), 0, int64(stream.Len()))
	require.NoError(t, err)

	p, err := NewPageReader(rd, nrows, compress.Codecs.Uncompressed, memory.DefaultAllocator, &CryptoContext{
		StartDecryptWithDictionaryPage: startWithDictionaryPage,
		MetaDecryptor:                  fdec.GetFooterDecryptorForColumnMeta(""),
		DataDecryptor:                  fdec.GetFooterDecryptorForColumnData(""),
	})
	require.NoError(t, err)
	return p.(*serializedPageReader)
}

func dictPageSpec(values ...int32) pageSpec {
	hdr := format.NewPageHeader()
	hdr.Type = format.PageType_DICTIONARY_PAGE
	hdr.DictionaryPageHeader = format.NewDictionaryPageHeader()
	hdr.DictionaryPageHeader.NumValues = int32(len(values))
	hdr.DictionaryPageHeader.Encoding = format.Encoding_PLAIN
	body := make([]byte, 4*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint32(body[4*i:], uint32(v))
	}
	return pageSpec{hdr, body}
}

func dataPageSpec(numValues int32, enc format.Encoding, body []byte) pageSpec {
	hdr := format.NewPageHeader()
	hdr.Type = format.PageType_DATA_PAGE
	hdr.DataPageHeader = format.NewDataPageHeader()
	hdr.DataPageHeader.NumValues = numValues
	hdr.DataPageHeader.Encoding = enc
	return pageSpec{hdr, body}
}

func requiredInt32Column(t *testing.T, p PageReader) *Int32ColumnChunkReader {
	t.Helper()
	sc := schema.NewColumn(schema.NewInt32Node("a", parquet.Repetitions.Required, -1), 0, 0)
	return NewColumnReader(sc, p, memory.DefaultAllocator, nil).(*Int32ColumnChunkReader)
}

// Pages of an encrypted chunk are decrypted with AADs that name their ordinal,
// so a probe of the first page after other pages were read would fail to
// authenticate.
func TestEncryptedChunkWithoutDictionaryIsNotProbed(t *testing.T) {
	p := encryptedChunk(t, false, 3, dataPageSpec(3, format.Encoding_PLAIN, []byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}))
	cr := requiredInt32Column(t, p)

	vals := make([]int32, 3)
	_, n, err := cr.ReadBatch(3, vals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 2, 3}, vals[:n])

	dict, err := p.GetDictionaryPage()
	require.NoError(t, err)
	require.Nil(t, dict)
}

// A seek to row 0 of an encrypted chunk with no dictionary page offset still
// loads the dictionary before decoding the first data page.
func TestSeekToRowZeroOnEncryptedChunkStartingWithDictionaryPage(t *testing.T) {
	// RLE_DICTIONARY indices 0,1,1,0,0,0,1,0 at bit width 1
	indices := []byte{1, 3, 0x46}
	p := encryptedChunk(t, true, 8, dictPageSpec(10, 20), dataPageSpec(8, format.Encoding_RLE_DICTIONARY, indices))
	cr := requiredInt32Column(t, p)

	require.NoError(t, cr.SeekToRow(0))
	vals := make([]int32, 8)
	_, n, err := cr.ReadBatch(8, vals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int32{10, 20, 20, 10, 10, 10, 20, 10}, vals[:n])
}

// A seek goes back to the dictionary page, whose header takes the dictionary AAD
// again, whether or not the chunk records a dictionary page offset.
func TestReseekOnEncryptedChunkWithSharedPageOffset(t *testing.T) {
	// RLE_DICTIONARY indices 0,1,1,0,0,0,1,0 at bit width 1
	indices := []byte{1, 3, 0x46}
	want := []int32{10, 20, 20, 10, 10, 10, 20, 10}

	for _, tc := range []struct {
		name                               string
		baseOffset, dataOffset, dictOffset int64
	}{
		{"SharedPageOffset", 1, 1, 1},
		{"NoDictionaryOffset", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := encryptedChunk(t, true, 8, dictPageSpec(10, 20), dataPageSpec(8, format.Encoding_RLE_DICTIONARY, indices))
			p.baseOffset, p.dataOffset, p.dictOffset = tc.baseOffset, tc.dataOffset, tc.dictOffset
			cr := requiredInt32Column(t, p)
			t.Cleanup(func() { require.NoError(t, cr.Close()) })

			vals := make([]int32, 8)
			_, n, err := cr.ReadBatch(8, vals, nil, nil)
			require.NoError(t, err)
			require.Equal(t, want, vals[:n])

			require.NoError(t, cr.SeekToRow(0))
			_, n, err = cr.ReadBatch(8, vals, nil, nil)
			require.NoError(t, err)
			require.Equal(t, want, vals[:n])
		})
	}
}

type countingReader struct {
	utils.Reader
	read *int
}

func (c countingReader) ReadAt(b []byte, off int64) (int, error) {
	n, err := c.Reader.ReadAt(b, off)
	*c.read += n
	return n, err
}

type countingBufferedReader struct {
	parquet.BufferedReader
	outer countingReader
}

func (c countingBufferedReader) Outer() utils.Reader { return c.outer }

func unencryptedChunkReader(t *testing.T, nrows int64, chunk []byte) (*serializedPageReader, *int) {
	t.Helper()
	read := new(int)
	br := utils.NewByteReader(chunk)
	p, err := NewPageReader(countingBufferedReader{br, countingReader{br, read}}, nrows,
		compress.Codecs.Uncompressed, memory.DefaultAllocator, nil)
	require.NoError(t, err)
	return p.(*serializedPageReader), read
}

func serializePage(t *testing.T, pg pageSpec) []byte {
	t.Helper()
	pg.hdr.CompressedPageSize, pg.hdr.UncompressedPageSize = int32(len(pg.body)), int32(len(pg.body))
	hdrBytes, err := thrift.NewThriftSerializer().Write(context.Background(), pg.hdr)
	require.NoError(t, err)
	return append(hdrBytes, pg.body...)
}

func TestGetDictionaryPageWithoutRecordedOffset(t *testing.T) {
	malformed := dataPageSpec(1, format.Encoding_PLAIN, make([]byte, 8))
	malformed.hdr.DictionaryPageHeader = format.NewDictionaryPageHeader()
	malformed.hdr.DictionaryPageHeader.NumValues = 2
	malformed.hdr.DictionaryPageHeader.Encoding = format.Encoding_PLAIN

	tests := []struct {
		name  string
		chunk []byte
	}{
		{"empty chunk", nil},
		{"first page is a data page", serializePage(t, dataPageSpec(1, format.Encoding_PLAIN, make([]byte, 4)))},
		{"data page carrying a dictionary header", serializePage(t, malformed)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := unencryptedChunkReader(t, 1, tt.chunk)
			dict, err := p.GetDictionaryPage()
			require.NoError(t, err)
			require.Nil(t, dict)
		})
	}
}

// The probe must not buffer the chunk, since the stream of a default reader
// holds the whole chunk and a buffer that large is allocated and filled per seek.
func TestGetDictionaryPageWithoutRecordedOffsetReadsOnlyTheDictionary(t *testing.T) {
	const dictValues, bodyBytes = 16 * 1024, 1 << 20
	values := make([]int32, dictValues)
	for i := range values {
		values[i] = int32(i)
	}
	dict := serializePage(t, dictPageSpec(values...))
	chunk := append(dict, serializePage(t, dataPageSpec(1, format.Encoding_PLAIN, make([]byte, bodyBytes)))...)

	p, read := unencryptedChunkReader(t, 1, chunk)
	got, err := p.GetDictionaryPage()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.EqualValues(t, dictValues, got.NumValues())
	require.Less(t, *read, 2*len(dict), "read %d of %d chunk bytes for a %d byte dictionary page", *read, len(chunk), len(dict))
}
