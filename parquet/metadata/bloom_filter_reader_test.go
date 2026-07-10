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

package metadata_test

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// shortReadAtSeeker deliberately returns short successful ReadAt calls to
// exercise callers that must keep reading until their requested buffer is full.
type shortReadAtSeeker struct {
	data   []byte
	offset int64
	max    int
}

func (r *shortReadAtSeeker) ReadAt(p []byte, off int64) (int, error) {
	return r.readAt(p, off)
}

func (r *shortReadAtSeeker) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = int64(len(r.data)) + offset
	default:
		return 0, os.ErrInvalid
	}
	if next < 0 {
		return 0, os.ErrInvalid
	}
	r.offset = next
	return next, nil
}

func (r *shortReadAtSeeker) readAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}

	n := len(p)
	if r.max > 0 && n > r.max {
		n = r.max
	}
	if remaining := len(r.data) - int(off); n > remaining {
		n = remaining
	}
	start := int(off)
	copy(p, r.data[start:start+n])
	if n < len(p) && int(off)+n >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

type BloomFilterBuilderSuite struct {
	suite.Suite

	sc    *schema.Schema
	props *parquet.WriterProperties
	mem   *memory.CheckedAllocator
	buf   bytes.Buffer
}

func (suite *BloomFilterBuilderSuite) SetupTest() {
	suite.props = parquet.NewWriterProperties()
	suite.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
	suite.buf.Reset()
}

func (suite *BloomFilterBuilderSuite) TearDownTest() {
	runtime.GC() // we use setfinalizer to clean up the buffers, so run the GC
	runtime.GC()
	suite.mem.AssertSize(suite.T(), 0)
}

func (suite *BloomFilterBuilderSuite) TestSingleRowGroup() {
	suite.sc = schema.NewSchema(schema.MustGroup(
		schema.NewGroupNode("schema", parquet.Repetitions.Repeated,
			schema.FieldList{
				schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c2", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c3", parquet.Repetitions.Optional, -1),
			}, -1)))

	bldr := metadata.FileBloomFilterBuilder{Schema: suite.sc}

	metaBldr := metadata.NewFileMetadataBuilder(suite.sc, suite.props, nil)

	{
		rgMeta := metaBldr.AppendRowGroup()
		filterMap := make(map[string]metadata.BloomFilterBuilder)
		bldr.AppendRowGroup(rgMeta, filterMap)

		bf1 := metadata.NewBloomFilter(32, 1024, suite.mem)
		bf2 := metadata.NewAdaptiveBlockSplitBloomFilter(1024, 5, 0.01, suite.sc.Column(2), suite.mem)

		h1, h2 := bf1.Hasher(), bf2.Hasher()

		rgMeta.NextColumnChunk()
		rgMeta.NextColumnChunk()
		rgMeta.NextColumnChunk()
		rgMeta.Finish(0, 0)

		bf1.InsertHash(metadata.GetHash(h1, parquet.ByteArray("Hello")))
		bf2.InsertHash(metadata.GetHash(h2, parquet.ByteArray("World")))
		filterMap["c1"] = bf1
		filterMap["c3"] = bf2

		wr := &utils.TellWrapper{Writer: &suite.buf}
		wr.Write([]byte("PAR1")) // offset of 0 means unset, so write something
		// to force the offset to be set as a non-zero value
		suite.Require().NoError(bldr.WriteTo(wr))
	}
	runtime.GC()

	finalMeta, err := metaBldr.Finish()
	suite.Require().NoError(err)
	{
		bufferPool := &sync.Pool{
			New: func() interface{} {
				buf := memory.NewResizableBuffer(suite.mem)
				runtime.SetFinalizer(buf, func(obj *memory.Buffer) {
					obj.Release()
				})
				return buf
			},
		}

		rdr := metadata.BloomFilterReader{
			Input:        bytes.NewReader(suite.buf.Bytes()),
			FileMetadata: finalMeta,
			BufferPool:   bufferPool,
		}

		bfr, err := rdr.RowGroup(0)
		suite.Require().NoError(err)
		suite.Require().NotNil(bfr)

		{
			bf1, err := bfr.GetColumnBloomFilter(0)
			suite.Require().NoError(err)
			suite.Require().NotNil(bf1)
			suite.False(bf1.CheckHash(metadata.GetHash(bf1.Hasher(), parquet.ByteArray("World"))))
			suite.True(bf1.CheckHash(metadata.GetHash(bf1.Hasher(), parquet.ByteArray("Hello"))))
		}
		runtime.GC() // force GC to run to put the buffer back into the pool
		runtime.GC() // finalizers need two GC cycles to run reliably
		{
			bf2, err := bfr.GetColumnBloomFilter(1)
			suite.Require().NoError(err)
			suite.Require().Nil(bf2)
		}
		{
			bf3, err := bfr.GetColumnBloomFilter(2)
			suite.Require().NoError(err)
			suite.Require().NotNil(bf3)
			suite.False(bf3.CheckHash(metadata.GetHash(bf3.Hasher(), parquet.ByteArray("Hello"))))
			suite.True(bf3.CheckHash(metadata.GetHash(bf3.Hasher(), parquet.ByteArray("World"))))
		}
		runtime.GC() // we're using setfinalizer, so force release
		runtime.GC() // finalizers need two GC cycles to run reliably
	}
	runtime.GC()
	runtime.GC() // finalizers need two GC cycles to run reliably
}

const (
	FooterEncryptionKey    = "0123456789012345"
	ColumnEncryptionKey1   = "1234567890123450"
	ColumnEncryptionKey2   = "1234567890123451"
	FooterEncryptionKeyID  = "kf"
	ColumnEncryptionKey1ID = "kc1"
	ColumnEncryptionKey2ID = "kc2"
)

type EncryptedBloomFilterBuilderSuite struct {
	suite.Suite

	sc           *schema.Schema
	props        *parquet.WriterProperties
	decryptProps *parquet.FileDecryptionProperties
	mem          *memory.CheckedAllocator
	buf          bytes.Buffer
}

func (suite *EncryptedBloomFilterBuilderSuite) SetupTest() {
	encryptedCols := parquet.ColumnPathToEncryptionPropsMap{
		"c1": parquet.NewColumnEncryptionProperties("c1",
			parquet.WithKey(ColumnEncryptionKey1), parquet.WithKeyID(ColumnEncryptionKey1ID)),
		"c2": parquet.NewColumnEncryptionProperties("c2",
			parquet.WithKey(ColumnEncryptionKey2), parquet.WithKeyID(ColumnEncryptionKey2ID)),
	}

	encProps := parquet.NewFileEncryptionProperties(FooterEncryptionKey,
		parquet.WithFooterKeyID(FooterEncryptionKeyID),
		parquet.WithEncryptedColumns(encryptedCols))

	suite.decryptProps = parquet.NewFileDecryptionProperties(
		parquet.WithFooterKey(FooterEncryptionKey),
		parquet.WithColumnKeys(parquet.ColumnPathToDecryptionPropsMap{
			"c1": parquet.NewColumnDecryptionProperties("c1", parquet.WithDecryptKey(ColumnEncryptionKey1)),
			"c2": parquet.NewColumnDecryptionProperties("c2", parquet.WithDecryptKey(ColumnEncryptionKey2)),
		}))

	suite.props = parquet.NewWriterProperties(parquet.WithEncryptionProperties(encProps))
	suite.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
	suite.buf.Reset()
}

func (suite *EncryptedBloomFilterBuilderSuite) TearDownTest() {
	runtime.GC() // we use setfinalizer to clean up the buffers, so run the GC
	runtime.GC() // finalizers need two GC cycles to run reliably
	suite.mem.AssertSize(suite.T(), 0)
}

func (suite *EncryptedBloomFilterBuilderSuite) TestEncryptedBloomFilters() {
	suite.sc = schema.NewSchema(schema.MustGroup(
		schema.NewGroupNode("schema", parquet.Repetitions.Repeated,
			schema.FieldList{
				schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c2", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c3", parquet.Repetitions.Optional, -1),
			}, -1)))

	encryptor := encryption.NewFileEncryptor(suite.props.FileEncryptionProperties(), suite.mem)
	metaBldr := metadata.NewFileMetadataBuilder(suite.sc, suite.props, nil)
	metaBldr.SetFileEncryptor(encryptor)
	bldr := metadata.FileBloomFilterBuilder{Schema: suite.sc, Encryptor: encryptor}
	{
		rgMeta := metaBldr.AppendRowGroup()
		filterMap := make(map[string]metadata.BloomFilterBuilder)
		bldr.AppendRowGroup(rgMeta, filterMap)

		bf1 := metadata.NewBloomFilter(32, 1024, suite.mem)
		bf2 := metadata.NewAdaptiveBlockSplitBloomFilter(1024, 5, 0.01, suite.sc.Column(1), suite.mem)
		h1, h2 := bf1.Hasher(), bf2.Hasher()

		bf1.InsertHash(metadata.GetHash(h1, parquet.ByteArray("Hello")))
		bf2.InsertHash(metadata.GetHash(h2, parquet.ByteArray("World")))
		filterMap["c1"] = bf1
		filterMap["c2"] = bf2

		colChunk1 := rgMeta.NextColumnChunk()
		colChunk1.Finish(metadata.ChunkMetaInfo{}, false, false, metadata.EncodingStats{})

		colChunk2 := rgMeta.NextColumnChunk()
		colChunk2.Finish(metadata.ChunkMetaInfo{}, false, false, metadata.EncodingStats{})

		colChunk3 := rgMeta.NextColumnChunk()
		colChunk3.Finish(metadata.ChunkMetaInfo{}, false, false, metadata.EncodingStats{})

		wr := &utils.TellWrapper{Writer: &suite.buf}
		wr.Write([]byte("PAR1")) // offset of 0 means unset, so write something
		// to force the offset to be set as a non-zero value
		suite.Require().NoError(bldr.WriteTo(wr))

		rgMeta.Finish(0, 0)
	}

	finalMeta, err := metaBldr.Finish()
	suite.Require().NoError(err)
	finalMeta.FileDecryptor = encryption.NewFileDecryptor(suite.decryptProps,
		suite.props.FileEncryptionProperties().FileAad(),
		suite.props.FileEncryptionProperties().Algorithm().Algo, "", suite.mem)
	{
		bufferPool := &sync.Pool{
			New: func() interface{} {
				buf := memory.NewResizableBuffer(suite.mem)
				runtime.SetFinalizer(buf, func(obj *memory.Buffer) {
					obj.Release()
				})
				return buf
			},
		}
		defer runtime.GC()

		rdr := metadata.BloomFilterReader{
			Input:         bytes.NewReader(suite.buf.Bytes()),
			FileMetadata:  finalMeta,
			BufferPool:    bufferPool,
			FileDecryptor: finalMeta.FileDecryptor,
		}

		bfr, err := rdr.RowGroup(0)
		suite.Require().NoError(err)
		suite.Require().NotNil(bfr)

		{
			bf1, err := bfr.GetColumnBloomFilter(0)
			suite.Require().NoError(err)
			suite.Require().NotNil(bf1)
			suite.False(bf1.CheckHash(metadata.GetHash(bf1.Hasher(), parquet.ByteArray("World"))))
			suite.True(bf1.CheckHash(metadata.GetHash(bf1.Hasher(), parquet.ByteArray("Hello"))))
		}
	}
}

func TestBloomFilterRoundTrip(t *testing.T) {
	suite.Run(t, new(BloomFilterBuilderSuite))
	suite.Run(t, new(EncryptedBloomFilterBuilderSuite))
}

func TestBloomFilterReaderHandlesShortReads(t *testing.T) {
	data, fileMeta, insertedHash := makeBloomFilterReaderTestData(t, 1024)

	t.Run("with bloom filter length", func(t *testing.T) {
		bf := readBloomFilterWithShortReads(t, data, fileMeta, 7)
		assert.True(t, bf.CheckHash(insertedHash))
	})

	t.Run("without bloom filter length", func(t *testing.T) {
		data, fileMeta, insertedHash := makeBloomFilterReaderTestData(t, 1024)
		fileMeta.RowGroups[0].Columns[0].MetaData.BloomFilterLength = nil

		bf := readBloomFilterWithShortReads(t, data, fileMeta, 256)
		assert.True(t, bf.CheckHash(insertedHash))
	})

	t.Run("without bloom filter length and small section", func(t *testing.T) {
		data, fileMeta, insertedHash := makeBloomFilterReaderTestData(t, 32)
		fileMeta.RowGroups[0].Columns[0].MetaData.BloomFilterLength = nil

		bf := readBloomFilterWithShortReads(t, data, fileMeta, 7)
		assert.True(t, bf.CheckHash(insertedHash))
	})
}

func makeBloomFilterReaderTestData(t *testing.T, filterSize uint32) ([]byte, *metadata.FileMetaData, uint64) {
	t.Helper()

	sc := schema.NewSchema(schema.MustGroup(
		schema.NewGroupNode("schema", parquet.Repetitions.Repeated,
			schema.FieldList{
				schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
			}, -1)))

	props := parquet.NewWriterProperties()
	bldr := metadata.FileBloomFilterBuilder{Schema: sc}
	metaBldr := metadata.NewFileMetadataBuilder(sc, props, nil)
	rgMeta := metaBldr.AppendRowGroup()
	filterMap := make(map[string]metadata.BloomFilterBuilder)
	bldr.AppendRowGroup(rgMeta, filterMap)

	bf := metadata.NewBloomFilter(filterSize, filterSize, memory.NewGoAllocator())
	insertedHash := (uint64(^uint32(0)) << 32) | 1
	bf.InsertHash(insertedHash)
	filterMap["c1"] = bf

	rgMeta.NextColumnChunk()
	require.NoError(t, rgMeta.Finish(0, 0))

	var buf bytes.Buffer
	wr := &utils.TellWrapper{Writer: &buf}
	_, err := wr.Write([]byte("PAR1"))
	require.NoError(t, err)
	require.NoError(t, bldr.WriteTo(wr))

	fileMeta, err := metaBldr.Finish()
	require.NoError(t, err)
	fileMeta.SetSourceFileSize(int64(buf.Len()))

	return buf.Bytes(), fileMeta, insertedHash
}

func readBloomFilterWithShortReads(t *testing.T, data []byte, fileMeta *metadata.FileMetaData, maxRead int) metadata.BloomFilter {
	t.Helper()

	rdr := metadata.BloomFilterReader{
		Input:        &shortReadAtSeeker{data: data, max: maxRead},
		FileMetadata: fileMeta,
		BufferPool: &sync.Pool{
			New: func() interface{} {
				return memory.NewResizableBuffer(memory.DefaultAllocator)
			},
		},
	}

	rg, err := rdr.RowGroup(0)
	require.NoError(t, err)
	require.NotNil(t, rg)

	bf, err := rg.GetColumnBloomFilter(0)
	require.NoError(t, err)
	require.NotNil(t, bf)
	return bf
}

func TestReadBloomFilter(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	files := []string{"data_index_bloom_encoding_stats.parquet",
		"data_index_bloom_encoding_with_length.parquet"}

	for _, testfile := range files {
		t.Run(testfile, func(t *testing.T) {
			rdr, err := file.OpenParquetFile(dir+"/"+testfile, false)
			require.NoError(t, err)
			defer rdr.Close()

			bloomFilterRdr := rdr.GetBloomFilterReader()
			rg0, err := bloomFilterRdr.RowGroup(0)
			require.NoError(t, err)
			require.NotNil(t, rg0)

			rg1, err := bloomFilterRdr.RowGroup(1)
			assert.Nil(t, rg1)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "row group index 1 out of range")

			bf, err := rg0.GetColumnBloomFilter(0)
			require.NoError(t, err)
			require.NotNil(t, bf)

			bf1, err := rg0.GetColumnBloomFilter(1)
			assert.Nil(t, bf1)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "column index 1 out of range")

			baBloomFilter := metadata.TypedBloomFilter[parquet.ByteArray]{bf}
			assert.True(t, baBloomFilter.Check([]byte("Hello")))
			assert.False(t, baBloomFilter.Check([]byte("NOT_EXISTS")))
		})
	}
}

func TestBloomFilterReaderFileNotHaveFilter(t *testing.T) {
	// can still get a BloomFilterReader and a RowGroupBloomFilterReader
	// but cannot get a non-null BloomFilter
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	rdr, err := file.OpenParquetFile(dir+"/alltypes_plain.parquet", false)
	require.NoError(t, err)
	defer rdr.Close()

	bloomFilterRdr := rdr.GetBloomFilterReader()
	rg0, err := bloomFilterRdr.RowGroup(0)
	require.NoError(t, err)
	require.NotNil(t, rg0)

	rg1, err := bloomFilterRdr.RowGroup(1)
	assert.Nil(t, rg1)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "row group index 1 out of range")

	bf, err := rg0.GetColumnBloomFilter(0)
	require.NoError(t, err)
	require.Nil(t, bf)
}
