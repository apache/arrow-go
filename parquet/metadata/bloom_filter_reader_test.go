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
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/suite"
)

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
	}
	runtime.GC()
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
