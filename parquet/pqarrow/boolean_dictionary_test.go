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

package pqarrow_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// dictionaryBooleanFile builds a file whose BOOLEAN column chunk has a
// dictionary page and an RLE_DICTIONARY data page. The file writer never
// dictionary encodes BOOLEAN, so the pages are written with the page writer.
func dictionaryBooleanFile(t *testing.T) []byte {
	t.Helper()

	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{schema.NewBooleanNode("b", parquet.Repetitions.Required, -1)}, -1)))

	metaBldr := metadata.NewFileMetadataBuilder(sc, nil, nil)
	rgMeta := metaBldr.AppendRowGroup()
	rgMeta.SetNumRows(3)

	var buf bytes.Buffer
	sink := &utils.TellWrapper{Writer: &buf}
	_, err := sink.Write([]byte("PAR1"))
	require.NoError(t, err)

	pager, err := file.NewPageWriter(sink, compress.Codecs.Uncompressed, compress.DefaultCompressionLevel,
		rgMeta.NextColumnChunk(), 0, 0, memory.DefaultAllocator, false, nil, nil)
	require.NoError(t, err)

	// dictionary [false, true], then indexes 1, 0, 1 as bit width 1, a
	// bit-packed run header of one group, and the packed bits
	dict := memory.NewBufferBytes([]byte{0b10})
	_, err = pager.WriteDictionaryPage(file.NewDictionaryPage(dict, 2, parquet.Encodings.Plain))
	require.NoError(t, err)
	indexes := memory.NewBufferBytes([]byte{1, 3, 0b101})
	_, err = pager.WriteDataPage(file.NewDataPageV1(indexes, 3, parquet.Encodings.RLEDict,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(indexes.Len())))
	require.NoError(t, err)
	require.NoError(t, pager.Close(true, false))
	require.NoError(t, rgMeta.Finish(0, 0))

	fileMeta, err := metaBldr.Finish()
	require.NoError(t, err)
	n, err := fileMeta.WriteTo(sink, nil)
	require.NoError(t, err)
	require.NoError(t, binary.Write(sink, binary.LittleEndian, uint32(n)))
	_, err = sink.Write([]byte("PAR1"))
	require.NoError(t, err)
	return buf.Bytes()
}

func TestReadDictionaryEncodedBooleanReturnsError(t *testing.T) {
	const msg = "dictionary encoding not implemented for boolean type"
	data := dictionaryBooleanFile(t)

	openReader := func(t *testing.T, props pqarrow.ArrowReadProperties) *pqarrow.FileReader {
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		require.NoError(t, err)
		t.Cleanup(func() { pf.Close() })
		fr, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
		require.NoError(t, err)
		return fr
	}

	t.Run("ReadTable", func(t *testing.T) {
		_, err := openReader(t, pqarrow.ArrowReadProperties{}).ReadTable(context.Background())
		require.ErrorContains(t, err, msg)
	})

	t.Run("NextParallel", func(t *testing.T) {
		rr, err := openReader(t, pqarrow.ArrowReadProperties{Parallel: true}).GetRecordReader(context.Background(), nil, nil)
		require.NoError(t, err)
		defer rr.Release()

		require.False(t, rr.Next())
		require.ErrorContains(t, rr.Err(), msg)
	})

	t.Run("SeekToRow", func(t *testing.T) {
		rr, err := openReader(t, pqarrow.ArrowReadProperties{}).GetRecordReader(context.Background(), nil, nil)
		require.NoError(t, err)
		defer rr.Release()

		require.ErrorContains(t, rr.SeekToRow(1), msg)
	})
}
