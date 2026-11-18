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

// The parallel record reader decodes on goroutines that do not recover, so a
// panic for an unsupported encoding ends the process rather than failing a test.
func TestParallelRecordReaderUnsupportedEncodingReturnsError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{schema.NewFloat64Node("v", parquet.Repetitions.Required, -1)}, -1)))
	metaBldr := metadata.NewFileMetadataBuilder(sc, nil, nil)
	rgMeta := metaBldr.AppendRowGroup()
	rgMeta.SetNumRows(3)

	var buf bytes.Buffer
	sink := &utils.TellWrapper{Writer: &buf}
	_, err := sink.Write([]byte("PAR1"))
	require.NoError(t, err)
	pager, err := file.NewPageWriter(sink, compress.Codecs.Uncompressed, compress.DefaultCompressionLevel,
		rgMeta.NextColumnChunk(), 0, 0, mem, false, nil, nil)
	require.NoError(t, err)
	body := memory.NewBufferBytes(make([]byte, 24))
	_, err = pager.WriteDataPage(file.NewDataPageV1(body, 3, parquet.Encodings.DeltaBinaryPacked,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len())))
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

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{Parallel: true}, mem)
	require.NoError(t, err)
	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	require.False(t, rr.Next())
	require.ErrorContains(t, rr.Err(), "DELTA_BINARY_PACKED encoding only supports INT32 and INT64, got DOUBLE")
}
