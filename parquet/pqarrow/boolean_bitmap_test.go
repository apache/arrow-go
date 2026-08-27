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

package pqarrow

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/stretchr/testify/require"
)

func TestBooleanBitmapReadAcrossPages(t *testing.T) {
	const numValues = 128
	for _, tc := range []struct {
		name     string
		encoding parquet.Encoding
	}{
		{name: "plain", encoding: parquet.Encodings.Plain},
		{name: "rle", encoding: parquet.Encodings.RLE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			schema := arrow.NewSchema([]arrow.Field{{
				Name:     "bools",
				Type:     arrow.FixedWidthTypes.Boolean,
				Nullable: true,
			}}, nil)

			bldr := array.NewBooleanBuilder(mem)
			defer bldr.Release()
			for i := 0; i < numValues; i++ {
				if i%5 == 0 {
					bldr.AppendNull()
				} else {
					bldr.Append(i%2 == 0)
				}
			}
			expected := bldr.NewBooleanArray()
			defer expected.Release()

			var buf bytes.Buffer
			writer, err := NewFileWriter(schema, &buf,
				parquet.NewWriterProperties(
					parquet.WithCompression(compress.Codecs.Uncompressed),
					parquet.WithDataPageSize(1),
					parquet.WithEncoding(tc.encoding),
				),
				NewArrowWriterProperties(WithAllocator(mem)),
			)
			require.NoError(t, err)

			record := array.NewRecordBatch(schema, []arrow.Array{expected}, numValues)
			require.NoError(t, writer.WriteBuffered(record))
			record.Release()
			require.NoError(t, writer.Close())

			parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer parquetReader.Close()

			reader, err := NewFileReader(parquetReader, ArrowReadProperties{}, mem)
			require.NoError(t, err)

			column, err := reader.GetColumn(context.Background(), 0)
			require.NoError(t, err)
			defer column.Release()

			got, err := column.NextBatch(numValues)
			require.NoError(t, err)
			defer got.Release()

			require.Len(t, got.Chunks(), 1)
			require.True(t, array.Equal(expected, got.Chunk(0)))
		})
	}
}

func TestBooleanRecordReaderValuesRemainByteEncoded(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const numValues = 10
	schema := arrow.NewSchema([]arrow.Field{{
		Name:     "bools",
		Type:     arrow.FixedWidthTypes.Boolean,
		Nullable: false,
	}}, nil)
	want := []byte{1, 0, 1, 1, 0, 0, 1, 0, 1, 1}

	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()
	for _, value := range want {
		bldr.Append(value != 0)
	}
	expected := bldr.NewBooleanArray()
	defer expected.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(schema, &buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
		NewArrowWriterProperties(WithAllocator(mem)),
	)
	require.NoError(t, err)
	record := array.NewRecordBatch(schema, []arrow.Array{expected}, numValues)
	require.NoError(t, writer.WriteBuffered(record))
	record.Release()
	require.NoError(t, writer.Close())

	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer parquetReader.Close()

	pageReader, err := parquetReader.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	rr := file.NewRecordReader(
		parquetReader.MetaData().Schema.Column(0),
		file.LevelInfo{}, arrow.FixedWidthTypes.Boolean, mem, parquetReader.BufferPool())
	defer rr.Release()
	rr.SetPageReader(pageReader)
	require.NoError(t, rr.Reserve(numValues))
	read, err := rr.ReadRecords(numValues)
	require.NoError(t, err)
	require.EqualValues(t, numValues, read)

	require.Equal(t, want, rr.Values())
	values := rr.ReleaseValues()
	defer values.Release()
	require.Equal(t, want, values.Bytes())
}
