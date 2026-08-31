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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWriterRowGroupNumRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 1, "two": null},
		{"one": null, "two": 2},
		{"one": null, "two": null}
	]`
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)

	output := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.Write(record))
	numRows, err := writer.RowGroupNumRows()
	require.NoError(t, err)
	assert.Equal(t, 4, numRows)

	// Make sure that row group stats are up-to-date immediately after writing
	bytesWritten := writer.RowGroupTotalBytesWritten()
	require.NoError(t, writer.Close())
	require.Equal(t, bytesWritten, writer.RowGroupTotalBytesWritten())
}

func TestFileWriterNumRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 1, "two": null},
		{"one": null, "two": 2},
		{"one": null, "two": null}
	]`
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)

	maxRowGroupLength := 2

	output := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(maxRowGroupLength)))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.Write(record))
	rowGroupNumRows, err := writer.RowGroupNumRows()
	require.NoError(t, err)
	assert.Equal(t, maxRowGroupLength, rowGroupNumRows)

	require.NoError(t, writer.Close())
	assert.Equal(t, 4, writer.NumRows())
}

func TestFileWriterBuffered(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 1, "two": null},
		{"one": null, "two": 2},
		{"one": null, "two": null}
	]`

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	record, _, err := array.RecordFromJSON(alloc, schema, strings.NewReader(data))
	require.NoError(t, err)
	defer record.Release()

	output := &bytes.Buffer{}
	writer, err := pqarrow.NewFileWriter(
		schema,
		output,
		parquet.NewWriterProperties(
			parquet.WithAllocator(alloc),
			// Ensure enough space so we can close the writer with rows still buffered
			parquet.WithMaxRowGroupLength(math.MaxInt64),
		),
		pqarrow.NewArrowWriterProperties(
			pqarrow.WithAllocator(alloc),
		),
	)
	require.NoError(t, err)

	require.NoError(t, writer.WriteBuffered(record))

	require.NoError(t, writer.Close())
	assert.Equal(t, 4, writer.NumRows())
}

func TestFileWriterTotalBytes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 3, "two": 4}
	]`
	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)
	defer record1.Release()

	data2 := `[
		{"one": 5, "two": 6},
		{"one": 7, "two": 8}
	]`
	record2, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data2))
	require.NoError(t, err)
	defer record2.Release()

	output := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	// Write first record
	require.NoError(t, writer.Write(record1))

	// Write second record, which creates a new row group
	require.NoError(t, writer.Write(record2))

	// Close the writer and verify final bytes
	require.NoError(t, writer.Close())

	// Verify total bytes & compressed bytes are correct
	assert.Equal(t, int64(332), writer.TotalCompressedBytes())
	assert.Equal(t, int64(786), writer.TotalBytesWritten())
}

func TestFileWriterTotalBytesBuffered(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 3, "two": 4},
		{"one": 5, "two": 6},
		{"one": 7, "two": 8},
		{"one": 9, "two": 10}
	]`
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)
	defer record.Release()

	output := &bytes.Buffer{}
	// Use a large max row group length to ensure both records go into the same row group
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	// Write record using WriteBuffered
	require.NoError(t, writer.WriteBuffered(record))

	// Close the writer and verify final bytes
	require.NoError(t, writer.Close())

	// Verify total bytes & compressed bytes are correct
	assert.Equal(t, int64(482), writer.TotalCompressedBytes())
	assert.Equal(t, int64(1120), writer.TotalBytesWritten())
}

func TestFileWriterRangeWritesPreserveData(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[
		{"number": 1, "text": "one", "values": [1, 2]},
		{"number": null, "text": "two", "values": []},
		{"number": 3, "text": null, "values": null},
		{"number": 4, "text": "four", "values": [4]},
		{"number": 5, "text": "five", "values": [5, 6, 7]}
	]`))
	require.NoError(t, err)
	defer record.Release()

	writeAndRead := func(t *testing.T, write func(*pqarrow.FileWriter) error) {
		t.Helper()

		var output bytes.Buffer
		writer, err := pqarrow.NewFileWriter(
			schema,
			&output,
			parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2)),
			pqarrow.DefaultWriterProps(),
		)
		require.NoError(t, err)
		require.NoError(t, write(writer))
		require.NoError(t, writer.Close())

		reader, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
		require.NoError(t, err)
		require.Equal(t, 3, reader.NumRowGroups())
		require.Equal(t, int64(5), reader.NumRows())
		require.NoError(t, reader.Close())

		got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(output.Bytes()), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		defer got.Release()
		require.Equal(t, int64(5), got.NumRows())
		for i := 0; i < int(record.NumCols()); i++ {
			expected := arrow.NewChunked(record.Column(i).DataType(), []arrow.Array{record.Column(i)})
			require.Truef(t, array.ChunkedEqual(expected, got.Column(i).Data()), "column %d differs", i)
			expected.Release()
		}
	}

	t.Run("Write", func(t *testing.T) {
		writeAndRead(t, func(writer *pqarrow.FileWriter) error {
			return writer.Write(record)
		})
	})

	t.Run("WriteBuffered", func(t *testing.T) {
		writeAndRead(t, func(writer *pqarrow.FileWriter) error {
			return writer.WriteBuffered(record)
		})
	})

	t.Run("WriteBufferedAcrossCalls", func(t *testing.T) {
		first := record.NewSlice(0, 1)
		defer first.Release()
		second := record.NewSlice(1, record.NumRows())
		defer second.Release()

		writeAndRead(t, func(writer *pqarrow.FileWriter) error {
			if err := writer.WriteBuffered(first); err != nil {
				return err
			}
			return writer.WriteBuffered(second)
		})
	})

	t.Run("WriteBufferedAtFullBoundary", func(t *testing.T) {
		first := record.NewSlice(0, 2)
		defer first.Release()
		second := record.NewSlice(2, record.NumRows())
		defer second.Release()

		writeAndRead(t, func(writer *pqarrow.FileWriter) error {
			if err := writer.WriteBuffered(first); err != nil {
				return err
			}
			return writer.WriteBuffered(second)
		})
	})
}

func TestFileWriterZeroRowRecord(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	for _, test := range []struct {
		name  string
		write func(*pqarrow.FileWriter, arrow.RecordBatch) error
	}{
		{name: "Write", write: (*pqarrow.FileWriter).Write},
		{name: "WriteBuffered", write: (*pqarrow.FileWriter).WriteBuffered},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			writer, err := pqarrow.NewFileWriter(
				schema,
				&output,
				parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2)),
				pqarrow.DefaultWriterProps(),
			)
			require.NoError(t, err)
			require.NoError(t, test.write(writer, record))
			require.NoError(t, writer.Close())

			reader, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
			require.NoError(t, err)
			require.Equal(t, 1, reader.NumRowGroups())
			require.Equal(t, int64(0), reader.NumRows())
			require.NoError(t, reader.Close())
		})
	}
}

func TestWriteOnClosedFileWriter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	output := &bytes.Buffer{}
	writer, err := pqarrow.NewFileWriter(schema, output, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	// Close the writer
	require.NoError(t, writer.Close())

	// Call each write method and ensure they all return an error stating the writer is already closed
	require.ErrorContains(t, writer.WriteBuffered(nil), "already closed")
	require.ErrorContains(t, writer.Write(nil), "already closed")
	require.ErrorContains(t, writer.WriteColumnChunked(nil, 0, 0), "already closed")
	require.ErrorContains(t, writer.WriteColumnData(nil), "already closed")
}
