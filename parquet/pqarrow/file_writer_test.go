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
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
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

type byteLimitSink struct {
	buf    bytes.Buffer
	limit  int
	closed bool
}

func (s *byteLimitSink) Close() error {
	s.closed = true
	return nil
}

var errSinkFull = errors.New("sink full")

func (s *byteLimitSink) Write(p []byte) (int, error) {
	if room := s.limit - s.buf.Len(); len(p) > room {
		s.buf.Write(p[:max(room, 0)])
		return max(room, 0), errSinkFull
	}
	return s.buf.Write(p)
}

func TestFileWriterCloseReleasesBuffersWhenSinkFails(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "num", Nullable: true, Type: arrow.PrimitiveTypes.Int8},
		{Name: "dict", Nullable: true, Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}},
		{Name: "str", Nullable: true, Type: arrow.BinaryTypes.String},
	}, nil)

	buildRecord := func(mem memory.Allocator) arrow.RecordBatch {
		bldr := array.NewRecordBuilder(mem, schema)
		defer bldr.Release()
		num := bldr.Field(0).(*array.Int8Builder)
		dict := bldr.Field(1).(*array.BinaryDictionaryBuilder)
		str := bldr.Field(2).(*array.StringBuilder)
		for i := 0; i < 300; i++ {
			require.NoError(t, dict.AppendString("value-"+strconv.Itoa(i%10)))
			str.Append("row-" + strconv.Itoa(i))
			num.Append(int8(i))
		}
		return bldr.NewRecordBatch()
	}

	writeFile := func(t *testing.T, mem memory.Allocator, sink io.Writer, buffered bool) error {
		rec := buildRecord(mem)
		defer rec.Release()

		writer, err := pqarrow.NewFileWriter(schema, sink,
			parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithDictionaryFor("str", false),
				parquet.WithDataPageSize(1024), parquet.WithBatchSize(100)),
			pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()))
		require.NoError(t, err)

		if buffered {
			err = writer.WriteBuffered(rec)
		} else {
			err = writer.Write(rec)
		}
		return errors.Join(err, writer.Close())
	}

	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%t", buffered), func(t *testing.T) {
			var full writeOffsetsSink
			require.NoError(t, writeFile(t, memory.DefaultAllocator, &full, buffered))

			for _, limit := range sinkFailureLimits(t, &full) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				sink := &byteLimitSink{limit: limit}
				err := writeFile(t, mem, sink, buffered)
				require.ErrorIs(t, err, errSinkFull, "limit %d", limit)
				require.True(t, sink.closed, "sink not closed, limit %d", limit)
				mem.AssertSize(t, 0)
				if t.Failed() {
					t.Fatalf("buffers leaked when the sink fails after %d of %d bytes", limit, full.Len())
				}
			}
		})
	}
}

// writeOffsetsSink records where each Write starts, which marks every page
// header, page body and footer boundary of the written file.
type writeOffsetsSink struct {
	bytes.Buffer
	starts []int
}

func (s *writeOffsetsSink) Write(p []byte) (int, error) {
	s.starts = append(s.starts, s.Len())
	return s.Buffer.Write(p)
}

// sinkFailureLimits returns the byte counts after which to fail the sink:
// one byte either side of every page and column chunk boundary of the file
// in full, plus a stride sample of everything in between.
func sinkFailureLimits(t *testing.T, full *writeOffsetsSink) []int {
	t.Helper()
	const stride = 37
	boundaries := slices.Clone(full.starts)
	rdr, err := file.NewParquetReader(bytes.NewReader(full.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()
	for i := 0; i < rdr.NumRowGroups(); i++ {
		rg := rdr.MetaData().RowGroup(i)
		for j := 0; j < rg.NumColumns(); j++ {
			col, err := rg.ColumnChunk(j)
			require.NoError(t, err)
			start := col.DataPageOffset()
			if col.HasDictionaryPage() {
				boundaries = append(boundaries, int(col.DictionaryPageOffset()))
				start = min(start, col.DictionaryPageOffset())
			}
			boundaries = append(boundaries, int(col.DataPageOffset()), int(start+col.TotalCompressedSize()))
		}
	}
	for off := 0; off < full.Len(); off += stride {
		boundaries = append(boundaries, off)
	}

	var limits []int
	for _, b := range boundaries {
		for _, limit := range []int{b - 1, b, b + 1} {
			if limit >= 4 && limit < full.Len() {
				limits = append(limits, limit)
			}
		}
	}
	slices.Sort(limits)
	return slices.Compact(limits)
}
