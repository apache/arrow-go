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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// columnPages returns the NumValues of every data page of column col, one
// slice per row group, skipping dictionary pages.
func columnPages(t *testing.T, buf []byte, col int) [][]int64 {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(buf))
	require.NoError(t, err)
	defer rdr.Close()

	var out [][]int64
	for i := 0; i < rdr.NumRowGroups(); i++ {
		pr, err := rdr.RowGroup(i).GetColumnPageReader(col)
		require.NoError(t, err)
		pages := []int64{}
		for pr.Next() {
			switch p := pr.Page().(type) {
			case *file.DataPageV1:
				pages = append(pages, int64(p.NumValues()))
			case *file.DataPageV2:
				pages = append(pages, int64(p.NumValues()))
			}
		}
		require.NoError(t, pr.Err())
		out = append(out, pages)
	}
	return out
}

func cdcProps(opts parquet.CDCOptions) *parquet.WriterProperties {
	return parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithContentDefinedChunking(opts))
}

func int64Record(mem memory.Allocator, start, n int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	for i := start; i < start+n; i++ {
		bldr.Field(0).(*array.Int64Builder).Append(i)
	}
	return bldr.NewRecordBatch()
}

func writeRecords(t *testing.T, mem memory.Allocator, props *parquet.WriterProperties, arrprops pqarrow.ArrowWriterProperties, buffered bool, recs ...arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(recs[0].Schema(), &buf, props, arrprops)
	require.NoError(t, err)
	for _, rec := range recs {
		if buffered {
			require.NoError(t, fw.WriteBuffered(rec))
		} else {
			require.NoError(t, fw.Write(rec))
		}
	}
	require.NoError(t, fw.Close())
	return buf.Bytes()
}

func readCDCTable(t *testing.T, mem memory.Allocator, data []byte) arrow.Table {
	t.Helper()
	tbl, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	return tbl
}

func requireRoundTrip(t *testing.T, mem memory.Allocator, data []byte, rec arrow.RecordBatch) {
	t.Helper()
	tbl := readCDCTable(t, mem, data)
	defer tbl.Release()
	require.Equal(t, rec.NumRows(), tbl.NumRows())
	for i := 0; i < int(rec.NumCols()); i++ {
		expected := arrow.NewChunked(rec.Column(i).DataType(), []arrow.Array{rec.Column(i)})
		require.Truef(t, array.ChunkedEqual(expected, tbl.Column(i).Data()), "column %d differs", i)
		expected.Release()
	}
}

func TestCDCFileWriterCreatesChunkersOnOpen(t *testing.T) {
	var buf bytes.Buffer
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	props := parquet.NewWriterProperties(parquet.WithContentDefinedChunking(parquet.CDCOptions{MinChunkSize: 1024 * 1024, MaxChunkSize: 256 * 1024}))
	_, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.DefaultWriterProps())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max chunk size")
	assert.Equal(t, 0, buf.Len())
}

func TestCDCWriteReturnsChunkErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithCoerceTimestamps(arrow.Millisecond))
	write := func(props *parquet.WriterProperties) error {
		var buf bytes.Buffer
		fw, err := pqarrow.NewFileWriter(schema, &buf, props, arrprops)
		require.NoError(t, err)
		return fw.Write(rec)
	}

	errPlain := write(parquet.NewWriterProperties())
	require.Error(t, errPlain)
	var errCDC error
	require.NotPanics(t, func() {
		errCDC = write(parquet.NewWriterProperties(parquet.WithContentDefinedChunking(parquet.DefaultCDCOptions())))
	})
	require.Error(t, errCDC)
	require.EqualError(t, errCDC, errPlain.Error())
}

func TestCDCPagesWithinChunkSizes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := int64Record(mem, 0, 65536)
	defer rec.Release()
	data := writeRecords(t, mem, cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384}),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()), false, rec)

	pages := columnPages(t, data, 0)
	require.Len(t, pages, 1)
	require.Greater(t, len(pages[0]), 1)
	var total int64
	for i, n := range pages[0] {
		total += n
		if i < len(pages[0])-1 {
			assert.GreaterOrEqual(t, n, int64(512))
		}
		assert.LessOrEqual(t, n, int64(2048))
	}
	assert.Equal(t, int64(65536), total)
	requireRoundTrip(t, mem, data, rec)
}

func TestCDCLastChunkDoesntCutPage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	props := cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384})
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	a, b, all := int64Record(mem, 0, 8000), int64Record(mem, 8000, 8000), int64Record(mem, 0, 16000)
	defer a.Release()
	defer b.Release()
	defer all.Release()

	buffered := writeRecords(t, mem, props, arrprops, true, a, b)
	single := writeRecords(t, mem, props, arrprops, false, all)
	assert.Equal(t, columnPages(t, single, 0), columnPages(t, buffered, 0))
}

func TestCDCLowLevelWritersIgnoreCDC(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const n = 64 * 1024
	props := cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384})
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("a", parquet.Repetitions.Required, -1),
	}, -1)))

	vals := make([]int32, n)
	for i := range vals {
		vals[i] = int32(i)
	}
	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(vals, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, sc.Root(), file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())

	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	rgw = w.AppendRowGroup()
	cw, err = rgw.NextColumn()
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteArrowToColumn(pqarrow.NewArrowWriteContext(context.Background(), &arrprops), cw, arr, nil, nil, false))
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	pages := columnPages(t, buf.Bytes(), 0)
	require.Len(t, pages, 2)
	assert.Equal(t, []int64{n}, pages[0])
	assert.Equal(t, []int64{n}, pages[1])
}

func TestCDCFixedSizeListNullSlots(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const rows = 16 * 1024
	schema := arrow.NewSchema([]arrow.Field{{Name: "l", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	lb := bldr.Field(0).(*array.FixedSizeListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)
	for i := 0; i < rows; i++ {
		if i%10 == 0 {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for j := 0; j < 4; j++ {
			vb.Append(int32(i*4 + j))
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	data := writeRecords(t, mem, cdcProps(parquet.CDCOptions{MinChunkSize: 1024, MaxChunkSize: 4096}),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()), false, rec)
	pages := columnPages(t, data, 0)
	require.Len(t, pages, 1)
	assert.Greater(t, len(pages[0]), 1)
	requireRoundTrip(t, mem, data, rec)
}

func TestCDCEmptyAndAllNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	props := cdcProps(parquet.DefaultCDCOptions())
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.Null, Nullable: true},
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	empty := array.NewRecordBuilder(mem, schema)
	defer empty.Release()
	emptyRec := empty.NewRecordBatch()
	defer emptyRec.Release()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(schema, &buf, props, arrprops)
	require.NoError(t, err)
	require.NoError(t, fw.Write(emptyRec))
	require.NoError(t, fw.Close())

	const rows = 10 * 1024
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	for i := 0; i < rows; i++ {
		bldr.Field(0).AppendNull()
		bldr.Field(1).AppendNull()
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	data := writeRecords(t, mem, props, arrprops, false, rec)
	requireRoundTrip(t, mem, data, rec)
	for col := 0; col < 2; col++ {
		for _, rg := range columnPages(t, data, col) {
			for _, n := range rg {
				assert.NotZero(t, n)
			}
		}
	}
}
