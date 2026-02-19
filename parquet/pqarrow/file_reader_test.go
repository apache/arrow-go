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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDataDir() string {
	datadir := os.Getenv("PARQUET_TEST_DATA")
	if datadir == "" {
		panic("please point PARQUET_TEST_DATA env var to the test data directory")
	}
	return datadir
}

func TestArrowReaderAdHocReadDecimals(t *testing.T) {
	tests := []struct {
		file string
		typ  *arrow.Decimal128Type
	}{
		{"int32_decimal", &arrow.Decimal128Type{Precision: 4, Scale: 2}},
		{"int64_decimal", &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{"fixed_length_decimal", &arrow.Decimal128Type{Precision: 25, Scale: 2}},
		{"fixed_length_decimal_legacy", &arrow.Decimal128Type{Precision: 13, Scale: 2}},
		{"byte_array_decimal", &arrow.Decimal128Type{Precision: 4, Scale: 2}},
	}

	dataDir := getDataDir()
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			filename := filepath.Join(dataDir, tt.file+".parquet")
			require.FileExists(t, filename)

			rdr, err := file.OpenParquetFile(filename, false, file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()
			arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
			require.NoError(t, err)

			tbl, err := arrowRdr.ReadTable(context.Background())
			require.NoError(t, err)
			defer tbl.Release()

			assert.EqualValues(t, 1, tbl.NumCols())
			assert.Truef(t, arrow.TypeEqual(tbl.Schema().Field(0).Type, tt.typ), "expected: %s\ngot: %s", tbl.Schema().Field(0).Type, tt.typ)

			const expectedLen = 24
			valCol := tbl.Column(0)

			assert.EqualValues(t, expectedLen, valCol.Len())
			assert.Len(t, valCol.Data().Chunks(), 1)

			chunk := valCol.Data().Chunk(0)
			bldr := array.NewDecimal128Builder(mem, tt.typ)
			defer bldr.Release()
			for i := 0; i < expectedLen; i++ {
				bldr.Append(decimal128.FromI64(int64((i + 1) * 100)))
			}

			expectedArr := bldr.NewDecimal128Array()
			defer expectedArr.Release()

			assert.Truef(t, array.Equal(expectedArr, chunk), "expected: %s\ngot: %s", expectedArr, chunk)
		})
	}
}

func TestArrowReaderAdHocReadFloat16s(t *testing.T) {
	tests := []struct {
		file string
		len  int
		vals []float16.Num
	}{
		{"float16_nonzeros_and_nans", 8,
			[]float16.Num{
				float16.New(1.0),
				float16.New(-2.0),
				float16.NaN(),
				float16.New(0.0),
				float16.New(-1.0),
				float16.New(0.0).Negate(),
				float16.New(2.0),
			}},
		{"float16_zeros_and_nans", 3,
			[]float16.Num{
				float16.New(0.0),
				float16.NaN(),
			}},
	}

	dataDir := getDataDir()
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			filename := filepath.Join(dataDir, tt.file+".parquet")
			require.FileExists(t, filename)

			rdr, err := file.OpenParquetFile(filename, false, file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()

			arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
			require.NoError(t, err)

			tbl, err := arrowRdr.ReadTable(context.Background())
			require.NoError(t, err)
			defer tbl.Release()

			assert.EqualValues(t, 1, tbl.NumCols())
			assert.Truef(t, arrow.TypeEqual(tbl.Schema().Field(0).Type, &arrow.Float16Type{}), "expected: %s\ngot: %s", tbl.Schema().Field(0).Type, arrow.Float16Type{})

			valCol := tbl.Column(0)
			assert.EqualValues(t, tt.len, valCol.Len())
			assert.Len(t, valCol.Data().Chunks(), 1)

			chunk := valCol.Data().Chunk(0).(*array.Float16)
			assert.True(t, chunk.IsNull(0))
			for i := 0; i < tt.len-1; i++ {
				expected := tt.vals[i]
				actual := chunk.Value(i + 1)
				if expected.IsNaN() {
					// NaN representations aren't guaranteed to be exact on a binary level
					assert.True(t, actual.IsNaN())
				} else {
					assert.Equal(t, expected.Uint16(), actual.Uint16())
				}
			}
		})
	}
}

func TestArrowReaderCanceledContext(t *testing.T) {
	dataDir := getDataDir()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	filename := filepath.Join(dataDir, "int32_decimal.parquet")
	require.FileExists(t, filename)

	rdr, err := file.OpenParquetFile(filename, false, file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer rdr.Close()
	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	// create a canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = arrowRdr.ReadTable(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRecordReaderParallel(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 3, Parallel: true}, mem)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	records := make([]arrow.RecordBatch, 0)
	for rr.Next() {
		rec := rr.RecordBatch()
		defer rec.Release()

		assert.Truef(t, sc.Equal(rec.Schema()), "expected: %s\ngot: %s", sc, rec.Schema())
		rec.Retain()
		records = append(records, rec)
	}

	assert.False(t, rr.Next())

	tr := array.NewTableReader(tbl, 3)
	defer tr.Release()

	assert.True(t, tr.Next())
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), records[0]), "expected: %s\ngot: %s", tr.RecordBatch(), records[0])
	assert.True(t, tr.Next())
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), records[1]), "expected: %s\ngot: %s", tr.RecordBatch(), records[1])
}

func TestRecordReaderSerial(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2}, mem)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	rec, err := rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.Same(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestRecordReaderSeekToRow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)

			reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2, Parallel: parallel}, mem)
			require.NoError(t, err)

			sc, err := reader.Schema()
			assert.NoError(t, err)
			assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

			rr, err := reader.GetRecordReader(context.Background(), nil, nil)
			assert.NoError(t, err)
			assert.NotNil(t, rr)
			defer rr.Release()

			tr := array.NewTableReader(tbl, 2)
			defer tr.Release()

			rec, err := rr.Read()
			assert.NoError(t, err)
			tr.Next()
			assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

			require.NoError(t, rr.SeekToRow(0))
			rec, err = rr.Read()
			assert.NoError(t, err)
			assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

			rec, err = rr.Read()
			assert.NoError(t, err)
			tr.Next()
			assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

			require.NoError(t, rr.SeekToRow(2))
			rec, err = rr.Read()
			assert.NoError(t, err)
			assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

			require.NoError(t, rr.SeekToRow(4))
			rec, err = rr.Read()
			tr.Next()
			assert.NoError(t, err)
			assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)
		})
	}
}

func TestRecordReaderMultiRowGroup(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 2, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2}, mem)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	rec, err := rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.Same(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestRecordReaderSeekToRowMultiRowGroup(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 2, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2}, mem)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	rec, err := rr.Read()
	assert.NoError(t, err)
	tr.Next()
	first := tr.RecordBatch()
	first.Retain()
	defer first.Release()

	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	require.NoError(t, rr.SeekToRow(0))
	rec, err = rr.Read()
	assert.NoError(t, err)
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	require.NoError(t, rr.SeekToRow(2))
	rec, err = rr.Read()
	assert.NoError(t, err)
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	require.NoError(t, rr.SeekToRow(4))
	rec, err = rr.Read()
	tr.Next()
	assert.NoError(t, err)
	assert.Truef(t, array.RecordEqual(tr.RecordBatch(), rec), "expected: %s\ngot: %s", tr.RecordBatch(), rec)

	require.NoError(t, rr.SeekToRow(0))
	rec, err = rr.Read()
	assert.NoError(t, err)
	assert.Truef(t, array.RecordEqual(first, rec), "expected: %s\ngot: %s", first, rec)
}

func TestFileReaderWriterMetadata(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, true, true)
	defer tbl.Release()

	meta := arrow.NewMetadata([]string{"foo", "bar"}, []string{"bar", "baz"})
	sc := arrow.NewSchema(tbl.Schema().Fields(), &meta)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(sc, &buf, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	require.NoError(t, err)
	require.NoError(t, writer.WriteTable(tbl, tbl.NumRows()))
	require.NoError(t, writer.Close())

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pf.Close()

	kvMeta := pf.MetaData().KeyValueMetadata()
	assert.Equal(t, []string{"foo", "bar"}, kvMeta.Keys())
	assert.Equal(t, []string{"bar", "baz"}, kvMeta.Values())
}

func TestFileReaderColumnChunkBoundsErrors(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "zero", Type: arrow.PrimitiveTypes.Float64},
		{Name: "g", Type: arrow.StructOf(
			arrow.Field{Name: "one", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "two", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "three", Type: arrow.PrimitiveTypes.Float64},
		)},
	}, nil)

	// generate Parquet data with four columns
	// that are represented by two logical fields
	data := `[
		{
			"zero": 1,
			"g": {
				"one": 1,
				"two": 1,
				"three": 1
			}
		},
		{
			"zero": 2,
			"g": {
				"one": 2,
				"two": 2,
				"three": 2
			}
		}
	]`

	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)

	output := &bytes.Buffer{}
	writer, err := pqarrow.NewFileWriter(schema, output, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	fileReader, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
	require.NoError(t, err)

	arrowReader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	require.NoError(t, err)

	// assert that errors are returned for indexes outside the bounds of the logical fields (instead of the physical columns)
	ctx := pqarrow.NewArrowWriteContext(context.Background(), nil)
	assert.Greater(t, fileReader.NumRowGroups(), 0)
	for rowGroupIndex := 0; rowGroupIndex < fileReader.NumRowGroups(); rowGroupIndex += 1 {
		rowGroupReader := arrowReader.RowGroup(rowGroupIndex)
		for fieldNum := 0; fieldNum < schema.NumFields(); fieldNum += 1 {
			_, err := rowGroupReader.Column(fieldNum).Read(ctx)
			assert.NoError(t, err, "reading field num: %d", fieldNum)
		}

		_, subZeroErr := rowGroupReader.Column(-1).Read(ctx)
		assert.Error(t, subZeroErr)

		_, tooHighErr := rowGroupReader.Column(schema.NumFields()).Read(ctx)
		assert.ErrorContains(t, tooHighErr, fmt.Sprintf("there are only %d columns", schema.NumFields()))
	}
}

func TestReadParquetFile(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_BAD_DATA")
	if dir == "" {
		t.Skip("no path supplied with PARQUET_TEST_BAD_DATA")
	}
	assert.DirExists(t, dir)
	filename := path.Join(dir, "ARROW-GH-43605.parquet")
	ctx := context.TODO()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)

	rdr, err := file.OpenParquetFile(
		filename,
		false,
		file.WithReadProps(parquet.NewReaderProperties(mem)),
	)
	require.NoError(t, err)
	defer func() {
		if err2 := rdr.Close(); err2 != nil {
			t.Errorf("unexpected error: %v", err2)
		}
	}()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 0,
	}, mem)
	require.NoError(t, err)

	_, err = arrowRdr.ReadTable(ctx)
	assert.NoError(t, err)
}

func TestPartialStructColumnRead(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Create schema with nested struct containing multiple fields
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "nested", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Float64},
		)},
	}, nil)

	// Write parquet file with sample data
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(schema, buf, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	sb := b.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Float64Builder).Append(1.0)
	sb.FieldBuilder(1).(*array.Float64Builder).Append(2.0)
	sb.FieldBuilder(2).(*array.Float64Builder).Append(3.0)

	rec := b.NewRecordBatch()
	defer rec.Release()

	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())

	// Read back with partial column projection (only nested.a and nested.c)
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer pf.Close()

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	// Only read leaf indices 0 (nested.a) and 2 (nested.c), skip 1 (nested.b)
	partialLeaves := map[int]bool{0: true, 2: true}
	fieldIdx, err := fr.Manifest.GetFieldIndices([]int{0})
	require.NoError(t, err)

	// This should NOT panic
	reader, err := fr.GetFieldReader(context.Background(), fieldIdx[0], partialLeaves, []int{0})
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Verify the filtered struct type only has 2 fields (a and c)
	field := reader.Field()
	structType := field.Type.(*arrow.StructType)
	require.Equal(t, 2, structType.NumFields())
	require.Equal(t, "a", structType.Field(0).Name)
	require.Equal(t, "c", structType.Field(1).Name)

	// Read and verify data
	chunked, err := reader.NextBatch(1)
	require.NoError(t, err)
	defer chunked.Release()
	require.Equal(t, 1, chunked.Len())

	// Get the first chunk and verify values (should have a=1.0, c=3.0, no b)
	arr := chunked.Chunk(0).(*array.Struct)

	aArr := arr.Field(0).(*array.Float64)
	require.Equal(t, 1.0, aArr.Value(0))

	cArr := arr.Field(1).(*array.Float64)
	require.Equal(t, 3.0, cArr.Value(0))
}

// TestMapColumnWithFilters tests that map columns can be read correctly when
// using column filtering. This is a regression test for a bug where reading
// a map column with filters would fail because the code tried to filter the
// individual key and value columns of the map's internal key-value struct.
// Maps require both key and value columns to be read together, so leaf filtering
// must be disabled for map types.
func TestMapColumnWithFilters(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Create schema with a map column and other columns
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "properties", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Build test data
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Build ID column
	idBuilder := b.Field(0).(*array.Int64Builder)
	idBuilder.AppendValues([]int64{1, 2}, nil)

	// Build map column
	mapBuilder := b.Field(1).(*array.MapBuilder)
	kb := mapBuilder.KeyBuilder().(*array.StringBuilder)
	vb := mapBuilder.ItemBuilder().(*array.Int32Builder)

	// First map: {"key1": 100, "key2": 200}
	mapBuilder.Append(true)
	kb.AppendValues([]string{"key1", "key2"}, nil)
	vb.AppendValues([]int32{100, 200}, nil)

	// Second map: {"key3": 300}
	mapBuilder.Append(true)
	kb.AppendValues([]string{"key3"}, nil)
	vb.AppendValues([]int32{300}, nil)

	// Build value column
	valueBuilder := b.Field(2).(*array.Float64Builder)
	valueBuilder.AppendValues([]float64{1.5, 2.5}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	// Write to parquet
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(schema, buf, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())

	// Read back with column filtering (only read id and properties, skip value)
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer pf.Close()

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	// Read only columns for the first two fields (id and map)
	// Column 0 = id
	// Column 1 = map.key
	// Column 2 = map.value
	// Column 3 = value (skipped)
	// This exercises the code path where maps need both key and value columns
	// even when column filtering is active
	ctx := context.Background()
	colIndices := []int{0, 1, 2} // id, map.key, map.value
	rr, err := fr.GetRecordReader(ctx, colIndices, nil)
	require.NoError(t, err)
	require.NotNil(t, rr)
	defer rr.Release()

	// Read the record batch
	require.True(t, rr.Next())
	result := rr.RecordBatch()
	defer result.Release()

	// Verify schema - should have only 2 fields (id and properties)
	require.Equal(t, 2, int(result.NumCols()))
	require.Equal(t, "id", result.Schema().Field(0).Name)
	require.Equal(t, "properties", result.Schema().Field(1).Name)

	// Verify ID column
	idCol := result.Column(0).(*array.Int64)
	require.Equal(t, int64(1), idCol.Value(0))
	require.Equal(t, int64(2), idCol.Value(1))

	// Verify map column - this is the critical test for the fix
	// The key test is that reading succeeds without panic
	mapCol := result.Column(1).(*array.Map)
	require.Equal(t, 2, mapCol.Len())

	// Verify the map has the correct structure (keys and items arrays exist)
	keys := mapCol.Keys().(*array.String)
	vals := mapCol.Items().(*array.Int32)
	require.NotNil(t, keys)
	require.NotNil(t, vals)

	// Verify total number of key-value pairs across all maps
	require.Equal(t, 3, keys.Len()) // Total: 2 from first map + 1 from second map
	require.Equal(t, 3, vals.Len())

	// Verify the map offsets are correct
	start0, end0 := mapCol.ValueOffsets(0)
	require.Equal(t, int64(0), start0)
	require.Equal(t, int64(2), end0) // First map has entries from 0 to 2 (2 entries)

	start1, end1 := mapCol.ValueOffsets(1)
	require.Equal(t, int64(2), start1)
	require.Equal(t, int64(3), end1) // Second map has entries from 2 to 3 (1 entry)

	// Verify key-value pairs
	require.Equal(t, "key1", keys.Value(0))
	require.Equal(t, int32(100), vals.Value(0))
	require.Equal(t, "key2", keys.Value(1))
	require.Equal(t, int32(200), vals.Value(1))
	require.Equal(t, "key3", keys.Value(2))
	require.Equal(t, int32(300), vals.Value(2))
}
