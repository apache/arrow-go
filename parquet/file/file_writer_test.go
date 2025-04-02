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

package file_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/internal/testutils"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SerializeTestSuite struct {
	testutils.PrimitiveTypedTest
	suite.Suite

	numCols      int
	numRowGroups int
	rowsPerRG    int
	rowsPerBatch int
}

func (t *SerializeTestSuite) SetupTest() {
	t.numCols = 4
	t.numRowGroups = 4
	t.rowsPerRG = 50
	t.rowsPerBatch = 10
	t.SetupSchema(parquet.Repetitions.Optional, t.numCols)
}

func (t *SerializeTestSuite) fileSerializeTest(codec compress.Compression, expected compress.Compression) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < t.numCols; i++ {
		opts = append(opts, parquet.WithCompressionFor(t.Schema.Column(i).Name(), codec))
	}

	props := parquet.NewWriterProperties(opts...)

	writer := file.NewParquetWriter(sink, t.Schema.Root(), file.WithWriterProps(props))
	t.GenerateData(int64(t.rowsPerRG))

	t.serializeGeneratedData(writer)
	writer.FlushWithFooter()

	t.validateSerializedData(writer, sink, expected)

	t.serializeGeneratedData(writer)
	writer.Close()

	t.numRowGroups *= 2
	t.validateSerializedData(writer, sink, expected)
}

func (t *SerializeTestSuite) serializeGeneratedData(writer *file.Writer) {
	for rg := 0; rg < t.numRowGroups/2; rg++ {
		rgw := writer.AppendRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := rgw.NextColumn()
			t.WriteBatchValues(cw, t.DefLevels, nil)
			cw.Close()
			// ensure column() api which is specific to bufferedrowgroups cannot be called
			t.Panics(func() { rgw.(file.BufferedRowGroupWriter).Column(col) })
		}
		rgw.Close()
	}

	// write half buffered row groups
	for rg := 0; rg < t.numRowGroups/2; rg++ {
		rgw := writer.AppendBufferedRowGroup()
		for batch := 0; batch < (t.rowsPerRG / t.rowsPerBatch); batch++ {
			for col := 0; col < t.numCols; col++ {
				cw, _ := rgw.Column(col)
				offset := batch * t.rowsPerBatch
				t.WriteBatchSubset(t.rowsPerBatch, offset, cw, t.DefLevels[offset:t.rowsPerBatch+offset], nil)
				// Ensure NextColumn api which is specific to RowGroup cannot be called
				t.Panics(func() { rgw.(file.SerialRowGroupWriter).NextColumn() })
			}
		}
		for col := 0; col < t.numCols; col++ {
			cw, _ := rgw.Column(col)
			cw.Close()
		}
		rgw.Close()
	}
}

func (t *SerializeTestSuite) validateSerializedData(writer *file.Writer, sink *encoding.BufferWriter, expected compress.Compression) {
	nrows := t.numRowGroups * t.rowsPerRG
	t.EqualValues(nrows, writer.NumRows())

	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	t.NoError(err)
	t.Equal(t.numCols, reader.MetaData().Schema.NumColumns())
	t.Equal(t.numRowGroups, reader.NumRowGroups())
	t.EqualValues(nrows, reader.NumRows())

	for rg := 0; rg < t.numRowGroups; rg++ {
		rgr := reader.RowGroup(rg)
		t.Equal(t.numCols, rgr.NumColumns())
		t.EqualValues(t.rowsPerRG, rgr.NumRows())
		chunk, _ := rgr.MetaData().ColumnChunk(0)
		t.Equal(expected, chunk.Compression())

		valuesRead := int64(0)

		for i := 0; i < t.numCols; i++ {
			chunk, _ := rgr.MetaData().ColumnChunk(i)
			t.False(chunk.HasIndexPage())
			t.DefLevelsOut = make([]int16, t.rowsPerRG)
			t.RepLevelsOut = make([]int16, t.rowsPerRG)
			colReader, err := rgr.Column(i)
			t.NoError(err)
			t.SetupValuesOut(int64(t.rowsPerRG))
			valuesRead = t.ReadBatch(colReader, int64(t.rowsPerRG), 0, t.DefLevelsOut, t.RepLevelsOut)
			t.EqualValues(t.rowsPerRG, valuesRead)
			t.Equal(t.Values, t.ValuesOut)
			t.Equal(t.DefLevels, t.DefLevelsOut)
		}
	}
}

func (t *SerializeTestSuite) unequalNumRows(maxRows int64, rowsPerCol []int64) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	props := parquet.NewWriterProperties()
	writer := file.NewParquetWriter(sink, t.Schema.Root(), file.WithWriterProps(props))
	defer writer.Close()

	rgw := writer.AppendRowGroup()
	t.GenerateData(maxRows)
	for col := 0; col < t.numCols; col++ {
		cw, _ := rgw.NextColumn()
		t.WriteBatchSubset(int(rowsPerCol[col]), 0, cw, t.DefLevels[:rowsPerCol[col]], nil)
		cw.Close()
	}
	err := rgw.Close()
	t.Error(err)
	t.ErrorContains(err, "row mismatch for unbuffered row group")
}

func (t *SerializeTestSuite) unequalNumRowsBuffered(maxRows int64, rowsPerCol []int64) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())
	defer writer.Close()

	rgw := writer.AppendBufferedRowGroup()
	t.GenerateData(maxRows)
	for col := 0; col < t.numCols; col++ {
		cw, _ := rgw.Column(col)
		t.WriteBatchSubset(int(rowsPerCol[col]), 0, cw, t.DefLevels[:rowsPerCol[col]], nil)
		cw.Close()
	}
	err := rgw.Close()
	t.Error(err)
	t.ErrorContains(err, "row mismatch for buffered row group")
}

func (t *SerializeTestSuite) TestZeroRows() {
	t.NotPanics(func() {
		sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
		writer := file.NewParquetWriter(sink, t.Schema.Root())
		defer writer.Close()

		srgw := writer.AppendRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := srgw.NextColumn()
			cw.Close()
		}
		srgw.Close()

		brgw := writer.AppendBufferedRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := brgw.Column(col)
			cw.Close()
		}
		brgw.Close()
	})
}

func (t *SerializeTestSuite) TestTooManyColumns() {
	t.SetupSchema(parquet.Repetitions.Optional, 1)
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())
	rgw := writer.AppendRowGroup()

	rgw.NextColumn()                      // first column
	t.Panics(func() { rgw.NextColumn() }) // only one column!
}

func (t *SerializeTestSuite) TestRepeatedTooFewRows() {
	// optional and repeated, so definition and repetition levels
	t.SetupSchema(parquet.Repetitions.Repeated, 1)
	const nrows = 100
	t.GenerateData(nrows)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())

	rgw := writer.AppendRowGroup()
	t.RepLevels = make([]int16, nrows)
	for idx := range t.RepLevels {
		t.RepLevels[idx] = 0
	}

	cw, _ := rgw.NextColumn()
	t.WriteBatchValues(cw, t.DefLevels, t.RepLevels)
	cw.Close()

	t.RepLevels[3] = 1 // this makes it so that values 2 and 3 are a single row
	// as a result there's one too few rows in the result

	t.Panics(func() {
		cw, _ = rgw.NextColumn()
		t.WriteBatchValues(cw, t.DefLevels, t.RepLevels)
		cw.Close()
	})
}

func (t *SerializeTestSuite) TestTooFewRows() {
	rowsPerCol := []int64{100, 100, 100, 99}
	t.NotPanics(func() { t.unequalNumRows(100, rowsPerCol) })
	t.NotPanics(func() { t.unequalNumRowsBuffered(100, rowsPerCol) })
}

func (t *SerializeTestSuite) TestTooManyRows() {
	rowsPerCol := []int64{100, 100, 100, 101}
	t.NotPanics(func() { t.unequalNumRows(101, rowsPerCol) })
	t.NotPanics(func() { t.unequalNumRowsBuffered(101, rowsPerCol) })
}

func (t *SerializeTestSuite) TestSmallFile() {
	codecs := []compress.Compression{
		compress.Codecs.Uncompressed,
		compress.Codecs.Snappy,
		compress.Codecs.Brotli,
		compress.Codecs.Gzip,
		compress.Codecs.Zstd,
		compress.Codecs.Lz4Raw,
		// compress.Codecs.Lzo,
	}
	for _, c := range codecs {
		t.Run(c.String(), func() {
			t.NotPanics(func() { t.fileSerializeTest(c, c) })
		})
	}
}

func TestBufferedDisabledDictionary(t *testing.T) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	fields := schema.FieldList{schema.NewInt32Node("col", parquet.Repetitions.Required, 1)}
	sc, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, 0)
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))

	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))
	rgw := writer.AppendBufferedRowGroup()
	cwr, _ := rgw.Column(0)
	cw := cwr.(*file.Int32ColumnChunkWriter)
	cw.WriteBatch([]int32{1}, nil, nil)
	rgw.Close()
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()
	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, reader.NumRowGroups())
	rgReader := reader.RowGroup(0)
	assert.EqualValues(t, 1, rgReader.NumRows())
	chunk, _ := rgReader.MetaData().ColumnChunk(0)
	assert.False(t, chunk.HasDictionaryPage())
}

func TestBufferedMultiPageDisabledDictionary(t *testing.T) {
	const (
		valueCount = 10000
		pageSize   = 16384
	)
	var (
		sink  = encoding.NewBufferWriter(0, memory.DefaultAllocator)
		props = parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(pageSize))
		sc, _ = schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
			schema.NewInt32Node("col", parquet.Repetitions.Required, -1),
		}, -1)
	)

	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))
	rgWriter := writer.AppendBufferedRowGroup()
	cwr, _ := rgWriter.Column(0)
	cw := cwr.(*file.Int32ColumnChunkWriter)
	valuesIn := make([]int32, 0, valueCount)
	for i := int32(0); i < valueCount; i++ {
		valuesIn = append(valuesIn, (i%100)+1)
	}
	cw.WriteBatch(valuesIn, nil, nil)
	rgWriter.Close()
	writer.Close()
	buffer := sink.Finish()
	defer buffer.Release()

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, reader.NumRowGroups())
	valuesOut := make([]int32, valueCount)

	for r := 0; r < reader.NumRowGroups(); r++ {
		rgr := reader.RowGroup(r)
		assert.EqualValues(t, 1, rgr.NumColumns())
		assert.EqualValues(t, valueCount, rgr.NumRows())

		var totalRead int64
		col, err := rgr.Column(0)
		assert.NoError(t, err)
		colReader := col.(*file.Int32ColumnChunkReader)
		for colReader.HasNext() {
			total, _, _ := colReader.ReadBatch(valueCount-totalRead, valuesOut[totalRead:], nil, nil)
			totalRead += total
		}
		assert.EqualValues(t, valueCount, totalRead)
		assert.Equal(t, valuesIn, valuesOut)
	}
}

func TestAllNulls(t *testing.T) {
	sc, _ := schema.NewGroupNode("root", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("nulls", parquet.Repetitions.Optional, -1),
	}, -1)
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	writer := file.NewParquetWriter(sink, sc)
	rgw := writer.AppendRowGroup()
	cwr, _ := rgw.NextColumn()
	cw := cwr.(*file.Int32ColumnChunkWriter)

	var (
		values    [3]int32
		defLevels = [...]int16{0, 0, 0}
	)

	cw.WriteBatch(values[:], defLevels[:], nil)
	cw.Close()
	rgw.Close()
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.BufferedStreamEnabled = true

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()), file.WithReadProps(props))
	assert.NoError(t, err)

	rgr := reader.RowGroup(0)
	col, err := rgr.Column(0)
	assert.NoError(t, err)
	cr := col.(*file.Int32ColumnChunkReader)

	defLevels[0] = -1
	defLevels[1] = -1
	defLevels[2] = -1
	valRead, read, _ := cr.ReadBatch(3, values[:], defLevels[:], nil)
	assert.EqualValues(t, 3, valRead)
	assert.EqualValues(t, 0, read)
	assert.Equal(t, []int16{0, 0, 0}, defLevels[:])
}

func TestKeyValueMetadata(t *testing.T) {
	fields := schema.FieldList{
		schema.NewInt32Node("unused", parquet.Repetitions.Optional, -1),
	}
	sc, _ := schema.NewGroupNode("root", parquet.Repetitions.Required, fields, -1)
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	writer := file.NewParquetWriter(sink, sc)

	testKey := "testKey"
	testValue := "testValue"
	writer.AppendKeyValueMetadata(testKey, testValue)
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.BufferedStreamEnabled = true

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()), file.WithReadProps(props))
	assert.NoError(t, err)

	metadata := reader.MetaData()
	got := metadata.KeyValueMetadata().FindValue(testKey)
	require.NotNil(t, got)
	assert.Equal(t, testValue, *got)
}

func createSerializeTestSuite(typ reflect.Type) suite.TestingSuite {
	return &SerializeTestSuite{PrimitiveTypedTest: testutils.NewPrimitiveTypedTest(typ)}
}

func TestSerialize(t *testing.T) {
	t.Parallel()
	types := []struct {
		typ reflect.Type
	}{
		{reflect.TypeOf(true)},
		{reflect.TypeOf(int32(0))},
		{reflect.TypeOf(int64(0))},
		{reflect.TypeOf(float32(0))},
		{reflect.TypeOf(float64(0))},
		{reflect.TypeOf(parquet.Int96{})},
		{reflect.TypeOf(parquet.ByteArray{})},
	}
	for _, tt := range types {
		tt := tt
		t.Run(tt.typ.String(), func(t *testing.T) {
			t.Parallel()
			suite.Run(t, createSerializeTestSuite(tt.typ))
		})
	}
}

type errCloseWriter struct {
	sink *encoding.BufferWriter
}

func (c *errCloseWriter) Write(p []byte) (n int, err error) {
	return c.sink.Write(p)
}
func (c *errCloseWriter) Close() error {
	return fmt.Errorf("error during close")
}
func (c *errCloseWriter) Bytes() []byte {
	return c.sink.Bytes()
}

func TestCloseError(t *testing.T) {
	fields := schema.FieldList{schema.NewInt32Node("col", parquet.Repetitions.Required, 1)}
	sc, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, 0)
	sink := &errCloseWriter{sink: encoding.NewBufferWriter(0, memory.DefaultAllocator)}
	writer := file.NewParquetWriter(sink, sc)
	assert.Error(t, writer.Close())
}

func TestBatchedByteStreamSplitFileRoundtrip(t *testing.T) {
	input := []parquet.FixedLenByteArray{
		{1, 2},
		{3, 4},
		{5, 6},
		{7, 8},
	}

	size := len(input)
	chunk := size / 2

	props := parquet.NewWriterProperties(
		parquet.WithEncoding(parquet.Encodings.ByteStreamSplit),
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(int64(chunk)),
		parquet.WithDataPageSize(int64(size)*2),
	)

	field, err := schema.NewPrimitiveNodeLogical("f16", parquet.Repetitions.Required, schema.Float16LogicalType{}, parquet.Types.FixedLenByteArray, 2, 1)
	require.NoError(t, err)

	schema, err := schema.NewGroupNode("test", parquet.Repetitions.Required, schema.FieldList{field}, 0)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, schema, file.WithWriterProps(props))

	rgw := writer.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)

	f16ColumnWriter, ok := cw.(*file.FixedLenByteArrayColumnChunkWriter)
	require.True(t, ok)

	nVals, err := f16ColumnWriter.WriteBatch(input[:chunk], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, nVals)

	nVals, err = f16ColumnWriter.WriteBatch(input[chunk:], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, nVals)

	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)

	require.Equal(t, 1, rdr.NumRowGroups())
	require.EqualValues(t, size, rdr.NumRows())

	rgr := rdr.RowGroup(0)
	cr, err := rgr.Column(0)
	require.NoError(t, err)

	f16ColumnReader, ok := cr.(*file.FixedLenByteArrayColumnChunkReader)
	require.True(t, ok)

	output := make([]parquet.FixedLenByteArray, size)

	total, valuesRead, err := f16ColumnReader.ReadBatch(int64(chunk), output[:chunk], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, total)
	require.EqualValues(t, chunk, valuesRead)

	total, valuesRead, err = f16ColumnReader.ReadBatch(int64(chunk), output[chunk:], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, total)
	require.EqualValues(t, chunk, valuesRead)

	require.Equal(t, input, output)

	require.NoError(t, rdr.Close())
}

func TestLZ4RawFileRoundtrip(t *testing.T) {
	input := []int64{
		-1, 0, 1, 2, 3, 4, 5, 123456789, -123456789,
	}

	size := len(input)

	field, err := schema.NewPrimitiveNodeLogical("int64", parquet.Repetitions.Required, nil, parquet.Types.Int64, 0, 1)
	require.NoError(t, err)

	schema, err := schema.NewGroupNode("test", parquet.Repetitions.Required, schema.FieldList{field}, 0)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, schema, file.WithWriterProps(parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Lz4Raw))))

	rgw := writer.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)

	i64ColumnWriter, ok := cw.(*file.Int64ColumnChunkWriter)
	require.True(t, ok)

	nVals, err := i64ColumnWriter.WriteBatch(input, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, size, nVals)

	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)

	require.Equal(t, 1, rdr.NumRowGroups())
	require.EqualValues(t, size, rdr.NumRows())

	rgr := rdr.RowGroup(0)
	cr, err := rgr.Column(0)
	require.NoError(t, err)

	i64ColumnReader, ok := cr.(*file.Int64ColumnChunkReader)
	require.True(t, ok)

	output := make([]int64, size)

	total, valuesRead, err := i64ColumnReader.ReadBatch(int64(size), output, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, size, total)
	require.EqualValues(t, size, valuesRead)

	require.Equal(t, input, output)

	require.NoError(t, rdr.Close())
}

type ColumnIndexObject struct {
	nullPages     []bool
	minValues     [][]byte
	maxValues     [][]byte
	boundaryOrder metadata.BoundaryOrder
	nullCounts    []int64
}

func NewColumnIndexObject(colIdx metadata.ColumnIndex) (ret ColumnIndexObject) {
	if colIdx == nil {
		return
	}

	ret.nullPages = colIdx.GetNullPages()
	ret.minValues = colIdx.GetMinValues()
	ret.maxValues = colIdx.GetMaxValues()
	ret.boundaryOrder = colIdx.GetBoundaryOrder()
	if colIdx.IsSetNullCounts() {
		ret.nullCounts = colIdx.GetNullCounts()
	}
	return
}

func simpleEncode[T int32 | int64 | float32 | float64](val T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&val)), unsafe.Sizeof(val))
}

type PageIndexRoundTripSuite struct {
	suite.Suite

	mem           *memory.CheckedAllocator
	buf           bytes.Buffer
	columnIndexes []ColumnIndexObject
	pageVersion   parquet.DataPageVersion
}

func (t *PageIndexRoundTripSuite) SetupTest() {
	t.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())

	t.buf.Reset()
	t.columnIndexes = make([]ColumnIndexObject, 0)
}

func (t *PageIndexRoundTripSuite) TearDownTest() {
	t.mem.AssertSize(t.T(), 0)
}

func (t *PageIndexRoundTripSuite) writeFile(props *parquet.WriterProperties, tbl arrow.Table) {
	schema := tbl.Schema()
	arrWriterProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(t.mem))

	wr, err := pqarrow.NewFileWriter(schema, &t.buf, props, arrWriterProps)
	t.Require().NoError(err)

	t.Require().NoError(wr.WriteTable(tbl, tbl.NumRows()))
	t.Require().NoError(wr.Close())
}

func (t *PageIndexRoundTripSuite) readPageIndexes(expectNumRG, expectNumPages int, expectColsWithoutIdx []int) {
	rdr, err := file.NewParquetReader(bytes.NewReader(t.buf.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(t.mem)))
	t.Require().NoError(err)
	defer rdr.Close()

	fileMeta := rdr.MetaData()
	t.Equal(expectNumRG, fileMeta.NumRowGroups())

	pageIdxReader := rdr.GetPageIndexReader()
	t.Require().NotNil(pageIdxReader)

	offsetLowerBound := int64(0)
	for rg := range fileMeta.NumRowGroups() {
		rgIdxRdr, err := pageIdxReader.RowGroup(rg)
		t.Require().NoError(err)
		t.Require().NotNil(rgIdxRdr)

		rgReader := rdr.RowGroup(rg)
		t.Require().NotNil(rgReader)

		for col := range fileMeta.Schema.NumColumns() {
			colIdx, err := rgIdxRdr.GetColumnIndex(col)
			t.Require().NoError(err)

			t.columnIndexes = append(t.columnIndexes, NewColumnIndexObject(colIdx))
			expectNoPageIndex := slices.Contains(expectColsWithoutIdx, col)

			offsetIdx, err := rgIdxRdr.GetOffsetIndex(col)
			t.Require().NoError(err)
			if expectNoPageIndex {
				t.Nil(offsetIdx)
			} else {
				t.checkOffsetIndex(offsetIdx, expectNumPages, &offsetLowerBound)
			}

			// verify page stats are not written to page header if page index
			// is enabled
			pgRdr, err := rgReader.GetColumnPageReader(col)
			t.Require().NoError(err)
			t.Require().NotNil(pgRdr)

			for pgRdr.Next() {
				page := pgRdr.Page()
				t.Require().NotNil(page)

				if page.Type() == file.PageTypeDataPage || page.Type() == file.PageTypeDataPageV2 {
					stats := page.(file.DataPage).Statistics()
					t.Equalf(expectNoPageIndex, stats.IsSet(), "rg: %d, col: %d", rg, col)
				}
			}

			t.Require().NoError(pgRdr.Err())
		}
	}
}

func (t *PageIndexRoundTripSuite) checkOffsetIndex(offsetIdx metadata.OffsetIndex, expectNumPages int, offsetLowerBoundInOut *int64) {
	t.Require().NotNil(offsetIdx)
	locations := offsetIdx.GetPageLocations()
	t.Len(locations, expectNumPages)

	prevFirstRowIdx := int64(-1)
	for _, loc := range locations {
		// make sure first row index is ascending within a row group
		t.Greater(loc.FirstRowIndex, prevFirstRowIdx)
		// make sure page offset is ascending across the file
		t.GreaterOrEqual(loc.Offset, *offsetLowerBoundInOut)
		// make sure page size is positive
		t.Greater(loc.CompressedPageSize, int32(0))
		prevFirstRowIdx = loc.FirstRowIndex
		*offsetLowerBoundInOut = loc.Offset + int64(loc.CompressedPageSize)
	}
}

func (t *PageIndexRoundTripSuite) TestSimpleRoundTrip() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithMaxRowGroupLength(4))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{`[
		{"c0": 1,    "c1": "a",  "c2": [1]},
		{"c0": 2,    "c1": "b",  "c2": [1, 2]},
		{"c0": 3,    "c1": "c",  "c2": [null]},
		{"c0": null, "c1": "d",  "c2": []},
		{"c0": 5,    "c1": null, "c2": [3, 3, 3]},
		{"c0": 6,    "c1": "f",  "c2": null}
	]`})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(2, 1, []int{})

	t.Equal(t.columnIndexes, []ColumnIndexObject{
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(1))},
			maxValues:     [][]byte{simpleEncode(int64(3))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{1},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{[]byte("a")},
			maxValues:     [][]byte{[]byte("d")},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(1))},
			maxValues:     [][]byte{simpleEncode(int64(2))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{2},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(5))},
			maxValues:     [][]byte{simpleEncode(int64(6))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{[]byte("f")},
			maxValues:     [][]byte{[]byte("f")},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{1},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(3))},
			maxValues:     [][]byte{simpleEncode(int64(3))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{1},
		},
	})
}

func (t *PageIndexRoundTripSuite) TestRoundTripWithStatsDisabled() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithStats(false))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{`[
		{"c0": 1,    "c1": "a",  "c2": [1]},
		{"c0": 2,    "c1": "b",  "c2": [1, 2]},
		{"c0": 3,    "c1": "c",  "c2": [null]},
		{"c0": null, "c1": "d",  "c2": []},
		{"c0": 5,    "c1": null, "c2": [3, 3, 3]},
		{"c0": 6,    "c1": "f",  "c2": null}
	]`})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(1, 1, []int{})

	for _, colidx := range t.columnIndexes {
		t.Zero(colidx)
	}
}

func (t *PageIndexRoundTripSuite) TestRoundTripWithColumnStatsDisabled() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithStatsFor("c0", false),
		parquet.WithMaxRowGroupLength(4))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{`[
		{"c0": 1,    "c1": "a",  "c2": [1]},
		{"c0": 2,    "c1": "b",  "c2": [1, 2]},
		{"c0": 3,    "c1": "c",  "c2": [null]},
		{"c0": null, "c1": "d",  "c2": []},
		{"c0": 5,    "c1": null, "c2": [3, 3, 3]},
		{"c0": 6,    "c1": "f",  "c2": null}
	]`})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(2, 1, []int{})

	var emptyColIndex ColumnIndexObject
	t.Equal(t.columnIndexes, []ColumnIndexObject{
		emptyColIndex,
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{[]byte("a")},
			maxValues:     [][]byte{[]byte("d")},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(1))},
			maxValues:     [][]byte{simpleEncode(int64(2))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{2},
		},
		emptyColIndex,
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{[]byte("f")},
			maxValues:     [][]byte{[]byte("f")},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{1},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(3))},
			maxValues:     [][]byte{simpleEncode(int64(3))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{1},
		},
	})
}

func (t *PageIndexRoundTripSuite) TestDropLargeStats() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithMaxRowGroupLength(1),
		parquet.WithMaxStatsSize(20))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{`[
		{"c0": "short_string"},
		{"c0": "very_large_string_to_drop_stats"}
	]`})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(2, 1, []int{})

	t.Equal(t.columnIndexes, []ColumnIndexObject{
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{[]byte("short_string")},
			maxValues:     [][]byte{[]byte("short_string")},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{},
	})
}

func (t *PageIndexRoundTripSuite) TestMultiplePages() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithDataPageSize(1))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{
		`[{"c0": 1, "c1": "a"},{"c0": 2, "c1": "b"}]`,
		`[{"c0": 3, "c1": "c"}, {"c0": 4, "c1": "d"}]`,
		`[{"c0": null, "c1": null}, {"c0": 6, "c1": "f"}]`,
		`[{"c0": null, "c1": null}, {"c0": null, "c1": null}]`,
	})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(1, 4, []int{})

	t.Equal(t.columnIndexes, []ColumnIndexObject{
		{
			nullPages: []bool{false, false, false, true},
			minValues: [][]byte{simpleEncode(int64(1)), simpleEncode(int64(3)),
				simpleEncode(int64(6)), {}},
			maxValues: [][]byte{simpleEncode(int64(2)), simpleEncode(int64(4)),
				simpleEncode(int64(6)), {}},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0, 0, 1, 2},
		},
		{
			nullPages:     []bool{false, false, false, true},
			minValues:     [][]byte{[]byte("a"), []byte("c"), []byte("f"), {}},
			maxValues:     [][]byte{[]byte("b"), []byte("d"), []byte("f"), {}},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0, 0, 1, 2},
		},
	})
}

func (t *PageIndexRoundTripSuite) TestDoubleWithNaNs() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true), parquet.WithMaxRowGroupLength(3))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	bldr := array.NewFloat64Builder(t.mem)
	defer bldr.Release()

	chunks := make([]arrow.Array, 4)
	defer func() {
		for _, c := range chunks {
			if c != nil {
				c.Release()
			}
		}
	}()

	nan := math.NaN()
	bldr.AppendValues([]float64{1, nan, 0.1}, nil)
	chunks[0] = bldr.NewArray()
	bldr.AppendValues([]float64{0, nan, 0}, nil)
	chunks[1] = bldr.NewArray()
	bldr.AppendValues([]float64{math.Copysign(0, -1), nan, math.Copysign(0, -1)}, nil)
	chunks[2] = bldr.NewArray()
	bldr.AppendValues([]float64{nan, nan, nan}, nil)
	chunks[3] = bldr.NewArray()

	tbl := array.NewTableFromSlice(sc, [][]arrow.Array{chunks})
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(4, 1, []int{})

	t.Equal(t.columnIndexes, []ColumnIndexObject{
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(float64(0.1))},
			maxValues:     [][]byte{simpleEncode(float64(1))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(math.Copysign(0, -1))},
			maxValues:     [][]byte{simpleEncode(float64(0))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(math.Copysign(0, -1))},
			maxValues:     [][]byte{simpleEncode(float64(0))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{}, // page with only NaN values does not have column index built
	})
}

func (t *PageIndexRoundTripSuite) TestEnablePerColumn() {
	props := parquet.NewWriterProperties(parquet.WithAllocator(t.mem),
		parquet.WithDataPageVersion(t.pageVersion),
		parquet.WithPageIndexEnabled(true),
		parquet.WithPageIndexEnabledFor("c0", true),
		parquet.WithPageIndexEnabledFor("c1", false))

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(t.mem, sc, []string{
		`[{"c0": 0, "c1": 1, "c2": 2}]`,
	})
	t.Require().NoError(err)
	defer tbl.Release()

	t.writeFile(props, tbl)
	t.readPageIndexes(1, 1, []int{1})

	t.Equal(t.columnIndexes, []ColumnIndexObject{
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(0))},
			maxValues:     [][]byte{simpleEncode(int64(0))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
		{}, // page index of c1 is disabled
		{
			nullPages:     []bool{false},
			minValues:     [][]byte{simpleEncode(int64(2))},
			maxValues:     [][]byte{simpleEncode(int64(2))},
			boundaryOrder: metadata.Ascending, nullCounts: []int64{0},
		},
	})
}

func TestPageIndexRoundTripSuite(t *testing.T) {
	t.Run("datapagev1", func(t *testing.T) {
		suite.Run(t, &PageIndexRoundTripSuite{pageVersion: parquet.DataPageV1})
	})
	t.Run("datapagev2", func(t *testing.T) {
		suite.Run(t, &PageIndexRoundTripSuite{pageVersion: parquet.DataPageV2})
	})
}

func TestWriteBloomFilters(t *testing.T) {
	input1 := []parquet.ByteArray{
		parquet.ByteArray("hello"),
		parquet.ByteArray("world"),
		parquet.ByteArray("hello"),
		parquet.ByteArray("parquet"),
	}

	input2 := []parquet.ByteArray{
		parquet.ByteArray("foo"),
		parquet.ByteArray("bar"),
		parquet.ByteArray("baz"),
		parquet.ByteArray("columns"),
	}

	size := len(input1)
	chunk := size / 2

	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithBloomFilterEnabledFor("col1", true),
		parquet.WithBatchSize(int64(chunk)),
	)

	field1, err := schema.NewPrimitiveNode("col1", parquet.Repetitions.Required,
		parquet.Types.ByteArray, -1, -1)
	require.NoError(t, err)
	field2, err := schema.NewPrimitiveNode("col2", parquet.Repetitions.Required,
		parquet.Types.ByteArray, -1, -1)
	require.NoError(t, err)
	sc, err := schema.NewGroupNode("test", parquet.Repetitions.Required,
		schema.FieldList{field1, field2}, -1)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))

	rgw := writer.AppendRowGroup()
	cwr, err := rgw.NextColumn()
	require.NoError(t, err)

	cw, ok := cwr.(*file.ByteArrayColumnChunkWriter)
	require.True(t, ok)

	nVals, err := cw.WriteBatch(input1[:chunk], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, nVals)

	nVals, err = cw.WriteBatch(input1[chunk:], nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, chunk, nVals)

	cwr, err = rgw.NextColumn()
	require.NoError(t, err)
	cw, ok = cwr.(*file.ByteArrayColumnChunkWriter)
	require.True(t, ok)

	nVals, err = cw.WriteBatch(input2, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, size, nVals)

	require.NoError(t, cwr.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)

	bloom := rdr.GetBloomFilterReader()
	bloomRgr, err := bloom.RowGroup(0)
	require.NoError(t, err)

	filter, err := bloomRgr.GetColumnBloomFilter(1)
	require.NoError(t, err)
	require.Nil(t, filter) // no filter written for col2

	filter, err = bloomRgr.GetColumnBloomFilter(0)
	require.NoError(t, err)
	require.NotNil(t, filter)

	byteArrayFilter := metadata.TypedBloomFilter[parquet.ByteArray]{BloomFilter: filter}
	assert.True(t, byteArrayFilter.Check(parquet.ByteArray("hello")))
	assert.True(t, byteArrayFilter.Check(parquet.ByteArray("world")))
	assert.True(t, byteArrayFilter.Check(parquet.ByteArray("parquet")))
	assert.False(t, byteArrayFilter.Check(parquet.ByteArray("foo")))
	assert.False(t, byteArrayFilter.Check(parquet.ByteArray("bar")))
	assert.False(t, byteArrayFilter.Check(parquet.ByteArray("baz")))
}
