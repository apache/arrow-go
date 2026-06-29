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
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrutils "github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vectorColumnDescr builds the leaf Column for a non-nullable
// FixedSizeList<float, vectorLen> encoded as a reduced VECTOR leaf.
func vectorColumnDescr(t *testing.T, vectorLen int32) *schema.Column {
	t.Helper()
	return vectorPrimitiveColumnDescr(t, parquet.Types.Float, -1, vectorLen)
}

// vectorPrimitiveColumnDescr builds the leaf Column for a non-nullable
// FixedSizeList<primitive, vectorLen> VECTOR leaf of the given physical type.
// (FixedLenByteArray, written by FixedLenByteArrayColumnChunkWriter's custom
// batching loop, is reached by passing parquet.Types.FixedLenByteArray.)
func vectorPrimitiveColumnDescr(t *testing.T, physical parquet.Type, typeLen int, vectorLen int32) *schema.Column {
	t.Helper()
	leaf := schema.MustPrimitive(schema.NewPrimitiveNodeLogicalVector("v", nil, physical, typeLen, vectorLen, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{leaf}, -1))
	return schema.NewSchema(root).Column(0)
}

// newVectorChunkWriter wires up a buffer sink, metadata builder, page writer and
// column-chunk writer for descr. Callers type-assert the returned writer to the
// concrete type they need and read back the bytes via sink.Finish().
func newVectorChunkWriter(t *testing.T, descr *schema.Column, props *parquet.WriterProperties) (file.ColumnChunkWriter, *encoding.BufferWriter) {
	t.Helper()
	sink := encoding.NewBufferWriter(0, mem)
	meta := metadata.NewColumnChunkMetaDataBuilder(props, descr)
	pager, err := file.NewPageWriter(sink, compress.Codecs.Uncompressed, compress.DefaultCompressionLevel, meta, -1, -1, mem, false, nil, nil)
	require.NoError(t, err)
	return file.NewColumnChunkWriter(meta, pager, props), sink
}

// newVectorChunkReader opens a column reader over an in-memory chunk of
// totalValues physical leaf values.
func newVectorChunkReader(t *testing.T, descr *schema.Column, data []byte, totalValues int64) file.ColumnChunkReader {
	t.Helper()
	bufPool := sync.Pool{New: func() interface{} { return memory.NewResizableBuffer(mem) }}
	pages, err := file.NewPageReader(arrutils.NewByteReader(data), totalValues, compress.Codecs.Uncompressed, mem, nil)
	require.NoError(t, err)
	return file.NewColumnReader(descr, pages, mem, &bufPool)
}

// assertWholeVectorPages verifies every data page holds a whole number of
// vectors (no vector value split across a page boundary), and optionally that
// there is more than one data page so the invariant is meaningfully exercised.
func assertWholeVectorPages(t *testing.T, data []byte, totalValues int64, vectorLen int32, requireMultiple bool) {
	t.Helper()
	pr, err := file.NewPageReader(arrutils.NewByteReader(data), totalValues, compress.Codecs.Uncompressed, mem, nil)
	require.NoError(t, err)
	pages := 0
	for pr.Next() {
		if dp, ok := pr.Page().(file.DataPage); ok {
			pages++
			assert.Zero(t, dp.NumValues()%vectorLen, "page %d has %d values, not a multiple of vector length %d", pages, dp.NumValues(), vectorLen)
		}
	}
	require.NoError(t, pr.Err())
	if requireMultiple {
		assert.Greater(t, pages, 1, "expected multiple data pages so the page-boundary invariant is meaningfully exercised")
	}
}

// A non-nullable VECTOR column must (1) account rows as values/vector_length
// (not one row per leaf slot) and (2) never split a vector value across data
// pages.
func TestVectorColumnWriterRowAccountingAndPaging(t *testing.T) {
	const vectorLen = 4
	const numRows = 512

	descr := vectorColumnDescr(t, vectorLen)
	require.True(t, descr.InVectorColumn())
	require.EqualValues(t, vectorLen, descr.EffectiveVectorLength())
	require.EqualValues(t, 0, descr.MaxDefinitionLevel())
	require.EqualValues(t, 0, descr.MaxRepetitionLevel())

	// Small page size + small batch size to force many pages and a batch size
	// that is not a multiple of vectorLen, exercising vector-aligned batching.
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024),
		parquet.WithBatchSize(37),
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.Float32ColumnChunkWriter)

	values := make([]float32, numRows*vectorLen)
	for i := range values {
		values[i] = float32(i)
	}
	n, err := w.WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(values), n)
	require.NoError(t, w.Close())

	// (1) Rows are counted as vectors, not leaf slots.
	assert.Equal(t, numRows, w.RowsWritten())

	readbuf := sink.Finish()
	defer readbuf.Release()
	totalValues := int64(numRows * vectorLen)

	// (2) Every data page holds a whole number of vectors.
	assertWholeVectorPages(t, readbuf.Bytes(), totalValues, vectorLen, true)

	// Values round-trip intact.
	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.Float32ColumnChunkReader)
	out := make([]float32, totalValues)
	var got int64
	for got < totalValues && rdr.HasNext() {
		_, read, err := rdr.ReadBatch(totalValues-got, out[got:], nil, nil)
		require.NoError(t, err)
		got += int64(read)
	}
	assert.Equal(t, totalValues, got)
	assert.Equal(t, values, out)
}

// WriteBatchSpaced has its own batching path; it must still batch VECTOR data
// on whole-vector boundaries even when WriteBatchSize is not a multiple of the
// vector length.
func TestVectorColumnWriterWriteBatchSpacedAligns(t *testing.T) {
	const vectorLen = 3
	const numRows = 50

	descr := vectorColumnDescr(t, vectorLen)
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(128),
		parquet.WithBatchSize(10), // not a multiple of vectorLen
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.Float32ColumnChunkWriter)

	values := make([]float32, numRows*vectorLen)
	for i := range values {
		values[i] = float32(i)
	}
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values))))
	bitutil.SetBitsTo(validBits, 0, int64(len(values)), true)
	w.WriteBatchSpaced(values, nil, nil, validBits, 0)
	require.NoError(t, w.Close())
	assert.Equal(t, numRows, w.RowsWritten())

	readbuf := sink.Finish()
	defer readbuf.Release()
	assertWholeVectorPages(t, readbuf.Bytes(), int64(len(values)), vectorLen, false)
}

// WriteBitmapBatchSpaced is the boolean-specific spaced path. It must preserve
// vector-aligned page boundaries and row accounting.
func TestVectorColumnWriterWriteBitmapBatchSpacedAligns(t *testing.T) {
	const vectorLen = 3
	const numRows = 50
	const totalValues = numRows * vectorLen

	descr := vectorPrimitiveColumnDescr(t, parquet.Types.Boolean, -1, vectorLen)
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(128),
		parquet.WithBatchSize(10), // not a multiple of vectorLen
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.BooleanColumnChunkWriter)

	bitmap := make([]byte, bitutil.BytesForBits(totalValues))
	validBits := make([]byte, bitutil.BytesForBits(totalValues))
	bitutil.SetBitsTo(validBits, 0, totalValues, true)
	expected := make([]bool, totalValues)
	for i := range expected {
		expected[i] = i%3 != 0
		bitutil.SetBitTo(bitmap, i, expected[i])
	}

	w.WriteBitmapBatchSpaced(bitmap, 0, totalValues, nil, nil, validBits, 0)
	require.NoError(t, w.Close())
	assert.Equal(t, numRows, w.RowsWritten())

	readbuf := sink.Finish()
	defer readbuf.Release()
	assertWholeVectorPages(t, readbuf.Bytes(), totalValues, vectorLen, false)

	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.BooleanColumnChunkReader)
	out := make([]bool, totalValues)
	var got int64
	for got < totalValues && rdr.HasNext() {
		_, read, err := rdr.ReadBatch(totalValues-got, out[got:], nil, nil)
		require.NoError(t, err)
		got += int64(read)
	}
	assert.EqualValues(t, totalValues, got)
	assert.Equal(t, expected, out)
}

// WriteDictIndices is used when callers provide pre-dictionary-encoded Arrow
// indices. It has its own writer path and must also keep VECTOR page batches on
// whole-vector boundaries.
func TestVectorColumnWriterWriteDictIndicesAligns(t *testing.T) {
	const vectorLen = 3
	const numRows = 50
	const totalValues = numRows * vectorLen

	descr := vectorPrimitiveColumnDescr(t, parquet.Types.Int32, -1, vectorLen)
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(128),
		parquet.WithBatchSize(10), // not a multiple of vectorLen
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.Int32ColumnChunkWriter)

	dictValues := []int32{10, 20, 30}
	dictBuilder := array.NewInt32Builder(mem)
	defer dictBuilder.Release()
	dictBuilder.AppendValues(dictValues, nil)
	dict := dictBuilder.NewArray()
	defer dict.Release()
	require.NoError(t, w.CurrentEncoder().(encoding.DictEncoder).PutDictionary(dict))

	idxBuilder := array.NewInt32Builder(mem)
	defer idxBuilder.Release()
	expected := make([]int32, totalValues)
	for i := range expected {
		idx := int32(i % len(dictValues))
		idxBuilder.Append(idx)
		expected[i] = dictValues[idx]
	}
	indices := idxBuilder.NewArray()
	defer indices.Release()

	require.NoError(t, w.WriteDictIndices(indices, nil, nil))
	require.NoError(t, w.Close())
	assert.Equal(t, numRows, w.RowsWritten())

	readbuf := sink.Finish()
	defer readbuf.Release()
	assertWholeVectorPages(t, readbuf.Bytes(), totalValues, vectorLen, false)

	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.Int32ColumnChunkReader)
	out := make([]int32, totalValues)
	var got int64
	for got < totalValues && rdr.HasNext() {
		_, read, err := rdr.ReadBatch(totalValues-got, out[got:], nil, nil)
		require.NoError(t, err)
		got += int64(read)
	}
	assert.EqualValues(t, totalValues, got)
	assert.Equal(t, expected, out)
}

// Writing a partial vector (a leaf-slot count that is not a whole multiple of
// the vector length) is rejected up front with the typed
// file.ErrVectorBatchMisaligned on both the batch and spaced write paths, rather
// than producing a malformed column or a recovered "unknown error type" panic.
func TestVectorColumnWriterRejectsPartialVector(t *testing.T) {
	const vectorLen = 4
	descr := vectorColumnDescr(t, vectorLen)
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))

	// 10 is not a multiple of vectorLen=4. The error must come from the up-front
	// validateVectorBatch, not from the recovered internal panic: the deferred
	// recover() prefixes "unknown error type", so its absence proves rejection
	// happened before any values were written. (The internal panic also wraps
	// ErrVectorBatchMisaligned, so ErrorIs alone would not catch its removal.)
	cw, _ := newVectorChunkWriter(t, descr, props)
	_, err := cw.(*file.Float32ColumnChunkWriter).WriteBatch(make([]float32, 10), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, file.ErrVectorBatchMisaligned)
	assert.NotContains(t, err.Error(), "unknown error type")

	// The spaced write path validates the same invariant up front.
	cw2, _ := newVectorChunkWriter(t, descr, props)
	values := make([]float32, 10)
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values))))
	bitutil.SetBitsTo(validBits, 0, int64(len(values)), true)
	_, err = cw2.(*file.Float32ColumnChunkWriter).WriteBatchSpacedWithError(values, nil, nil, validBits, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, file.ErrVectorBatchMisaligned)
	assert.NotContains(t, err.Error(), "unknown error type")
}

// A VECTOR-repeated leaf contributes no definition or repetition level, so
// LevelInfo.Increment must leave the running level info unchanged, and the leaf
// column of a non-nullable vector has no inner levels at all.
func TestVectorLevelInfoNoOp(t *testing.T) {
	const vectorLen = 4
	leaf := schema.MustPrimitive(schema.NewPrimitiveNodeLogicalVector("v", nil, parquet.Types.Float, -1, vectorLen, -1))

	// Increment over the VECTOR leaf is a no-op.
	before := file.LevelInfo{DefLevel: 2, RepLevel: 1, RepeatedAncestorDefLevel: 1}
	after := before
	after.Increment(leaf)
	assert.Equal(t, before, after)

	// Sanity: an optional group still increments the definition level.
	opt := schema.MustGroup(schema.NewGroupNode("g", parquet.Repetitions.Optional, schema.FieldList{
		schema.MustPrimitive(schema.NewPrimitiveNode("c", parquet.Repetitions.Required, parquet.Types.Int32, -1, -1)),
	}, -1))
	li := file.LevelInfo{}
	li.Increment(opt)
	assert.EqualValues(t, 1, li.DefLevel)

	// The leaf column of a required vector has DefLevel/RepLevel 0 (no inner levels).
	cw, _ := newVectorChunkWriter(t, vectorColumnDescr(t, vectorLen), parquet.NewWriterProperties(parquet.WithDictionaryDefault(false)))
	got := cw.LevelInfo()
	assert.EqualValues(t, 0, got.DefLevel)
	assert.EqualValues(t, 0, got.RepLevel)
	assert.EqualValues(t, 0, got.RepeatedAncestorDefLevel)
	require.NoError(t, cw.Close())
}

// A FixedLenByteArray VECTOR column must (1) write successfully even when the
// vector length does not divide WriteBatchSize, (2) account rows as
// values/vector_length, and (3) never split a vector value across data pages.
// Regression test: the FLBA WriteBatch loop previously batched without any
// vector awareness, so a non-dividing vector length produced a partial-vector
// batch and the write failed with a recovered panic.
func TestVectorFLBAColumnWriterPaging(t *testing.T) {
	const vectorLen = 3 // does not divide the default WriteBatchSize (1024)
	const typeLen = 16
	const numRows = 256

	descr := vectorPrimitiveColumnDescr(t, parquet.Types.FixedLenByteArray, typeLen, vectorLen)
	require.True(t, descr.InVectorColumn())
	require.EqualValues(t, vectorLen, descr.EffectiveVectorLength())

	// Small page size + a batch size that is not a multiple of vectorLen, to
	// stress vector-aligned batching and force many pages.
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024),
		parquet.WithBatchSize(37),
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.FixedLenByteArrayColumnChunkWriter)

	values := make([]parquet.FixedLenByteArray, numRows*vectorLen)
	for i := range values {
		v := make([]byte, typeLen)
		for k := 0; k < typeLen; k++ {
			v[k] = byte((i + k) % 251)
		}
		values[i] = v
	}
	n, err := w.WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(values), n)
	require.NoError(t, w.Close())

	// (1) Rows counted as vectors, not leaf slots.
	assert.Equal(t, numRows, w.RowsWritten())

	readbuf := sink.Finish()
	defer readbuf.Release()
	totalValues := int64(numRows * vectorLen)

	// (2) Every data page holds a whole number of vectors.
	assertWholeVectorPages(t, readbuf.Bytes(), totalValues, vectorLen, true)

	// (3) Values round-trip intact.
	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.FixedLenByteArrayColumnChunkReader)
	out := make([]parquet.FixedLenByteArray, totalValues)
	var got int64
	for got < totalValues && rdr.HasNext() {
		_, read, err := rdr.ReadBatch(totalValues-got, out[got:], nil, nil)
		require.NoError(t, err)
		got += int64(read)
	}
	assert.Equal(t, totalValues, got)
	assert.Equal(t, values, out)
}

// The low-level columnChunkReader.SeekToRow must translate a row ordinal to a
// fixed value stride for VECTOR columns. Regression test: it previously called
// skipRows, which treats one leaf slot as one row and undershot the seek by a
// factor of the vector length. Both DataPageV1 (the row-group-reset fallback)
// and DataPageV2 (the page-level stride branch) are exercised.
func TestVectorColumnReaderSeekToRow(t *testing.T) {
	cases := []struct {
		name      string
		props     *parquet.WriterProperties
		vectorLen int32
		numRows   int
		rows      []int64
	}{
		{
			name:      "datapage-v1",
			props:     parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(256)),
			vectorLen: 3, numRows: 500, rows: []int64{0, 1, 7, 250, 499},
		},
		{
			name:      "datapage-v2",
			props:     parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithDataPageVersion(parquet.DataPageV2), parquet.WithDataPageSize(256)),
			vectorLen: 5, numRows: 400, rows: []int64{0, 1, 13, 199, 399},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			descr := vectorColumnDescr(t, tc.vectorLen)
			cw, sink := newVectorChunkWriter(t, descr, tc.props)
			w := cw.(*file.Float32ColumnChunkWriter)
			values := make([]float32, tc.numRows*int(tc.vectorLen))
			for i := range values {
				values[i] = float32(i)
			}
			_, err := w.WriteBatch(values, nil, nil)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			readbuf := sink.Finish()
			defer readbuf.Release()
			totalValues := int64(tc.numRows) * int64(tc.vectorLen)
			rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.Float32ColumnChunkReader)

			for _, row := range tc.rows {
				require.NoError(t, rdr.SeekToRow(row), "seek row %d", row)
				out := make([]float32, tc.vectorLen)
				_, read, err := rdr.ReadBatch(int64(tc.vectorLen), out, nil, nil)
				require.NoError(t, err)
				require.EqualValues(t, tc.vectorLen, read, "row %d", row)
				start := int(row) * int(tc.vectorLen)
				assert.Equal(t, values[start:start+int(tc.vectorLen)], out, "seek row %d landed at wrong stride", row)
			}

			assert.Error(t, rdr.SeekToRow(-1))
			assert.Error(t, rdr.SeekToRow(int64(tc.numRows)), "row index equal to parent row count must be out of range")
		})
	}
}

// A dictionary-encoded VECTOR column has a dictionary page before its data
// pages. The V1/no-offset-index value-stride seek rewinds to the row-group
// start, which re-streams that dictionary page; the reader must treat the
// already-configured dictionary as a no-op rather than failing with "more than
// one dictionary". Seeks include row 0 (fresh) and backward seeks so the rewind
// path is exercised every time. (Existing seek tests disable dictionaries.)
func TestVectorColumnReaderSeekToRowDictEncoded(t *testing.T) {
	const vectorLen = 3
	const numRows = 400

	descr := vectorColumnDescr(t, vectorLen)
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(128), // small pages -> many data pages after the dictionary
	)
	cw, sink := newVectorChunkWriter(t, descr, props)
	w := cw.(*file.Float32ColumnChunkWriter)

	values := make([]float32, numRows*vectorLen)
	for i := range values {
		values[i] = float32(i % 17) // few distinct values -> dictionary-encoded
	}
	_, err := w.WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	readbuf := sink.Finish()
	defer readbuf.Release()
	totalValues := int64(numRows * vectorLen)
	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), totalValues).(*file.Float32ColumnChunkReader)

	for _, row := range []int64{0, int64(numRows - 1), 3, 200, 1, 0} {
		require.NoError(t, rdr.SeekToRow(row), "seek row %d", row)
		out := make([]float32, vectorLen)
		_, read, err := rdr.ReadBatch(int64(vectorLen), out, nil, nil)
		require.NoError(t, err, "read after seek row %d", row)
		require.EqualValues(t, vectorLen, read, "row %d", row)
		start := int(row) * vectorLen
		assert.Equal(t, values[start:start+vectorLen], out, "seek row %d landed at wrong stride", row)
	}
}

func TestVectorColumnReaderRejectsMalformedValueCount(t *testing.T) {
	const vectorLen = 3
	const numRows = 10

	descr := vectorColumnDescr(t, vectorLen)
	cw, sink := newVectorChunkWriter(t, descr, parquet.NewWriterProperties(parquet.WithDictionaryDefault(false)))
	w := cw.(*file.Float32ColumnChunkWriter)
	_, err := w.WriteBatch(make([]float32, numRows*vectorLen), nil, nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	readbuf := sink.Finish()
	defer readbuf.Release()

	// The low-level NewPageReader API takes a raw value count; make it malformed
	// to verify VECTOR readers reject chunks that cannot contain whole vectors.
	rdr := newVectorChunkReader(t, descr, readbuf.Bytes(), int64(numRows*vectorLen-1)).(*file.Float32ColumnChunkReader)
	assert.Error(t, rdr.SeekToRow(0))
}

func TestVectorRowGroupReaderRejectsMalformedNumValues(t *testing.T) {
	const vectorLen = 3
	const numRows = 4

	leaf := schema.MustPrimitive(schema.NewPrimitiveNodeLogicalVector("v", nil, parquet.Types.Float, -1, vectorLen, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{leaf}, -1))
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))

	var buf bytes.Buffer
	writer, err := file.NewParquetWriterWithError(&buf, root, file.WithWriterProps(props))
	require.NoError(t, err)
	rgw := writer.AppendRowGroup()
	cwr, err := rgw.NextColumn()
	require.NoError(t, err)
	cw := cwr.(*file.Float32ColumnChunkWriter)
	_, err = cw.WriteBatch(make([]float32, numRows*vectorLen), nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	// Keep num_values a multiple of vector_length but inconsistent with the row
	// count, so the row-group metadata validation catches malformed VECTOR chunks
	// before a page reader is handed out.
	reader.MetaData().RowGroups[0].Columns[0].MetaData.NumValues -= int64(vectorLen)
	_, err = reader.RowGroup(0).GetColumnPageReader(0)
	require.ErrorContains(t, err, "has 9 values for 4 rows and vector length 3")
}
