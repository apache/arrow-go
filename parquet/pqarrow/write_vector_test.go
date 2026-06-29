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
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// End-to-end WRITE path: a non-nullable FixedSizeList<float32, N> written
// with WithVectorEncoding produces a VECTOR logical group on disk whose leaf
// holds the flattened values with no inner levels, and the row count is the
// number of vectors (not leaf slots).
func TestWriteFixedSizeListAsVector(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const (
		listSize = int32(4)
		numRows  = 256
	)
	elemType := arrow.PrimitiveTypes.Float32
	elemField := arrow.Field{Name: "element", Type: elemType, Nullable: false}
	fslType := arrow.FixedSizeListOfField(listSize, elemField)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "emb", Type: fslType, Nullable: false}}, nil)

	bldr := array.NewFixedSizeListBuilderWithField(mem, listSize, elemField)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Float32Builder)
	expected := make([]float32, 0, numRows*int(listSize))
	for i := 0; i < numRows; i++ {
		bldr.Append(true)
		for j := int32(0); j < listSize; j++ {
			v := float32(i)*float32(listSize) + float32(j)
			vb.Append(v)
			expected = append(expected, v)
		}
	}
	fslArr := bldr.NewArray()
	defer fslArr.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{fslArr}, numRows)
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithDataPageSize(2048))
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding())
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, numRows, props, arrProps))

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	md := rdr.MetaData()
	assert.EqualValues(t, numRows, md.NumRows)
	require.Equal(t, 1, md.Schema.NumColumns())

	descr := md.Schema.Column(0)
	assert.True(t, descr.InVectorColumn(), "round-tripped schema column should be a VECTOR column")
	assert.EqualValues(t, listSize, descr.EffectiveVectorLength())
	assert.EqualValues(t, 0, descr.MaxDefinitionLevel())
	assert.EqualValues(t, 0, descr.MaxRepetitionLevel())
	assert.Equal(t, "emb.list.element", descr.Path())
	assert.Equal(t, parquet.Types.Float, descr.PhysicalType())

	rgr := rdr.RowGroup(0)
	assert.EqualValues(t, numRows, rgr.NumRows())
	cr, err := rgr.Column(0)
	require.NoError(t, err)
	fr := cr.(*file.Float32ColumnChunkReader)

	out := make([]float32, numRows*int(listSize))
	var total int64
	for fr.HasNext() && total < int64(len(out)) {
		n, _, err := fr.ReadBatch(int64(len(out))-total, out[total:], nil, nil)
		require.NoError(t, err)
		total += n
	}
	assert.EqualValues(t, numRows*int(listSize), total)
	assert.Equal(t, expected, out)
}

// With the flag off, the same FixedSizeList is written as a standard LIST and
// the on-disk column is not a VECTOR column.
func TestWriteFixedSizeListWithoutVectorFlag(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const listSize = int32(3)
	elemType := arrow.PrimitiveTypes.Int32
	elemField := arrow.Field{Name: "element", Type: elemType, Nullable: false}
	fslType := arrow.FixedSizeListOfField(listSize, elemField)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: fslType, Nullable: false}}, nil)

	bldr := array.NewFixedSizeListBuilderWithField(mem, listSize, elemField)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Int32Builder)
	for i := 0; i < 10; i++ {
		bldr.Append(true)
		for j := int32(0); j < listSize; j++ {
			vb.Append(int32(i))
		}
	}
	fslArr := bldr.NewArray()
	defer fslArr.Release()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{fslArr}, 10)
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 10, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	descr := rdr.MetaData().Schema.Column(0)
	assert.False(t, descr.InVectorColumn(), "without the flag the column must use the standard LIST encoding")
}

// TestVectorAdditionalElementTypesRoundTrip is the single end-to-end VECTOR
// round-trip matrix: it builds a non-nullable FixedSizeList<elem, listSize> of
// numRows rows, writes it with WithVectorEncoding (plus per-case writer props),
// reads it back, and asserts the reconstructed column is a VECTOR-encoded
// FixedSizeList equal to the input. It covers every supported element family and
// cross-feature combination (compression, dictionary-read), so the per-type
// round-trips do not need standalone tests.
func TestVectorAdditionalElementTypesRoundTrip(t *testing.T) {
	// flbaPageProps stresses the FixedLenByteArray writer's custom batching loop
	// (used by FixedSizeBinary/Float16) across many pages. Combined with a list
	// size that does NOT divide WriteBatchSize (1024), it guards the regression
	// where a vector value was split across a page boundary and the write failed.
	flbaPageProps := []parquet.WriterProperty{parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(2048)}

	cases := []struct {
		name      string
		elemField arrow.Field
		listSize  int32
		numRows   int
		fill      func(vb array.Builder, numRows int, listSize int32)
		wrProps   []parquet.WriterProperty
		readDict  bool
	}{
		{
			name:      "bool-snappy",
			elemField: arrow.Field{Name: "element", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			listSize:  2, numRows: 3,
			wrProps: []parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Snappy)},
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.BooleanBuilder)
				b.AppendValues([]bool{true, false, true, true, false, false}, nil)
			},
		},
		{
			name:      "int64-zstd-read-dict-requested",
			elemField: arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			listSize:  2, numRows: 3,
			wrProps:  []parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Zstd)},
			readDict: true,
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.Int64Builder)
				b.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
			},
		},
		{
			name:      "date32",
			elemField: arrow.Field{Name: "element", Type: arrow.FixedWidthTypes.Date32, Nullable: false},
			listSize:  2, numRows: 3,
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.Date32Builder)
				b.AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6}, nil)
			},
		},
		{
			name:      "timestamp-us",
			elemField: arrow.Field{Name: "element", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			listSize:  2, numRows: 3,
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.TimestampBuilder)
				b.AppendValues([]arrow.Timestamp{10, 20, 30, 40, 50, 60}, nil)
			},
		},
		{
			name:      "decimal128",
			elemField: arrow.Field{Name: "element", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: false},
			listSize:  2, numRows: 3,
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.Decimal128Builder)
				for _, v := range []string{"1.23", "2.34", "3.45", "4.56", "5.67", "6.78"} {
					require.NoError(t, b.AppendValueFromString(v))
				}
			},
		},
		{
			name:      "decimal256",
			elemField: arrow.Field{Name: "element", Type: &arrow.Decimal256Type{Precision: 40, Scale: 4}, Nullable: false},
			listSize:  2, numRows: 3,
			fill: func(vb array.Builder, _ int, _ int32) {
				b := vb.(*array.Decimal256Builder)
				for _, v := range []string{"1.2345", "2.3456", "3.4567", "4.5678", "5.6789", "6.7890"} {
					require.NoError(t, b.AppendValueFromString(v))
				}
			},
		},
		{
			name:      "float32-many-pages",
			elemField: arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
			listSize:  4, numRows: 256,
			wrProps: []parquet.WriterProperty{parquet.WithDataPageSize(2048)},
			fill:    fillFloat32Vector,
		},
		{
			name:      "float64",
			elemField: arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			listSize:  8, numRows: 128,
			fill: func(vb array.Builder, numRows int, listSize int32) {
				b := vb.(*array.Float64Builder)
				for i := 0; i < numRows*int(listSize); i++ {
					b.Append(float64(i))
				}
			},
		},
		// FixedSizeBinary and Float16 map to the FixedLenByteArray physical type
		// (the custom batching loop). The list sizes 3/768/100 do not divide
		// WriteBatchSize (1024), exercising the FLBA whole-vector page batching.
		{
			name:      "fixed_size_binary-len3",
			elemField: arrow.Field{Name: "element", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Nullable: false},
			listSize:  3, numRows: 256, wrProps: flbaPageProps, fill: fillFixedSizeBinaryVector,
		},
		{
			name:      "fixed_size_binary-len768",
			elemField: arrow.Field{Name: "element", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Nullable: false},
			listSize:  768, numRows: 8, wrProps: flbaPageProps, fill: fillFixedSizeBinaryVector,
		},
		{
			name:      "float16-len3",
			elemField: arrow.Field{Name: "element", Type: &arrow.Float16Type{}, Nullable: false},
			listSize:  3, numRows: 256, wrProps: flbaPageProps, fill: fillFloat16Vector,
		},
		{
			name:      "float16-len100",
			elemField: arrow.Field{Name: "element", Type: &arrow.Float16Type{}, Nullable: false},
			listSize:  100, numRows: 128, wrProps: flbaPageProps, fill: fillFloat16Vector,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			fslType := arrow.FixedSizeListOfField(tc.listSize, tc.elemField)
			field := arrow.Field{Name: "v", Type: fslType, Nullable: false}
			arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)
			bldr := array.NewFixedSizeListBuilderWithField(mem, tc.listSize, tc.elemField)
			defer bldr.Release()
			for i := 0; i < tc.numRows; i++ {
				bldr.Append(true)
			}
			tc.fill(bldr.ValueBuilder(), tc.numRows, tc.listSize)
			arr := bldr.NewArray()
			defer arr.Release()
			require.EqualValues(t, tc.numRows, arr.Len())
			rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(tc.numRows))
			defer rec.Release()
			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
			defer tbl.Release()

			var buf bytes.Buffer
			wrProps := append([]parquet.WriterProperty{parquet.WithAllocator(mem)}, tc.wrProps...)
			require.NoError(t, pqarrow.WriteTable(tbl, &buf, int64(tc.numRows),
				parquet.NewWriterProperties(wrProps...),
				pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding())))
			assertColumnIsVector(t, buf.Bytes(), tc.listSize)

			readProps := pqarrow.ArrowReadProperties{}
			if tc.readDict {
				readProps.SetReadDict(0, true)
			}
			got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()),
				parquet.NewReaderProperties(mem), readProps, mem)
			require.NoError(t, err)
			defer got.Release()
			require.EqualValues(t, tc.numRows, got.NumRows())
			require.Equal(t, field.Type, got.Schema().Field(0).Type)
			require.False(t, got.Schema().Field(0).Nullable)

			// Concatenate across however many chunks the reader produced, then
			// compare the whole reconstructed array (type + values) to the input.
			concat, err := array.Concatenate(got.Column(0).Data().Chunks(), mem)
			require.NoError(t, err)
			defer concat.Release()
			assert.Truef(t, array.Equal(arr, concat), "round-trip mismatch\n want %v\n got  %v", arr, concat)
		})
	}
}

func fillFloat32Vector(vb array.Builder, numRows int, listSize int32) {
	b := vb.(*array.Float32Builder)
	for i := 0; i < numRows*int(listSize); i++ {
		b.Append(float32(i))
	}
}

func fillFixedSizeBinaryVector(vb array.Builder, numRows int, listSize int32) {
	b := vb.(*array.FixedSizeBinaryBuilder)
	for i := 0; i < numRows*int(listSize); i++ {
		v := make([]byte, 8)
		for k := range v {
			v[k] = byte((i + k) % 251)
		}
		b.Append(v)
	}
}

func fillFloat16Vector(vb array.Builder, numRows int, listSize int32) {
	b := vb.(*array.Float16Builder)
	for i := 0; i < numRows*int(listSize); i++ {
		b.Append(float16.New(float32(i) * 0.5))
	}
}

func TestVectorEncodingRejectsUnexpectedNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const listSize = int32(2)
	elem := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	fslType := arrow.FixedSizeListOfField(listSize, elem)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: fslType, Nullable: false}}, nil)

	cases := []struct {
		name string
		fill func(*array.FixedSizeListBuilder)
	}{
		{
			name: "null vector value",
			fill: func(b *array.FixedSizeListBuilder) {
				vb := b.ValueBuilder().(*array.Int32Builder)
				b.Append(false)
				vb.AppendValues([]int32{1, 2}, nil)
			},
		},
		{
			name: "null element value",
			fill: func(b *array.FixedSizeListBuilder) {
				vb := b.ValueBuilder().(*array.Int32Builder)
				b.Append(true)
				vb.Append(1)
				vb.AppendNull()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bldr := array.NewFixedSizeListBuilderWithField(mem, listSize, elem)
			defer bldr.Release()
			tc.fill(bldr)
			arr := bldr.NewArray()
			defer arr.Release()
			rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(arr.Len()))
			defer rec.Release()
			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
			defer tbl.Release()

			var buf bytes.Buffer
			err := pqarrow.WriteTable(tbl, &buf, int64(arr.Len()), parquet.NewWriterProperties(parquet.WithAllocator(mem)),
				pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding()))
			require.Error(t, err)
		})
	}
}

func TestVectorRecordReaderSeekToRow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const (
		listSize = int32(3)
		numRows  = 10
	)
	elemField := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	fslType := arrow.FixedSizeListOfField(listSize, elemField)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: fslType, Nullable: false}}, nil)

	bldr := array.NewFixedSizeListBuilderWithField(mem, listSize, elemField)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Int32Builder)
	for i := 0; i < numRows; i++ {
		bldr.Append(true)
		for j := int32(0); j < listSize; j++ {
			vb.Append(int32(i*10 + int(j)))
		}
	}
	fslArr := bldr.NewArray()
	defer fslArr.Release()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{fslArr}, numRows)
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithAllocator(mem),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(64),
	)
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding())
	// Use multiple row groups so SeekToRow must select the row group by parent
	// row, not by row*vector_length.
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 5, props, arrProps))

	pr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer pr.Close()
	reader, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{BatchSize: 2}, mem)
	require.NoError(t, err)
	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	for _, row := range []int64{0, 1, 2, 4, 5, 8, 9} {
		require.NoError(t, rr.SeekToRow(row), "seek row %d", row)
		require.True(t, rr.Next(), "next after seek row %d: %v", row, rr.Err())

		batch := rr.RecordBatch()
		require.Greater(t, int(batch.NumRows()), 0)
		fsl := batch.Column(0).(*array.FixedSizeList)
		child := fsl.ListValues().(*array.Int32)
		base := fsl.Offset() * int(listSize)
		for j := int32(0); j < listSize; j++ {
			assert.Equal(t, int32(row*10+int64(j)), child.Value(base+int(j)), "row %d element %d", row, j)
		}
	}
}

func TestVectorColumnReaderSeekToRowWithOffsetIndex(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const (
		listSize = int32(3)
		numRows  = 200
	)
	elemField := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	fslType := arrow.FixedSizeListOfField(listSize, elemField)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: fslType, Nullable: false}}, nil)

	bldr := array.NewFixedSizeListBuilderWithField(mem, listSize, elemField)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Int32Builder)
	for i := 0; i < numRows; i++ {
		bldr.Append(true)
		for j := int32(0); j < listSize; j++ {
			vb.Append(int32(i*10 + int(j)))
		}
	}
	fslArr := bldr.NewArray()
	defer fslArr.Release()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{fslArr}, numRows)
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithAllocator(mem),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(64),
		parquet.WithBatchSize(30),
		parquet.WithPageIndexEnabled(true),
	)
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding())
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, numRows, props, arrProps))

	pr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pr.Close()

	rgPageIndex, err := pr.GetPageIndexReader().RowGroup(0)
	require.NoError(t, err)
	offsetIndex, err := rgPageIndex.GetOffsetIndex(0)
	require.NoError(t, err)
	require.NotNil(t, offsetIndex)
	require.Greater(t, len(offsetIndex.GetPageLocations()), 1, "test must exercise offset-index page selection")

	cr, err := pr.RowGroup(0).Column(0)
	require.NoError(t, err)
	defer cr.Close()
	rdr := cr.(*file.Int32ColumnChunkReader)
	for _, row := range []int64{0, 1, 11, 97, int64(numRows - 1)} {
		require.NoError(t, rdr.SeekToRow(row), "seek row %d", row)
		out := make([]int32, listSize)
		_, read, err := rdr.ReadBatch(int64(listSize), out, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, listSize, read)
		for j := int32(0); j < listSize; j++ {
			assert.Equal(t, int32(row*10+int64(j)), out[j], "row %d element %d", row, j)
		}
	}
	assert.Error(t, rdr.SeekToRow(numRows))
}

// assertColumnIsVector opens the written parquet bytes and asserts the single
// top-level column was actually encoded as VECTOR, not as a LIST fallback, so
// the round-trip really exercises the VECTOR write/read path.
func assertColumnIsVector(t *testing.T, b []byte, vectorLen int32) {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(b))
	require.NoError(t, err)
	defer rdr.Close()
	col := rdr.MetaData().Schema.Column(0)
	assert.True(t, col.InVectorColumn(), "expected a VECTOR-encoded column, got rep %s", col.SchemaNode().RepetitionType())
	assert.EqualValues(t, vectorLen, col.EffectiveVectorLength())
}

// storeSchemaRoundTrip writes a non-nullable FixedSizeList<timestamp[us, tz],2>
// carrying user field metadata, with WithStoreSchema() and optionally
// WithVectorEncoding(), then reads it back and returns the reconstructed field.
func storeSchemaRoundTrip(t *testing.T, vector bool) arrow.Field {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}
	elem := arrow.Field{Name: "element", Type: tsType, Nullable: false}
	fsl := arrow.FixedSizeListOfField(2, elem)
	colMeta := arrow.NewMetadata([]string{"unit", "model"}, []string{"meters", "abc"})
	field := arrow.Field{Name: "v", Type: fsl, Nullable: false, Metadata: colMeta}
	sc := arrow.NewSchema([]arrow.Field{field}, nil)

	b := array.NewFixedSizeListBuilderWithField(mem, 2, elem)
	defer b.Release()
	vb := b.ValueBuilder().(*array.TimestampBuilder)
	for i := 0; i < 4; i++ {
		b.Append(true)
		vb.Append(arrow.Timestamp(int64(i)))
		vb.Append(arrow.Timestamp(int64(i) + 100))
	}
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(sc, []arrow.Array{arr}, 4)
	defer rec.Release()
	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	awopts := []pqarrow.WriterOption{pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()}
	if vector {
		awopts = append(awopts, pqarrow.WithVectorEncoding())
	}
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 4,
		parquet.NewWriterProperties(parquet.WithAllocator(mem)),
		pqarrow.NewArrowWriterProperties(awopts...)))

	got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()),
		parquet.NewReaderProperties(mem), pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	defer got.Release()
	return got.Schema().Field(0)
}

// With WithStoreSchema(), the reader restores element-type refinements that
// Parquet cannot carry (here, a timestamp timezone) and user field metadata from
// the embedded Arrow schema. Regression test for the bug where a VECTOR column,
// reconstructed directly as a FixedSizeList, took an early return in
// applyOriginalStorageMetadata (getNestedFactory had no inferred FIXED_SIZE_LIST
// case) and silently dropped both. The standard LIST-encoded path is the control.
func TestVectorStoreSchemaRestoresElementMetadata(t *testing.T) {
	listF := storeSchemaRoundTrip(t, false)
	vecF := storeSchemaRoundTrip(t, true)

	listElem := listF.Type.(*arrow.FixedSizeListType).Elem().(*arrow.TimestampType)
	vecElem := vecF.Type.(*arrow.FixedSizeListType).Elem().(*arrow.TimestampType)

	// Control: the standard LIST path restores the original timezone + metadata.
	require.Equal(t, "America/New_York", listElem.TimeZone)
	require.ElementsMatch(t, []string{"PARQUET:field_id", "unit", "model"}, listF.Metadata.Keys())

	// The VECTOR path must now match the LIST control.
	assert.Equal(t, listElem.TimeZone, vecElem.TimeZone, "VECTOR store-schema must restore element timezone")
	assert.ElementsMatch(t, listF.Metadata.Keys(), vecF.Metadata.Keys(), "VECTOR store-schema must preserve field metadata")
	assert.Equal(t, "meters", metaValue(vecF.Metadata, "unit"))
	assert.Equal(t, "abc", metaValue(vecF.Metadata, "model"))
}

func metaValue(m arrow.Metadata, key string) string {
	if i := m.FindKey(key); i >= 0 {
		return m.Values()[i]
	}
	return ""
}
