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
	"context"
	"crypto/rand"
	"runtime"
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

// TestLargeByteArrayValuesDoNotOverflowInt32 tests that writing large byte array
// values that would exceed the adaptive batch sizing threshold does not cause
// an int32 overflow panic in FlushCurrentPage.
func TestLargeByteArrayValuesDoNotOverflowInt32(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	// Create schema with a single byte array column
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("large_data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024),
	)

	out := &bytes.Buffer{}
	writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
	defer writer.Close()

	rgw := writer.AppendRowGroup()
	colWriter, _ := rgw.NextColumn()

	// Create 700 values of 1.5MB each (1.05GB total)
	// This triggers adaptive batch sizing in WriteBatch
	const valueSize = 1.5 * 1024 * 1024
	const numValues = 700

	largeValue := make([]byte, int(valueSize))
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = largeValue
	}

	byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
	_, err := byteArrayWriter.WriteBatch(values, nil, nil)
	assert.NoError(t, err)

	err = colWriter.Close()
	assert.NoError(t, err)

	err = rgw.Close()
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	assert.Greater(t, out.Len(), 0, "should have written data to buffer")
}

// TestLargeStringArrayWithArrow tests the pqarrow integration path with large values.
func TestLargeStringArrayWithArrow(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	mem := memory.NewGoAllocator()

	field := arrow.Field{Name: "large_strings", Type: arrow.BinaryTypes.LargeString, Nullable: true}
	arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)

	out := &bytes.Buffer{}
	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024),
	)

	pqw, err := pqarrow.NewFileWriter(arrowSchema, out, props, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)

	const valueSize = 10 * 1024 * 1024
	const valuesPerBatch = 10
	const numBatches = 11

	largeStr := string(make([]byte, valueSize))

	for batchNum := 0; batchNum < numBatches; batchNum++ {
		builder := array.NewLargeStringBuilder(mem)
		for i := 0; i < valuesPerBatch; i++ {
			builder.Append(largeStr)
		}
		arr := builder.NewArray()

		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(valuesPerBatch))

		err = pqw.Write(rec)

		rec.Release()
		arr.Release()
		builder.Release()

		assert.NoError(t, err)
	}

	err = pqw.Close()
	assert.NoError(t, err)

	assert.Greater(t, out.Len(), 0, "should have written data to buffer")
}

// TestLargeByteArrayRoundTripCorrectness verifies that large ByteArray values
// written through the pqarrow path are read back correctly. This ensures the
// adaptive batch sizing in WriteBatch produces valid data pages (levels and
// values stay in sync).
func TestLargeByteArrayRoundTripCorrectness(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	mem := memory.NewGoAllocator()

	// Use LARGE_STRING (int64 offsets) to handle >2GB of string data
	field := arrow.Field{Name: "data", Type: arrow.BinaryTypes.LargeString, Nullable: false}
	arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)

	out := &bytes.Buffer{}
	props := parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024),
	)

	pqw, err := pqarrow.NewFileWriter(arrowSchema, out, props, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)

	// Create values with distinguishable content (2MB each, ~1GB total)
	const valueSize = 2 * 1024 * 1024
	const numValues = 512

	// Generate unique values: first 8 bytes encode the index, rest is random
	expectedValues := make([]string, numValues)
	buf := make([]byte, valueSize)
	for i := 0; i < numValues; i++ {
		rand.Read(buf[8:]) // random payload
		// Encode index in first 8 bytes for identification
		buf[0] = byte(i >> 24)
		buf[1] = byte(i >> 16)
		buf[2] = byte(i >> 8)
		buf[3] = byte(i)
		buf[4] = byte(^i >> 24)
		buf[5] = byte(^i >> 16)
		buf[6] = byte(^i >> 8)
		buf[7] = byte(^i)
		expectedValues[i] = string(buf)
	}

	// Write all values in a single batch to exercise adaptive batch sizing
	builder := array.NewLargeStringBuilder(mem)
	for _, v := range expectedValues {
		builder.Append(v)
	}
	arr := builder.NewArray()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numValues))

	err = pqw.Write(rec)
	require.NoError(t, err)

	rec.Release()
	arr.Release()
	builder.Release()

	err = pqw.Close()
	require.NoError(t, err)

	// Read back and verify
	rdr, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	tbl, err := arrowRdr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	require.EqualValues(t, numValues, tbl.NumRows())
	require.Equal(t, 1, int(tbl.NumCols()))

	// Verify each value matches.
	// Parquet reads LARGE_STRING back as STRING (both map to ByteArray).
	col := tbl.Column(0)
	rowIdx := 0
	for _, chunk := range col.Data().Chunks() {
		strArr := chunk.(*array.String)
		for j := 0; j < strArr.Len(); j++ {
			got := strArr.Value(j)
			require.Equal(t, len(expectedValues[rowIdx]), len(got),
				"value %d: length mismatch", rowIdx)
			require.Equal(t, expectedValues[rowIdx][:8], got[:8],
				"value %d: header mismatch (data corruption)", rowIdx)
			require.Equal(t, expectedValues[rowIdx], got,
				"value %d: full content mismatch", rowIdx)
			rowIdx++
		}
	}
	require.Equal(t, numValues, rowIdx, "did not read back all values")
}

// TestLargeByteArrayRoundTripWithNulls verifies correctness of the
// WriteBatchSpaced path (nullable column) with large values.
func TestLargeByteArrayRoundTripWithNulls(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	mem := memory.NewGoAllocator()

	field := arrow.Field{Name: "data", Type: arrow.BinaryTypes.LargeString, Nullable: true}
	arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)

	out := &bytes.Buffer{}
	props := parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024),
	)

	pqw, err := pqarrow.NewFileWriter(arrowSchema, out, props, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)

	// 1MB values, every 3rd value is null, ~340MB total
	const valueSize = 1 * 1024 * 1024
	const numValues = 512

	type expected struct {
		isNull bool
		value  string
	}
	expectedValues := make([]expected, numValues)
	buf := make([]byte, valueSize)

	builder := array.NewLargeStringBuilder(mem)
	for i := 0; i < numValues; i++ {
		if i%3 == 0 {
			builder.AppendNull()
			expectedValues[i] = expected{isNull: true}
		} else {
			// Unique content per value
			buf[0] = byte(i >> 24)
			buf[1] = byte(i >> 16)
			buf[2] = byte(i >> 8)
			buf[3] = byte(i)
			for j := 4; j < valueSize; j++ {
				buf[j] = byte(i + j)
			}
			s := string(buf)
			builder.Append(s)
			expectedValues[i] = expected{isNull: false, value: s}
		}
	}
	arr := builder.NewArray()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numValues))

	err = pqw.Write(rec)
	require.NoError(t, err)

	rec.Release()
	arr.Release()
	builder.Release()

	err = pqw.Close()
	require.NoError(t, err)

	// Read back and verify
	rdr, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	tbl, err := arrowRdr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	require.EqualValues(t, numValues, tbl.NumRows())

	col := tbl.Column(0)
	rowIdx := 0
	for _, chunk := range col.Data().Chunks() {
		strArr := chunk.(*array.String)
		for j := 0; j < strArr.Len(); j++ {
			exp := expectedValues[rowIdx]
			if exp.isNull {
				require.True(t, strArr.IsNull(j), "value %d: expected null", rowIdx)
			} else {
				require.False(t, strArr.IsNull(j), "value %d: unexpected null", rowIdx)
				got := strArr.Value(j)
				require.Equal(t, len(exp.value), len(got),
					"value %d: length mismatch", rowIdx)
				require.Equal(t, exp.value[:4], got[:4],
					"value %d: header mismatch (data corruption)", rowIdx)
				require.Equal(t, exp.value, got,
					"value %d: full content mismatch", rowIdx)
			}
			rowIdx++
		}
	}
	require.Equal(t, numValues, rowIdx, "did not read back all values")
}
