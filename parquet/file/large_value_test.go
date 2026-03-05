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
	"fmt"
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
// values totalling over 1GB in a single WriteBatch call triggers adaptive batch
// sizing and does not cause an int32 overflow panic in FlushCurrentPage.
//
// Memory note: input values all share one 1.5MB buffer so input memory is low,
// but the parquet output buffer grows to ~1GB (unavoidable for this boundary test).
func TestLargeByteArrayValuesDoNotOverflowInt32(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

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

	// 700 values × 1.5MB = 1.05GB total, triggers adaptive batch split at ~1GB.
	// All values share the same underlying buffer (only ~1.5MB of input memory).
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

	assert.NoError(t, colWriter.Close())
	assert.NoError(t, rgw.Close())
	assert.NoError(t, writer.Close())
	assert.Greater(t, out.Len(), 0)
}

// TestLargeStringArrayWithArrow tests the pqarrow integration path with large values.
// Writes >1GB total through multiple small batches via the Arrow writer.
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

	// 11 batches × 10 values × 10MB = 1.1GB total.
	// Only one batch (~100MB) is live at a time.
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

	assert.NoError(t, pqw.Close())
	assert.Greater(t, out.Len(), 0)
}

// TestLargeByteArrayRoundTripCorrectness verifies that ByteArray values
// written through the pqarrow path are read back correctly. This ensures the
// adaptive batch sizing loop in WriteBatch produces valid data pages where
// levels and values stay in sync.
//
// Uses modest value sizes (~10KB each, ~2MB total) to keep memory low while
// still exercising the full write→encode→flush→read→decode→compare path.
// The >1GB adaptive-split boundary is tested by TestLargeByteArrayValuesDoNotOverflowInt32.
func TestLargeByteArrayRoundTripCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()

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

	// 200 values × 10KB = ~2MB total. Each value has a unique, deterministic
	// pattern so we can regenerate expected content during verification
	// without storing all values in memory.
	const valueSize = 10 * 1024
	const numValues = 200

	makeValue := func(idx int) string {
		buf := make([]byte, valueSize)
		// First 4 bytes: index for identification
		buf[0] = byte(idx >> 24)
		buf[1] = byte(idx >> 16)
		buf[2] = byte(idx >> 8)
		buf[3] = byte(idx)
		// Remaining bytes: deterministic pattern
		for j := 4; j < valueSize; j++ {
			buf[j] = byte(idx*31 + j)
		}
		return string(buf)
	}

	builder := array.NewLargeStringBuilder(mem)
	for i := 0; i < numValues; i++ {
		builder.Append(makeValue(i))
	}
	arr := builder.NewArray()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numValues))

	err = pqw.Write(rec)
	require.NoError(t, err)

	rec.Release()
	arr.Release()
	builder.Release()

	require.NoError(t, pqw.Close())

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

	col := tbl.Column(0)
	rowIdx := 0
	for _, chunk := range col.Data().Chunks() {
		strArr := chunk.(*array.String)
		for j := 0; j < strArr.Len(); j++ {
			got := strArr.Value(j)
			expected := makeValue(rowIdx)
			require.Equal(t, len(expected), len(got),
				"value %d: length mismatch", rowIdx)
			require.Equal(t, expected[:4], got[:4],
				"value %d: header mismatch (data corruption)", rowIdx)
			require.Equal(t, expected, got,
				"value %d: full content mismatch", rowIdx)
			rowIdx++
		}
	}
	require.Equal(t, numValues, rowIdx, "did not read back all values")
}

// TestLargeByteArrayRoundTripWithNulls verifies correctness of the
// WriteBatchSpaced path (nullable column) with moderately-sized values.
// Every 3rd value is null. Uses ~3MB total.
func TestLargeByteArrayRoundTripWithNulls(t *testing.T) {
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

	const valueSize = 10 * 1024
	const numValues = 300

	makeValue := func(idx int) string {
		buf := make([]byte, valueSize)
		buf[0] = byte(idx >> 24)
		buf[1] = byte(idx >> 16)
		buf[2] = byte(idx >> 8)
		buf[3] = byte(idx)
		for j := 4; j < valueSize; j++ {
			buf[j] = byte(idx*17 + j)
		}
		return string(buf)
	}

	// Track which indices are null for verification
	isNull := func(i int) bool { return i%3 == 0 }

	builder := array.NewLargeStringBuilder(mem)
	for i := 0; i < numValues; i++ {
		if isNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(makeValue(i))
		}
	}
	arr := builder.NewArray()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numValues))

	err = pqw.Write(rec)
	require.NoError(t, err)

	rec.Release()
	arr.Release()
	builder.Release()

	require.NoError(t, pqw.Close())

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
			if isNull(rowIdx) {
				require.True(t, strArr.IsNull(j), "value %d: expected null", rowIdx)
			} else {
				require.False(t, strArr.IsNull(j), "value %d: unexpected null", rowIdx)
				got := strArr.Value(j)
				expected := makeValue(rowIdx)
				require.Equal(t, len(expected), len(got),
					"value %d: length mismatch", rowIdx)
				require.Equal(t, expected[:4], got[:4],
					"value %d: header mismatch (data corruption)", rowIdx)
				require.Equal(t, expected, got,
					fmt.Sprintf("value %d: full content mismatch", rowIdx))
			}
			rowIdx++
		}
	}
	require.Equal(t, numValues, rowIdx, "did not read back all values")
}
