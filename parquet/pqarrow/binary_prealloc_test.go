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
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

// writeParquetTable writes tbl to an in-memory parquet file using the given
// row group size and writer properties, returning the raw bytes.
func writeParquetTable(t *testing.T, tbl arrow.Table, rowGroupSize int64, writerProps *parquet.WriterProperties) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, rowGroupSize, writerProps, pqarrow.DefaultWriterProps()))
	return buf.Bytes()
}

// readParquetTable reads a parquet file from data using ReadTable (single pass,
// ignores BatchSize) with the given read properties.
func readParquetTable(t *testing.T, data []byte, props pqarrow.ArrowReadProperties) arrow.Table {
	t.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	reader, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := reader.ReadTable(context.Background())
	require.NoError(t, err)
	return tbl
}

// readParquetRecords reads a parquet file by streaming record batches via
// GetRecordReader (which honours BatchSize) and returns all rows as a table.
func readParquetRecords(t *testing.T, data []byte, props pqarrow.ArrowReadProperties) arrow.Table {
	t.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	reader, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	require.NoError(t, err)
	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	var batches []arrow.RecordBatch
	for rr.Next() {
		rec := rr.RecordBatch()
		rec.Retain()
		batches = append(batches, rec)
	}
	require.NoError(t, rr.Err())
	require.NotEmpty(t, batches, "expected at least one record batch")

	tbl := array.NewTableFromRecords(rr.Schema(), batches)
	for _, b := range batches {
		b.Release()
	}
	return tbl
}

// assertTableColumnsEqual compares two arrow tables column by column.
// Chunks are concatenated before comparison so differences in chunking layout
// do not cause false failures.
func assertTableColumnsEqual(t *testing.T, want, got arrow.Table) {
	t.Helper()
	require.Equal(t, want.NumRows(), got.NumRows(), "row count mismatch")
	require.Equal(t, want.NumCols(), got.NumCols(), "column count mismatch")
	mem := memory.DefaultAllocator
	for i := 0; i < int(want.NumCols()); i++ {
		wantArr, err := array.Concatenate(want.Column(i).Data().Chunks(), mem)
		require.NoError(t, err)
		defer wantArr.Release()
		gotArr, err := array.Concatenate(got.Column(i).Data().Chunks(), mem)
		require.NoError(t, err)
		defer gotArr.Release()
		require.True(t, array.Equal(wantArr, gotArr),
			"column %d (%s) data mismatch", i, want.Schema().Field(i).Name)
	}
}

// buildBinaryTable builds a single-column binary table with totalRows rows,
// value lengths drawn uniformly from [minLen, maxLen], and nullFrac fraction
// of rows set to null. Uses a fixed seed for reproducibility.
func buildBinaryTable(t *testing.T, totalRows, minLen, maxLen int, nullFrac float64) arrow.Table {
	t.Helper()
	mem := memory.DefaultAllocator
	rng := rand.New(rand.NewPCG(42, 0))
	bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bldr.Release()
	for i := 0; i < totalRows; i++ {
		if nullFrac > 0 && rng.Float64() < nullFrac {
			bldr.AppendNull()
			continue
		}
		length := minLen + int(rng.IntN(maxLen-minLen+1))
		val := make([]byte, length)
		for j := range val {
			val[j] = byte(rng.IntN(256))
		}
		bldr.Append(val)
	}
	arr := bldr.NewArray()
	defer arr.Release()
	sc := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	col := arrow.NewColumnFromArr(sc.Field(0), arr)
	defer col.Release()
	return array.NewTable(sc, []arrow.Column{*col}, int64(totalRows))
}

// TestPreAllocBinaryData_DefaultIsDisabled verifies that the zero value of
// ArrowReadProperties leaves PreAllocBinaryData as false, so existing callers
// are unaffected without any code changes.
func TestPreAllocBinaryData_DefaultIsDisabled(t *testing.T) {
	props := pqarrow.ArrowReadProperties{}
	require.False(t, props.PreAllocBinaryData)
}

// TestPreAllocBinaryData_CorrectOutput verifies that enabling PreAllocBinaryData
// produces bit-identical output to reading with the flag disabled, across a
// range of column types and configurations.
func TestPreAllocBinaryData_CorrectOutput(t *testing.T) {
	const (
		numRGs    = 2
		rowsPerRG = 100
		totalRows = numRGs * rowsPerRG
	)
	mem := memory.DefaultAllocator
	ctx := context.Background()
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithCompression(compress.Codecs.Zstd),
	)

	t.Run("binary_column", func(t *testing.T) {
		tbl := buildBinaryTable(t, totalRows, 100, 1000, 0)
		defer tbl.Release()
		data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

		baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
		defer baseline.Release()
		got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
		defer got.Release()

		assertTableColumnsEqual(t, baseline, got)
	})

	t.Run("string_column", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(7, 0))
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		for i := 0; i < totalRows; i++ {
			length := 5 + int(rng.IntN(50))
			val := make([]byte, length)
			for j := range val {
				val[j] = byte('a' + rng.IntN(26))
			}
			bldr.Append(string(val))
		}
		arr := bldr.NewArray()
		defer arr.Release()
		sc := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
		col := arrow.NewColumnFromArr(sc.Field(0), arr)
		defer col.Release()
		tbl := array.NewTable(sc, []arrow.Column{*col}, int64(totalRows))
		defer tbl.Release()

		data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

		baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
		defer baseline.Release()
		got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
		defer got.Release()

		assertTableColumnsEqual(t, baseline, got)
	})

	t.Run("nullable_with_nulls", func(t *testing.T) {
		// 10% null values — verifies null positions are preserved exactly.
		tbl := buildBinaryTable(t, totalRows, 50, 500, 0.10)
		defer tbl.Release()
		data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

		baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
		defer baseline.Release()
		got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
		defer got.Release()

		assertTableColumnsEqual(t, baseline, got)
	})

	t.Run("int32_column_unaffected", func(t *testing.T) {
		// Non-binary columns must be a no-op; the type assertion in
		// reserveBinaryData should not panic or corrupt data.
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		for i := 0; i < totalRows; i++ {
			bldr.Append(int32(i))
		}
		arr := bldr.NewArray()
		defer arr.Release()
		sc := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: false}}, nil)
		col := arrow.NewColumnFromArr(sc.Field(0), arr)
		defer col.Release()
		tbl := array.NewTable(sc, []arrow.Column{*col}, int64(totalRows))
		defer tbl.Release()

		data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

		baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
		defer baseline.Release()
		got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
		defer got.Release()

		assertTableColumnsEqual(t, baseline, got)
	})

	t.Run("flba_column_unaffected", func(t *testing.T) {
		// Fixed-length byte array columns use a no-op ReserveData on
		// flbaRecordReader; verify no panic and correct output.
		const byteWidth = 16
		bldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
		defer bldr.Release()
		rng := rand.New(rand.NewPCG(13, 0))
		for i := 0; i < totalRows; i++ {
			val := make([]byte, byteWidth)
			for j := range val {
				val[j] = byte(rng.IntN(256))
			}
			bldr.Append(val)
		}
		arr := bldr.NewArray()
		defer arr.Release()
		sc := arrow.NewSchema([]arrow.Field{{Name: "fixed", Type: &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}, Nullable: false}}, nil)
		col := arrow.NewColumnFromArr(sc.Field(0), arr)
		defer col.Release()
		tbl := array.NewTable(sc, []arrow.Column{*col}, int64(totalRows))
		defer tbl.Release()

		data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

		baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
		defer baseline.Release()
		got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
		defer got.Release()

		assertTableColumnsEqual(t, baseline, got)
	})

	t.Run("dict_encoded_binary", func(t *testing.T) {
		// Dict-encoded binary columns have a no-op ReserveData (BinaryDictionaryBuilder
		// does not expose ReserveData); verify no panic and correct output.
		dictWriterProps := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true))
		bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bldr.Release()
		words := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}
		rng := rand.New(rand.NewPCG(99, 0))
		for i := 0; i < totalRows; i++ {
			bldr.Append(words[rng.IntN(len(words))])
		}
		arr := bldr.NewArray()
		defer arr.Release()
		sc := arrow.NewSchema([]arrow.Field{{Name: "d", Type: arrow.BinaryTypes.Binary, Nullable: false}}, nil)
		col := arrow.NewColumnFromArr(sc.Field(0), arr)
		defer col.Release()
		tbl := array.NewTable(sc, []arrow.Column{*col}, int64(totalRows))
		defer tbl.Release()

		data := writeParquetTable(t, tbl, rowsPerRG, dictWriterProps)

		readProps := pqarrow.ArrowReadProperties{PreAllocBinaryData: true}
		readProps.SetReadDict(0, true)

		pf, err := file.NewParquetReader(bytes.NewReader(data))
		require.NoError(t, err)
		reader, err := pqarrow.NewFileReader(pf, readProps, mem)
		require.NoError(t, err)
		got, err := reader.ReadTable(ctx)
		require.NoError(t, err)
		defer got.Release()

		// Row count correctness is sufficient — dict vs non-dict comparison
		// is not the focus of this test.
		require.Equal(t, int64(totalRows), got.NumRows())
	})
}

// TestPreAllocBinaryData_BatchSizes verifies correctness across the full range
// of batch size configurations when PreAllocBinaryData is enabled, exercising
// the proportional pre-allocation, multi-batch-per-row-group, and row group
// boundary crossing paths.
func TestPreAllocBinaryData_BatchSizes(t *testing.T) {
	const (
		numRGs    = 3
		rowsPerRG = 48
		totalRows = numRGs * rowsPerRG
	)
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithCompression(compress.Codecs.Zstd),
	)
	// Use 5% nulls to exercise nullable code paths alongside the pre-allocation.
	tbl := buildBinaryTable(t, totalRows, 200, 2000, 0.05)
	defer tbl.Release()
	data := writeParquetTable(t, tbl, rowsPerRG, writerProps)

	// Baseline: read everything in one pass with the flag off.
	baseline := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: false})
	defer baseline.Release()

	tests := []struct {
		name      string
		batchSize int64
	}{
		// batchSize=0: entire file in one RecordReader pass; pre-allocation
		// reserves the full row group on entry.
		{"batchAll", 0},
		// batchSize=rowsPerRG: exactly one batch per row group; builder reset
		// and re-reserved once per RG.
		{"batchPerRG", rowsPerRG},
		// batchSize=rowsPerRG/2: two batches per row group; proportional
		// estimate used for batches 2+.
		{"batchHalfRG", rowsPerRG / 2},
		// batchSize=rowsPerRG/4: four batches per row group; most stress on
		// the proportional path — the key case fixed by this feature.
		{"batchQuarterRG", rowsPerRG / 4},
		// batchSize spans a row group boundary; nextRowGroup is called
		// mid-batch and extends the reservation for the incoming row group.
		{"batchSpansBoundary", rowsPerRG * 3 / 2},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			props := pqarrow.ArrowReadProperties{
				PreAllocBinaryData: true,
				BatchSize:          tt.batchSize,
			}
			got := readParquetRecords(t, data, props)
			defer got.Release()
			assertTableColumnsEqual(t, baseline, got)
		})
	}
}

// TestPreAllocBinaryData_SingleRow verifies that a single-row file (where
// numRows == 1 and the guard numRows > 0 is exercised) reads correctly.
func TestPreAllocBinaryData_SingleRow(t *testing.T) {
	mem := memory.DefaultAllocator
	bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bldr.Release()
	bldr.Append([]byte("hello world"))
	arr := bldr.NewArray()
	defer arr.Release()
	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.Binary, Nullable: false}}, nil)
	col := arrow.NewColumnFromArr(sc.Field(0), arr)
	defer col.Release()
	tbl := array.NewTable(sc, []arrow.Column{*col}, 1)
	defer tbl.Release()

	writerProps := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	data := writeParquetTable(t, tbl, 1, writerProps)

	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{PreAllocBinaryData: true})
	defer got.Release()
	require.Equal(t, int64(1), got.NumRows())

	gotArr, err := array.Concatenate(got.Column(0).Data().Chunks(), mem)
	require.NoError(t, err)
	defer gotArr.Release()
	require.Equal(t, []byte("hello world"), gotArr.(*array.Binary).Value(0))
}