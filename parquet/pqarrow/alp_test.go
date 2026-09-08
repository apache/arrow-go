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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The rows are enough for several vectors of the default 1024 values, and the
// chunk size below splits them across row groups.
const alpNumRows = 5000

// alpColumns builds the pair of columns the tests write: mostly two-decimal
// values, which ALP encodes with no exceptions at all, with the cases that
// reach the exception path mixed in. Nulls occupy a definition level but no
// value, so they exercise the path where the two run at different rates.
func alpColumns() (floats []float32, doubles []float64, valid []bool) {
	floats = make([]float32, alpNumRows)
	doubles = make([]float64, alpNumRows)
	valid = make([]bool, alpNumRows)

	for i := range floats {
		floats[i] = float32(i%1000) / 100
		doubles[i] = float64(i%100000) / 1000
		valid[i] = i%100 != 7
	}
	// A vector of one repeated value packs at a bit width of zero, and a vector
	// of full-mantissa values is nothing but exceptions.
	for i := 1024; i < 2048; i++ {
		floats[i], doubles[i] = 7.77, 7.77
	}
	for i := 2048; i < 3072; i++ {
		floats[i] = float32(math.Pi) * float32(i)
		doubles[i] = math.Pi * float64(i)
	}
	// Values with no decimal representation at all. The NaNs carry payloads,
	// which ALP stores as exceptions and has to return bit for bit.
	for _, s := range []struct {
		row  int
		bits uint32
		dbl  uint64
	}{
		{10, 0x7FC00000, 0x7FF8000000000000}, // quiet NaN
		{11, 0x7FC0DEAD, 0x7FF800DEADBEEF00}, // quiet NaN with a payload
		{12, 0xFFC00001, 0xFFF8000000000001}, // negative quiet NaN
		{13, 0x7F800000, 0x7FF0000000000000}, // +Inf
		{14, 0xFF800000, 0xFFF0000000000000}, // -Inf
		{15, 0x80000000, 0x8000000000000000}, // -0.0
		{16, 0x00000001, 0x0000000000000001}, // subnormal
		{17, 0x7F7FFFFF, 0x7FEFFFFFFFFFFFFF}, // largest finite
	} {
		floats[s.row] = math.Float32frombits(s.bits)
		doubles[s.row] = math.Float64frombits(s.dbl)
		valid[s.row] = true
	}
	return floats, doubles, valid
}

// alpTable holds the columns as an Arrow table, ready to write.
func alpTable(t *testing.T, mem memory.Allocator) arrow.Table {
	t.Helper()

	floats, doubles, valid := alpColumns()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "d", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	fb := array.NewFloat32Builder(mem)
	defer fb.Release()
	fb.AppendValues(floats, valid)
	farr := fb.NewArray()
	defer farr.Release()

	db := array.NewFloat64Builder(mem)
	defer db.Release()
	db.AppendValues(doubles, valid)
	darr := db.NewArray()
	defer darr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{farr, darr}, alpNumRows)
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
}

// TestAlpArrowRoundtrip writes float and double columns with ALP through the
// Arrow writer and reads them back through the Arrow reader, which is the path
// an application takes rather than the column reader the encoding tests use.
// Compression sits on top of the encoding, so each codec gets its own turn.
func TestAlpArrowRoundtrip(t *testing.T) {
	for _, codec := range []compress.Compression{
		compress.Codecs.Uncompressed,
		compress.Codecs.Snappy,
		compress.Codecs.Zstd,
	} {
		t.Run(codec.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			tbl := alpTable(t, mem)
			defer tbl.Release()

			props := parquet.NewWriterProperties(
				parquet.WithAllocator(mem),
				parquet.WithEncoding(parquet.Encodings.ALP),
				// A dictionary would win before the encoding is ever asked for.
				parquet.WithDictionaryDefault(false),
				parquet.WithAlpEncoding(true),
				parquet.WithCompression(codec),
				// Small pages, so a column chunk holds more than one of them.
				parquet.WithDataPageSize(4096),
			)

			var buf bytes.Buffer
			// The chunk size leaves three row groups, the last of them short.
			require.NoError(t, pqarrow.WriteTable(tbl, &buf, 2048, props, pqarrow.DefaultWriterProps()))

			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()

			require.Equal(t, 3, rdr.NumRowGroups())
			for rg := range rdr.NumRowGroups() {
				for c := range rdr.MetaData().Schema.NumColumns() {
					chunk, err := rdr.RowGroup(rg).MetaData().ColumnChunk(c)
					require.NoError(t, err)
					assert.Contains(t, chunk.Encodings(), parquet.Encodings.ALP,
						"row group %d column %d should be ALP encoded", rg, c)
				}
			}

			fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 137}, mem)
			require.NoError(t, err)
			got, err := fr.ReadTable(context.Background())
			require.NoError(t, err)
			defer got.Release()

			require.EqualValues(t, alpNumRows, got.NumRows())
			floats, doubles, valid := alpColumns()
			assertAlpFloat32Column(t, got.Column(0), floats, valid)
			assertAlpFloat64Column(t, got.Column(1), doubles, valid)
		})
	}
}

// assertAlpFloat32Column compares a column against the values written, on bit
// patterns rather than on values: a NaN does not equal itself, and -0.0 equals
// 0.0 although the two are different values.
func assertAlpFloat32Column(t *testing.T, col *arrow.Column, want []float32, valid []bool) {
	t.Helper()

	row := 0
	for _, chunk := range col.Data().Chunks() {
		values := chunk.(*array.Float32)
		for i := range values.Len() {
			if !valid[row] {
				require.Truef(t, values.IsNull(i), "row %d should be null", row)
				row++
				continue
			}
			require.Falsef(t, values.IsNull(i), "row %d should hold a value", row)
			if math.Float32bits(values.Value(i)) != math.Float32bits(want[row]) {
				t.Fatalf("row %d: got %#08x, want %#08x",
					row, math.Float32bits(values.Value(i)), math.Float32bits(want[row]))
			}
			row++
		}
	}
	require.Equal(t, alpNumRows, row)
}

func assertAlpFloat64Column(t *testing.T, col *arrow.Column, want []float64, valid []bool) {
	t.Helper()

	row := 0
	for _, chunk := range col.Data().Chunks() {
		values := chunk.(*array.Float64)
		for i := range values.Len() {
			if !valid[row] {
				require.Truef(t, values.IsNull(i), "row %d should be null", row)
				row++
				continue
			}
			require.Falsef(t, values.IsNull(i), "row %d should hold a value", row)
			if math.Float64bits(values.Value(i)) != math.Float64bits(want[row]) {
				t.Fatalf("row %d: got %#016x, want %#016x",
					row, math.Float64bits(values.Value(i)), math.Float64bits(want[row]))
			}
			row++
		}
	}
	require.Equal(t, alpNumRows, row)
}
