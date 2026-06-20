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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vectorWriteRead writes cols under sc with WithVectorEncoding enabled (and the
// Arrow schema stored, so FixedSizeList types are preserved on read), then reads
// the file back. It returns the parquet bytes (for on-disk schema inspection)
// and the reconstructed table (caller releases).
func vectorWriteRead(t *testing.T, mem memory.Allocator, sc *arrow.Schema, cols []arrow.Array, chunkSize int64) ([]byte, arrow.Table) {
	t.Helper()
	nrows := int64(cols[0].Len())
	rec := array.NewRecordBatch(sc, cols, nrows)
	defer rec.Release()
	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	wp := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	awp := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithVectorEncoding(), pqarrow.WithStoreSchema())
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, chunkSize, wp, awp))

	got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()),
		parquet.NewReaderProperties(mem), pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	return buf.Bytes(), got
}

// columnIsVector reports whether leaf column i of the written parquet file is a
// VECTOR column.
func columnIsVector(t *testing.T, data []byte, i int) bool {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer rdr.Close()
	return rdr.MetaData().Schema.Column(i).InVectorColumn()
}

// TestVectorFallbackRoundTrips checks that, with WithVectorEncoding enabled, a
// FixedSizeList that is NOT VECTOR-eligible is transparently written as the
// standard LIST encoding (no VECTOR leaf on disk) and still round-trips
// losslessly through pqarrow.
func TestVectorFallbackRoundTrips(t *testing.T) {
	mem := memory.DefaultAllocator
	f32 := arrow.PrimitiveTypes.Float32

	cases := []struct {
		name  string
		field arrow.Field
		build func() arrow.Array
	}{
		{
			// A nullable FixedSizeList *value* (field.Nullable) is ineligible
			// regardless of whether actual nulls are present, so it falls back to
			// LIST. (Writing an actual null FixedSizeList row is a separate,
			// pre-existing arrow-go limitation and is not exercised here.)
			name:  "nullable_value_type",
			field: fslField("v", f32, 3, true /*valNullable*/, false),
			build: func() arrow.Array {
				b := array.NewFixedSizeListBuilderWithField(mem, 3, arrow.Field{Name: "element", Type: f32})
				defer b.Release()
				vb := b.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < 4; i++ {
					b.Append(true)
					vb.AppendValues([]float32{float32(i), float32(i) + 1, float32(i) + 2}, nil)
				}
				return b.NewArray()
			},
		},
		{
			name:  "nullable_element",
			field: fslField("v", f32, 2, false, true /*elemNullable*/),
			build: func() arrow.Array {
				b := array.NewFixedSizeListBuilderWithField(mem, 2, arrow.Field{Name: "element", Type: f32, Nullable: true})
				defer b.Release()
				vb := b.ValueBuilder().(*array.Float32Builder)
				b.Append(true)
				vb.Append(1)
				vb.AppendNull()
				b.Append(true)
				vb.Append(3)
				vb.Append(4)
				return b.NewArray()
			},
		},
		{
			name:  "nested_fixed_size_list_element",
			field: fslField("v", arrow.FixedSizeListOf(2, f32), 3, false, false),
			build: func() arrow.Array {
				outerElem := arrow.Field{Name: "element", Type: arrow.FixedSizeListOf(2, f32)}
				b := array.NewFixedSizeListBuilderWithField(mem, 3, outerElem)
				defer b.Release()
				inner := b.ValueBuilder().(*array.FixedSizeListBuilder)
				vb := inner.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < 4; i++ { // 4 outer rows
					b.Append(true)
					for k := 0; k < 3; k++ { // 3 inner lists per outer row
						inner.Append(true)
						vb.AppendValues([]float32{float32(i*10 + k), float32(i*10 + k + 1)}, nil)
					}
				}
				return b.NewArray()
			},
		},
		{
			name:  "string_element",
			field: fslField("v", arrow.BinaryTypes.String, 2, false, false),
			build: func() arrow.Array {
				b := array.NewFixedSizeListBuilderWithField(mem, 2, arrow.Field{Name: "element", Type: arrow.BinaryTypes.String})
				defer b.Release()
				vb := b.ValueBuilder().(*array.StringBuilder)
				for i := 0; i < 5; i++ {
					b.Append(true)
					vb.Append("a")
					vb.Append("bb")
				}
				return b.NewArray()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.build()
			defer arr.Release()
			sc := arrow.NewSchema([]arrow.Field{tc.field}, nil)

			data, got := vectorWriteRead(t, mem, sc, []arrow.Array{arr}, int64(arr.Len()))
			defer got.Release()

			// Every leaf column must use the standard LIST encoding, not VECTOR.
			rdr, err := file.NewParquetReader(bytes.NewReader(data))
			require.NoError(t, err)
			for i := 0; i < rdr.MetaData().Schema.NumColumns(); i++ {
				assert.False(t, rdr.MetaData().Schema.Column(i).InVectorColumn(),
					"ineligible FixedSizeList must fall back to LIST, but leaf %d is VECTOR", i)
			}
			rdr.Close()

			require.EqualValues(t, arr.Len(), got.NumRows())
			require.Truef(t, arrow.TypeEqual(arr.DataType(), got.Schema().Field(0).Type),
				"type changed on round-trip: %s -> %s", arr.DataType(), got.Schema().Field(0).Type)
			concat, err := array.Concatenate(got.Column(0).Data().Chunks(), mem)
			require.NoError(t, err)
			defer concat.Release()
			assert.Truef(t, array.Equal(arr, concat), "fallback round-trip mismatch\n want %v\n got  %v", arr, concat)
		})
	}
}

// TestVectorMultipleAndMixedColumns writes a schema that mixes two
// VECTOR-eligible FixedSizeList columns with a plain primitive column and an
// ineligible (nullable) FixedSizeList column, and checks that exactly the
// eligible columns are encoded as VECTOR while everything round-trips.
func TestVectorMultipleAndMixedColumns(t *testing.T) {
	mem := memory.DefaultAllocator
	const numRows = 200

	// col 0: plain int64
	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	// col 1: VECTOR-eligible FixedSizeList<float32, 4>
	emb1Field := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32, Nullable: false}
	emb1B := array.NewFixedSizeListBuilderWithField(mem, 4, emb1Field)
	defer emb1B.Release()
	emb1V := emb1B.ValueBuilder().(*array.Float32Builder)
	// col 2: VECTOR-eligible FixedSizeList<int32, 3>
	emb2Field := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	emb2B := array.NewFixedSizeListBuilderWithField(mem, 3, emb2Field)
	defer emb2B.Release()
	emb2V := emb2B.ValueBuilder().(*array.Int32Builder)
	// col 3: ineligible (nullable) FixedSizeList<float32, 2> -> LIST fallback
	emb3B := array.NewFixedSizeListBuilderWithField(mem, 2, arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32})
	defer emb3B.Release()
	emb3V := emb3B.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		emb1B.Append(true)
		for j := 0; j < 4; j++ {
			emb1V.Append(float32(i) + float32(j)*0.1)
		}
		emb2B.Append(true)
		for j := int32(0); j < 3; j++ {
			emb2V.Append(int32(i)*10 + j)
		}
		emb3B.Append(true)
		emb3V.Append(float32(i))
		emb3V.Append(float32(i) + 0.5)
	}

	cols := []arrow.Array{idB.NewArray(), emb1B.NewArray(), emb2B.NewArray(), emb3B.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "emb1", Type: cols[1].DataType(), Nullable: false},
		{Name: "emb2", Type: cols[2].DataType(), Nullable: false},
		{Name: "emb3", Type: cols[3].DataType(), Nullable: true},
	}, nil)

	data, got := vectorWriteRead(t, mem, sc, cols, numRows)
	defer got.Release()

	// Leaf columns: 0=id, 1=emb1 element, 2=emb2 element, 3=emb3 element.
	assert.False(t, columnIsVector(t, data, 0), "plain int64 column must not be VECTOR")
	assert.True(t, columnIsVector(t, data, 1), "emb1 must be VECTOR")
	assert.True(t, columnIsVector(t, data, 2), "emb2 must be VECTOR")
	assert.False(t, columnIsVector(t, data, 3), "nullable emb3 must fall back to LIST")

	require.EqualValues(t, numRows, got.NumRows())
	for i := 0; i < len(cols); i++ {
		concat, err := array.Concatenate(got.Column(i).Data().Chunks(), mem)
		require.NoError(t, err)
		assert.Truef(t, array.Equal(cols[i], concat), "column %d round-trip mismatch", i)
		concat.Release()
	}
}

// TestVectorMultiRowGroupRowCount writes a VECTOR column across several row
// groups and explicitly checks that each row group's metadata reports parent
// rows (not leaf slots) and that the values round-trip across the boundaries.
func TestVectorMultiRowGroupRowCount(t *testing.T) {
	mem := memory.DefaultAllocator
	const (
		listSize     = int32(3)
		numRows      = 1000
		rowsPerGroup = 250 // -> 4 row groups
	)

	elemField := arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32, Nullable: false}
	b := array.NewFixedSizeListBuilderWithField(mem, listSize, elemField)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < numRows; i++ {
		b.Append(true)
		for j := int32(0); j < listSize; j++ {
			vb.Append(float32(i)*float32(listSize) + float32(j))
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	sc := arrow.NewSchema([]arrow.Field{{Name: "emb", Type: arr.DataType(), Nullable: false}}, nil)

	data, got := vectorWriteRead(t, mem, sc, []arrow.Array{arr}, rowsPerGroup)
	defer got.Release()

	require.True(t, columnIsVector(t, data, 0), "column must be VECTOR")

	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer rdr.Close()
	md := rdr.MetaData()
	require.Equal(t, numRows/rowsPerGroup, rdr.NumRowGroups(), "expected one row group per chunk")
	require.EqualValues(t, numRows, md.NumRows, "file num_rows must be parent rows")

	var totalRows int64
	for i := 0; i < rdr.NumRowGroups(); i++ {
		rg := md.RowGroup(i)
		// Parent rows per group, not leaf slots (= rows * vector_length).
		require.EqualValues(t, rowsPerGroup, rg.NumRows(), "row group %d num_rows", i)
		col, err := rg.ColumnChunk(0)
		require.NoError(t, err)
		require.EqualValues(t, int64(rowsPerGroup)*int64(listSize), col.NumValues(), "row group %d num_values", i)
		totalRows += rg.NumRows()
	}
	require.EqualValues(t, numRows, totalRows)

	require.EqualValues(t, numRows, got.NumRows())
	concat, err := array.Concatenate(got.Column(0).Data().Chunks(), mem)
	require.NoError(t, err)
	defer concat.Release()
	assert.True(t, array.Equal(arr, concat), "multi-row-group VECTOR round-trip mismatch")
}
