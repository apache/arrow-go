// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqarrow_test

import (
	"bytes"
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

func numericRowGroups(t testing.TB, rowsPerGroup, groups int) []byte {
	t.Helper()
	rows := groups * rowsPerGroup
	mem := memory.DefaultAllocator
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.Reserve(rows)
	for i := range rows {
		builder.Append(int64(i))
	}
	values := builder.NewInt64Array()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "c", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{values, values, values}, int64(rows))
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()
	var data bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &data, int64(rowsPerGroup),
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps()))
	return data.Bytes()
}

func TestReadNumericRowGroupsRetainedBatch(t *testing.T) {
	const rowsPerGroup = 1024
	const groups = 8
	data := numericRowGroups(t, rowsPerGroup, groups)
	mem := memory.DefaultAllocator
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: rowsPerGroup, Parallel: true}, mem)
	require.NoError(t, err)
	records, err := reader.GetRecordReader(t.Context(), nil, nil)
	require.NoError(t, err)
	require.True(t, records.Next())
	first := records.RecordBatch()
	first.Retain()
	defer first.Release()
	count := first.NumRows()
	for records.Next() {
		count += records.RecordBatch().NumRows()
	}
	require.NoError(t, records.Err())
	records.Release()
	require.NoError(t, pf.Close())
	require.EqualValues(t, groups*rowsPerGroup, count)
	for _, column := range first.Columns() {
		values := column.(*array.Int64).Int64Values()
		for i, value := range values {
			if value != int64(i) {
				t.Fatalf("retained row %d contains %d", i, value)
			}
		}
	}
}

func BenchmarkReadNumericRowGroups(b *testing.B) {
	const rowsPerGroup = 8192
	const groups = 32
	mem := memory.DefaultAllocator
	data := numericRowGroups(b, rowsPerGroup, groups)
	b.ReportAllocs()
	for b.Loop() {
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		require.NoError(b, err)
		reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: rowsPerGroup}, mem)
		require.NoError(b, err)
		records, err := reader.GetRecordReader(b.Context(), nil, nil)
		require.NoError(b, err)
		var count int64
		for records.Next() {
			values := records.RecordBatch().Column(0).(*array.Int64).Int64Values()
			for _, value := range values {
				if value != count {
					b.Fatalf("row %d contains %d", count, value)
				}
				count++
			}
		}
		require.NoError(b, records.Err())
		records.Release()
		require.NoError(b, pf.Close())
		if count != groups*rowsPerGroup {
			b.Fatalf("read %d rows", count)
		}
	}
}
