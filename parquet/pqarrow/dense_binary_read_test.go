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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func TestReadDenseByteArrays(t *testing.T) {
	for _, tt := range []struct {
		name       string
		values     []string
		dictionary bool
	}{
		{name: "plain", values: []string{"", "abc", "long value", "", "last"}},
		{name: "dictionary", values: []string{"", "abc", "long value", "abc", ""}, dictionary: true},
		{name: "large payload", values: []string{"before", strings.Repeat("x", 9<<20), "after"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			builder := array.NewStringBuilder(mem)
			defer builder.Release()
			builder.AppendValues(tt.values, nil)
			input := builder.NewStringArray()
			defer input.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{input}, int64(len(tt.values)))
			defer record.Release()
			table := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
			defer table.Release()

			data := writeDenseByteArrayTable(t, table, tt.dictionary)
			parquetReader, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer parquetReader.Close()
			reader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 3}, mem)
			require.NoError(t, err)
			actual, err := reader.ReadTable(t.Context())
			require.NoError(t, err)
			defer actual.Release()
			require.Equal(t, table.NumRows(), actual.NumRows())
			require.True(t, array.ChunkedEqual(table.Column(0).Data(), actual.Column(0).Data()))
		})
	}
}

func writeDenseByteArrayTable(tb testing.TB, table arrow.Table, dictionary bool) []byte {
	tb.Helper()
	var data bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(dictionary))
	require.NoError(tb, pqarrow.WriteTable(table, &data, table.NumRows(), props, pqarrow.DefaultWriterProps()))
	return data.Bytes()
}

func BenchmarkReadDenseByteArrays(b *testing.B) {
	const rows = 64 * 1024
	tbl, dataBytes := benchmarkBinaryTable(memory.DefaultAllocator, rows)
	defer tbl.Release()
	for _, dictionary := range []bool{false, true} {
		b.Run("dictionary="+strconv.FormatBool(dictionary), func(b *testing.B) {
			data := writeDenseByteArrayTable(b, tbl, dictionary)
			b.SetBytes(dataBytes)
			b.ReportAllocs()
			for b.Loop() {
				pf, err := file.NewParquetReader(bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
				reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 8192}, memory.DefaultAllocator)
				if err != nil {
					b.Fatal(err)
				}
				batches, err := reader.GetRecordReader(b.Context(), nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				count := int64(0)
				for batches.Next() {
					count += batches.RecordBatch().NumRows()
				}
				err = batches.Err()
				batches.Release()
				closeErr := pf.Close()
				if err != nil || closeErr != nil || count != rows {
					b.Fatalf("read %d rows: read error %v, close error %v", count, err, closeErr)
				}
			}
		})
	}
}
