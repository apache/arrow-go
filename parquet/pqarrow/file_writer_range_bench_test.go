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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkFileWriterRecordBatchRanges(b *testing.B) {
	tests := []struct {
		name     string
		numCols  int
		numRows  int
		rowGroup int64
	}{
		{name: "1col_16rows_rg16", numCols: 1, numRows: 16, rowGroup: 16},
		{name: "8cols_8192rows_rg256", numCols: 8, numRows: 8192, rowGroup: 256},
		{name: "32cols_4096rows_rg64", numCols: 32, numRows: 4096, rowGroup: 64},
	}

	for _, test := range tests {
		schema, record := makeRangeWriteRecord(test.numCols, test.numRows)
		b.Run(test.name, func(b *testing.B) {
			for _, method := range []struct {
				name  string
				write func(*pqarrow.FileWriter, arrow.RecordBatch) error
			}{
				{name: "Write", write: (*pqarrow.FileWriter).Write},
				{name: "WriteBuffered", write: (*pqarrow.FileWriter).WriteBuffered},
			} {
				b.Run(method.name, func(b *testing.B) {
					props := parquet.NewWriterProperties(
						parquet.WithDictionaryDefault(false),
						parquet.WithMaxRowGroupLength(test.rowGroup),
					)
					arrProps := pqarrow.DefaultWriterProps()
					var output bytes.Buffer
					output.Grow(test.numCols * test.numRows * 8)

					b.ReportAllocs()
					b.SetBytes(int64(test.numCols * test.numRows * 8))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						output.Reset()
						writer, err := pqarrow.NewFileWriter(schema, &output, props, arrProps)
						if err != nil {
							b.Fatal(err)
						}
						if err := method.write(writer, record); err != nil {
							b.Fatal(err)
						}
						if err := writer.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
		record.Release()
	}
}

func makeRangeWriteRecord(numCols, numRows int) (*arrow.Schema, arrow.RecordBatch) {
	fields := make([]arrow.Field, numCols)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("column%d", i), Type: arrow.PrimitiveTypes.Int64}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	values := make([]int64, numRows)
	for col := 0; col < numCols; col++ {
		for row := range values {
			values[row] = int64(row*numCols + col)
		}
		builder.Field(col).(*array.Int64Builder).AppendValues(values, nil)
	}
	return schema, builder.NewRecordBatch()
}
