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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkReadTableSerial(b *testing.B) {
	const (
		rows         = 1024
		rowGroupSize = 64
	)

	for _, numColumns := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("columns=%d", numColumns), func(b *testing.B) {
			mem := memory.DefaultAllocator
			tbl := makeWideInt32Table(mem, numColumns, rows)
			defer tbl.Release()

			var buf bytes.Buffer
			if err := pqarrow.WriteTable(tbl, &buf, rowGroupSize, nil, pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}
			parquetData := buf.Bytes()

			b.ReportAllocs()
			b.SetBytes(int64(len(parquetData)))
			b.ResetTimer()
			for range b.N {
				pf, err := file.NewParquetReader(bytes.NewReader(parquetData))
				if err != nil {
					b.Fatal(err)
				}

				reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{Parallel: false}, mem)
				if err != nil {
					_ = pf.Close()
					b.Fatal(err)
				}

				out, err := reader.ReadTable(context.Background())
				if err != nil {
					_ = pf.Close()
					b.Fatal(err)
				}
				out.Release()
				if err := pf.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func makeWideInt32Table(mem memory.Allocator, numColumns, numRows int) arrow.Table {
	values := make([]int32, numRows)
	for i := range values {
		values[i] = int32(i)
	}

	fields := make([]arrow.Field, numColumns)
	columns := make([]arrow.Column, numColumns)
	for i := range columns {
		fields[i] = arrow.Field{Name: fmt.Sprintf("column_%d", i), Type: arrow.PrimitiveTypes.Int32}

		builder := array.NewInt32Builder(mem)
		builder.AppendValues(values, nil)
		arr := builder.NewInt32Array()
		builder.Release()

		columns[i] = arrow.NewColumnFromArr(fields[i], arr)
		arr.Release()
	}

	table := array.NewTable(arrow.NewSchema(fields, nil), columns, int64(numRows))
	for i := range columns {
		columns[i].Release()
	}
	return table
}
