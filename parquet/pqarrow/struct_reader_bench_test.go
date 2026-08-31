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
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkReadNestedStructSerial(b *testing.B) {
	const (
		nrows        = 1024
		rowGroupSize = 64
	)

	for _, nchildren := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("children=%d", nchildren), func(b *testing.B) {
			mem := memory.DefaultAllocator
			tbl := makeWideNestedInt32Table(mem, nchildren, nrows)
			defer tbl.Release()

			var buf bytes.Buffer
			writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
			if err := pqarrow.WriteTable(tbl, &buf, rowGroupSize, writerProps, pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}
			parquetData := buf.Bytes()

			pf, err := file.NewParquetReader(bytes.NewReader(parquetData))
			if err != nil {
				b.Fatal(err)
			}
			defer pf.Close()

			reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
				BatchSize: nrows,
				Parallel:  false,
			}, mem)
			if err != nil {
				b.Fatal(err)
			}

			rowGroups := make([]int, nrows/rowGroupSize)
			for i := range rowGroups {
				rowGroups[i] = i
			}
			includedLeaves := make(map[int]bool, nchildren)
			for i := 0; i < nchildren; i++ {
				includedLeaves[i] = true
			}
			fieldReader, err := reader.GetFieldReader(context.Background(), 0, includedLeaves, rowGroups)
			if err != nil {
				b.Fatal(err)
			}
			defer fieldReader.Release()

			b.ReportAllocs()
			b.SetBytes(int64(len(parquetData)))
			b.ResetTimer()
			for range b.N {
				if err := fieldReader.SeekToRow(0); err != nil {
					b.Fatal(err)
				}
				out, err := fieldReader.NextBatch(nrows)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func makeWideNestedInt32Table(mem memory.Allocator, nchildren, nrows int) arrow.Table {
	childFields := make([]arrow.Field, nchildren)
	for i := range childFields {
		childFields[i] = arrow.Field{
			Name: fmt.Sprintf("child_%d", i),
			Type: arrow.PrimitiveTypes.Int32,
		}
	}

	structType := arrow.StructOf(childFields...)
	schema := arrow.NewSchema([]arrow.Field{{Name: "nested", Type: structType}}, nil)
	builder := array.NewStructBuilder(mem, structType)
	defer builder.Release()

	valid := make([]bool, nrows)
	values := make([]int32, nrows)
	for i := range valid {
		valid[i] = true
		values[i] = int32(i)
	}
	builder.AppendValues(valid)
	for i := 0; i < nchildren; i++ {
		builder.FieldBuilder(i).(*array.Int32Builder).AppendValues(values, nil)
	}

	arr := builder.NewStructArray()
	chunked := arrow.NewChunked(structType, []arrow.Array{arr})
	column := arrow.NewColumn(schema.Field(0), chunked)
	table := array.NewTable(schema, []arrow.Column{*column}, int64(nrows))
	column.Release()
	chunked.Release()
	arr.Release()
	return table
}
