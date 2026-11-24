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

func BenchmarkReadRepeatedStrings(b *testing.B) {
	const rows = 1 << 18
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)
	ids := array.NewInt64Builder(mem)
	ids.Reserve(rows)
	builder := array.NewStringBuilder(mem)
	labels := make([]string, 256)
	for i := range labels {
		labels[i] = fmt.Sprintf("region-%03d", i)
	}
	builder.Reserve(rows)
	state := uint32(1)
	for i := range rows {
		state = state*1664525 + 1013904223
		builder.Append(labels[state>>24])
		ids.UnsafeAppend(int64(i))
	}
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	idValues := ids.NewArray()
	ids.Release()
	defer idValues.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{idValues, values}, rows)
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	for _, codec := range []compress.Compression{compress.Codecs.Zstd, compress.Codecs.Snappy} {
		b.Run(codec.String(), func(b *testing.B) {
			var output bytes.Buffer
			props := parquet.NewWriterProperties(parquet.WithCompression(codec))
			if err := pqarrow.WriteTable(table, &output, rows, props, pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}
			readProps := pqarrow.ArrowReadProperties{BatchSize: 32 * 1024}
			ctx := context.Background()
			b.ReportAllocs()
			b.SetBytes(rows * int64(len(labels[0])))
			for b.Loop() {
				f, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
				if err != nil {
					b.Fatal(err)
				}
				reader, err := pqarrow.NewFileReader(f, readProps, mem)
				if err != nil {
					_ = f.Close()
					b.Fatal(err)
				}
				records, err := reader.GetRecordReader(ctx, []int{1}, nil)
				if err != nil {
					_ = f.Close()
					b.Fatal(err)
				}
				read := int64(0)
				for records.Next() {
					read += records.RecordBatch().NumRows()
				}
				err = records.Err()
				records.Release()
				closeErr := f.Close()
				if err != nil || closeErr != nil || read != rows {
					b.Fatalf("read %d of %d rows: %v, close: %v", read, rows, err, closeErr)
				}
			}
		})
	}
}
