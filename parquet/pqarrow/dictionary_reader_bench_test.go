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
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkReadDictionaryColumn(b *testing.B) { benchmarkReadDictionaryColumn(b, false) }

// BenchmarkReadOptionalDictionaryColumn reads an optional column that holds no nulls.
func BenchmarkReadOptionalDictionaryColumn(b *testing.B) { benchmarkReadDictionaryColumn(b, true) }

func benchmarkReadDictionaryColumn(b *testing.B, nullable bool) {
	const rows = 1 << 18

	for _, distinct := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			parquetData := writeDictionaryStringColumn(b, memory.DefaultAllocator, rows, distinct, nullable)

			for _, batchSize := range []int64{8192, 65536} {
				b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(parquetData)))
					for b.Loop() {
						if n := readDictionaryColumn(b, memory.DefaultAllocator, parquetData, batchSize); n != rows {
							b.Fatalf("read %d rows, want %d", n, rows)
						}
					}
				})
			}
		})
	}
}

// writeDictionaryStringColumn writes rows 25-byte strings with the given number
// of distinct values to one row group whose data pages are all dictionary
// encoded. The column holds no nulls; nullable makes it optional rather than
// required.
func writeDictionaryStringColumn(tb testing.TB, mem memory.Allocator, rows, distinct int, nullable bool) []byte {
	tb.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String, Nullable: nullable}}, nil)

	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	for i := range rows {
		bldr.Append(fmt.Sprintf("value-%019d", i%distinct))
	}
	arr := bldr.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(rows))
	defer rec.Release()

	props := parquet.NewWriterProperties(
		parquet.WithDictionaryPageSizeLimit(64<<20),
		parquet.WithDictionaryCostFallbackFor("s", false),
	)
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		tb.Fatal(err)
	}
	defer pf.Close()
	if n := pf.NumRowGroups(); n != 1 {
		tb.Fatalf("wrote %d row groups, want 1", n)
	}
	chunk, err := pf.MetaData().RowGroup(0).ColumnChunk(0)
	if err != nil {
		tb.Fatal(err)
	}
	for _, s := range chunk.EncodingStats() {
		if s.PageType != file.PageTypeDictionaryPage && s.Encoding != parquet.Encodings.RLEDict {
			tb.Fatalf("wrote a %s data page, want only %s", s.Encoding, parquet.Encodings.RLEDict)
		}
	}
	return buf.Bytes()
}

// readDictionaryColumn reads the first column of parquetData as a dictionary
// array in batches of batchSize rows and returns the number of rows read.
func readDictionaryColumn(tb testing.TB, mem memory.Allocator, parquetData []byte, batchSize int64) int64 {
	tb.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(parquetData))
	if err != nil {
		tb.Fatal(err)
	}
	defer pf.Close()

	props := pqarrow.ArrowReadProperties{BatchSize: batchSize}
	props.SetReadDict(0, true)
	fr, err := pqarrow.NewFileReader(pf, props, mem)
	if err != nil {
		tb.Fatal(err)
	}
	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer rr.Release()

	var n int64
	for rr.Next() {
		n += rr.RecordBatch().NumRows()
	}
	if err := rr.Err(); err != nil {
		tb.Fatal(err)
	}
	return n
}
