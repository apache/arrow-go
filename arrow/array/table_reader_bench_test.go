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

package array

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkTableReaderNext(b *testing.B) {
	const rowsPerChunk = 256
	for _, numCols := range []int{4, 32, 256} {
		for _, numChunks := range []int{32, 256} {
			b.Run(fmt.Sprintf("columns=%d/chunks=%d", numCols, numChunks), func(b *testing.B) {
				table := makeTableReaderBenchmarkTable(memory.DefaultAllocator, numCols, numChunks, rowsPerChunk)
				defer table.Release()

				reader := NewTableReader(table, rowsPerChunk)
				defer reader.Release()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var rows int64
					for reader.Next() {
						rows += reader.RecordBatch().NumRows()
					}
					if rows != table.NumRows() {
						b.Fatalf("invalid row count: got=%d, want=%d", rows, table.NumRows())
					}

					reader.cur = 0
					clear(reader.slots)
					clear(reader.offsets)
				}
			})
		}
	}
}

func makeTableReaderBenchmarkTable(mem memory.Allocator, numCols, numChunks, rowsPerChunk int) arrow.Table {
	fields := make([]arrow.Field, numCols)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col_%d", i), Type: arrow.PrimitiveTypes.Int32}
	}
	schema := arrow.NewSchema(fields, nil)

	chunks := make([]arrow.Array, numChunks)
	for i := range chunks {
		bldr := NewInt32Builder(mem)
		bldr.Reserve(rowsPerChunk)
		for j := 0; j < rowsPerChunk; j++ {
			bldr.Append(int32(i*rowsPerChunk + j))
		}
		chunks[i] = bldr.NewInt32Array()
		bldr.Release()
	}

	cols := make([]arrow.Column, numCols)
	for i, field := range fields {
		chunked := arrow.NewChunked(field.Type, chunks)
		cols[i] = *arrow.NewColumn(field, chunked)
		chunked.Release()
	}
	table := NewTable(schema, cols, -1)

	for i := range cols {
		cols[i].Release()
	}
	for _, chunk := range chunks {
		chunk.Release()
	}
	return table
}
