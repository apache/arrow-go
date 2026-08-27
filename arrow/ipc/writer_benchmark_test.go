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

package ipc

import (
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func benchmarkRecordBatch(numColumns, numRows int) arrow.RecordBatch {
	fields := make([]arrow.Field, numColumns)
	columns := make([]arrow.Array, numColumns)
	values := make([]int32, numRows)
	for i := range values {
		values[i] = int32(i)
	}

	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrow.PrimitiveTypes.Int32}

		builder := array.NewInt32Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		columns[i] = builder.NewArray()
		builder.Release()
	}

	schema := arrow.NewSchema(fields, nil)
	record := array.NewRecordBatch(schema, columns, int64(numRows))
	for _, column := range columns {
		column.Release()
	}
	return record
}

func BenchmarkWriterRecordEncoderReuse(b *testing.B) {
	for _, numColumns := range []int{1, 16, 64} {
		for _, numRows := range []int{16, 256, 4096} {
			b.Run(fmt.Sprintf("%dcols/%drows", numColumns, numRows), func(b *testing.B) {
				record := benchmarkRecordBatch(numColumns, numRows)
				defer record.Release()

				writer := NewWriter(io.Discard, WithSchema(record.Schema()))
				if err := writer.Write(record); err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.SetBytes(int64(numColumns * numRows * arrow.Int32SizeBytes))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := writer.Write(record); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
