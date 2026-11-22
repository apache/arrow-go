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
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkReadRepeatedStrings(b *testing.B) {
	const rows = 1 << 18
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewStringBuilder(mem)
	labels := make([]string, 256)
	for i := range labels {
		labels[i] = fmt.Sprintf("region-%03d", i)
	}
	builder.Reserve(rows)
	state := uint32(1)
	for range rows {
		state = state*1664525 + 1013904223
		builder.Append(labels[state>>24])
	}
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{values}, rows)
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var output bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	if err := pqarrow.WriteTable(table, &output, rows, props, pqarrow.DefaultWriterProps()); err != nil {
		b.Fatal(err)
	}
	readProps := pqarrow.ArrowReadProperties{BatchSize: rows}
	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(rows * int64(len(labels[0])))
	for b.Loop() {
		result, err := pqarrow.ReadTable(ctx, bytes.NewReader(output.Bytes()), nil, readProps, mem)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
