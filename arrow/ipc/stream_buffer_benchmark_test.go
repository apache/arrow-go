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
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkIPCStreamRead(b *testing.B) {
	const rows = 1 << 18
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(mem)
	builder.Reserve(rows)
	for i := range rows {
		builder.UnsafeAppend(int64(i))
	}
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{values}, rows)
	defer batch.Release()

	var output bytes.Buffer
	writer := NewWriter(&output, WithSchema(schema))
	if err := writer.Write(batch); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	stream := output.Bytes()
	buffer := memory.NewBufferBytes(stream)
	for _, tc := range []struct {
		name string
		open func() (*Reader, error)
	}{
		{"io-reader", func() (*Reader, error) { return NewReader(bytes.NewReader(stream)) }},
		{"buffer", func() (*Reader, error) { return NewReaderFromBuffer(buffer) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(stream)))
			for b.Loop() {
				r, err := tc.open()
				if err != nil {
					b.Fatal(err)
				}
				if !r.Next() || r.RecordBatch().NumRows() != rows || r.Next() || r.Err() != nil {
					err := r.Err()
					r.Release()
					b.Fatalf("expected one complete record batch, got %v", err)
				}
				r.Release()
			}
		})
	}
}
