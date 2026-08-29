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

//go:build go1.24

package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newTakeInt32Array(mem memory.Allocator, n int) arrow.Array {
	values := make([]int32, n)
	for i := range values {
		values[i] = int32(i)
	}
	bldr := array.NewInt32Builder(mem)
	bldr.AppendValues(values, nil)
	result := bldr.NewInt32Array()
	bldr.Release()
	return result
}

func newTakeIndices(mem memory.Allocator, n int) arrow.Array {
	values := make([]int32, n)
	for i := range values {
		values[i] = int32(n - i - 1)
	}
	bldr := array.NewInt32Builder(mem)
	bldr.AppendValues(values, nil)
	result := bldr.NewInt32Array()
	bldr.Release()
	return result
}

func BenchmarkTakeRecordSingleColumn(b *testing.B) {
	const (
		nrows   = 256
		nselect = 128
	)

	mem := memory.NewGoAllocator()
	ctx := compute.WithAllocator(context.Background(), mem)
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	values := newTakeInt32Array(mem, nrows)
	defer values.Release()
	indices := newTakeIndices(mem, nselect)
	defer indices.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{values}, nrows)
	defer batch.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: batch}, &compute.ArrayDatum{Value: indices.Data()})
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkTakeRecordMultiColumnSerial(b *testing.B) {
	const (
		nrows   = 256
		ncols   = 16
		nselect = 128
	)

	mem := memory.NewGoAllocator()
	ctx := serialTakeContext(mem, 1)
	fields := make([]arrow.Field, ncols)
	values := make([]arrow.Array, ncols)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("value_%d", i), Type: arrow.PrimitiveTypes.Int32}
		values[i] = newTakeInt32Array(mem, nrows)
	}
	schema := arrow.NewSchema(fields, nil)
	batch := array.NewRecordBatch(schema, values, nrows)
	for _, value := range values {
		value.Release()
	}
	defer batch.Release()
	indices := newTakeIndices(mem, nselect)
	defer indices.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: batch}, &compute.ArrayDatum{Value: indices.Data()})
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkTakeTableSingleColumn(b *testing.B) {
	const (
		nrows   = 256
		nselect = 128
	)

	mem := memory.NewGoAllocator()
	ctx := compute.WithAllocator(context.Background(), mem)
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	values := newTakeInt32Array(mem, nrows)
	defer values.Release()
	column := arrow.NewColumnFromArr(field, values)
	table := array.NewTable(schema, []arrow.Column{column}, nrows)
	column.Release()
	defer table.Release()
	indices := newTakeIndices(mem, nselect)
	defer indices.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.TableDatum{Value: table}, &compute.ArrayDatum{Value: indices.Data()})
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkTakeArraySingleIndexChunk(b *testing.B) {
	const (
		nrows   = 256
		nselect = 128
	)

	mem := memory.NewGoAllocator()
	ctx := compute.WithAllocator(context.Background(), mem)
	values := newTakeInt32Array(mem, nrows)
	defer values.Release()
	indices := newTakeIndices(mem, nselect)
	defer indices.Release()
	chunkedIndices := arrow.NewChunked(indices.DataType(), []arrow.Array{indices})
	defer chunkedIndices.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.ArrayDatum{Value: values.Data()}, &compute.ChunkedDatum{Value: chunkedIndices})
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkTakeArrayMultiChunkSerial(b *testing.B) {
	const (
		nrows        = 256
		chunksCount  = 8
		rowsPerChunk = 16
	)

	mem := memory.NewGoAllocator()
	ctx := serialTakeContext(mem, 1)
	values := newTakeInt32Array(mem, nrows)
	defer values.Release()
	chunks := make([]arrow.Array, chunksCount)
	for i := range chunks {
		chunks[i] = newTakeIndices(mem, rowsPerChunk)
	}
	chunkedIndices := arrow.NewChunked(arrow.PrimitiveTypes.Int32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	defer chunkedIndices.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.ArrayDatum{Value: values.Data()}, &compute.ChunkedDatum{Value: chunkedIndices})
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
