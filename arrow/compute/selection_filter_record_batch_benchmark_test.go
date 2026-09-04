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

//go:build go1.18

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

var benchmarkFilterRecordBatchRows int64

func BenchmarkFilterRecordBatchSerial(b *testing.B) {
	for _, numCols := range []int{1, 8, 32, 128} {
		for _, numRows := range []int{16, 256, 4096} {
			b.Run(fmt.Sprintf("columns=%d/rows=%d", numCols, numRows), func(b *testing.B) {
				batch, filter := makeFilterRecordBatchBenchmarkInput(b, numCols, numRows)
				defer batch.Release()
				defer filter.Release()

				execCtx := compute.DefaultExecCtx()
				execCtx.NumParallel = 1
				ctx := compute.SetExecCtx(context.Background(), execCtx)

				b.ReportAllocs()
				b.SetBytes(int64(numCols * numRows * 8))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := compute.FilterRecordBatch(ctx, batch, filter, compute.DefaultFilterOptions())
					if err != nil {
						b.Fatal(err)
					}
					benchmarkFilterRecordBatchRows = result.NumRows()
					result.Release()
				}
			})
		}
	}
}

var benchmarkLargeFilterRecordBatchRows int64

func BenchmarkFilterRecordBatchGetTakeIndices(b *testing.B) {
	for _, numRows := range []int{64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			batch, filter := makeFilterRecordBatchBenchmarkInput(b, 1, numRows)
			defer batch.Release()
			defer filter.Release()

			execCtx := compute.DefaultExecCtx()
			execCtx.NumParallel = 1
			ctx := compute.SetExecCtx(context.Background(), execCtx)

			b.ReportAllocs()
			b.SetBytes(int64(numRows * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := compute.FilterRecordBatch(ctx, batch, filter, compute.DefaultFilterOptions())
				if err != nil {
					b.Fatal(err)
				}
				benchmarkLargeFilterRecordBatchRows = result.NumRows()
				result.Release()
			}
		})
	}
}

func makeFilterRecordBatchBenchmarkInput(b *testing.B, numCols, numRows int) (arrow.RecordBatch, arrow.Array) {
	b.Helper()
	mem := memory.DefaultAllocator
	fields := make([]arrow.Field, numCols)
	cols := make([]arrow.Array, numCols)
	for col := 0; col < numCols; col++ {
		fields[col] = arrow.Field{Name: fmt.Sprintf("col_%d", col), Type: arrow.PrimitiveTypes.Int64}
		builder := array.NewInt64Builder(mem)
		builder.Reserve(numRows)
		for row := 0; row < numRows; row++ {
			builder.Append(int64(col*numRows + row))
		}
		cols[col] = builder.NewInt64Array()
		builder.Release()
	}

	schema := arrow.NewSchema(fields, nil)
	batch := array.NewRecordBatch(schema, cols, int64(numRows))
	for _, col := range cols {
		col.Release()
	}

	filterBuilder := array.NewBooleanBuilder(mem)
	filterBuilder.Reserve(numRows)
	for row := 0; row < numRows; row++ {
		filterBuilder.Append(row%2 == 0)
	}
	filter := filterBuilder.NewBooleanArray()
	filterBuilder.Release()
	return batch, filter
}
