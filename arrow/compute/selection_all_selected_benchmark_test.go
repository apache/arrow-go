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

var allSelectedBenchmarkRows int64

func allSelectedBenchmarkInputs(numCols, numRows int) (arrow.RecordBatch, arrow.Array) {
	mem := memory.DefaultAllocator
	values := make([]int64, numRows)
	for i := range values {
		values[i] = int64(i)
	}
	fields := make([]arrow.Field, numCols)
	columns := make([]arrow.Array, numCols)
	for i := range columns {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col_%d", i), Type: arrow.PrimitiveTypes.Int64}
		b := array.NewInt64Builder(mem)
		b.AppendValues(values, nil)
		columns[i] = b.NewInt64Array()
		b.Release()
	}
	batch := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, int64(numRows))
	for _, col := range columns {
		col.Release()
	}
	maskValues := make([]bool, numRows)
	for i := range maskValues {
		maskValues[i] = true
	}
	filterBuilder := array.NewBooleanBuilder(mem)
	filterBuilder.AppendValues(maskValues, nil)
	filter := filterBuilder.NewBooleanArray()
	filterBuilder.Release()
	return batch, filter
}

func BenchmarkFilterAllSelectedRecordBatch(b *testing.B) {
	for _, cols := range []int{1, 8, 64} {
		for _, rows := range []int{4096, 65536} {
			b.Run(fmt.Sprintf("columns=%d/rows=%d", cols, rows), func(b *testing.B) {
				batch, filter := allSelectedBenchmarkInputs(cols, rows)
				defer batch.Release()
				defer filter.Release()
				execCtx := compute.DefaultExecCtx()
				execCtx.NumParallel = 1
				ctx := compute.SetExecCtx(context.Background(), execCtx)
				b.ReportAllocs()
				b.SetBytes(int64(cols * rows * 8))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := compute.FilterRecordBatch(ctx, batch, filter, compute.DefaultFilterOptions())
					if err != nil {
						b.Fatal(err)
					}
					allSelectedBenchmarkRows = result.NumRows()
					result.Release()
				}
			})
		}
	}
}

func BenchmarkFilterAllSelectedTable(b *testing.B) {
	for _, cols := range []int{8, 64} {
		for _, rows := range []int{4096, 65536} {
			b.Run(fmt.Sprintf("columns=%d/rows=%d", cols, rows), func(b *testing.B) {
				batch, filter := allSelectedBenchmarkInputs(cols, rows)
				tbl := array.NewTableFromRecords(batch.Schema(), []arrow.RecordBatch{batch})
				selection := compute.NewDatum(filter)
				defer selection.Release()
				defer tbl.Release()
				defer batch.Release()
				defer filter.Release()
				execCtx := compute.DefaultExecCtx()
				execCtx.NumParallel = 1
				ctx := compute.SetExecCtx(context.Background(), execCtx)
				b.ReportAllocs()
				b.SetBytes(int64(cols * rows * 8))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := compute.FilterTable(ctx, tbl, selection, compute.DefaultFilterOptions())
					if err != nil {
						b.Fatal(err)
					}
					allSelectedBenchmarkRows = result.NumRows()
					result.Release()
				}
			})
		}
	}
}
