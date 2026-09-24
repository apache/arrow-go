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

//go:build go1.22

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/internal/kernels"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// benchNestedSortBatch returns a record batch with two columns carrying the
// same values/null pattern:
//
//   - "s": struct{id: int32, info{rank: int32, nullable}}, nullable info.
//   - "flat": a top-level int32 mirroring s.info.rank.
//
// withNulls controls whether info is null every 10th row to help exercise null
// handling behavior
func benchNestedSortBatch(b *testing.B, mem memory.Allocator, n int, withNulls bool) arrow.RecordBatch {
	b.Helper()
	nestedType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "info", Type: arrow.StructOf(
			arrow.Field{Name: "rank", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		), Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: nestedType},
		{Name: "flat", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	structBldr := array.NewStructBuilder(mem, nestedType)
	defer structBldr.Release()
	idBldr := structBldr.FieldBuilder(0).(*array.Int32Builder)
	infoBldr := structBldr.FieldBuilder(1).(*array.StructBuilder)
	rankBldr := infoBldr.FieldBuilder(0).(*array.Int32Builder)

	flatBldr := array.NewInt32Builder(mem)
	defer flatBldr.Release()

	for i := range n {
		structBldr.Append(true)
		idBldr.Append(int32(i))
		if withNulls && i%10 == 0 {
			infoBldr.Append(false)
			flatBldr.AppendNull()
		} else {
			rank := int32((i * 2654435761) % n)
			infoBldr.Append(true)
			rankBldr.Append(rank)
			flatBldr.Append(rank)
		}
	}

	structArr := structBldr.NewArray()
	defer structArr.Release()
	flatArr := flatBldr.NewArray()
	defer flatArr.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{structArr, flatArr}, int64(n))
	b.Cleanup(func() { batch.Release() })
	return batch
}

func BenchmarkSortRecordBatch(b *testing.B) {
	const rows = 65536
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	withNulls := benchNestedSortBatch(b, mem, rows, true)
	noNulls := benchNestedSortBatch(b, mem, rows, false)

	tests := []struct {
		name  string
		batch arrow.RecordBatch
		keys  []kernels.SortKey
	}{
		{
			name:  "ColumnPath",
			batch: withNulls,
			keys:  []kernels.SortKey{{ColumnPath: []int{0, 1, 0}, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}},
		},
		{
			name:  "FlatColumnIndex",
			batch: withNulls,
			keys:  []kernels.SortKey{{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}},
		},
		{
			name:  "ColumnPath_NoNulls",
			batch: noNulls,
			keys:  []kernels.SortKey{{ColumnPath: []int{0, 1, 0}, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}},
		},
		{
			name:  "FlatColumnIndex_NoNulls",
			batch: noNulls,
			keys:  []kernels.SortKey{{ColumnIndex: 1, Order: kernels.Ascending, NullPlacement: kernels.NullsAtEnd}},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := compute.SortRecordBatch(ctx, tc.batch, tc.keys)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}
