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

func BenchmarkIsInMemoPreSizing(b *testing.B) {
	cases := []struct {
		name         string
		typ          arrow.DataType
		valueSetSize int
		cardinality  int
	}{
		{name: "bool/repeated-64k", typ: arrow.FixedWidthTypes.Boolean, valueSetSize: 65_536, cardinality: 2},
		{name: "uint8/repeated-64k", typ: arrow.PrimitiveTypes.Uint8, valueSetSize: 65_536, cardinality: 256},
		{name: "uint16/repeated-256k", typ: arrow.PrimitiveTypes.Uint16, valueSetSize: 262_144, cardinality: 65_536},
		{name: "int64/unique-1k", typ: arrow.PrimitiveTypes.Int64, valueSetSize: 1_000, cardinality: 1_000},
		{name: "int64/unique-64k", typ: arrow.PrimitiveTypes.Int64, valueSetSize: 64_000, cardinality: 64_000},
		{name: "int64/repeated-64k", typ: arrow.PrimitiveTypes.Int64, valueSetSize: 64_000, cardinality: 64},
		{name: "string/unique-1k", typ: arrow.BinaryTypes.String, valueSetSize: 1_000, cardinality: 1_000},
		{name: "string/unique-64k", typ: arrow.BinaryTypes.String, valueSetSize: 64_000, cardinality: 64_000},
		{name: "string/repeated-64k", typ: arrow.BinaryTypes.String, valueSetSize: 64_000, cardinality: 64},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			mem := memory.DefaultAllocator
			ctx := compute.WithAllocator(context.Background(), mem)
			valueSet := newMemoBenchmarkArray(b, tc.typ, tc.valueSetSize, tc.cardinality)
			defer valueSet.Release()
			input := newMemoBenchmarkArray(b, tc.typ, 4_096, tc.cardinality*2)
			defer input.Release()

			opts := compute.SetOptions{
				ValueSet: compute.NewDatumWithoutOwning(valueSet),
			}
			inputDatum := compute.NewDatumWithoutOwning(input)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := compute.IsIn(ctx, opts, inputDatum)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func newMemoBenchmarkArray(b *testing.B, typ arrow.DataType, length, cardinality int) arrow.Array {
	b.Helper()
	switch typ.ID() {
	case arrow.BOOL:
		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		builder.Reserve(length)
		for i := 0; i < length; i++ {
			builder.Append(i%cardinality != 0)
		}
		result := builder.NewArray()
		builder.Release()
		return result
	case arrow.UINT8:
		builder := array.NewUint8Builder(memory.DefaultAllocator)
		builder.Reserve(length)
		for i := 0; i < length; i++ {
			builder.Append(uint8(i % cardinality))
		}
		result := builder.NewArray()
		builder.Release()
		return result
	case arrow.UINT16:
		builder := array.NewUint16Builder(memory.DefaultAllocator)
		builder.Reserve(length)
		for i := 0; i < length; i++ {
			builder.Append(uint16(i % cardinality))
		}
		result := builder.NewArray()
		builder.Release()
		return result
	case arrow.INT64:
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.Reserve(length)
		for i := 0; i < length; i++ {
			builder.Append(int64(i % cardinality))
		}
		result := builder.NewArray()
		builder.Release()
		return result
	case arrow.STRING:
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.Reserve(length)
		for i := 0; i < length; i++ {
			builder.Append(fmt.Sprintf("value-%08d", i%cardinality))
		}
		result := builder.NewArray()
		builder.Release()
		return result
	default:
		b.Fatalf("unsupported benchmark type: %s", typ)
		return nil
	}
}
