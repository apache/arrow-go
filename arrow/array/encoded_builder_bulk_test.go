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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkRunEndEncodedBuilderBulkAppend(b *testing.B) {
	runEndTypes := []struct {
		name    string
		typ     arrow.DataType
		maxRows int
	}{
		{name: "int16", typ: arrow.PrimitiveTypes.Int16, maxRows: 1 << 14},
		{name: "int32", typ: arrow.PrimitiveTypes.Int32, maxRows: 1 << 16},
		{name: "int64", typ: arrow.PrimitiveTypes.Int64, maxRows: 1 << 16},
	}
	encodedTypes := []struct {
		name string
		typ  arrow.DataType
	}{
		{name: "int32", typ: arrow.PrimitiveTypes.Int32},
		{name: "string", typ: arrow.BinaryTypes.String},
	}

	for _, runEndType := range runEndTypes {
		b.Run(runEndType.name, func(b *testing.B) {
			for _, encodedType := range encodedTypes {
				b.Run(encodedType.name, func(b *testing.B) {
					for _, rows := range []int{1, 16, 1024, 1 << 14, 1 << 16} {
						if rows > runEndType.maxRows {
							continue
						}

						b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
							b.Run("nulls", func(b *testing.B) {
								benchmarkRunEndEncodedBuilderBulkAppend(b, runEndType.typ, encodedType.typ, rows, false)
							})
							b.Run("empty", func(b *testing.B) {
								benchmarkRunEndEncodedBuilderBulkAppend(b, runEndType.typ, encodedType.typ, rows, true)
							})
						})
					}
				})
			}
		})
	}
}

func benchmarkRunEndEncodedBuilderBulkAppend(b *testing.B, runEndType, encodedType arrow.DataType, rows int, empty bool) {
	builder := array.NewRunEndEncodedBuilder(memory.DefaultAllocator, runEndType, encodedType)
	defer builder.Release()
	b.ReportAllocs()

	for b.Loop() {
		if empty {
			builder.AppendEmptyValues(rows)
		} else {
			builder.AppendNulls(rows)
		}

		arr := builder.NewArray()
		arr.Release()
	}
}
