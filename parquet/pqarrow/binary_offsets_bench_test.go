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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func benchmarkBinaryTable(mem memory.Allocator, n int) (arrow.Table, int64) {
	builder := array.NewStringBuilder(mem)
	builder.Reserve(n)
	var inputBytes int64
	for i := 0; i < n; i++ {
		value := append([]byte("partition/"), strconv.AppendInt(nil, int64(i), 10)...)
		builder.Append(string(value))
		inputBytes += int64(len(value))
	}
	arr := builder.NewArray()
	builder.Release()

	sch := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	col := arrow.NewColumnFromArr(sch.Field(0), arr)
	arr.Release()
	tbl := array.NewTable(sch, []arrow.Column{col}, int64(n))
	col.Release()
	return tbl, inputBytes
}

func BenchmarkWriteArrowBinaryOffsets(b *testing.B) {
	const n = 64 * 1024
	mem := memory.DefaultAllocator
	tbl, inputBytes := benchmarkBinaryTable(mem, n)
	defer tbl.Release()

	for _, stats := range []bool{false, true} {
		b.Run("stats="+strconv.FormatBool(stats), func(b *testing.B) {
			props := parquet.NewWriterProperties(
				parquet.WithDictionaryDefault(false),
				parquet.WithStats(stats),
				parquet.WithCompression(compress.Codecs.Uncompressed),
			)
			b.SetBytes(inputBytes)
			b.ReportAllocs()
			for b.Loop() {
				var buf bytes.Buffer
				if err := pqarrow.WriteTable(tbl, &buf, int64(n), props, pqarrow.DefaultWriterProps()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
