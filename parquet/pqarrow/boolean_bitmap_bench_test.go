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

package pqarrow

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
)

// Benchmark writing boolean columns with direct bitmap path
func BenchmarkBooleanBitmapWrite(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			benchmarkBooleanWrite(b, size, false)
		})
	}
}

// Benchmark writing nullable boolean columns with direct bitmap path
func BenchmarkBooleanBitmapWriteNullable(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			benchmarkBooleanWrite(b, size, true)
		})
	}
}

func benchmarkBooleanWrite(b *testing.B, size int, nullable bool) {
	mem := memory.NewGoAllocator()

	// Create Arrow schema
	var arrowSchema *arrow.Schema
	if nullable {
		arrowSchema = arrow.NewSchema([]arrow.Field{
			{Name: "bools", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		}, nil)
	} else {
		arrowSchema = arrow.NewSchema([]arrow.Field{
			{Name: "bools", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		}, nil)
	}

	// Create test data
	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()

	for i := 0; i < size; i++ {
		if nullable && i%10 == 0 {
			bldr.AppendNull()
		} else {
			bldr.Append(i%2 == 0)
		}
	}

	arr := bldr.NewBooleanArray()
	defer arr.Release()

	rec := array.NewRecord(arrowSchema, []arrow.Array{arr}, int64(size))
	defer rec.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer, err := NewFileWriter(arrowSchema, &buf,
			parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
			NewArrowWriterProperties(WithAllocator(mem)))
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.WriteBuffered(rec); err != nil {
			b.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(int64(size / 8)) // Report bits as bytes for throughput
}

func formatSize(size int) string {
	switch {
	case size >= 1000000:
		return "1M"
	case size >= 100000:
		return "100K"
	case size >= 10000:
		return "10K"
	case size >= 1000:
		return "1K"
	default:
		return "small"
	}
}
