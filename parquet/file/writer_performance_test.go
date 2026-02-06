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

package file_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// Benchmark writing small ByteArray values (typical case)
// This tests the common scenario where values are small (< 1KB)
func BenchmarkWriteSmallByteArrayValues(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	// Small values: 100 bytes each
	const valueSize = 100
	const numValues = 10000
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = value
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
		byteArrayWriter.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}

// Benchmark writing medium ByteArray values (10KB each)
func BenchmarkWriteMediumByteArrayValues(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	// Medium values: 10KB each
	const valueSize = 10 * 1024
	const numValues = 1000
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = value
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
		byteArrayWriter.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}

// Benchmark writing large ByteArray values (1MB each)
func BenchmarkWriteLargeByteArrayValues(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	// Large values: 1MB each
	const valueSize = 1024 * 1024
	const numValues = 100
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = value
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
		byteArrayWriter.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}

// Benchmark writing Int32 values (control - unaffected by fix)
func BenchmarkWriteInt32Values(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	const numValues = 10000
	values := make([]int32, numValues)
	for i := range values {
		values[i] = int32(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		int32Writer := colWriter.(*file.Int32ColumnChunkWriter)
		int32Writer.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}

// Benchmark writing variable-sized ByteArray values (mixed workload)
func BenchmarkWriteMixedByteArrayValues(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	// Mix of small (100B), medium (10KB), and large (1MB) values
	const numValues = 1000
	values := make([]parquet.ByteArray, numValues)

	for i := range values {
		var size int
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5, 6, 7: // 80% small
			size = 100
		case 8: // 10% medium
			size = 10 * 1024
		case 9: // 10% large
			size = 100 * 1024
		}
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(j % 256)
		}
		values[i] = value
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
		byteArrayWriter.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}

// Benchmark writing tiny ByteArray values (worst case for checking overhead)
// Values are 10 bytes each - this maximizes the ratio of checking overhead to actual work
func BenchmarkWriteTinyByteArrayValues(b *testing.B) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
	)

	// Tiny values: 10 bytes each - worst case for overhead
	const valueSize = 10
	const numValues = 100000
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = value
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out := &bytes.Buffer{}
		writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
		rgw := writer.AppendRowGroup()
		colWriter, _ := rgw.NextColumn()
		byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
		byteArrayWriter.WriteBatch(values, nil, nil)
		colWriter.Close()
		rgw.Close()
		writer.Close()
	}
}
