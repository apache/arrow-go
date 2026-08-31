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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func fixedSizeBinaryTable(t *testing.T, values [][]byte, byteWidth int) arrow.Table {
	t.Helper()
	mem := memory.DefaultAllocator
	builder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
	for _, value := range values {
		if value == nil {
			builder.AppendNull()
		} else {
			builder.Append(value)
		}
	}
	arr := builder.NewArray()
	builder.Release()

	field := arrow.Field{
		Name:     "value",
		Type:     &arrow.FixedSizeBinaryType{ByteWidth: byteWidth},
		Nullable: true,
	}
	sch := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumnFromArr(field, arr)
	arr.Release()
	tbl := array.NewTable(sch, []arrow.Column{col}, int64(len(values)))
	col.Release()
	return tbl
}

func TestWriteArrowFixedSizeBinaryDirect(t *testing.T) {
	tbl := fixedSizeBinaryTable(t, [][]byte{
		{0x03, 0x02, 0x01},
		nil,
		{0x09, 0x08, 0x07},
		{0x06, 0x05, 0x04},
		{0x0c, 0x0b, 0x0a},
		{0x0f, 0x0e, 0x0d},
	}, 3)
	defer tbl.Release()

	for _, encoding := range []parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.ByteStreamSplit} {
		t.Run(encoding.String(), func(t *testing.T) {
			writerProps := parquet.NewWriterProperties(
				parquet.WithDictionaryDefault(false),
				parquet.WithEncodingFor("value", encoding),
				parquet.WithStats(true),
				parquet.WithBatchSize(2),
				parquet.WithDataPageSize(16),
				parquet.WithPageIndexEnabled(true),
				parquet.WithBloomFilterEnabledFor("value", true),
				parquet.WithBloomFilterNDVFor("value", tbl.NumRows()),
			)
			data := writeParquetTable(t, tbl, tbl.NumRows(), writerProps)
			got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
			defer got.Release()
			assertTableColumnsEqual(t, tbl, got)
		})
	}
}

func TestWriteArrowFixedSizeBinaryDirectWithSlice(t *testing.T) {
	full := fixedSizeBinaryTable(t, [][]byte{
		{0x00, 0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06, 0x07},
		{0x08, 0x09, 0x0a, 0x0b},
		nil,
		{0x10, 0x11, 0x12, 0x13},
		{0x14, 0x15, 0x16, 0x17},
	}, 4)
	defer full.Release()

	sliced := array.NewSlice(full.Column(0).Data().Chunk(0), 1, 5)
	defer sliced.Release()
	field := arrow.Field{Name: "value", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true}
	sch := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumnFromArr(field, sliced)
	tbl := array.NewTable(sch, []arrow.Column{col}, int64(sliced.Len()))
	col.Release()
	defer tbl.Release()

	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(2),
		parquet.WithStats(true),
	)
	data := writeParquetTable(t, tbl, tbl.NumRows(), writerProps)
	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
	defer got.Release()
	assertTableColumnsEqual(t, tbl, got)
}

func TestWriteArrowFixedSizeBinaryDirectNestedList(t *testing.T) {
	mem := memory.DefaultAllocator
	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	builder := array.NewListBuilder(mem, dtype)
	values := builder.ValueBuilder().(*array.FixedSizeBinaryBuilder)

	builder.Append(true)
	values.Append([]byte("aaa"))
	values.Append([]byte("bbb"))
	builder.AppendNull()
	builder.Append(true)
	builder.Append(true)
	values.Append([]byte("ccc"))
	values.AppendNull()
	arr := builder.NewListArray()
	builder.Release()

	field := arrow.Field{Name: "value", Type: arr.DataType(), Nullable: true}
	sch := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumnFromArr(field, arr)
	arr.Release()
	tbl := array.NewTable(sch, []arrow.Column{col}, 4)
	col.Release()
	defer tbl.Release()

	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(2),
		parquet.WithStats(true),
		parquet.WithPageIndexEnabled(true),
	)
	data := writeParquetTable(t, tbl, tbl.NumRows(), writerProps)
	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
	defer got.Release()
	assertTableColumnsEqual(t, tbl, got)
}

func TestWriteArrowFixedSizeBinaryDictionaryFallbackPath(t *testing.T) {
	tbl := fixedSizeBinaryTable(t, [][]byte{
		[]byte("foo!"),
		[]byte("bar!"),
		nil,
		[]byte("foo!"),
		[]byte("baz!"),
	}, 4)
	defer tbl.Release()

	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
	)
	data := writeParquetTable(t, tbl, tbl.NumRows(), writerProps)
	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
	defer got.Release()
	assertTableColumnsEqual(t, tbl, got)
}

func TestWriteArrowFixedSizeBinaryAllNull(t *testing.T) {
	tbl := fixedSizeBinaryTable(t, [][]byte{nil, nil, nil, nil}, 8)
	defer tbl.Release()

	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithStats(true),
	)
	data := writeParquetTable(t, tbl, tbl.NumRows(), writerProps)
	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
	defer got.Release()
	assertTableColumnsEqual(t, tbl, got)
	require.Equal(t, tbl.NumRows(), got.NumRows())
}

func TestWriteArrowFixedSizeBinaryNestedNullStatistics(t *testing.T) {
	for _, pageVersion := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		for _, nullableValues := range []bool{false, true} {
			t.Run(fmt.Sprintf("page-%d/nullable-values-%t", pageVersion, nullableValues), func(t *testing.T) {
				builder := array.NewListBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 3})
				defer builder.Release()
				values := builder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
				builder.Append(true)
				values.Append([]byte("aaa"))
				builder.AppendNull()
				builder.Append(true)
				builder.Append(true)
				values.Append([]byte("zzz"))
				nullCount := int64(2)
				if nullableValues {
					values.AppendNull()
					nullCount++
				}
				arr := builder.NewListArray()
				defer arr.Release()
				field := arrow.Field{Name: "value", Type: arr.DataType(), Nullable: true}
				column := arrow.NewColumnFromArr(field, arr)
				defer column.Release()
				tbl := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Column{column}, int64(arr.Len()))
				defer tbl.Release()
				props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false),
					parquet.WithDataPageVersion(pageVersion), parquet.WithBatchSize(2))
				data := writeParquetTable(t, tbl, tbl.NumRows(), props)
				reader, err := file.NewParquetReader(bytes.NewReader(data))
				require.NoError(t, err)
				defer reader.Close()
				chunk, err := reader.MetaData().RowGroup(0).ColumnChunk(0)
				require.NoError(t, err)
				stats, err := chunk.Statistics()
				require.NoError(t, err)
				require.Equal(t, nullCount, stats.NullCount())
				require.Equal(t, int64(2), stats.NumValues())
				require.Equal(t, []byte("aaa"), stats.EncodeMin())
				require.Equal(t, []byte("zzz"), stats.EncodeMax())
			})
		}
	}
}

func TestWriteArrowFixedSizeBinaryBatchSizeFallback(t *testing.T) {
	for _, batchSize := range []int64{-1, 0, (1<<30)/7 + 1} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch-%d/nullable-%t", batchSize, nullable), func(t *testing.T) {
				values := [][]byte{[]byte("aaa"), []byte("zzz")}
				if nullable {
					values = append(values, nil)
				}
				tbl := fixedSizeBinaryTable(t, values, 3)
				defer tbl.Release()
				props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false),
					parquet.WithBatchSize(batchSize))
				data := writeParquetTable(t, tbl, tbl.NumRows(), props)
				got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
				defer got.Release()
				assertTableColumnsEqual(t, tbl, got)
			})
		}
	}
}
