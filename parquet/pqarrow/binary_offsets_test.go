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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func makeArrowBinaryOffsetsTables(t *testing.T) []struct {
	name  string
	table arrow.Table
} {
	t.Helper()
	mem := memory.DefaultAllocator
	result := make([]struct {
		name  string
		table arrow.Table
	}, 0, 3)

	binaryBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	for _, value := range [][]byte{[]byte("alpha"), []byte("beta"), nil, []byte("gamma"), []byte{}, []byte("delta")} {
		if value == nil {
			binaryBuilder.AppendNull()
		} else {
			binaryBuilder.Append(value)
		}
	}
	binaryArray := binaryBuilder.NewArray()
	binaryBuilder.Release()
	binarySchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	binaryColumn := arrow.NewColumnFromArr(binarySchema.Field(0), binaryArray)
	binaryArray.Release()
	result = append(result, struct {
		name  string
		table arrow.Table
	}{"binary", array.NewTable(binarySchema, []arrow.Column{binaryColumn}, 6)})
	binaryColumn.Release()

	stringBuilder := array.NewStringBuilder(mem)
	for _, value := range []string{"alpha", "beta", "", "gamma", "delta", "epsilon"} {
		stringBuilder.Append(value)
	}
	stringArray := stringBuilder.NewArray()
	stringBuilder.Release()
	stringSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
	stringColumn := arrow.NewColumnFromArr(stringSchema.Field(0), stringArray)
	stringArray.Release()
	result = append(result, struct {
		name  string
		table arrow.Table
	}{"string", array.NewTable(stringSchema, []arrow.Column{stringColumn}, 6)})
	stringColumn.Release()

	largeStringBuilder := array.NewLargeStringBuilder(mem)
	for _, value := range []string{"alpha", "beta", "", "gamma", "delta", "epsilon"} {
		largeStringBuilder.Append(value)
	}
	largeStringArray := largeStringBuilder.NewArray()
	largeStringBuilder.Release()
	largeStringSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.LargeString, Nullable: false}}, nil)
	largeStringColumn := arrow.NewColumnFromArr(largeStringSchema.Field(0), largeStringArray)
	largeStringArray.Release()
	result = append(result, struct {
		name  string
		table arrow.Table
	}{"large-string", array.NewTable(largeStringSchema, []arrow.Column{largeStringColumn}, 6)})
	largeStringColumn.Release()

	return result
}

func TestWriteArrowBinaryOffsets(t *testing.T) {
	encodings := []parquet.Encoding{
		parquet.Encodings.Plain,
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	}

	for _, input := range makeArrowBinaryOffsetsTables(t) {
		input := input
		t.Run(input.name, func(t *testing.T) {
			defer input.table.Release()
			for _, encoding := range encodings {
				t.Run(encoding.String(), func(t *testing.T) {
					writerProps := parquet.NewWriterProperties(
						parquet.WithDictionaryDefault(false),
						parquet.WithEncodingFor("value", encoding),
						parquet.WithStats(true),
						parquet.WithBatchSize(2),
						parquet.WithDataPageSize(32),
						parquet.WithPageIndexEnabled(true),
						parquet.WithBloomFilterEnabledFor("value", true),
						parquet.WithBloomFilterNDVFor("value", input.table.NumRows()),
					)
					data := writeParquetTable(t, input.table, input.table.NumRows(), writerProps)
					got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
					defer got.Release()
					if input.name == "large-string" {
						wantArr := input.table.Column(0).Data().Chunk(0)
						gotArr := got.Column(0).Data().Chunk(0)
						for i := 0; i < wantArr.Len(); i++ {
							require.Equal(t, wantArr.IsNull(i), gotArr.IsNull(i))
							if !wantArr.IsNull(i) {
								require.Equal(t, binaryOffsetValue(wantArr, i), binaryOffsetValue(gotArr, i))
							}
						}
					} else {
						assertTableColumnsEqual(t, input.table, got)
					}
				})
			}
		})
	}
}

func binaryOffsetValue(arr arrow.Array, index int) []byte {
	switch arr := arr.(type) {
	case *array.Binary:
		return arr.Value(index)
	case *array.LargeBinary:
		return arr.Value(index)
	case *array.String:
		return []byte(arr.Value(index))
	case *array.LargeString:
		return []byte(arr.Value(index))
	default:
		panic("unexpected binary array type")
	}
}

func TestWriteArrowBinaryOffsetsWithSlice(t *testing.T) {
	mem := memory.DefaultAllocator
	builder := array.NewStringBuilder(mem)
	for _, value := range []string{"zero", "one", "two", "three", "four", "five"} {
		builder.Append(value)
	}
	full := builder.NewArray()
	builder.Release()
	defer full.Release()

	sliced := array.NewSlice(full, 1, 5)
	defer sliced.Release()
	field := arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	column := arrow.NewColumnFromArr(field, sliced)
	table := array.NewTable(schema, []arrow.Column{column}, int64(sliced.Len()))
	column.Release()
	defer table.Release()

	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(2),
		parquet.WithEncodingFor("value", parquet.Encodings.Plain),
	)
	data := writeParquetTable(t, table, table.NumRows(), writerProps)
	got := readParquetTable(t, data, pqarrow.ArrowReadProperties{})
	defer got.Release()

	wantArr := table.Column(0).Data().Chunk(0)
	gotArr := got.Column(0).Data().Chunk(0)
	for i := 0; i < wantArr.Len(); i++ {
		require.Equal(t, binaryOffsetValue(wantArr, i), binaryOffsetValue(gotArr, i))
	}
}
