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
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunksToSingle(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("empty chunked array", func(t *testing.T) {
		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len())
	})

	t.Run("single chunk", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		bldr.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
		arr := bldr.NewInt32Array()
		defer arr.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len())
	})

	t.Run("multiple chunks", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues([]int32{1, 2, 3}, nil)
		chunk1 := bldr.NewInt32Array()
		defer chunk1.Release()

		bldr.AppendValues([]int32{4, 5, 6}, nil)
		chunk2 := bldr.NewInt32Array()
		defer chunk2.Release()

		bldr.AppendValues([]int32{7, 8, 9, 10}, nil)
		chunk3 := bldr.NewInt32Array()
		defer chunk3.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk1, chunk2, chunk3})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 10, result.Len())

		// Verify concatenated values
		resultArr := array.MakeFromData(result).(*array.Int32)
		defer resultArr.Release()
		for i, expected := range []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
			assert.Equal(t, expected, resultArr.Value(i))
		}
	})

	t.Run("multiple chunks with nulls", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues([]int32{1, 0, 3}, []bool{true, false, true})
		chunk1 := bldr.NewInt32Array()
		defer chunk1.Release()

		bldr.AppendValues([]int32{4, 0, 6}, []bool{true, false, true})
		chunk2 := bldr.NewInt32Array()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 6, result.Len())
		assert.Equal(t, 2, result.NullN())

		resultArr := array.MakeFromData(result).(*array.Int32)
		defer resultArr.Release()
		assert.False(t, resultArr.IsValid(1))
		assert.False(t, resultArr.IsValid(4))
		assert.Equal(t, int32(1), resultArr.Value(0))
		assert.Equal(t, int32(3), resultArr.Value(2))
	})

	t.Run("multiple chunks string type", func(t *testing.T) {
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues([]string{"hello", "world"}, nil)
		chunk1 := bldr.NewStringArray()
		defer chunk1.Release()

		bldr.AppendValues([]string{"arrow", "parquet"}, nil)
		chunk2 := bldr.NewStringArray()
		defer chunk2.Release()

		chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{chunk1, chunk2})
		defer chunked.Release()

		result, err := chunksToSingle(chunked, mem)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 4, result.Len())

		resultArr := array.MakeFromData(result).(*array.String)
		defer resultArr.Release()
		assert.Equal(t, "hello", resultArr.Value(0))
		assert.Equal(t, "parquet", resultArr.Value(3))
	})
}

func TestChunkedTableRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "string_col", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	// Test data across 3 chunks: 5 + 3 + 2 = 10 rows
	allInt64Values := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	allStringValues := []string{"hello", "world", "arrow", "parquet", "go", "chunked", "table", "test", "final", "chunk"}

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithAllocator(mem),
	)

	writerProps := NewArrowWriterProperties(
		WithAllocator(mem),
	)

	writer, err := NewFileWriter(schema, &buf, props, writerProps)
	require.NoError(t, err)

	// Write three chunks: 5 rows, 3 rows, 2 rows
	chunks := []struct{ start, end int }{
		{0, 5},  // First chunk: 5 rows
		{5, 8},  // Second chunk: 3 rows
		{8, 10}, // Third chunk: 2 rows
	}

	for _, chunk := range chunks {
		int64Builder := array.NewInt64Builder(mem)
		int64Builder.AppendValues(allInt64Values[chunk.start:chunk.end], nil)
		int64Arr := int64Builder.NewInt64Array()
		int64Builder.Release()

		stringBuilder := array.NewStringBuilder(mem)
		stringBuilder.AppendValues(allStringValues[chunk.start:chunk.end], nil)
		stringArr := stringBuilder.NewStringArray()
		stringBuilder.Release()

		rec := array.NewRecordBatch(schema, []arrow.Array{int64Arr, stringArr}, int64(chunk.end-chunk.start))

		err = writer.Write(rec)
		require.NoError(t, err)

		rec.Release()
		int64Arr.Release()
		stringArr.Release()
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read back from parquet
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pf.Close()

	reader, err := NewFileReader(pf, ArrowReadProperties{}, mem)
	require.NoError(t, err)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	var records []arrow.RecordBatch
	for {
		rec, err := rr.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rec.Retain()
		records = append(records, rec)
	}

	readTable := array.NewTableFromRecords(schema, records)
	defer readTable.Release()

	for _, rec := range records {
		rec.Release()
	}

	// Verify the read table
	require.Equal(t, int64(10), readTable.NumRows())
	require.Equal(t, int64(2), readTable.NumCols())

	// Verify int64 column values
	int64Col := readTable.Column(0).Data()
	int64Single, err := chunksToSingle(int64Col, mem)
	require.NoError(t, err)
	defer int64Single.Release()
	int64Arr := array.MakeFromData(int64Single).(*array.Int64)
	defer int64Arr.Release()
	for i := 0; i < int64Arr.Len(); i++ {
		assert.Equal(t, allInt64Values[i], int64Arr.Value(i))
	}

	// Verify string column values
	stringCol := readTable.Column(1).Data()
	stringSingle, err := chunksToSingle(stringCol, mem)
	require.NoError(t, err)
	defer stringSingle.Release()
	stringArr := array.MakeFromData(stringSingle).(*array.String)
	defer stringArr.Release()
	for i := 0; i < stringArr.Len(); i++ {
		assert.Equal(t, allStringValues[i], stringArr.Value(i))
	}
}
