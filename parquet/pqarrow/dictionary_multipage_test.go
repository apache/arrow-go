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
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

// TestArrowDictionaryTypeMultiplePages tests reading Arrow Dictionary types
// with multiple data pages in a single row group.
//
// This test exercises byteArrayDictRecordReader which has a bug at line 966
// in maybeWriteNewDictionary() that resets newDictionary=false.
//
// The bug manifests when:
// 1. Arrow schema has Dictionary type (not just parquet dictionary encoding)
// 2. Multiple data pages exist in a row group
// 3. Reading with large batch size that spans multiple pages
func TestArrowDictionaryTypeMultiplePages(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Schema with Arrow Dictionary type
	schema := arrow.NewSchema(
		[]arrow.Field{
			{
				Name: "dict_col",
				Type: &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Uint32,
					ValueType: arrow.BinaryTypes.String,
				},
				Nullable: false,
			},
		},
		nil,
	)

	var buf bytes.Buffer

	// CRITICAL: Use WithStoreSchema() to preserve Arrow Dictionary type metadata
	// Without this, arrow-go converts Dictionary type to plain string
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDataPageSize(10), // Small page size to force multiple pages
		parquet.WithMaxRowGroupLength(100000),
		parquet.WithAllocator(mem),
	)

	writerProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(), // KEY: Preserve Arrow Dictionary type
		pqarrow.WithAllocator(mem),
	)

	writer, err := pqarrow.NewFileWriter(schema, &buf, props, writerProps)
	require.NoError(t, err)

	// Create dictionary array with many values to span multiple pages
	dictBuilder := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint32,
		ValueType: arrow.BinaryTypes.String,
	}).(*array.BinaryDictionaryBuilder)
	defer dictBuilder.Release()

	// Create data with few unique values (good for dictionary)
	values := []string{"ValueA", "ValueB", "ValueC", "ValueD"}
	numRows := 2000

	for i := 0; i < numRows; i++ {
		require.NoError(t, dictBuilder.AppendString(values[i%len(values)]))
	}

	dictArray := dictBuilder.NewDictionaryArray()
	defer dictArray.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{dictArray}, int64(numRows))
	defer rec.Release()

	err = writer.Write(rec)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	t.Logf("Written %d bytes", buf.Len())

	// Read back
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pf.Close()

	t.Logf("File has %d row groups", pf.NumRowGroups())

	reader, err := pqarrow.NewFileReader(pf,
		pqarrow.ArrowReadProperties{BatchSize: pf.NumRows()}, mem)
	require.NoError(t, err)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	// Read all data - this should trigger the bug if present
	totalRows := int64(0)
	for {
		rec, err := rr.Read()
		if err == io.EOF {
			break
		}

		// This will fail with "parquet: column chunk cannot have more than one dictionary"
		// if the bug is present
		require.NoError(t, err, "Failed to read Arrow Dictionary type with multiple pages")

		totalRows += rec.NumRows()
		// Note: Don't call rec.Release() here - the record reader manages record lifecycle
	}

	require.Equal(t, int64(numRows), totalRows, "Should read all rows")
	t.Logf("Successfully read %d rows", totalRows)
}

func TestArrowDictionaryTypePreservesIndexType(t *testing.T) {
	for _, indexType := range []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
	} {
		t.Run(indexType.Name(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			dictType := &arrow.DictionaryType{IndexType: indexType, ValueType: arrow.BinaryTypes.String}
			sc := arrow.NewSchema([]arrow.Field{{Name: "dict_col", Type: dictType}}, nil)
			builder := array.NewDictionaryBuilder(mem, dictType).(*array.BinaryDictionaryBuilder)
			defer builder.Release()
			for _, value := range []string{"a", "b", "a", "c"} {
				require.NoError(t, builder.AppendString(value))
			}
			values := builder.NewDictionaryArray()
			defer values.Release()

			var buf bytes.Buffer
			writer, err := pqarrow.NewFileWriter(sc, &buf,
				parquet.NewWriterProperties(parquet.WithAllocator(mem)),
				pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(mem)))
			require.NoError(t, err)
			rec := array.NewRecordBatch(sc, []arrow.Array{values}, int64(values.Len()))
			require.NoError(t, writer.Write(rec))
			require.NoError(t, writer.Close())
			rec.Release()

			pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer pf.Close()
			reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
			require.NoError(t, err)

			readSchema, err := reader.Schema()
			require.NoError(t, err)
			require.True(t, arrow.TypeEqual(dictType, readSchema.Field(0).Type))

			tbl, err := reader.ReadTable(context.Background())
			require.NoError(t, err)
			defer tbl.Release()
			require.True(t, arrow.TypeEqual(dictType, tbl.Column(0).DataType()))
			require.True(t, array.Equal(values, tbl.Column(0).Data().Chunk(0)))
		})
	}
}
