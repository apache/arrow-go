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
		rec.Release()
	}

	require.Equal(t, int64(numRows), totalRows, "Should read all rows")
	t.Logf("Successfully read %d rows", totalRows)
}
