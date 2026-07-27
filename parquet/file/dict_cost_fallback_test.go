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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeAllDistinctInt64Chunk writes one buffered row group with a single int64
// column of all-distinct values, for which the dictionary plus its indices can
// never beat the raw PLAIN size, and returns the resulting column chunk metadata.
func writeAllDistinctInt64Chunk(t *testing.T, props *parquet.WriterProperties) *metadata.ColumnChunkMetaData {
	t.Helper()

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	fields := schema.FieldList{schema.NewInt64Node("col", parquet.Repetitions.Required, -1)}
	sc, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	require.NoError(t, err)

	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))
	rgw := writer.AppendBufferedRowGroup()
	cwr, err := rgw.Column(0)
	require.NoError(t, err)
	cw := cwr.(*file.Int64ColumnChunkWriter)

	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(i)*1_000_003 + 7
	}
	_, err = cw.WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	buffer := sink.Finish()
	t.Cleanup(buffer.Release)
	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })

	require.EqualValues(t, 1, reader.NumRowGroups())
	chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
	require.NoError(t, err)
	return chunk
}

func TestDictionaryCostFallback(t *testing.T) {
	t.Run("enabled by default discards non-paying dictionary", func(t *testing.T) {
		chunk := writeAllDistinctInt64Chunk(t, parquet.NewWriterProperties())
		assert.False(t, chunk.HasDictionaryPage())
	})

	t.Run("disabled keeps explicitly requested dictionary", func(t *testing.T) {
		chunk := writeAllDistinctInt64Chunk(t, parquet.NewWriterProperties(
			parquet.WithDictionaryFor("col", true),
			parquet.WithDictionaryCostFallback(false),
		))
		assert.True(t, chunk.HasDictionaryPage())
	})

	t.Run("per-column disable keeps only that column's dictionary", func(t *testing.T) {
		chunk := writeAllDistinctInt64Chunk(t, parquet.NewWriterProperties(
			parquet.WithDictionaryFor("col", true),
			parquet.WithDictionaryCostFallbackFor("col", false),
		))
		assert.True(t, chunk.HasDictionaryPage())
	})

	t.Run("per-column disable for another column leaves the fallback active", func(t *testing.T) {
		chunk := writeAllDistinctInt64Chunk(t, parquet.NewWriterProperties(
			parquet.WithDictionaryFor("col", true),
			parquet.WithDictionaryCostFallbackFor("other", false),
		))
		assert.False(t, chunk.HasDictionaryPage())
	})

	t.Run("page size limit fallback still applies when disabled", func(t *testing.T) {
		chunk := writeAllDistinctInt64Chunk(t, parquet.NewWriterProperties(
			parquet.WithDictionaryFor("col", true),
			parquet.WithDictionaryCostFallback(false),
			parquet.WithDictionaryPageSizeLimit(512),
		))
		assert.False(t, chunk.HasDictionaryPage())
	})
}
