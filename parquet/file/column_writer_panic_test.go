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

package file

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// A write handler that recovers a panic has usually buffered the levels
// without their values, so the column must keep the error and Close must
// not finish the chunk.
func TestWriteBatchRecoveredPanicIsKeptForClose(t *testing.T) {
	sc, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{schema.NewInt32Node("col", parquet.Repetitions.Optional, -1)}, -1)
	require.NoError(t, err)
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	writer := NewParquetWriter(&buf, sc, WithWriterProps(props))
	rg := writer.AppendRowGroup()
	col, err := rg.NextColumn()
	require.NoError(t, err)
	w := col.(*Int32ColumnChunkWriter)

	// three defined slots but only one value
	_, err = w.WriteBatch([]int32{1}, []int16{1, 1, 1}, nil)
	require.ErrorContains(t, err, "slice bounds out of range")
	failedSize := buf.Len()
	for i := 0; i < 2; i++ {
		require.ErrorContains(t, w.Close(), "slice bounds out of range", "column close %d", i)
		require.ErrorContains(t, rg.Close(), "slice bounds out of range", "row group close %d", i)
		require.ErrorContains(t, writer.Close(), "slice bounds out of range", "file close %d", i)
	}
	require.Equal(t, failedSize, buf.Len(), "close must not append a footer")
	_, readErr := NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, readErr, "failed writes must not produce a readable footer")
}

// The hand-written Arrow write paths recover the same way.
func TestWriteBatchArrowRecoveredPanicIsKeptForClose(t *testing.T) {
	sc, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{schema.NewFixedLenByteArrayNode("col", parquet.Repetitions.Optional, 12, -1)}, -1)
	require.NoError(t, err)
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	writer := NewParquetWriter(&buf, sc, WithWriterProps(props))
	rg := writer.AppendRowGroup()
	col, err := rg.NextColumn()
	require.NoError(t, err)
	w := col.(*FixedLenByteArrayColumnChunkWriter)

	// two defined slots but only one 12-byte value
	_, err = w.WriteBatchArrow(make([]byte, 12), []int16{1, 1}, nil)
	require.ErrorContains(t, err, "slice bounds out of range")
	failedSize := buf.Len()
	for i := 0; i < 2; i++ {
		require.ErrorContains(t, w.Close(), "slice bounds out of range", "column close %d", i)
		require.ErrorContains(t, rg.Close(), "slice bounds out of range", "row group close %d", i)
		require.ErrorContains(t, writer.Close(), "slice bounds out of range", "file close %d", i)
	}
	require.Equal(t, failedSize, buf.Len(), "close must not append a footer")
	_, readErr := NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, readErr, "failed writes must not produce a readable footer")
}
