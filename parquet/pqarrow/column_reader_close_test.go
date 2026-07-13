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
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
)

type pageReaderReplacementErrorRecordReader struct {
	file.RecordReader
	err       error
	seekCalls int
}

func (r *pageReaderReplacementErrorRecordReader) SetPageReaderWithError(pr file.PageReader) error {
	if pr != nil {
		_ = pr.Close()
	}
	return r.err
}

func (r *pageReaderReplacementErrorRecordReader) SeekToRow(row int64) error {
	r.seekCalls++
	return r.RecordReader.SeekToRow(row)
}

func (r *pageReaderReplacementErrorRecordReader) Release() {
	r.RecordReader.SetPageReader(nil)
	r.RecordReader.Release()
}

func TestLeafReaderSeekToRowPropagatesPageReaderReplacementError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{1, 2}, nil)
	values := builder.NewInt32Array()
	builder.Release()
	defer values.Release()

	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	column := arrow.NewColumnFromArr(field, values)
	tbl := array.NewTable(schema, []arrow.Column{column}, -1)
	column.Release()
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, WriteTable(tbl, &buf, tbl.NumRows(), nil, NewArrowWriterProperties(WithAllocator(mem))))

	pfile, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pfile.Close()

	reader, err := NewFileReader(pfile, ArrowReadProperties{}, mem)
	require.NoError(t, err)

	columnReader, err := reader.GetColumn(context.Background(), 0)
	require.NoError(t, err)
	defer columnReader.Release()

	leaf, ok := columnReader.colReaderImpl.(*leafReader)
	require.True(t, ok)

	closeErr := errors.New("page reader close failed")
	wrapped := &pageReaderReplacementErrorRecordReader{
		RecordReader: leaf.recordRdr,
		err:          closeErr,
	}
	leaf.recordRdr = wrapped

	require.ErrorIs(t, columnReader.SeekToRow(0), closeErr)
	require.Zero(t, wrapped.seekCalls)
}
