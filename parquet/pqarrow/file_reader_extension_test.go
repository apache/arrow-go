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
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
)

type mismatchingExtensionType struct {
	arrow.ExtensionBase
	storageTypeCalls int
}

type mismatchingExtensionArray struct {
	array.ExtensionArrayBase
}

type stableExtensionType struct {
	arrow.ExtensionBase
}

type stableExtensionArray struct {
	array.ExtensionArrayBase
}

type panickingExtensionType struct {
	arrow.ExtensionBase
}

func (*panickingExtensionType) ArrayType() reflect.Type {
	panic("malformed extension array type")
}

func (*panickingExtensionType) ExtensionName() string { return "test.panicking" }

func (*panickingExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*panickingExtensionType)
	return ok
}

func (*panickingExtensionType) Serialize() string { return "" }

func (*panickingExtensionType) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return &panickingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}, nil
}

func (*stableExtensionType) StorageType() arrow.DataType { return arrow.PrimitiveTypes.Int32 }

func (*stableExtensionType) ArrayType() reflect.Type {
	return reflect.TypeFor[stableExtensionArray]()
}

func (*stableExtensionType) ExtensionName() string { return "test.stable" }

func (*stableExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*stableExtensionType)
	return ok
}

func (*stableExtensionType) Serialize() string { return "" }

func (*stableExtensionType) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return &stableExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}, nil
}

func (t *mismatchingExtensionType) StorageType() arrow.DataType {
	t.storageTypeCalls++
	if t.storageTypeCalls > 2 {
		return arrow.PrimitiveTypes.Int64
	}
	return arrow.PrimitiveTypes.Int32
}

func (*mismatchingExtensionType) ArrayType() reflect.Type {
	return reflect.TypeFor[mismatchingExtensionArray]()
}

func (*mismatchingExtensionType) ExtensionName() string { return "test.mismatching" }

func (*mismatchingExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*mismatchingExtensionType)
	return ok
}

func (*mismatchingExtensionType) Serialize() string { return "" }

func (*mismatchingExtensionType) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return &mismatchingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}, nil
}

type chunkedColumnReader struct {
	colReaderImpl
	chunks *arrow.Chunked
}

func (r *chunkedColumnReader) BuildArray(int64) (*arrow.Chunked, error) {
	return r.chunks, nil
}

func TestExtensionReaderBuildArrayReleasesPartialChunksOnPanic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewInt32Builder(mem)
	b.Append(1)
	first := b.NewInt32Array()
	b.Append(2)
	second := b.NewInt32Array()
	b.Release()

	chunks := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	first.Release()
	second.Release()

	extType := &mismatchingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}
	r := extensionReader{
		colReaderImpl: &chunkedColumnReader{chunks: chunks},
		fieldWithExt:  arrow.Field{Name: "extension", Type: extType},
	}

	require.Panics(t, func() {
		_, _ = r.BuildArray(0)
	})
	require.Zero(t, mem.CurrentAlloc())
}

func TestExtensionReaderBuildArrayReleasesChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewInt32Builder(mem)
	b.Append(1)
	first := b.NewInt32Array()
	b.Append(2)
	second := b.NewInt32Array()
	b.Release()

	chunks := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	first.Release()
	second.Release()

	extType := &stableExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}
	r := extensionReader{
		colReaderImpl: &chunkedColumnReader{chunks: chunks},
		fieldWithExt:  arrow.Field{Name: "extension", Type: extType},
	}

	out, err := r.BuildArray(0)
	require.NoError(t, err)
	out.Release()
}

func TestReadRowGroupsSerialRecoversFromPanic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewInt32Builder(mem)
	builder.Append(1)
	values := builder.NewInt32Array()
	builder.Release()
	defer values.Release()

	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(schema, &buf, nil, DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer parquetReader.Close()

	reader, err := NewFileReader(parquetReader, ArrowReadProperties{Parallel: false}, mem)
	require.NoError(t, err)
	reader.Manifest.Fields[0].Field.Type = &panickingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}

	var table arrow.Table
	var readErr error
	require.NotPanics(t, func() {
		table, readErr = reader.ReadRowGroups(context.Background(), []int{0}, []int{0})
	})
	require.Nil(t, table)
	require.ErrorContains(t, readErr, "panic while reading")
	require.ErrorContains(t, readErr, "malformed extension array type")
}

type cancelingExtensionType struct {
	stableExtensionType
	cancel context.CancelFunc
}

func (t *cancelingExtensionType) ArrayType() reflect.Type {
	t.cancel()
	return reflect.TypeFor[stableExtensionArray]()
}

func TestReadRowGroupsCanceledDuringFinalColumn(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewInt32Builder(mem)
	builder.Append(1)
	values := builder.NewInt32Array()
	builder.Release()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(schema, &buf, nil, DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer parquetReader.Close()
			reader, err := NewFileReader(parquetReader, ArrowReadProperties{Parallel: parallel}, mem)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader.Manifest.Fields[0].Field.Type = &cancelingExtensionType{cancel: cancel}

			table, err := reader.ReadTable(ctx)
			if table != nil {
				defer table.Release()
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, table)
		})
	}
}
