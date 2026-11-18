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

//go:build !pqarrow_read_only

package pqarrow_test

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestWriteFiniteVector(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typ := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)
	field := arrow.Field{Name: "v", Type: typ, Nullable: true}
	sc := arrow.NewSchema([]arrow.Field{field}, nil)
	write := func(values arrow.Array, opts ...pqarrow.WriterOption) ([]byte, error) {
		col := arrow.NewColumnFromArr(field, values)
		defer col.Release()
		table := array.NewTable(sc, []arrow.Column{col}, int64(values.Len()))
		defer table.Release()
		var sink bytes.Buffer
		opts = append(opts, pqarrow.WithAllocator(mem))
		err := pqarrow.WriteTable(table, &sink, 2, parquet.NewWriterProperties(parquet.WithAllocator(mem)), pqarrow.NewArrowWriterProperties(opts...))
		return sink.Bytes(), err
	}

	values, _, err := array.FromJSON(mem, typ, strings.NewReader("[[1, 2, 3], null, [4, 5, 6]]"))
	require.NoError(t, err)
	defer values.Release()
	for _, storeSchema := range []bool{false, true} {
		opts := []pqarrow.WriterOption{pqarrow.WithFiniteVectorLogicalType()}
		if storeSchema {
			opts = append(opts, pqarrow.WithStoreSchema())
		}
		encoded, err := write(values, opts...)
		require.NoError(t, err)
		reader, err := file.NewParquetReader(bytes.NewReader(encoded))
		require.NoError(t, err)
		node := reader.MetaData().Schema.Root().Field(0)
		logical, ok := node.LogicalType().(schema.FiniteVectorLogicalType)
		require.True(t, ok)
		require.Equal(t, int32(3), logical.NumElements())
		require.Equal(t, schema.ConvertedTypes.List, node.ConvertedType())
		require.NoError(t, reader.Close())

		result, err := pqarrow.ReadTable(ctx, bytes.NewReader(encoded), parquet.NewReaderProperties(mem), pqarrow.ArrowReadProperties{}, mem)
		require.NoError(t, err)
		defer result.Release()
		gotType := result.Schema().Field(0).Type.(*arrow.FixedSizeListType)
		require.Equal(t, int32(3), gotType.Len())
		require.Equal(t, storeSchema, gotType.ElemField().Nullable)
		got, err := array.Concatenate(result.Column(0).Data().Chunks(), mem)
		require.NoError(t, err)
		defer got.Release()
		want, _, err := array.FromJSON(mem, gotType, strings.NewReader("[[1, 2, 3], null, [4, 5, 6]]"))
		require.NoError(t, err)
		defer want.Release()
		require.True(t, array.Equal(want, got))
	}

	for _, tc := range []struct {
		name    string
		bad     float32
		valid   bool
		message string
	}{
		{"null", 0, false, "contains a null element"},
		{"NaN", float32(math.NaN()), true, "contains a non-finite element"},
		{"infinity", float32(math.Inf(1)), true, "contains a non-finite element"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
			defer b.Release()
			b.Append(true)
			items := b.ValueBuilder().(*array.Float32Builder)
			items.AppendValues([]float32{1, tc.bad, 3}, []bool{true, tc.valid, true})
			bad := b.NewArray()
			defer bad.Release()
			_, err := write(bad, pqarrow.WithFiniteVectorLogicalType())
			require.ErrorContains(t, err, tc.message)
			_, err = write(bad)
			require.NoError(t, err, "ordinary lists allow these values")
		})
	}
}

func TestFiniteVectorUnderNullStruct(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	vector := arrow.FixedSizeListOfField(3, arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32})
	typ := arrow.StructOf(
		arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "v", Type: vector},
	)
	field := arrow.Field{Name: "player", Type: typ, Nullable: true}
	values, _, err := array.FromJSON(mem, typ, strings.NewReader(`[{"score":1,"v":[1,2,3]},null,{"score":2,"v":[4,5,6]}]`))
	require.NoError(t, err)
	defer values.Release()
	col := arrow.NewColumnFromArr(field, values)
	defer col.Release()
	table := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Column{col}, 3)
	defer table.Release()

	var sink bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &sink, 3, parquet.NewWriterProperties(parquet.WithAllocator(mem)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithFiniteVectorLogicalType())))
	got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(sink.Bytes()),
		parquet.NewReaderProperties(mem), pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	defer got.Release()
	require.True(t, array.Equal(values, got.Column(0).Data().Chunk(0)))
}
