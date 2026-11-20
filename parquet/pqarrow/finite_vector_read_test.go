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

func TestReadFiniteVectorWithoutArrowSchema(t *testing.T) {
	for _, tc := range []struct {
		name    string
		defs    []int16
		reps    []int16
		values  []float32
		badSize bool
	}{
		{"fixed size and null vector", []int16{2, 2, 2, 0}, []int16{0, 1, 1, 0}, []float32{1, 2, 3}, false},
		{"wrong size", []int16{2, 2}, []int16{0, 1}, []float32{1, 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logical, err := schema.NewFiniteVectorLogicalType(3)
			require.NoError(t, err)
			element := schema.Must(schema.NewPrimitiveNodeLogical("element", parquet.Repetitions.Required,
				nil, parquet.Types.Float, -1, -1))
			list := schema.Must(schema.NewGroupNode("list", parquet.Repetitions.Repeated, schema.FieldList{element}, -1))
			vector := schema.Must(schema.NewGroupNodeLogical("v", parquet.Repetitions.Optional, schema.FieldList{list}, logical, -1))
			root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{vector}, -1))

			var sink bytes.Buffer
			writer := file.NewParquetWriter(&sink, root)
			group, err := writer.AppendRowGroupChecked()
			require.NoError(t, err)
			column, err := group.NextColumn()
			require.NoError(t, err)
			_, err = column.(*file.Float32ColumnChunkWriter).WriteBatch(tc.values, tc.defs, tc.reps)
			require.NoError(t, err)
			require.NoError(t, group.Close())
			require.NoError(t, writer.Close())

			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			got, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(sink.Bytes()), nil, pqarrow.ArrowReadProperties{}, mem)
			if tc.badSize {
				require.ErrorContains(t, err, "expected all lists to be of size=3")
				return
			}
			require.NoError(t, err)
			defer got.Release()
			wantType := arrow.FixedSizeListOfField(3, arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Float32})
			require.True(t, arrow.TypeEqual(wantType, got.Schema().Field(0).Type))
			want, _, err := array.FromJSON(mem, wantType, bytes.NewBufferString("[[1, 2, 3], null]"))
			require.NoError(t, err)
			defer want.Release()
			require.True(t, array.Equal(want, got.Column(0).Data().Chunk(0)))
		})
	}
}
