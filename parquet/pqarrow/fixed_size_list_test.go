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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

// TestWriteFixedSizeListHonorsOffset checks that the writer selects child
// values relative to the parent's offset, both for explicitly sliced arrays
// and for the implicit slices taken when a column spans several row groups.
func TestWriteFixedSizeListHonorsOffset(t *testing.T) {
	fsl := arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64)
	nested := arrow.FixedSizeListOf(2, fsl)

	tests := []struct {
		name       string
		dtype      arrow.DataType
		json       string
		sliceStart int64
		sliceEnd   int64
	}{
		{"flat", fsl, `[[10, 11], [20, 21], [30, 31], [40, 41]]`, 1, 3},
		{"null-elements", fsl, `[[10, 11], [null, 21], [30, null], [null, null], [50, 51]]`, 1, 4},
		{"nested", nested, `[[[1, 2], [3, 4]], [[5, 6], [7, null]], [[9, 10], [11, 12]], [[13, 14], [15, 16]]]`, 1, 4},
	}

	for _, tt := range tests {
		full, _, err := array.FromJSON(memory.DefaultAllocator, tt.dtype, strings.NewReader(tt.json))
		require.NoError(t, err)
		defer full.Release()
		sliced := array.NewSlice(full, tt.sliceStart, tt.sliceEnd)
		defer sliced.Release()

		for _, input := range []struct {
			name string
			arr  arrow.Array
		}{{"unsliced", full}, {"sliced", sliced}} {
			for _, rowGroupSize := range []int64{1, int64(input.arr.Len())} {
				t.Run(fmt.Sprintf("%s/%s/rowgroup-%d", tt.name, input.name, rowGroupSize), func(t *testing.T) {
					got := roundTripFixedSizeList(t, input.arr, rowGroupSize)
					defer got.Release()
					require.Truef(t, array.Equal(input.arr, got), "want %v, got %v", input.arr, got)
				})
			}
		}
	}
}

func roundTripFixedSizeList(t *testing.T, arr arrow.Array, rowGroupSize int64) arrow.Array {
	t.Helper()
	mem := memory.DefaultAllocator
	field := arrow.Field{Name: "value", Type: arr.DataType(), Nullable: true}
	sch := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumnFromArr(field, arr)
	defer col.Release()
	tbl := array.NewTable(sch, []arrow.Column{col}, int64(arr.Len()))
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, rowGroupSize, parquet.NewWriterProperties(),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())))

	out, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()),
		parquet.NewReaderProperties(mem), pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	defer out.Release()

	got, err := array.Concatenate(out.Column(0).Data().Chunks(), mem)
	require.NoError(t, err)
	return got
}
