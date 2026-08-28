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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

// TestByteStreamSplitFLBANullsMultiPage covers a BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY
// column whose chunk spans several data pages and contains nulls. The record reader
// reuses one value buffer across pages, and DecodeSpaced previously left aliased slice
// headers in it, so values decoded from the second page onwards came back shifted.
func TestByteStreamSplitFLBANullsMultiPage(t *testing.T) {
	for _, width := range []int{4, 17} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			const nrows = 5000
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			dt := &arrow.FixedSizeBinaryType{ByteWidth: width}
			sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: dt, Nullable: true}}, nil)

			bldr := array.NewFixedSizeBinaryBuilder(mem, dt)
			defer bldr.Release()

			expected := make([][]byte, nrows)
			for i := range expected {
				if i%7 == 3 {
					bldr.AppendNull()
					continue
				}
				v := make([]byte, width)
				for j := range v {
					v[j] = byte(i*width + j)
				}
				bldr.Append(v)
				expected[i] = v
			}

			arr := bldr.NewArray()
			defer arr.Release()
			rec := array.NewRecordBatch(sc, []arrow.Array{arr}, nrows)
			defer rec.Release()

			var buf bytes.Buffer
			props := parquet.NewWriterProperties(
				parquet.WithAllocator(mem),
				parquet.WithEncoding(parquet.Encodings.ByteStreamSplit),
				parquet.WithDictionaryDefault(false),
				// small pages so the column chunk spans more than one data page
				parquet.WithDataPageSize(512),
				parquet.WithBatchSize(128),
			)
			w, err := pqarrow.NewFileWriter(sc, &buf, props, pqarrow.DefaultWriterProps())
			require.NoError(t, err)
			require.NoError(t, w.Write(rec))
			require.NoError(t, w.Close())

			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()

			fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 137}, mem)
			require.NoError(t, err)
			tbl, err := fr.ReadTable(context.Background())
			require.NoError(t, err)
			defer tbl.Release()

			require.EqualValues(t, nrows, tbl.NumRows())

			row := 0
			for _, chunk := range tbl.Column(0).Data().Chunks() {
				fsb := chunk.(*array.FixedSizeBinary)
				for i := 0; i < fsb.Len(); i++ {
					if expected[row] == nil {
						require.Truef(t, fsb.IsNull(i), "row %d should be null", row)
					} else {
						require.Falsef(t, fsb.IsNull(i), "row %d should be valid", row)
						require.Equalf(t, expected[row], fsb.Value(i), "row %d", row)
					}
					row++
				}
			}
			require.Equal(t, nrows, row)
		})
	}
}
