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
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func TestRecordReaderRowRanges(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed=%t/parallel=%t", indexed, parallel), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				}, nil)
				data := `[{"id":0,"values":[1,2,3]},{"id":1,"values":[]},{"id":2,"values":null},
					{"id":3,"values":[4]},{"id":4,"values":[5,6]},{"id":5,"values":null},
					{"id":6,"values":[7,null,8]},{"id":7,"values":[]},{"id":8,"values":[9]},
					{"id":9,"values":[10,11,12]},{"id":10,"values":[13]},{"id":11,"values":null}]`
				tbl, err := array.TableFromJSON(mem, schema, []string{data})
				require.NoError(t, err)
				defer tbl.Release()

				var buf bytes.Buffer
				props := parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithPageIndexEnabled(indexed),
					parquet.WithDataPageSize(1), parquet.WithBatchSize(1), parquet.WithDictionaryDefault(false))
				require.NoError(t, pqarrow.WriteTable(tbl, &buf, 3, props, pqarrow.DefaultWriterProps()))
				pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
				require.NoError(t, err)
				defer pf.Close()
				reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2, Parallel: parallel}, mem)
				require.NoError(t, err)

				ranges := []pqarrow.RowRange{{Start: 1, End: 2}, {Start: 3, End: 5}, {Start: 5, End: 8}}
				rr, err := reader.GetRecordReaderWithRowRanges(t.Context(), nil, []int{3, 1, 2}, ranges)
				require.NoError(t, err)
				defer rr.Release()
				ranges[0] = pqarrow.RowRange{Start: 0, End: 1}

				var rows []int64
				var sizes []int64
				for rr.Next() {
					batch := rr.RecordBatch()
					sizes = append(sizes, batch.NumRows())
					for index, id := range batch.Column(0).(*array.Int64).Int64Values() {
						rows = append(rows, id)
						require.Equal(t, tbl.Column(1).Data().Chunk(0).ValueStr(int(id)), batch.Column(1).ValueStr(index))
					}
				}
				require.NoError(t, rr.Err())
				require.Equal(t, []int64{10, 3, 4, 5, 6, 7}, rows)
				require.Equal(t, []int64{1, 2, 2, 1}, sizes)
				_, err = rr.Read()
				require.ErrorIs(t, err, io.EOF)

				require.ErrorIs(t, rr.SeekToRow(6), arrow.ErrIndex)
				require.ErrorIs(t, rr.SeekToRow(-1), arrow.ErrIndex)
				require.NoError(t, rr.SeekToRow(2))
				batch, err := rr.Read()
				require.NoError(t, err)
				require.Equal(t, []int64{4, 5}, batch.Column(0).(*array.Int64).Int64Values())
				require.True(t, rr.Next())
				require.Equal(t, []int64{6, 7}, rr.RecordBatch().Column(0).(*array.Int64).Int64Values())
				require.False(t, rr.Next())
				require.NoError(t, rr.Err())
			})
		}
	}
}

func TestRecordReaderRowRangeValidation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	tbl := makeWideInt32Table(mem, 2, 8)
	defer tbl.Release()
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 4, nil, pqarrow.DefaultWriterProps()))
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer pf.Close()
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2}, mem)
	require.NoError(t, err)

	for _, ranges := range [][]pqarrow.RowRange{
		{{Start: -1, End: 1}}, {{Start: 2, End: 2}}, {{Start: 3, End: 2}}, {{Start: 3, End: 5}},
		{{Start: 0, End: 3}, {Start: 2, End: 4}}, {{Start: 2, End: 4}, {Start: 0, End: 1}},
	} {
		_, err := reader.GetRecordReaderWithRowRanges(t.Context(), []int{0}, []int{1}, ranges)
		require.ErrorIs(t, err, arrow.ErrInvalid)
	}

	for _, test := range []struct {
		ranges []pqarrow.RowRange
		groups []int
		want   []int32
	}{
		{[]pqarrow.RowRange{}, []int{1}, nil},
		{nil, []int{1}, []int32{4, 5, 6, 7}},
		{[]pqarrow.RowRange{{Start: 0, End: 2}, {Start: 2, End: 4}}, []int{1}, []int32{4, 5, 6, 7}},
		{[]pqarrow.RowRange{{Start: 9, End: 11}}, []int{0, 0, 0}, []int32{1, 2}},
	} {
		rr, err := reader.GetRecordReaderWithRowRanges(t.Context(), []int{0}, test.groups, test.ranges)
		require.NoError(t, err)
		defer rr.Release()
		require.Equal(t, 1, rr.Schema().NumFields())
		var got []int32
		for rr.Next() {
			got = append(got, rr.RecordBatch().Column(0).(*array.Int32).Int32Values()...)
		}
		require.NoError(t, rr.Err())
		require.Equal(t, test.want, got)
		if len(test.want) > 0 {
			require.NoError(t, rr.SeekToRow(0))
			require.True(t, rr.Next())
			require.Equal(t, test.want[0], rr.RecordBatch().Column(0).(*array.Int32).Value(0))
		}
	}
}
