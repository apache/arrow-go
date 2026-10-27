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
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// columnPages returns the NumValues of every data page of column col, one
// slice per row group, skipping dictionary pages.
func columnPages(t *testing.T, buf []byte, col int) [][]int64 {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(buf))
	require.NoError(t, err)
	defer rdr.Close()

	var out [][]int64
	for i := 0; i < rdr.NumRowGroups(); i++ {
		pr, err := rdr.RowGroup(i).GetColumnPageReader(col)
		require.NoError(t, err)
		pages := []int64{}
		for pr.Next() {
			switch p := pr.Page().(type) {
			case *file.DataPageV1:
				pages = append(pages, int64(p.NumValues()))
			case *file.DataPageV2:
				pages = append(pages, int64(p.NumValues()))
			}
		}
		require.NoError(t, pr.Err())
		out = append(out, pages)
	}
	return out
}

func cdcProps(opts parquet.CDCOptions) *parquet.WriterProperties {
	return parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithContentDefinedChunking(opts))
}

func int64Record(mem memory.Allocator, start, n int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	for i := start; i < start+n; i++ {
		bldr.Field(0).(*array.Int64Builder).Append(i)
	}
	return bldr.NewRecordBatch()
}

func writeRecords(t *testing.T, mem memory.Allocator, props *parquet.WriterProperties, arrprops pqarrow.ArrowWriterProperties, buffered bool, recs ...arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(recs[0].Schema(), &buf, props, arrprops)
	require.NoError(t, err)
	for _, rec := range recs {
		if buffered {
			require.NoError(t, fw.WriteBuffered(rec))
		} else {
			require.NoError(t, fw.Write(rec))
		}
	}
	require.NoError(t, fw.Close())
	return buf.Bytes()
}

func readCDCTable(t *testing.T, mem memory.Allocator, data []byte) arrow.Table {
	t.Helper()
	tbl, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	return tbl
}

func requireRoundTrip(t *testing.T, mem memory.Allocator, data []byte, rec arrow.RecordBatch) {
	t.Helper()
	tbl := readCDCTable(t, mem, data)
	defer tbl.Release()
	require.Equal(t, rec.NumRows(), tbl.NumRows())
	for i := 0; i < int(rec.NumCols()); i++ {
		expected := arrow.NewChunked(rec.Column(i).DataType(), []arrow.Array{rec.Column(i)})
		require.Truef(t, array.ChunkedEqual(expected, tbl.Column(i).Data()), "column %d differs", i)
		expected.Release()
	}
}

func TestCDCFileWriterCreatesChunkersOnOpen(t *testing.T) {
	var buf bytes.Buffer
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	props := parquet.NewWriterProperties(parquet.WithContentDefinedChunking(parquet.CDCOptions{MinChunkSize: 1024 * 1024, MaxChunkSize: 256 * 1024}))
	_, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.DefaultWriterProps())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max chunk size")
	assert.Equal(t, 0, buf.Len())
}

func TestCDCWriteReturnsChunkErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithCoerceTimestamps(arrow.Millisecond))
	write := func(props *parquet.WriterProperties) error {
		var buf bytes.Buffer
		fw, err := pqarrow.NewFileWriter(schema, &buf, props, arrprops)
		require.NoError(t, err)
		return fw.Write(rec)
	}

	errPlain := write(parquet.NewWriterProperties())
	require.Error(t, errPlain)
	var errCDC error
	require.NotPanics(t, func() {
		errCDC = write(parquet.NewWriterProperties(parquet.WithContentDefinedChunking(parquet.DefaultCDCOptions())))
	})
	require.Error(t, errCDC)
	require.EqualError(t, errCDC, errPlain.Error())
}

func TestCDCPagesWithinChunkSizes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := int64Record(mem, 0, 65536)
	defer rec.Release()
	data := writeRecords(t, mem, cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384}),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()), false, rec)

	pages := columnPages(t, data, 0)
	require.Len(t, pages, 1)
	require.Greater(t, len(pages[0]), 1)
	var total int64
	for i, n := range pages[0] {
		total += n
		if i < len(pages[0])-1 {
			assert.GreaterOrEqual(t, n, int64(512))
		}
		assert.LessOrEqual(t, n, int64(2048))
	}
	assert.Equal(t, int64(65536), total)
	requireRoundTrip(t, mem, data, rec)
}

func TestCDCLastChunkDoesntCutPage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	props := cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384})
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	a, b, all := int64Record(mem, 0, 8000), int64Record(mem, 8000, 8000), int64Record(mem, 0, 16000)
	defer a.Release()
	defer b.Release()
	defer all.Release()

	buffered := writeRecords(t, mem, props, arrprops, true, a, b)
	single := writeRecords(t, mem, props, arrprops, false, all)
	singlePages := columnPages(t, single, 0)
	require.Len(t, singlePages, 1)
	require.Greater(t, len(singlePages[0]), 1)
	assert.Equal(t, singlePages, columnPages(t, buffered, 0))
}

func TestCDCLowLevelWritersIgnoreCDC(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const n = 64 * 1024
	props := cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384})
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("a", parquet.Repetitions.Required, -1),
	}, -1)))

	vals := make([]int32, n)
	for i := range vals {
		vals[i] = int32(i)
	}
	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(vals, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, sc.Root(), file.WithWriterProps(props))
	rgw, err := w.AppendRowGroupChecked()
	require.NoError(t, err)
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())

	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	rgw, err = w.AppendRowGroupChecked()
	require.NoError(t, err)
	cw, err = rgw.NextColumn()
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteArrowToColumn(pqarrow.NewArrowWriteContext(context.Background(), &arrprops), cw, arr, nil, nil, false))
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	pages := columnPages(t, buf.Bytes(), 0)
	require.Len(t, pages, 2)
	assert.Equal(t, []int64{n}, pages[0])
	assert.Equal(t, []int64{n}, pages[1])
}

func TestCDCFixedSizeListNullSlots(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const rows = 16 * 1024
	schema := arrow.NewSchema([]arrow.Field{{Name: "l", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	lb := bldr.Field(0).(*array.FixedSizeListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)
	for i := 0; i < rows; i++ {
		if i%10 == 0 {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for j := 0; j < 4; j++ {
			vb.Append(int32(i*4 + j))
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	data := writeRecords(t, mem, cdcProps(parquet.CDCOptions{MinChunkSize: 1024, MaxChunkSize: 4096}),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema()), false, rec)
	pages := columnPages(t, data, 0)
	require.Len(t, pages, 1)
	assert.Greater(t, len(pages[0]), 1)
	requireRoundTrip(t, mem, data, rec)
}

func TestCDCEmptyAndAllNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	props := cdcProps(parquet.CDCOptions{MinChunkSize: 1024, MaxChunkSize: 4096})
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.Null, Nullable: true},
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	empty := array.NewRecordBuilder(mem, schema)
	defer empty.Release()
	emptyRec := empty.NewRecordBatch()
	defer emptyRec.Release()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(schema, &buf, props, arrprops)
	require.NoError(t, err)
	require.NoError(t, fw.Write(emptyRec))
	require.NoError(t, fw.Close())

	const rows = 10 * 1024
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	for i := 0; i < rows; i++ {
		bldr.Field(0).AppendNull()
		bldr.Field(1).AppendNull()
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	data := writeRecords(t, mem, props, arrprops, false, rec)
	requireRoundTrip(t, mem, data, rec)
	for col := 0; col < 2; col++ {
		for _, rg := range columnPages(t, data, col) {
			require.Greater(t, len(rg), 1)
			for _, n := range rg {
				assert.NotZero(t, n)
			}
		}
	}
}

func TestCDCGenerateArray(t *testing.T) {
	t.Run("Integer", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		arr := generateArray(t, mem, arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, 10, 0)
		defer arr.Release()
		require.Equal(t, 10, arr.Len())
		assert.True(t, arrow.TypeEqual(arr.DataType(), arrow.PrimitiveTypes.Int32))
		assert.Equal(t, 1, arr.NullN())
	})

	t.Run("ListOfInteger", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		field := arrow.Field{Name: "a", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}
		arr := generateArray(t, mem, field, 12, 0)
		defer arr.Release()
		require.Equal(t, 12, arr.Len())

		list := arr.(*array.List)
		for i := 0; i < 12; i += 4 {
			// null, empty list, list of 1 element, list of 3 elements
			assert.True(t, list.IsNull(i))
			assert.True(t, list.IsValid(i+1))
			assert.Equal(t, 0, int(list.Offsets()[i+2]-list.Offsets()[i+1]))
			assert.True(t, list.IsValid(i+2))
			assert.Equal(t, 1, int(list.Offsets()[i+3]-list.Offsets()[i+2]))
			assert.True(t, list.IsValid(i+3))
			assert.Equal(t, 3, int(list.Offsets()[i+4]-list.Offsets()[i+3]))
		}

		_, err := tryGenerateArray(mem, field, 3, 0)
		assert.Error(t, err)
		ok, err := tryGenerateArray(mem, field, 8, 0)
		require.NoError(t, err)
		ok.Release()
	})

	t.Run("MapOfInteger", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		field := arrow.Field{Name: "a", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: true}
		arr := generateArray(t, mem, field, 8, 0)
		defer arr.Release()
		m := arr.(*array.Map)
		require.Equal(t, 8, m.Len())
		// null, empty map, map of 1 entry, map of 3 entries, with unique non-null keys
		for i, entries := range []int{-1, 0, 1, 3, -1, 0, 1, 3} {
			assert.Equal(t, entries < 0, m.IsNull(i))
			if entries >= 0 {
				assert.Equal(t, entries, int(m.Offsets()[i+1]-m.Offsets()[i]))
			}
		}
		assert.Zero(t, m.Keys().NullN())
		keys := map[string]bool{}
		for i := 0; i < m.Keys().Len(); i++ {
			keys[m.Keys().ValueStr(i)] = true
		}
		assert.Len(t, keys, m.Keys().Len())
	})

	t.Run("Dictionary", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		field := arrow.Field{Name: "a", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}, Nullable: true}
		first := generateArray(t, mem, field, 100, 0).(*array.Dictionary)
		defer first.Release()
		second := generateArray(t, mem, field, 100, 1).(*array.Dictionary)
		defer second.Release()
		require.Equal(t, 100, first.Len())
		assert.True(t, arrow.TypeEqual(first.DataType(), field.Type))
		assert.Greater(t, first.NullN(), 0)

		// the arrays share the dictionary regardless of the seed, and its values are
		// unique so the column writers can write it directly
		assert.True(t, array.Equal(first.Dictionary(), second.Dictionary()))
		assert.False(t, array.Equal(first.Indices(), second.Indices()))
		unique := map[string]bool{}
		dict := first.Dictionary().(*array.String)
		for i := 0; i < dict.Len(); i++ {
			unique[dict.Value(i)] = true
		}
		assert.Len(t, unique, dict.Len())
	})

	t.Run("Unsupported", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		_, err := tryGenerateArray(mem, arrow.Field{Name: "a", Type: arrow.FixedWidthTypes.Duration_ns, Nullable: true}, 10, 0)
		assert.Error(t, err)
	})
}

func TestCDCPagesFollowContent(t *testing.T) {
	type pages = []int64
	rgs := func(p ...pages) [][]int64 { return p }

	t.Run("Identical", func(t *testing.T) {
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 200, 300}), nil))
		// without an edit the pages must end at the same positions
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 210, 290}), nil),
			"page ends only in the original: 300; page ends only in the modified: 310")
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 200, 100, 200}), nil),
			"page ends only in the modified: 400")
	})

	t.Run("Insertion", func(t *testing.T) {
		// inserting 10 levels at 150 moves the page ends after it back by 10, so 260 and
		// 610 are measured as 250 and 600, and the pages end at 600 again
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 160, 350, 400}),
			[]cdcEdit{{Offset: 150, Inserted: 10}}))
		// the page ends within the inserted levels have no position in the original
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 60, 290, 250, 400}),
			[]cdcEdit{{Offset: 150, Inserted: 100}}))
		// a page end at the offset may differ, since the chunker decides to end a page at
		// 100 after rolling the level at 100
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{115, 195, 300}),
			[]cdcEdit{{Offset: 100, Inserted: 10}}))
		// and both files ending a page at the offset doesn't mean that they end the same again
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 15, 195, 300}),
			[]cdcEdit{{Offset: 100, Inserted: 10}}))
	})

	t.Run("Deletion", func(t *testing.T) {
		// deleting 5 levels at 150 moves the page ends after it forward by 5
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 195, 300, 400}),
			[]cdcEdit{{Offset: 150, Deleted: 5}}))
		// the page ends within the deleted levels have no position in the modified data
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 200, 400}),
			[]cdcEdit{{Offset: 150, Deleted: 300}}))
		// so they don't need to end at the same position again after deleting up to the end
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 50}),
			[]cdcEdit{{Offset: 150, Deleted: 450}}))
	})

	t.Run("Update", func(t *testing.T) {
		// replacing 300 levels at 100 doesn't move the page ends, but they may differ
		// until the first page end after the replaced levels, even if both files end a
		// page at 130
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 30, 170, 300, 400}), rgs(pages{100, 30, 150, 320, 400}),
			[]cdcEdit{{Offset: 100, Deleted: 300, Inserted: 300}}))
	})

	t.Run("RowGroupEnds", func(t *testing.T) {
		// the row group ends cut the pages regardless of the content
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 50}, pages{150, 300}), nil))
		// but the next page must still end at 300, unlike after resetting the chunking
		// state at the start of the second row group
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300}), rgs(pages{100, 50}, pages{170, 280}), nil),
			"page ends only in the original: 300; page ends only in the modified: 320")
		// the row groups have a fixed length, so after inserting 10 levels at 150 the
		// first row group of the modified data ends at 590 of the original data
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300}, pages{400}), rgs(pages{100, 210, 290}, pages{410}),
			[]cdcEdit{{Offset: 150, Inserted: 10}}))
	})

	t.Run("PagesChangedAwayFromTheEdits", func(t *testing.T) {
		// before the edit at 150 the pages end at 90 instead of 100
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{90, 220, 300, 400}),
			[]cdcEdit{{Offset: 150, Inserted: 10}}),
			"page ends only in the original: 100; page ends only in the modified: 90")
		// after they ended at 300 again, they end at 580 instead of 600
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 210, 280, 420}),
			[]cdcEdit{{Offset: 150, Inserted: 10}}),
			"page ends only in the original: 600; page ends only in the modified: 580")
	})

	t.Run("PagesNotEndingTheSameAgain", func(t *testing.T) {
		// after the edit at 150 the pages never end at the same position again
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 205, 310, 395}),
			[]cdcEdit{{Offset: 150, Inserted: 10}}),
			"the pages never end at the same position again after the edits at: 150")
		// which must happen before the next edit, at 900 after the update at 150
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400, 500}), rgs(pages{100, 210, 300, 390, 500}),
			[]cdcEdit{{Offset: 150, Deleted: 10, Inserted: 10}, {Offset: 900, Deleted: 10, Inserted: 10}}),
			"the pages never end at the same position again after the edits at: 150")
		// or before the end, after the update at 650
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400, 500}), rgs(pages{100, 200, 300, 390, 510}),
			[]cdcEdit{{Offset: 150, Deleted: 10, Inserted: 10}, {Offset: 650, Deleted: 10, Inserted: 10}}),
			"the pages never end at the same position again after the edits at: 650")
		// unless the original doesn't end a page after the edit, like after deleting 5
		// levels from the last page or appending levels
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 200, 300, 200, 195}),
			[]cdcEdit{{Offset: 650, Deleted: 5}}))
		assert.NoError(t, pagesFollowContent(rgs(pages{100, 200}), rgs(pages{100, 200, 50}),
			[]cdcEdit{{Offset: 300, Inserted: 50}}))
	})

	t.Run("InvalidEdits", func(t *testing.T) {
		// the edits must be in order without overlapping, a deletion is followed by an
		// edit at its end at the earliest
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 200, 300, 400}),
			[]cdcEdit{{Offset: 650}, {Offset: 150}}),
			"the edits must be in order without overlapping")
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 200, 300, 400}),
			[]cdcEdit{{Offset: 150, Deleted: 50}, {Offset: 150, Inserted: 50}}),
			"the edits must be in order without overlapping")
		// and change the number of levels as much as the files differ
		assert.EqualError(t, pagesFollowContent(rgs(pages{100, 200, 300, 400}), rgs(pages{100, 160, 350, 400}),
			[]cdcEdit{{Offset: 150, Inserted: 20}}),
			"the edits change the number of levels by 20 instead of 10")
	})

	t.Run("WithoutContentDefinedChunking", func(t *testing.T) {
		// The pages follow the content only with content defined chunking: without it
		// the writer cuts them by their size, so after inserting values the pages never
		// end at the same positions again
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		const partLength, editLength = 32 * 1024, 100
		schema := arrow.NewSchema([]arrow.Field{{Name: "f0", Type: arrow.PrimitiveTypes.Int32}}, nil)
		part1 := generateTable(t, mem, schema, partLength, 0)
		defer part1.Release()
		part2 := generateTable(t, mem, schema, editLength, partLength)
		defer part2.Release()
		part3 := generateTable(t, mem, schema, partLength, 2*partLength)
		defer part3.Release()
		before := concatTables(t, mem, part1, part3)
		defer before.Release()
		after := concatTables(t, mem, part1, part2, part3)
		defer after.Release()
		edits := []cdcEdit{{Offset: partLength, Inserted: editLength}}

		beforeParquet := writeCDCTable(t, mem, before, 1024*1024, false, parquet.DataPageV1)
		afterParquet := writeCDCTable(t, mem, after, 1024*1024, false, parquet.DataPageV1)
		assert.NoError(t, pagesFollowContent(columnPages(t, beforeParquet, 0), columnPages(t, afterParquet, 0), edits))

		writeByPageSize := func(tbl arrow.Table) []byte {
			var buf bytes.Buffer
			props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(4*1024))
			require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))
			return buf.Bytes()
		}
		assert.EqualError(t, pagesFollowContent(columnPages(t, writeByPageSize(before), 0), columnPages(t, writeByPageSize(after), 0), edits),
			"the pages never end at the same position again after the edits at: 32768")
	})
}

func TestCDCWriteTableHelpers(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const minSize, maxSize = 4 * 1024, 16 * 1024
	for _, nullable := range []bool{false, true} {
		schema := arrow.NewSchema([]arrow.Field{{Name: "f0", Type: arrow.PrimitiveTypes.Int32, Nullable: nullable}}, nil)
		tbl := generateTable(t, mem, schema, 64*1024, 0)
		data := writeCDCTable(t, mem, tbl, 1024*1024, false, parquet.DataPageV2)
		pages := columnPages(t, data, 0)
		require.Len(t, pages, 1)
		assertPageSizes(t, tbl.Column(0).Data(), pages[0], nullable, minSize, maxSize)
		tbl.Release()
	}
}

// cdcHash generates deterministic and platform-independent data, the seeds select
// sequences far from each other so the data generated with different seeds is
// independent.
func cdcHash(seed, index uint64) uint64 {
	h := (index + seed*0x9e3779b97f4a7c15) * 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// tryGenerateArray generates length deterministic values of the field's type from
// the seed, about a tenth of them null if the field is nullable. It returns an error
// for the unsupported types and for the invalid lengths of lists.
func tryGenerateArray(mem memory.Allocator, field arrow.Field, length int64, seed uint64) (arrow.Array, error) {
	switch typ := field.Type.(type) {
	case *arrow.StructType:
		children := make([]arrow.ArrayData, typ.NumFields())
		for i := range children {
			child, err := tryGenerateArray(mem, typ.Field(i), length, seed+uint64(i)*10)
			if err != nil {
				return nil, err
			}
			defer child.Release()
			children[i] = child.Data()
		}
		data := array.NewData(typ, int(length), []*memory.Buffer{nil}, children, 0, 0)
		defer data.Release()
		return array.NewStructData(data), nil

	case *arrow.ListType, *arrow.MapType:
		// repeat the same pattern in the list or map array: null, empty, 1 element,
		// 3 elements. The entries of a map are structs generated like any other,
		// so their keys are never null.
		if length%4 != 0 {
			return nil, fmt.Errorf("length must be divisible by 4 when generating list and map arrays, but got: %d", length)
		}
		var elem arrow.DataType
		switch typ := typ.(type) {
		case *arrow.ListType:
			elem = typ.Elem()
		case *arrow.MapType:
			elem = typ.Elem()
		}
		values, err := tryGenerateArray(mem, arrow.Field{Name: "item", Type: elem, Nullable: true}, length*4, seed)
		if err != nil {
			return nil, err
		}
		defer values.Release()

		offsets := make([]int32, 1, length+1)
		validity := make([]byte, bitutil.BytesForBits(length))
		nulls := 0
		for offset := int32(0); offset < int32(length); offset += 4 {
			// a null, or an empty list if the field isn't nullable
			if field.Nullable {
				nulls++
			} else {
				bitutil.SetBit(validity, len(offsets)-1)
			}
			offsets = append(offsets, offset)
			// an empty list, a list of 1 element, a list of 3 elements
			bitutil.SetBit(validity, len(offsets)-1)
			offsets = append(offsets, offset)
			bitutil.SetBit(validity, len(offsets)-1)
			offsets = append(offsets, offset+1)
			bitutil.SetBit(validity, len(offsets)-1)
			offsets = append(offsets, offset+4)
		}
		data := array.NewData(typ, int(length),
			[]*memory.Buffer{memory.NewBufferBytes(validity), memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))},
			[]arrow.ArrayData{values.Data()}, nulls, 0)
		defer data.Release()
		return array.MakeFromData(data), nil

	case *arrow.DictionaryType:
		// the dictionary doesn't depend on the seed, so the generated arrays share it
		// and can be concatenated without unifying their dictionaries, only the
		// indices are generated from the seed
		const dictionaryLength = 100
		dictionary, err := tryGenerateArray(mem, arrow.Field{Name: "dictionary", Type: typ.ValueType}, dictionaryLength, 0)
		if err != nil {
			return nil, err
		}
		defer dictionary.Release()
		ib := array.NewBuilder(mem, typ.IndexType)
		defer ib.Release()
		for i := int64(0); i < length; i++ {
			val := cdcHash(seed, uint64(i))
			if field.Nullable && val%10 == 0 {
				ib.AppendNull()
				continue
			}
			switch b := ib.(type) {
			case *array.Int8Builder:
				b.Append(int8(val % dictionaryLength))
			case *array.Int16Builder:
				b.Append(int16(val % dictionaryLength))
			case *array.Int32Builder:
				b.Append(int32(val % dictionaryLength))
			case *array.Int64Builder:
				b.Append(int64(val % dictionaryLength))
			default:
				return nil, fmt.Errorf("unsupported dictionary index type %s", typ.IndexType)
			}
		}
		indices := ib.NewArray()
		defer indices.Release()
		return array.NewDictionaryArray(typ, indices, dictionary), nil

	case arrow.ExtensionType:
		storage, err := tryGenerateArray(mem, arrow.Field{Name: "storage", Type: typ.StorageType(), Nullable: true}, length, seed)
		if err != nil {
			return nil, err
		}
		defer storage.Release()
		return array.NewExtensionArrayWithStorage(typ, storage), nil
	}

	bldr := array.NewBuilder(mem, field.Type)
	defer bldr.Release()
	for i := int64(0); i < length; i++ {
		val := cdcHash(seed, uint64(i))
		if field.Nullable && val%10 == 0 {
			bldr.AppendNull()
			continue
		}
		switch b := bldr.(type) {
		case *array.BooleanBuilder:
			b.Append(val%2 == 0)
		case *array.Int8Builder:
			b.Append(int8(val))
		case *array.Int16Builder:
			b.Append(int16(val))
		case *array.Int32Builder:
			b.Append(int32(val))
		case *array.Int64Builder:
			b.Append(int64(val))
		case *array.Uint8Builder:
			b.Append(uint8(val))
		case *array.Uint16Builder:
			b.Append(uint16(val))
		case *array.Uint32Builder:
			b.Append(uint32(val))
		case *array.Uint64Builder:
			b.Append(val)
		case *array.Float16Builder:
			b.Append(float16.FromBits(uint16(val % 1000)))
		case *array.Float32Builder:
			b.Append(float32(val%1000) / 1000.0)
		case *array.Float64Builder:
			b.Append(float64(val%100000) / 1000.0)
		case *array.Decimal128Builder:
			// limit the value to fit within the specified precision
			dt := field.Type.(*arrow.Decimal128Type)
			b.Append(decimal128.FromU64(val % uint64(math.Pow10(int(dt.Precision-dt.Scale))-1)))
		case *array.Decimal256Builder:
			// limit the value to fit within the specified precision, capped at 9 to
			// avoid int64 overflow
			dt := field.Type.(*arrow.Decimal256Type)
			b.Append(decimal256.FromU64(val % uint64(math.Pow10(min(9, int(dt.Precision-dt.Scale)))-1)))
		case *array.Date32Builder:
			b.Append(arrow.Date32(int32(val)))
		case *array.Time32Builder:
			b.Append(arrow.Time32(max(int32(val)%86400000, -(int32(val) % 86400000))))
		case *array.Time64Builder:
			b.Append(arrow.Time64(max(int64(val)%86400000000, -(int64(val) % 86400000000))))
		case *array.TimestampBuilder:
			b.Append(arrow.Timestamp(val))
		case *array.StringBuilder:
			b.Append("str_" + strconv.FormatUint(val, 10))
		case *array.LargeStringBuilder:
			b.Append("str_" + strconv.FormatUint(val, 10))
		case *array.BinaryBuilder:
			b.Append([]byte("bin_" + strconv.FormatUint(val, 10)))
		case *array.FixedSizeBinaryBuilder:
			digits := strconv.FormatUint(val, 10)
			width := field.Type.(*arrow.FixedSizeBinaryType).ByteWidth
			if len(digits) < width-4 {
				return nil, fmt.Errorf("fixed size binary of width %d is too wide for %q", width, digits)
			}
			b.Append([]byte("bin_" + digits[:width-4]))
		default:
			return nil, fmt.Errorf("unsupported data type %s", field.Type)
		}
	}
	return bldr.NewArray(), nil
}

// generateArray is tryGenerateArray failing the test on errors.
func generateArray(t *testing.T, mem memory.Allocator, field arrow.Field, length int64, seed uint64) arrow.Array {
	t.Helper()
	arr, err := tryGenerateArray(mem, field, length, seed)
	require.NoError(t, err)
	if err := array.ValidateFull(arr); err != nil {
		arr.Release()
		require.NoError(t, err)
	}
	return arr
}

// generateTable generates a column per field of the schema, each from its own seed.
func generateTable(t *testing.T, mem memory.Allocator, schema *arrow.Schema, length int64, seed uint64) arrow.Table {
	t.Helper()
	cols := make([]arrow.Column, schema.NumFields())
	for i, field := range schema.Fields() {
		seed++
		arr := generateArray(t, mem, field, length, seed)
		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		arr.Release()
		cols[i] = *arrow.NewColumn(field, chunked)
		chunked.Release()
	}
	tbl := array.NewTable(schema, cols, length)
	for i := range cols {
		cols[i].Release()
	}
	return tbl
}

// concatTables concatenates the tables into a table of a single chunk per column, so
// it doesn't carry information about the modification points.
func concatTables(t *testing.T, mem memory.Allocator, tables ...arrow.Table) arrow.Table {
	t.Helper()
	schema := tables[0].Schema()
	cols := make([]arrow.Column, schema.NumFields())
	var rows int64
	for _, tbl := range tables {
		rows += tbl.NumRows()
	}
	for i, field := range schema.Fields() {
		var chunks []arrow.Array
		for _, tbl := range tables {
			chunks = append(chunks, tbl.Column(i).Data().Chunks()...)
		}
		combined, err := array.Concatenate(chunks, mem)
		require.NoError(t, err)
		chunked := arrow.NewChunked(field.Type, []arrow.Array{combined})
		combined.Release()
		cols[i] = *arrow.NewColumn(field, chunked)
		chunked.Release()
	}
	tbl := array.NewTable(schema, cols, rows)
	for i := range cols {
		cols[i].Release()
	}
	return tbl
}

// cdcTableProps returns the writer properties of the content defined chunking tests,
// with chunk sizes small enough to cut many pages out of a small table.
func cdcTableProps(t *testing.T, mem memory.Allocator, schema *arrow.Schema, rowGroupLength int64, dictionary bool, version parquet.DataPageVersion) (*parquet.WriterProperties, pqarrow.ArrowWriterProperties) {
	t.Helper()
	opts := []parquet.WriterProperty{
		parquet.WithContentDefinedChunking(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384}),
		parquet.WithMaxRowGroupLength(rowGroupLength),
		parquet.WithDictionaryDefault(dictionary),
		parquet.WithDataPageVersion(version),
		parquet.WithAllocator(mem),
	}
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	if dictionary {
		// keep the dictionaries regardless of their size, so the pages follow them
		sc, err := pqarrow.ToParquet(schema, parquet.NewWriterProperties(opts...), arrprops)
		require.NoError(t, err)
		for i := 0; i < sc.NumColumns(); i++ {
			opts = append(opts, parquet.WithDictionaryCostFallbackFor(sc.Column(i).Path(), false))
		}
	}
	return parquet.NewWriterProperties(opts...), arrprops
}

// containsDictionary returns whether the type is a dictionary or a list of one.
func containsDictionary(dt arrow.DataType) bool {
	switch typ := dt.(type) {
	case *arrow.DictionaryType:
		return true
	case *arrow.ListType:
		return containsDictionary(typ.Elem())
	}
	return false
}

// writeCDCTable writes the table with content defined chunking and returns the file
// after checking that its content reads back equal to the table.
func writeCDCTable(t *testing.T, mem memory.Allocator, tbl arrow.Table, rowGroupLength int64, dictionary bool, version parquet.DataPageVersion) []byte {
	t.Helper()
	props, arrprops := cdcTableProps(t, mem, tbl.Schema(), rowGroupLength, dictionary, version)
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, rowGroupLength, props, arrprops))

	readback := readCDCTable(t, mem, buf.Bytes())
	defer readback.Release()
	require.Equal(t, tbl.NumRows(), readback.NumRows())
	for i := 0; i < int(tbl.NumCols()); i++ {
		expected, actual := tbl.Column(i).Data(), readback.Column(i).Data()
		if containsDictionary(expected.DataType()) {
			// dictionary arrays are read back with other dictionaries, or as dense
			// arrays if the writer didn't write one, so compare their values
			values := func(c *arrow.Chunked) (out []string) {
				for _, chunk := range c.Chunks() {
					for row := 0; row < chunk.Len(); row++ {
						out = append(out, chunk.ValueStr(row))
					}
				}
				return out
			}
			require.Equalf(t, values(expected), values(actual), "column %d differs", i)
			continue
		}
		require.Truef(t, array.ChunkedEqual(expected, actual), "column %d differs", i)
	}
	return buf.Bytes()
}

// cdcEdit is an edit of the original data: deleting levels from an offset and
// inserting levels there, replacing the deleted levels if both are set.
type cdcEdit struct {
	Offset, Deleted, Inserted int64
}

// pagesFollowContent checks whether the pages of a column follow its content,
// returning nil if they do and the reason otherwise. original and modified hold the
// number of levels of each data page, per row group. Measuring the modified data in
// the original data, where the positions after an edit move by the number of levels
// it deleted minus the ones it inserted, both files must end their pages at the same
// positions, except
//
//   - at the ends of the row groups, which cut the pages regardless of the content, and
//   - from the offset of each edit up to the first position after the edited levels
//     where both files end a page again, which must come before the next edit unless
//     the original ends no page in between.
//
// A page end at the offset of an edit may differ too, since the chunker decides to end
// a page before a level after rolling that level. The edits must be in order without
// overlapping, and far enough apart for the pages to end at the same positions again.
//
// For example inserting 10 levels at offset 150 into a row group with pages of 100,
// 200, 300 and 400 levels, the pages end at:
//
//	original:               100       300       600       1000
//	modified:               100    260       610       1010
//	measured in original:   100    250       600       1000
//
// so they end at 300 and at 250 after the edit at 150 until both end a page at 600
// again, while 1000 is the end of the row group. A page end differing before an edit
// or after the pages ended at the same position again, or the pages never ending at
// the same position again after an edit, means that the pages don't follow the
// content.
func pagesFollowContent(original, modified [][]int64, edits []cdcEdit) error {
	// the page and row group ends of both files measured in the original data,
	// skipping the ends within the inserted levels which have no position there
	originalEnds, modifiedEnds, rowGroupEnds := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
	insertMeasured := func(ends map[int64]bool, position int64) {
		var shift int64
		for _, edit := range edits {
			offset := edit.Offset + shift
			if position <= offset {
				break
			}
			if position <= offset+edit.Inserted {
				return
			}
			shift += edit.Inserted - edit.Deleted
		}
		ends[position-shift] = true
	}
	var originalLevels, modifiedLevels int64
	for _, pages := range original {
		for _, length := range pages {
			originalLevels += length
			originalEnds[originalLevels] = true
		}
		rowGroupEnds[originalLevels] = true
	}
	for _, pages := range modified {
		for _, length := range pages {
			modifiedLevels += length
			insertMeasured(modifiedEnds, modifiedLevels)
		}
		insertMeasured(rowGroupEnds, modifiedLevels)
	}

	// the edits must be in order without overlapping and add up to the difference of
	// the files
	var editedLevels, editsEnd int64
	for _, edit := range edits {
		if edit.Offset < editsEnd || edit.Deleted < 0 || edit.Inserted < 0 {
			return errors.New("the edits must be in order without overlapping")
		}
		editsEnd = edit.Offset + edit.Deleted
		editedLevels += edit.Inserted - edit.Deleted
	}
	if editedLevels != modifiedLevels-originalLevels {
		return fmt.Errorf("the edits change the number of levels by %d instead of %d", editedLevels, modifiedLevels-originalLevels)
	}

	// walk the page ends in order
	pageEnds := slices.Sorted(maps.Keys(originalEnds))
	pageEnds = slices.AppendSeq(pageEnds, maps.Keys(modifiedEnds))
	slices.Sort(pageEnds)
	pageEnds = slices.Compact(pageEnds)

	var onlyOriginal, onlyModified, divergingEdits []int64
	nextEdit := 0
	// the edit whose page ends may still differ, and whether the original ends a page
	// after the levels it deleted
	var openEdit *cdcEdit
	originalEndsAfterEdit := false
	closeEdit := func() {
		if openEdit != nil && originalEndsAfterEdit {
			divergingEdits = append(divergingEdits, openEdit.Offset)
		}
	}
	for _, pageEnd := range pageEnds {
		if rowGroupEnds[pageEnd] {
			continue
		}
		for ; nextEdit < len(edits) && edits[nextEdit].Offset <= pageEnd; nextEdit++ {
			closeEdit()
			openEdit = &edits[nextEdit]
			originalEndsAfterEdit = false
		}
		if openEdit != nil && pageEnd <= openEdit.Offset+openEdit.Deleted {
			continue
		}
		inOriginal, inModified := originalEnds[pageEnd], modifiedEnds[pageEnd]
		switch {
		case inOriginal && inModified:
			openEdit = nil
		case openEdit != nil:
			originalEndsAfterEdit = originalEndsAfterEdit || inOriginal
		case inOriginal:
			onlyOriginal = append(onlyOriginal, pageEnd)
		default:
			onlyModified = append(onlyModified, pageEnd)
		}
	}
	closeEdit()

	var reasons []string
	for _, reason := range []struct {
		what      string
		positions []int64
	}{
		{"page ends only in the original", onlyOriginal},
		{"page ends only in the modified", onlyModified},
		{"the pages never end at the same position again after the edits at", divergingEdits},
	} {
		if len(reason.positions) > 0 {
			text := reason.what + ":"
			for _, position := range reason.positions {
				text += fmt.Sprintf(" %d", position)
			}
			reasons = append(reasons, text)
		}
	}
	if len(reasons) == 0 {
		return nil
	}
	return errors.New(strings.Join(reasons, "; "))
}

// assertPageSizes checks that the pages of the fixed width and binary columns are
// within the chunk size range, where page holds the number of levels of each page.
func assertPageSizes(t *testing.T, column *arrow.Chunked, pages []int64, nullable bool, minSize, maxSize int64) {
	t.Helper()
	switch column.DataType().(type) {
	case *arrow.BooleanType, arrow.FixedWidthDataType,
		*arrow.StringType, *arrow.LargeStringType, *arrow.BinaryType, *arrow.LargeBinaryType:
	default:
		// the size of the other types isn't tracked
		return
	}
	// the size the CDC chunker counts for the array elements belonging to a page
	cdcSize := func(arr arrow.Array) int64 {
		var size int64
		switch typ := arr.DataType().(type) {
		case *arrow.BooleanType:
			// the CDC chunker increments the chunk size by 1 for each boolean element
			size = int64(arr.Len() - arr.NullN())
		case arrow.FixedWidthDataType:
			size = int64(arr.Len()-arr.NullN()) * int64(typ.BitWidth()/8)
		default:
			switch a := arr.(type) {
			case *array.String:
				size = int64(len(a.ValueBytes()))
			case *array.LargeString:
				size = int64(len(a.ValueBytes()))
			case *array.Binary:
				size = int64(len(a.ValueBytes()))
			case *array.LargeBinary:
				size = int64(len(a.ValueBytes()))
			default:
				require.Failf(t, "unsupported type", "CDC size calculation for type %s is not implemented", arr.DataType())
			}
		}
		if nullable {
			// the size is calculated from the def levels and the valid values
			return size + int64(arr.Len())*2
		}
		// for non-nullable types the size is calculated purely from the values
		return size
	}

	require.NotEmpty(t, pages)
	var offset int64
	for i, length := range pages {
		// the chunking is applied on the logical values before any parquet encoding,
		// so slice the column to get the logical values of the page
		slice := array.NewChunkedSlice(column, offset, offset+length)
		var size int64
		for _, chunk := range slice.Chunks() {
			size += cdcSize(chunk)
		}
		slice.Release()
		offset += length

		assert.LessOrEqual(t, size, maxSize)
		// the min chunk size is not guaranteed for the last page
		if i < len(pages)-1 {
			assert.GreaterOrEqual(t, size, minSize)
		}
	}
	// the sum of the page lengths should be equal to the length of the column
	assert.Equal(t, int64(column.Len()), offset)
}

func TestCDCWriteTableAllTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	types := map[string]arrow.DataType{
		"bool": arrow.FixedWidthTypes.Boolean, "int8": arrow.PrimitiveTypes.Int8, "int16": arrow.PrimitiveTypes.Int16,
		"int32": arrow.PrimitiveTypes.Int32, "int64": arrow.PrimitiveTypes.Int64, "uint8": arrow.PrimitiveTypes.Uint8,
		"uint16": arrow.PrimitiveTypes.Uint16, "uint32": arrow.PrimitiveTypes.Uint32, "uint64": arrow.PrimitiveTypes.Uint64,
		"float16": arrow.FixedWidthTypes.Float16, "float32": arrow.PrimitiveTypes.Float32, "float64": arrow.PrimitiveTypes.Float64,
		"decimal128": &arrow.Decimal128Type{Precision: 18, Scale: 6}, "decimal256": &arrow.Decimal256Type{Precision: 40, Scale: 6},
		"string": arrow.BinaryTypes.String, "large_string": arrow.BinaryTypes.LargeString,
		"binary": arrow.BinaryTypes.Binary, "large_binary": arrow.BinaryTypes.LargeBinary,
		"fsb": &arrow.FixedSizeBinaryType{ByteWidth: 16}, "date32": arrow.FixedWidthTypes.Date32,
		"time32": arrow.FixedWidthTypes.Time32ms, "time64": arrow.FixedWidthTypes.Time64ns,
		"timestamp": arrow.FixedWidthTypes.Timestamp_ns, "uuid": extensions.NewUUIDType(),
		"list":   arrow.ListOf(arrow.PrimitiveTypes.Int32),
		"struct": arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}),
		"dict":   &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String},
	}
	for _, name := range slices.Sorted(maps.Keys(types)) {
		for _, dictionary := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dictionary=%v", name, dictionary), func(t *testing.T) {
				schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: types[name], Nullable: true}}, nil)
				tbl := generateTable(t, mem, schema, 8192, 0)
				defer tbl.Release()
				data := writeCDCTable(t, mem, tbl, 4096, dictionary, parquet.DataPageV1)
				assert.NotEmpty(t, columnPages(t, data, 0))
			})
		}
	}
}

// rowLevels returns the number of levels of the leaf column of each row: a list takes a
// level per element, or a single level if it is null or empty, while the other types of
// the test cases take a single level per row.
func rowLevels(arr arrow.Array) []int64 {
	levels := make([]int64, arr.Len())
	list, isList := arr.(*array.List)
	for i := range levels {
		levels[i] = 1
		if isList && list.IsValid(i) {
			offsets := list.Offsets()
			levels[i] = max(1, int64(offsets[i+1]-offsets[i]))
		}
	}
	return levels
}

// tableLevels is the number of levels of the first column of the tables, which the
// page lengths count.
func tableLevels(tables ...arrow.Table) int64 {
	var total int64
	for _, tbl := range tables {
		for _, chunk := range tbl.Column(0).Data().Chunks() {
			for _, n := range rowLevels(chunk) {
				total += n
			}
		}
	}
	return total
}

type cdcCase struct {
	name     string
	dtype    arrow.DataType
	nullable bool
	version  parquet.DataPageVersion
}

func cdcCases() []cdcCase {
	structOf := func(typ arrow.DataType) arrow.DataType {
		return arrow.StructOf(arrow.Field{Name: "f0", Type: typ, Nullable: true})
	}
	cases := []cdcCase{
		{dtype: arrow.FixedWidthTypes.Boolean},
		{dtype: arrow.PrimitiveTypes.Uint8}, {dtype: arrow.PrimitiveTypes.Uint16},
		{dtype: arrow.PrimitiveTypes.Uint32}, {dtype: arrow.PrimitiveTypes.Uint64, nullable: true},
		{dtype: arrow.PrimitiveTypes.Int8}, {dtype: arrow.PrimitiveTypes.Int16},
		{dtype: arrow.PrimitiveTypes.Int32}, {dtype: arrow.PrimitiveTypes.Int64, nullable: true},
		{dtype: arrow.FixedWidthTypes.Float16}, {dtype: arrow.PrimitiveTypes.Float32},
		{dtype: arrow.PrimitiveTypes.Float64, nullable: true},
		{dtype: &arrow.Decimal128Type{Precision: 18, Scale: 6}},
		{dtype: &arrow.Decimal256Type{Precision: 40, Scale: 6}},
		{dtype: arrow.BinaryTypes.String}, {dtype: arrow.BinaryTypes.Binary, nullable: true},
		{dtype: &arrow.FixedSizeBinaryType{ByteWidth: 16}, nullable: true},
		{dtype: arrow.FixedWidthTypes.Date32},
		{dtype: arrow.FixedWidthTypes.Time32ms, nullable: true},
		{dtype: arrow.FixedWidthTypes.Time64ns},
		{dtype: arrow.FixedWidthTypes.Timestamp_ns, nullable: true},
		{dtype: arrow.ListOf(arrow.PrimitiveTypes.Int16)},
		{dtype: arrow.ListOf(arrow.PrimitiveTypes.Int32), nullable: true},
		{dtype: arrow.ListOf(arrow.BinaryTypes.String), nullable: true},
		{dtype: structOf(arrow.PrimitiveTypes.Int32)},
		{dtype: structOf(arrow.PrimitiveTypes.Float64), nullable: true},
		{dtype: arrow.ListOf(structOf(arrow.PrimitiveTypes.Int32))},
		// the column writers write the dictionary of binary-like values directly and
		// convert the other dictionary arrays to dense arrays
		{dtype: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int64, ValueType: arrow.BinaryTypes.String}},
		{dtype: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}, nullable: true},
		{dtype: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int64}, nullable: true},
		{dtype: arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}), nullable: true},
		{dtype: extensions.NewUUIDType(), nullable: true},
		{dtype: arrow.BinaryTypes.LargeBinary, version: parquet.DataPageV2},
		{dtype: arrow.ListOf(arrow.BinaryTypes.String), nullable: true, version: parquet.DataPageV2},
	}
	for i := range cases {
		cases[i].name = fmt.Sprintf("%s/nullable=%v/V%d", cases[i].dtype, cases[i].nullable, cases[i].version+1)
	}
	return cases
}

func TestCDCSingleRowGroup(t *testing.T) {
	const (
		partSize       = 128 * 1024
		editSize       = 18 * 1024
		minChunkSize   = 4 * 1024
		maxChunkSize   = 16 * 1024
		rowGroupLength = 1024 * 1024
	)

	for _, c := range cdcCases() {
		t.Run(c.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			field := arrow.Field{Name: "f0", Type: c.dtype, Nullable: c.nullable}
			schema := arrow.NewSchema([]arrow.Field{field}, nil)

			// the chunk sizes are constant, so derive the number of rows from the size
			// of the type, unless it is nested or variable length where a hand picked
			// value avoids generating too large tables
			bytesPerRecord := int64(16)
			var fixedWidth int64
			switch typ := c.dtype.(type) {
			case *arrow.BooleanType:
				fixedWidth = 1
			case arrow.FixedWidthDataType:
				fixedWidth = int64(typ.BitWidth() / 8)
			}
			if fixedWidth > 0 {
				bytesPerRecord = fixedWidth
				if c.nullable {
					bytesPerRecord += 2
				}
			}
			partLength, editLength := partSize/bytesPerRecord, editSize/bytesPerRecord

			var parts [8]arrow.Table // 1 to 7, as in the C++ test
			for i, spec := range []struct{ length, seed int64 }{
				{partLength, 0}, {editLength, 1}, {partLength, partLength}, {editLength, 2},
				{partLength, 2 * partLength}, {editLength, 3}, {editLength, 4},
			} {
				parts[i+1] = generateTable(t, mem, schema, spec.length, uint64(spec.seed))
				defer parts[i+1].Release()
			}
			p1, p2, p3, p4, p5, p6, p7 := parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]

			assertEdited := func(t *testing.T, before, after []arrow.Table, edits []cdcEdit) {
				beforeTable := concatTables(t, mem, before...)
				defer beforeTable.Release()
				afterTable := concatTables(t, mem, after...)
				defer afterTable.Release()
				require.False(t, array.TableEqual(beforeTable, afterTable))

				for _, dictionary := range []bool{false, true} {
					var pages [2][][]int64
					for i, tbl := range []arrow.Table{beforeTable, afterTable} {
						data := writeCDCTable(t, mem, tbl, rowGroupLength, dictionary, c.version)
						pages[i] = columnPages(t, data, 0)
						require.Len(t, pages[i], 1)
						assertPageSizes(t, tbl.Column(0).Data(), pages[i][0], c.nullable, minChunkSize, maxChunkSize)

						// dictionary encoding writes a dictionary page, except for booleans
						rdr, err := file.NewParquetReader(bytes.NewReader(data))
						require.NoError(t, err)
						chunk, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
						require.NoError(t, err)
						assert.Equal(t, dictionary && c.dtype.ID() != arrow.BOOL, chunk.HasDictionaryPage(), "dictionary=%v", dictionary)
						require.NoError(t, rdr.Close())
					}
					assert.NoError(t, pagesFollowContent(pages[0], pages[1], edits), "dictionary=%v", dictionary)
				}
			}

			tables := func(ts ...arrow.Table) []arrow.Table { return ts }
			t.Run("DeleteOnce", func(t *testing.T) {
				assertEdited(t, tables(p1, p2, p3), tables(p1, p3),
					[]cdcEdit{{Offset: tableLevels(p1), Deleted: tableLevels(p2)}})
			})
			t.Run("DeleteTwice", func(t *testing.T) {
				assertEdited(t, tables(p1, p2, p3, p4, p5), tables(p1, p3, p5),
					[]cdcEdit{
						{Offset: tableLevels(p1), Deleted: tableLevels(p2)},
						{Offset: tableLevels(p1, p2, p3), Deleted: tableLevels(p4)},
					})
			})
			t.Run("UpdateOnce", func(t *testing.T) {
				// part 4 replaces part 2, which has the same number of values
				assertEdited(t, tables(p1, p2, p3), tables(p1, p4, p3),
					[]cdcEdit{{Offset: tableLevels(p1), Deleted: tableLevels(p2), Inserted: tableLevels(p4)}})
			})
			t.Run("UpdateTwice", func(t *testing.T) {
				// part 6 and 7 replace part 2 and 4
				assertEdited(t, tables(p1, p2, p3, p4, p5), tables(p1, p6, p3, p7, p5),
					[]cdcEdit{
						{Offset: tableLevels(p1), Deleted: tableLevels(p2), Inserted: tableLevels(p6)},
						{Offset: tableLevels(p1, p2, p3), Deleted: tableLevels(p4), Inserted: tableLevels(p7)},
					})
			})
			t.Run("InsertOnce", func(t *testing.T) {
				assertEdited(t, tables(p1, p3), tables(p1, p2, p3),
					[]cdcEdit{{Offset: tableLevels(p1), Inserted: tableLevels(p2)}})
			})
			t.Run("InsertTwice", func(t *testing.T) {
				assertEdited(t, tables(p1, p3, p5), tables(p1, p2, p3, p4, p5),
					[]cdcEdit{
						{Offset: tableLevels(p1), Inserted: tableLevels(p2)},
						{Offset: tableLevels(p1, p3), Inserted: tableLevels(p4)},
					})
			})
			t.Run("Prepend", func(t *testing.T) {
				assertEdited(t, tables(p1, p2, p3), tables(p4, p1, p2, p3),
					[]cdcEdit{{Offset: 0, Inserted: tableLevels(p4)}})
			})
			t.Run("Append", func(t *testing.T) {
				assertEdited(t, tables(p1, p2, p3), tables(p1, p2, p3, p4),
					[]cdcEdit{{Offset: tableLevels(p1, p2, p3), Inserted: tableLevels(p4)}})
			})

			t.Run("EmptyTable", func(t *testing.T) {
				empty := generateTable(t, mem, schema, 0, 0)
				defer empty.Release()
				require.Zero(t, empty.NumRows())
				for _, dictionary := range []bool{false, true} {
					data := writeCDCTable(t, mem, empty, rowGroupLength, dictionary, c.version)
					pages := columnPages(t, data, 0)
					// a single row group without data pages
					require.Len(t, pages, 1)
					assert.Empty(t, pages[0])
				}
			})

			t.Run("ArrayOffsets", func(t *testing.T) {
				// check that the array offsets are respected in the chunker
				table := concatTables(t, mem, p1, p2, p3)
				defer table.Release()
				for _, offset := range []int64{0, 512, 1024} {
					chunked := array.NewChunkedSlice(table.Column(0).Data(), offset, table.NumRows())
					col := arrow.NewColumn(field, chunked)
					sliced := array.NewTable(schema, []arrow.Column{*col}, table.NumRows()-offset)
					col.Release()
					chunked.Release()
					require.EqualValues(t, offset, sliced.Column(0).Data().Chunk(0).Data().Offset())
					data := writeCDCTable(t, mem, sliced, rowGroupLength, true, c.version)
					assert.Len(t, columnPages(t, data, 0), 1)
					sliced.Release()
				}
			})

			// The chunker state is carried over between the write calls of a column chunk,
			// so writing the column as a single array or as multiple arrays must produce
			// the same pages. Each page of the single array is written with two calls: the
			// first one starts at the page, so it starts with a chunk boundary, and the
			// second one starts in the middle of the page, so it continues the chunk of
			// the previous call:
			//
			//	single call:  |         page 0          |        page 1        |
			//	write calls:  |   call 0   |   call 1   |  call 2   |  call 3  | ...
			t.Run("IndependentOfWriteCalls", func(t *testing.T) {
				table := concatTables(t, mem, p1, p2, p3)
				defer table.Release()
				arr := table.Column(0).Data().Chunk(0)

				// the page lengths count levels, so find the rows where the pages end
				rowEnds := []int64{0}
				for _, n := range rowLevels(arr) {
					rowEnds = append(rowEnds, rowEnds[len(rowEnds)-1]+n)
				}

				for _, dictionary := range []bool{false, true} {
					props, arrprops := cdcTableProps(t, mem, schema, rowGroupLength, dictionary, c.version)
					data := writeCDCTable(t, mem, table, rowGroupLength, dictionary, c.version)
					pages := columnPages(t, data, 0)
					require.Len(t, pages, 1)
					require.Greater(t, len(pages[0]), 1)

					var recs []arrow.RecordBatch
					var row, level int64
					addRecord := func(begin, end int64) {
						slice := array.NewSlice(arr, begin, end)
						defer slice.Release()
						recs = append(recs, array.NewRecordBatch(schema, []arrow.Array{slice}, end-begin))
					}
					for _, length := range pages[0] {
						pageBegin := row
						level += length
						idx, _ := slices.BinarySearch(rowEnds, level)
						row = int64(idx)
						// the pages end at row boundaries
						require.Less(t, row, int64(len(rowEnds)))
						require.Equal(t, level, rowEnds[row])
						pageMiddle := pageBegin + (row-pageBegin)/2
						if pageMiddle > pageBegin {
							addRecord(pageBegin, pageMiddle)
						}
						addRecord(pageMiddle, row)
					}
					require.Equal(t, int64(arr.Len()), row)

					chunked := writeRecords(t, mem, props, arrprops, true, recs...)
					for _, rec := range recs {
						rec.Release()
					}
					assert.Equal(t, pages, columnPages(t, chunked, 0), "dictionary=%v", dictionary)
				}
			})
		})
	}
}

func TestCDCColumnByColumnMatchesWrite(t *testing.T) {
	const rows = 64 * 1024
	props := cdcProps(parquet.CDCOptions{MinChunkSize: 4096, MaxChunkSize: 16384})

	// the second column of the identical variant has the same content as the first, so
	// a column writer using the chunker of another column cuts other pages
	for _, identical := range []bool{false, true} {
		t.Run(fmt.Sprintf("identical=%v", identical), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			second := arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}
			if identical {
				second = arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
			}
			schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, second}, nil)
			first := generateArray(t, mem, schema.Field(0), rows, 1)
			defer first.Release()
			var other arrow.Array
			if identical {
				other = generateArray(t, mem, schema.Field(1), rows, 1)
			} else {
				other = generateArray(t, mem, schema.Field(1), rows, 2)
			}
			defer other.Release()
			rec := array.NewRecordBatch(schema, []arrow.Array{first, other}, rows)
			defer rec.Release()
			arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())

			written := writeRecords(t, mem, props, arrprops, false, rec)
			buffered := writeRecords(t, mem, props, arrprops, true, rec)

			var buf bytes.Buffer
			fw, err := pqarrow.NewFileWriter(schema, &buf, props, arrprops)
			require.NoError(t, err)
			fw.NewRowGroup()
			require.NoError(t, fw.WriteColumnData(first))
			require.NoError(t, fw.WriteColumnData(other))
			require.NoError(t, fw.Close())
			columnByColumn := buf.Bytes()
			requireRoundTrip(t, mem, columnByColumn, rec)

			for col := 0; col < 2; col++ {
				expected := columnPages(t, written, col)
				require.Len(t, expected, 1)
				require.Greater(t, len(expected[0]), 1)
				assert.Equal(t, expected, columnPages(t, buffered, col), "WriteBuffered column %d", col)
				assert.Equal(t, expected, columnPages(t, columnByColumn, col), "WriteColumnData column %d", col)
			}
			if identical {
				for name, data := range map[string][]byte{"Write": written, "WriteBuffered": buffered, "WriteColumnData": columnByColumn} {
					assert.Equal(t, columnPages(t, data, 0), columnPages(t, data, 1), name)
				}
			}
		})
	}
}

func TestCDCIndependentOfArrowChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const rows, chunks = 64 * 1024, 64
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	whole := generateTable(t, mem, schema, rows, 0)
	defer whole.Release()

	cols := make([]arrow.Column, schema.NumFields())
	for i, field := range schema.Fields() {
		arr := whole.Column(i).Data().Chunk(0)
		slices := make([]arrow.Array, chunks)
		for j := range slices {
			slices[j] = array.NewSlice(arr, int64(j*rows/chunks), int64((j+1)*rows/chunks))
		}
		chunked := arrow.NewChunked(field.Type, slices)
		for _, s := range slices {
			s.Release()
		}
		cols[i] = *arrow.NewColumn(field, chunked)
		chunked.Release()
	}
	split := array.NewTable(schema, cols, rows)
	defer split.Release()
	for i := range cols {
		cols[i].Release()
	}
	require.Equal(t, chunks, len(split.Column(0).Data().Chunks()))

	wholePages := writeCDCTable(t, mem, whole, rows, false, parquet.DataPageV1)
	splitPages := writeCDCTable(t, mem, split, rows, false, parquet.DataPageV1)
	for col := 0; col < 2; col++ {
		expected := columnPages(t, wholePages, col)
		require.Greater(t, len(expected[0]), 1)
		assert.Equal(t, expected, columnPages(t, splitPages, col), "column %d", col)
	}
}

// assertPagesFollowContent asserts that the files have the given number of row groups
// and that the pages of every column follow the content after the edits.
func assertPagesFollowContent(t *testing.T, original, modified []byte, originalRowGroups, modifiedRowGroups int, edits []cdcEdit) {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(original))
	require.NoError(t, err)
	defer rdr.Close()
	require.Equal(t, originalRowGroups, rdr.NumRowGroups())
	modifiedRdr, err := file.NewParquetReader(bytes.NewReader(modified))
	require.NoError(t, err)
	defer modifiedRdr.Close()
	require.Equal(t, modifiedRowGroups, modifiedRdr.NumRowGroups())

	for col := 0; col < rdr.MetaData().Schema.NumColumns(); col++ {
		assert.NoError(t, pagesFollowContent(columnPages(t, original, col), columnPages(t, modified, col), edits), "column %d", col)
	}
}

func TestCDCMultipleRowGroups(t *testing.T) {
	const (
		partLength = 128 * 1024
		// the edits shift the subsequent row groups by more than the maximum chunk size of
		// every column, otherwise chunkers starting over at the shifted row groups could
		// still end the pages at the same positions, but not by a multiple of the minimum
		// chunk size, so pages cut at a fixed size don't end at the same positions again
		editLength     = 18 * 1024
		rowGroupLength = 64 * 1024
	)
	nullable := func(name string, typ arrow.DataType) arrow.Field {
		return arrow.Field{Name: name, Type: typ, Nullable: true}
	}
	schema := arrow.NewSchema([]arrow.Field{
		nullable("int32", arrow.PrimitiveTypes.Int32),
		nullable("float64", arrow.PrimitiveTypes.Float64),
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		nullable("int64", arrow.PrimitiveTypes.Int64),
		nullable("float32", arrow.PrimitiveTypes.Float32),
		nullable("fixed_size_binary", &arrow.FixedSizeBinaryType{ByteWidth: 8}),
	}, nil)

	// the tables are released after the test, but before the allocator is checked
	setup := func(t *testing.T, mem memory.Allocator) (part1, part2, part3, edit1, edit2, edit3 arrow.Table) {
		var tables []arrow.Table
		for _, spec := range []struct{ length, seed int64 }{
			{partLength, 0}, {partLength, 2}, {partLength, 4}, {editLength, 1}, {editLength, 3}, {editLength, 5},
		} {
			tbl := generateTable(t, mem, schema, spec.length, uint64(spec.seed))
			t.Cleanup(tbl.Release)
			tables = append(tables, tbl)
		}
		return tables[0], tables[1], tables[2], tables[3], tables[4], tables[5]
	}

	// assertEditedTables writes the tables of the parts before and after the edits with
	// fixed length row groups and asserts that the pages of every column follow the
	// content after the edits, the edited tables span 7 row groups
	assertEditedTables := func(t *testing.T, mem memory.Allocator, before, after []arrow.Table, edits []cdcEdit) {
		beforeTable := concatTables(t, mem, before...)
		defer beforeTable.Release()
		afterTable := concatTables(t, mem, after...)
		defer afterTable.Release()
		require.False(t, array.TableEqual(beforeTable, afterTable))
		assertPagesFollowContent(t,
			writeCDCTable(t, mem, beforeTable, rowGroupLength, false, parquet.DataPageV1),
			writeCDCTable(t, mem, afterTable, rowGroupLength, false, parquet.DataPageV1), 7, 7, edits)
	}
	tables := func(ts ...arrow.Table) []arrow.Table { return ts }

	// The row groups have a fixed length, so the inserted values shift the data of the
	// subsequent row groups, whose pages must still end at the same positions except at
	// the row group ends.
	t.Run("InsertOnce", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, edit1, edit2, _ := setup(t, mem)
		assertEditedTables(t, mem, tables(part1, edit1, part2, part3), tables(part1, edit1, edit2, part2, part3),
			[]cdcEdit{{Offset: tableLevels(part1, edit1), Inserted: tableLevels(edit2)}})
	})

	// The inverse of InsertOnce, the deleted values shift the data of the subsequent row
	// groups the other way.
	t.Run("DeleteOnce", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, edit1, edit2, _ := setup(t, mem)
		assertEditedTables(t, mem, tables(part1, edit1, edit2, part2, part3), tables(part1, edit1, part2, part3),
			[]cdcEdit{{Offset: tableLevels(part1, edit1), Deleted: tableLevels(edit2)}})
	})

	// The updated values don't shift the data, so only the pages around them may change.
	t.Run("UpdateOnce", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, edit1, edit2, edit3 := setup(t, mem)
		assertEditedTables(t, mem, tables(part1, edit1, part2, part3, edit2), tables(part1, edit3, part2, part3, edit2),
			[]cdcEdit{{Offset: tableLevels(part1), Deleted: tableLevels(edit1), Inserted: tableLevels(edit3)}})
	})

	// The appended values only extend the last row group.
	t.Run("Append", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, edit1, edit2, _ := setup(t, mem)
		assertEditedTables(t, mem, tables(part1, edit1, part2, part3), tables(part1, edit1, part2, part3, edit2),
			[]cdcEdit{{Offset: tableLevels(part1, edit1, part2, part3), Inserted: tableLevels(edit2)}})
	})

	// The content defined page boundaries must not depend on how the data is split into
	// row groups: a multiple row group file must end its pages where a single row group
	// file does, except at the row group ends which cut the pages. The 400 rows long row
	// groups are shorter than the minimum chunk size of all the columns, so the chunks
	// span multiple row groups. With row groups as long as the first page of the first
	// column, the second row group starts at a content defined boundary of that column,
	// so its column writer receives an empty first chunk, which must not produce an
	// empty page.
	t.Run("IndependentOfRowGroupBoundaries", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, _, _, _ := setup(t, mem)
		table := concatTables(t, mem, part1, part2, part3)
		defer table.Release()

		single := writeCDCTable(t, mem, table, table.NumRows(), false, parquet.DataPageV1)
		firstPageLength := columnPages(t, single, 0)[0][0]
		for _, length := range []int64{rowGroupLength, 400, firstPageLength} {
			multi := writeCDCTable(t, mem, table, length, false, parquet.DataPageV1)
			for _, rg := range columnPages(t, multi, 0) {
				for _, n := range rg {
					require.NotZero(t, n, "row group length %d", length)
				}
			}
			assertPagesFollowContent(t, single, multi, 1, int((table.NumRows()+length-1)/length), nil)
		}
	})

	// Every leaf column of a nested schema has its own chunker which must be carried over
	// between the row groups: struct.a, struct.b and map.key only have definition levels,
	// while list.item and map.value have repetition levels too, so their chunks only end
	// at record boundaries. The records never cross the row groups, but the chunks do.
	t.Run("NestedIndependentOfRowGroupBoundaries", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		const nestedRowGroupLength = 16 * 1024
		nestedSchema := arrow.NewSchema([]arrow.Field{
			nullable("struct", arrow.StructOf(nullable("a", arrow.PrimitiveTypes.Int32), nullable("b", arrow.BinaryTypes.String))),
			nullable("list", arrow.ListOf(arrow.PrimitiveTypes.Int32)),
			nullable("map", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)),
		}, nil)
		table := generateTable(t, mem, nestedSchema, 4*nestedRowGroupLength, 0)
		defer table.Release()
		for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			single := writeCDCTable(t, mem, table, table.NumRows(), false, version)
			multi := writeCDCTable(t, mem, table, nestedRowGroupLength, false, version)
			rdr, err := file.NewParquetReader(bytes.NewReader(single))
			require.NoError(t, err)
			// the leaf columns struct.a, struct.b, list.item, map.key and map.value
			require.Equal(t, 5, rdr.MetaData().Schema.NumColumns())
			require.NoError(t, rdr.Close())
			assertPagesFollowContent(t, single, multi, 1, 4, nil)
		}
	})

	// The dictionary arrays are chunked by their indices. The column writers write the
	// dictionary and the indices directly if dictionary encoding is enabled and the
	// values are binary-like, otherwise they convert the dictionary arrays to dense
	// arrays. The chunker state must be carried over between the row groups in every
	// case.
	t.Run("DictionaryIndependentOfRowGroupBoundaries", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		const dictionaryRowGroupLength = 16 * 1024
		dictionarySchema := arrow.NewSchema([]arrow.Field{
			{Name: "string", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}},
			{Name: "int64", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int64}},
			{Name: "list", Type: arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String})},
		}, nil)
		table := generateTable(t, mem, dictionarySchema, 4*dictionaryRowGroupLength, 0)
		defer table.Release()
		for _, dictionary := range []bool{false, true} {
			single := writeCDCTable(t, mem, table, table.NumRows(), dictionary, parquet.DataPageV1)
			multi := writeCDCTable(t, mem, table, dictionaryRowGroupLength, dictionary, parquet.DataPageV1)
			// dictionary encoding writes a dictionary page into every column chunk
			for _, data := range [][]byte{single, multi} {
				rdr, err := file.NewParquetReader(bytes.NewReader(data))
				require.NoError(t, err)
				for rg := 0; rg < rdr.NumRowGroups(); rg++ {
					for col := 0; col < rdr.MetaData().Schema.NumColumns(); col++ {
						chunk, err := rdr.MetaData().RowGroup(rg).ColumnChunk(col)
						require.NoError(t, err)
						require.Equal(t, dictionary, chunk.HasDictionaryPage(), "dictionary=%v row group %d column %d", dictionary, rg, col)
					}
				}
				require.NoError(t, rdr.Close())
			}
			assertPagesFollowContent(t, single, multi, 1, 4, nil)
		}
	})

	// Writing record batches buffers the row groups, which create their column writers
	// upfront. The chunker state must be carried over between these row groups as well,
	// including the record batches split by the row group boundaries.
	t.Run("BufferedIndependentOfRowGroupBoundaries", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		part1, part2, part3, _, _, _ := setup(t, mem)
		table := concatTables(t, mem, part1, part2, part3)
		defer table.Release()
		single := writeCDCTable(t, mem, table, table.NumRows(), false, parquet.DataPageV1)

		props, arrprops := cdcTableProps(t, mem, schema, rowGroupLength, false, parquet.DataPageV1)
		var recs []arrow.RecordBatch
		for begin := int64(0); begin < table.NumRows(); begin += 10007 {
			end := min(begin+10007, table.NumRows())
			cols := make([]arrow.Array, schema.NumFields())
			for i := range cols {
				cols[i] = array.NewSlice(table.Column(i).Data().Chunk(0), begin, end)
			}
			recs = append(recs, array.NewRecordBatch(schema, cols, end-begin))
			for _, col := range cols {
				col.Release()
			}
		}
		defer func() {
			for _, rec := range recs {
				rec.Release()
			}
		}()
		buffered := writeRecords(t, mem, props, arrprops, true, recs...)

		readback := readCDCTable(t, mem, buffered)
		defer readback.Release()
		for i := 0; i < int(table.NumCols()); i++ {
			require.Truef(t, array.ChunkedEqual(table.Column(i).Data(), readback.Column(i).Data()), "column %d differs", i)
		}
		assertPagesFollowContent(t, single, buffered, 1, 6, nil)
	})
}

// The chunker hashes the indices of the dictionary arrays rather than their values, so
// the pages follow the indices even if the dictionary changes between the write calls.
// Unlike in C++ the writer falls back to plain encoding at the first change without
// ending the page, so the pages are the ones of the same indices with one dictionary:
//
//	indices:      |     page 0     |      page 1      |     page 2     |
//	write calls:  |    dictionary 0    |   dictionary 1   | dictionary 2 |
//	pages:        |     page 0     |      page 1      |     page 2     |
//	                                    ^^ fallback to plain encoding
func TestCDCDictionaryChangesBetweenWriteCalls(t *testing.T) {
	const length = 64 * 1024
	callEnds := []int64{length/3 + 1, 2*length/3 + 3, length}

	for _, valueType := range []arrow.DataType{arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64} {
		t.Run(valueType.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: valueType}
			field := arrow.Field{Name: "f0", Type: dictType, Nullable: true}
			schema := arrow.NewSchema([]arrow.Field{field}, nil)
			arr := generateArray(t, mem, field, length, 0).(*array.Dictionary)
			defer arr.Release()

			// records of the same indices, with the dictionary of the seed of each record
			records := func(seeds []uint64) []arrow.RecordBatch {
				var recs []arrow.RecordBatch
				begin := int64(0)
				for i, end := range callEnds {
					dictionary := generateArray(t, mem, arrow.Field{Name: "dictionary", Type: valueType}, int64(arr.Dictionary().Len()), seeds[i])
					indices := array.NewSlice(arr.Indices(), begin, end)
					chunk := array.NewDictionaryArray(dictType, indices, dictionary)
					recs = append(recs, array.NewRecordBatch(schema, []arrow.Array{chunk}, end-begin))
					chunk.Release()
					indices.Release()
					dictionary.Release()
					begin = end
				}
				return recs
			}
			changing, constant := records([]uint64{0, 1, 2}), records([]uint64{0, 0, 0})
			defer func() {
				for _, rec := range append(changing, constant...) {
					rec.Release()
				}
			}()

			for _, dictionary := range []bool{false, true} {
				props, arrprops := cdcTableProps(t, mem, schema, length, dictionary, parquet.DataPageV1)
				changed := writeRecords(t, mem, props, arrprops, true, changing...)
				unchanged := writeRecords(t, mem, props, arrprops, true, constant...)

				pages := columnPages(t, changed, 0)
				require.Len(t, pages, 1)
				require.Greater(t, len(pages[0]), 1)
				assert.Equal(t, columnPages(t, unchanged, 0), pages, "dictionary=%v", dictionary)

				// the data pages of the changed dictionary arrays are plain encoded after
				// the fallback, unlike the ones of the same dictionary
				plainPages := func(data []byte) (n int) {
					rdr, err := file.NewParquetReader(bytes.NewReader(data))
					require.NoError(t, err)
					defer rdr.Close()
					pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
					require.NoError(t, err)
					for pr.Next() {
						if p, ok := pr.Page().(*file.DataPageV1); ok && parquet.Encoding(p.Encoding()) == parquet.Encodings.Plain {
							n++
						}
					}
					require.NoError(t, pr.Err())
					return n
				}
				rdr, err := file.NewParquetReader(bytes.NewReader(changed))
				require.NoError(t, err)
				chunk, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
				require.NoError(t, err)
				assert.Contains(t, chunk.Encodings(), parquet.Encodings.Plain)
				require.NoError(t, rdr.Close())
				if dictionary && valueType.ID() == arrow.STRING {
					assert.Positive(t, plainPages(changed))
					assert.Zero(t, plainPages(unchanged))
				}
			}
		})
	}
}

func BenchmarkWriteContentDefinedChunking(b *testing.B) {
	const numRows = 1 << 20
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(b, 0)

	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String} {
		field := arrow.Field{Name: "c", Type: dt}
		arr, err := tryGenerateArray(mem, field, numRows, 42)
		require.NoError(b, err)
		defer arr.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{arr}, numRows)
		defer rec.Release()

		var size int64
		for _, buf := range arr.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}

		for _, cdc := range []bool{false, true} {
			opts := []parquet.WriterProperty{parquet.WithDictionaryDefault(false)}
			if cdc {
				opts = append(opts, parquet.WithContentDefinedChunking(parquet.DefaultCDCOptions()))
			}
			props := parquet.NewWriterProperties(opts...)
			b.Run(fmt.Sprintf("%s/cdc=%t", dt, cdc), func(b *testing.B) {
				b.SetBytes(size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fw, err := pqarrow.NewFileWriter(rec.Schema(), io.Discard, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
					require.NoError(b, err)
					require.NoError(b, fw.Write(rec))
					require.NoError(b, fw.Close())
				}
			})
		}
	}
}
