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
	assert.Equal(t, columnPages(t, single, 0), columnPages(t, buffered, 0))
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
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())

	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())
	rgw = w.AppendRowGroup()
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

	props := cdcProps(parquet.DefaultCDCOptions())
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

	case *arrow.ListType:
		// repeat the same pattern in the list array: null, empty list, list of 1
		// element, list of 3 elements
		if length%4 != 0 {
			return nil, fmt.Errorf("length must be divisible by 4 when generating list arrays, but got: %d", length)
		}
		values, err := tryGenerateArray(mem, arrow.Field{Name: "item", Type: typ.Elem(), Nullable: true}, length*4, seed)
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
		return array.NewListData(data), nil

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

// writeCDCTable writes the table with content defined chunking and returns the file
// after checking that its content reads back equal to the table.
func writeCDCTable(t *testing.T, mem memory.Allocator, tbl arrow.Table, rowGroupLength int64, dictionary bool, version parquet.DataPageVersion) []byte {
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
		sc, err := pqarrow.ToParquet(tbl.Schema(), parquet.NewWriterProperties(opts...), arrprops)
		require.NoError(t, err)
		for i := 0; i < sc.NumColumns(); i++ {
			opts = append(opts, parquet.WithDictionaryCostFallbackFor(sc.Column(i).Path(), false))
		}
	}

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, rowGroupLength, parquet.NewWriterProperties(opts...), arrprops))

	readback := readCDCTable(t, mem, buf.Bytes())
	defer readback.Release()
	require.Equal(t, tbl.NumRows(), readback.NumRows())
	for i := 0; i < int(tbl.NumCols()); i++ {
		expected, actual := tbl.Column(i).Data(), readback.Column(i).Data()
		if _, ok := expected.DataType().(*arrow.DictionaryType); ok {
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
				size = int64(a.ValueOffset(a.Len()) - a.ValueOffset(0))
			case *array.LargeString:
				size = a.ValueOffset(a.Len()) - a.ValueOffset(0)
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
