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

package file_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// TestWriteBatchSpacedV2RowBoundary is a regression test for #882: when a
// repeated column is written through the spaced path under DataPageV2, batches
// were split by a fixed WriteBatchSize instead of on row boundaries. Because a
// page flush is only ever checked at a batch boundary, a fixed-size batch that
// ended mid-row caused a data page to end (and the next one to begin) in the
// middle of a repeated row. DataPageV2 requires every page to start at a row
// boundary (first repetition level == 0) for offset-index page pruning to be
// correct.
//
// The spaced write path now routes through the rep-level-aware
// columnWriter.doBatches (the same batching WriteBatch already used), so every
// batch — and therefore every page — lands on a row boundary.
func TestWriteBatchSpacedV2RowBoundary(t *testing.T) {
	const (
		numRows    = 1000
		valsPerRow = 4
		total      = numRows * valsPerRow
		// A batch size that is not a multiple of valsPerRow guarantees that a
		// fixed-size split would land mid-row.
		batchSize    = 7
		dataPageSize = 1024
	)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("v", parquet.Repetitions.Repeated, -1),
	}, -1)
	require.NoError(t, err)

	values := make([]int32, total)
	defLevels := make([]int16, total)
	repLevels := make([]int16, total)
	for i := 0; i < total; i++ {
		values[i] = int32(i)
		defLevels[i] = 1 // present (maxDefLevel for a repeated leaf)
		if i%valsPerRow == 0 {
			repLevels[i] = 0 // start of a new row
		} else {
			repLevels[i] = 1 // continuation of the current repeated row
		}
	}
	validBits := make([]byte, (total+7)/8)
	for i := range validBits {
		validBits[i] = 0xFF
	}

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(batchSize),
		parquet.WithDataPageSize(dataPageSize),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	)

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatchSpacedWithError(values, defLevels, repLevels, validBits, 0)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	assertPagesStartOnRowBoundary(t, buf.Bytes())
}

// assertPagesStartOnRowBoundary reads the first column chunk back and requires
// that every DataPageV2 begins at a row boundary (its first repetition level is
// 0). The test forces multiple pages, so a mid-row split will surface here.
func assertPagesStartOnRowBoundary(t *testing.T, raw []byte) {
	t.Helper()

	r, err := file.NewParquetReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer r.Close()

	maxRep := r.MetaData().Schema.Column(0).MaxRepetitionLevel()
	require.Greater(t, maxRep, int16(0), "column must be repeated for this test to be meaningful")

	pr, err := r.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)

	pageCount, v2Count := 0, 0
	for pr.Next() {
		pageCount++
		v2, ok := pr.Page().(*file.DataPageV2)
		if !ok {
			continue
		}
		v2Count++

		var dec encoding.LevelDecoder
		require.NoError(t, dec.SetDataV2(v2.RepetitionLevelByteLen(), maxRep, int(v2.NumValues()), v2.Data()))

		levels := make([]int16, v2.NumValues())
		n, _ := dec.Decode(levels)
		require.Greater(t, n, 0, "page %d decoded zero repetition levels", v2Count)
		require.Zerof(t, levels[0],
			"DataPageV2 #%d starts mid-row (first repetition level = %d, want 0)", v2Count, levels[0])
	}
	require.NoError(t, pr.Err())
	require.Greater(t, v2Count, 1, "expected more than one DataPageV2 so a mid-row split could occur")
}

// TestWriteSpacedV2NullableListRowBoundary is a regression test for the second
// half of #882 (surfaced by punkeel while reviewing #883): when a nullable list
// element is written through the spaced path under DataPageV2 and a single
// repeated row is wider than the write-batch size, the row-boundary alignment
// has no earlier boundary to shrink back to. For fixed-width elements (float64)
// the batch shrank to zero and the writer looped without making progress (a
// hang); for byte-array elements (string, binary, fixed_size_binary) the batch
// clamped at one and a DataPageV2 was still emitted starting mid-row.
//
// punkeel's key observation: the trigger requires an actual null element in the
// data, because only then does the Arrow writer route the leaf through
// WriteBatchSpaced instead of WriteBatch. Every row here ends with a null
// element, and each row (width 5) is wider than the batch size (4), so there is
// no row boundary to shrink back to within a batch.
func TestWriteSpacedV2NullableListRowBoundary(t *testing.T) {
	cases := []struct {
		name     string
		elemType arrow.DataType
	}{
		{"string", arrow.BinaryTypes.String},
		{"binary", arrow.BinaryTypes.Binary},
		{"fixed_size_binary", &arrow.FixedSizeBinaryType{ByteWidth: 1}},
		{"float64", arrow.PrimitiveTypes.Float64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const (
				numRows  = 20
				rowWidth = 300 // much wider than the batch size below
				batch    = 5
				// A page size well under one row's worth of data forces
				// several page flushes inside every row, so any batch that
				// ends mid-row surfaces as a mid-row page.
				pageSize = 100
			)
			widths := make([]int, numRows)
			for i := range widths {
				widths[i] = rowWidth
			}
			raw := writeNullableListV2WithinTimeout(t, tc.elemType, false, widths, batch, pageSize, 30*time.Second)
			assertPagesStartOnRowBoundary(t, raw)
		})
	}
}

// TestWriteSpacedV2NullableListWideRow is primarily a hang guard for punkeel's
// long-list case: a single nullable list whose length (20001) exceeds the
// write-batch size (20000). There is no row boundary before the requested split,
// so the fix must grow the batch forward to keep the whole row in one page rather
// than shrinking to zero and looping without progress. A fixed-width element type
// makes the pre-fix failure deterministic: the batch shrinks to zero and the
// writer hangs (a byte-array element would instead silently clamp to one, which
// the timeout would not catch), so the 30s timeout in
// writeNullableListV2WithinTimeout is the real regression guard here. With only
// one row the page-boundary assertions below cannot themselves detect a mid-row
// split (a single row can never be split) and pass even with the fix reverted;
// they exist only to confirm the wide row still produces a single valid
// DataPageV2 once the write completes.
func TestWriteSpacedV2NullableListWideRow(t *testing.T) {
	const (
		listLen = 20001
		batch   = 20000
	)
	raw := writeNullableListV2WithinTimeout(t, arrow.PrimitiveTypes.Float64, true, []int{listLen}, batch, 1024*1024, 30*time.Second)

	r, err := file.NewParquetReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer r.Close()
	require.EqualValues(t, 1, r.NumRows())

	pr, err := r.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	maxRep := r.MetaData().Schema.Column(0).MaxRepetitionLevel()
	v2Count := 0
	for pr.Next() {
		v2, ok := pr.Page().(*file.DataPageV2)
		if !ok {
			continue
		}
		v2Count++
		var dec encoding.LevelDecoder
		require.NoError(t, dec.SetDataV2(v2.RepetitionLevelByteLen(), maxRep, int(v2.NumValues()), v2.Data()))
		levels := make([]int16, v2.NumValues())
		n, _ := dec.Decode(levels)
		require.Greater(t, n, 0)
		require.Zero(t, levels[0], "wide row page starts mid-row")
	}
	require.NoError(t, pr.Err())
	require.Equal(t, 1, v2Count, "a single wide row must occupy exactly one DataPageV2")
}

// writeNullableListV2WithinTimeout builds a list<nullable elem> Arrow column
// whose rows have the given widths (the last element of every row is null so
// the leaf writer takes the WriteBatchSpaced path), writes it as DataPageV2 with
// the given batch and page sizes, and returns the parquet bytes. The write runs
// under a timeout so that a regression to the no-progress batching loop (#882)
// fails the test instead of hanging the whole suite. On a hang the write
// goroutine is left spinning: the parquet writer exposes no cancellation hook
// into the batching loop, and once the timeout has failed the test the process
// exits, so the leak is bounded to the failing run.
func writeNullableListV2WithinTimeout(t *testing.T, elemType arrow.DataType, listNullable bool, rowWidths []int, batchSize, pageSize int64, timeout time.Duration) []byte {
	t.Helper()

	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := writeNullableListV2(elemType, listNullable, rowWidths, batchSize, pageSize)
		done <- result{b, err}
	}()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		return r.b
	case <-time.After(timeout):
		t.Fatalf("write did not finish within %s: batching made no progress on a row wider than the batch size (#882)", timeout)
		return nil
	}
}

func writeNullableListV2(elemType arrow.DataType, listNullable bool, rowWidths []int, batchSize, pageSize int64) ([]byte, error) {
	mem := memory.NewGoAllocator()

	listType := arrow.ListOf(elemType) // element field is nullable by default
	field := arrow.Field{Name: "l", Type: listType, Nullable: listNullable}
	arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)

	lb := array.NewBuilder(mem, listType).(*array.ListBuilder)
	defer lb.Release()
	appendElem := elemAppender(lb.ValueBuilder())

	valueIdx := 0
	for _, w := range rowWidths {
		lb.Append(true)
		for k := 0; k < w; k++ {
			if k == w-1 {
				lb.ValueBuilder().AppendNull() // an actual null element (spaced trigger)
			} else {
				appendElem(valueIdx)
			}
			valueIdx++
		}
	}
	arr := lb.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(len(rowWidths)))
	defer rec.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(batchSize),
		parquet.WithDataPageSize(pageSize),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	)
	pqw, err := pqarrow.NewFileWriter(arrowSchema, &buf, props, pqarrow.NewArrowWriterProperties())
	if err != nil {
		return nil, err
	}
	if err := pqw.Write(rec); err != nil {
		return nil, err
	}
	if err := pqw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func elemAppender(vb array.Builder) func(int) {
	switch b := vb.(type) {
	case *array.StringBuilder:
		return func(i int) { b.Append(fmt.Sprintf("v%d", i)) }
	case *array.BinaryBuilder:
		return func(i int) { b.Append([]byte(fmt.Sprintf("v%d", i))) }
	case *array.FixedSizeBinaryBuilder:
		return func(i int) { b.Append([]byte{byte(i)}) }
	case *array.Float64Builder:
		return func(i int) { b.Append(float64(i)) }
	default:
		panic(fmt.Sprintf("unsupported element builder %T", vb))
	}
}

// TestWriteBitmapBatchSpacedV2RowBoundary covers the boolean bitmap spaced path,
// which WriteBitmapBatchSpacedWithError now routes through the rep-aware
// columnWriter.doBatches. Like the Int32 case, a repeated boolean column written
// with a batch size that is not a multiple of the row width would, before the
// fix, split a data page mid-row. Reverting alignBatchToRowBoundary to
// `return batch` makes this fail with "starts mid-row".
func TestWriteBitmapBatchSpacedV2RowBoundary(t *testing.T) {
	const (
		numRows    = 2000
		valsPerRow = 4
		total      = numRows * valsPerRow
		batchSize  = 7 // not a multiple of valsPerRow: a fixed split lands mid-row
		// A small page size relative to the batch stride keeps page flushes from
		// coincidentally landing on row boundaries; with the fix reverted this
		// produces 12 of 16 pages starting mid-row.
		pageSize = 64
	)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewBooleanNode("v", parquet.Repetitions.Repeated, -1),
	}, -1)
	require.NoError(t, err)

	bitmap := make([]byte, bitutil.BytesForBits(total))
	validBits := make([]byte, bitutil.BytesForBits(total))
	defLevels := make([]int16, total)
	repLevels := make([]int16, total)
	for i := 0; i < total; i++ {
		defLevels[i] = 1 // present
		bitutil.SetBit(validBits, i)
		if i%3 == 0 {
			bitutil.SetBit(bitmap, i)
		}
		if i%valsPerRow == 0 {
			repLevels[i] = 0
		} else {
			repLevels[i] = 1
		}
	}

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(batchSize),
		parquet.WithDataPageSize(pageSize),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	)

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.BooleanColumnChunkWriter).WriteBitmapBatchSpacedWithError(bitmap, 0, total, defLevels, repLevels, validBits, 0)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	assertPagesStartOnRowBoundary(t, buf.Bytes())
}

// TestWriteDictIndicesV2RowBoundary covers the dictionary path: WriteDictIndices
// is now routed through the rep-aware doBatches too. A repeated int32 column with
// low-cardinality values keeps the dictionary (no fallback to plain), while a
// small data page size and many rows force several data pages so a mid-row split
// is possible. The values also round-trip, guarding the variable batch sizes the
// fix introduces against a value-cursor slip.
func TestWriteDictIndicesV2RowBoundary(t *testing.T) {
	const (
		numRows    = 4000
		valsPerRow = 3
		// A high dictionary page-size limit keeps the column dictionary-encoded;
		// a small data page size forces the indices across many data pages.
		batchSize = 5 // not a multiple of valsPerRow
		pageSize  = 512
	)

	mem := memory.NewGoAllocator()
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "l", Type: listType, Nullable: false}}, nil)

	lb := array.NewBuilder(mem, listType).(*array.ListBuilder)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Int32Builder)
	for r := 0; r < numRows; r++ {
		lb.Append(true)
		for k := 0; k < valsPerRow; k++ {
			vb.Append(int32((r + k) % 8)) // only 8 distinct values: dictionary stays
		}
	}
	arr := lb.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numRows))
	defer rec.Release()

	raw := writeRecordV2(t, rec, true, batchSize, pageSize)
	assertPagesStartOnRowBoundary(t, raw)
	requireRecordRoundTrips(t, arrowSchema, rec, raw)
}

// TestWriteSpacedV2NestedListRoundTrip locks in a value round-trip for a
// maxRep=2 column (list<list<int32>>) written through the spaced path under
// DataPageV2 with small pages, the deepest-nesting case punkeel and zeroshade
// flagged. Because the fix makes batch sizes variable, this guards against a
// value cursor drifting out of step with the levels.
func TestWriteSpacedV2NestedListRoundTrip(t *testing.T) {
	const (
		numRows   = 500
		batchSize = 7
		pageSize  = 512
	)

	mem := memory.NewGoAllocator()
	innerType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	outerType := arrow.ListOf(innerType)
	// Nullable inner element so the leaf takes the WriteBatchSpaced path.
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "l", Type: outerType, Nullable: true}}, nil)

	ob := array.NewBuilder(mem, outerType).(*array.ListBuilder)
	defer ob.Release()
	ib := ob.ValueBuilder().(*array.ListBuilder)
	vb := ib.ValueBuilder().(*array.Int32Builder)
	n := 0
	for r := 0; r < numRows; r++ {
		ob.Append(true)
		for i := 0; i < 3; i++ {
			ib.Append(true)
			for k := 0; k < 4; k++ {
				if k == 3 {
					vb.AppendNull() // an actual null element (spaced trigger)
				} else {
					vb.Append(int32(n))
				}
				n++
			}
		}
	}
	arr := ob.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(numRows))
	defer rec.Release()

	raw := writeRecordV2(t, rec, false, batchSize, pageSize)
	assertPagesStartOnRowBoundary(t, raw)
	requireRecordRoundTrips(t, arrowSchema, rec, raw)
}

// writeRecordV2 writes a single record batch as DataPageV2 with the given batch
// and page sizes, optionally keeping dictionary encoding on, and returns the
// parquet bytes.
func writeRecordV2(t *testing.T, rec arrow.RecordBatch, dictionary bool, batchSize, pageSize int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(dictionary),
		parquet.WithBatchSize(batchSize),
		parquet.WithDataPageSize(pageSize),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	)
	pqw, err := pqarrow.NewFileWriter(rec.Schema(), &buf, props, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)
	require.NoError(t, pqw.Write(rec))
	require.NoError(t, pqw.Close())
	return buf.Bytes()
}

// requireRecordRoundTrips reads the parquet bytes back through pqarrow and
// requires the single column to equal the source record's column.
func requireRecordRoundTrips(t *testing.T, arrowSchema *arrow.Schema, rec arrow.RecordBatch, raw []byte) {
	t.Helper()
	tbl, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(raw), nil, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	require.NoError(t, err)
	defer tbl.Release()

	require.EqualValues(t, rec.NumRows(), tbl.NumRows())
	want := rec.Column(0)

	// Concatenate the read-back chunks and equate with the source column.
	merged, err := array.Concatenate(tbl.Column(0).Data().Chunks(), memory.NewGoAllocator())
	require.NoError(t, err)
	defer merged.Release()
	require.Truef(t, array.Equal(want, merged), "round-trip mismatch\n want: %v\n got:  %v", want, merged)
}

// TestWriteBatchByteArrayV2OversizedRepLevels is a regression test for the
// byte-array and fixed-len-byte-array counterpart of #883's DataPageV2
// row-boundary alignment. Those writers run their own adaptive batching loop and
// passed the caller's full repLevels slice to alignBatchToRowBoundary, while the
// write length n is derived from defLevels/values. When repLevels is longer than
// n - which the low-level typed-writer API permits, and which
// columnWriter.doBatches already clamps against for the numeric/boolean paths -
// the alignment of a wide final row could grow the last batch past n, slicing
// defLevels/values out of range (recovered as an opaque error) or spilling extra
// levels into the column. The byte-array paths must clamp repLevels to n too.
func TestWriteBatchByteArrayV2OversizedRepLevels(t *testing.T) {
	const batchSize = 3

	// n = 6 requested levels. repLevels is deliberately longer (8) and its final
	// row is wide, so unclamped alignment would grow the last batch past n.
	// Clamped to repLevels[:6] = {0,1,1,0,1,1} the data is two rows of three.
	defLevels := []int16{1, 1, 1, 1, 1, 1}
	repLevels := []int16{0, 1, 1, 0, 1, 1, 1, 0}
	validBits := []byte{0xFF}
	wantVals := []string{"a", "b", "c", "d", "e", "f"}
	const wantRows = 2

	byteArrayVals := []parquet.ByteArray{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}
	flbaVals := []parquet.FixedLenByteArray{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithBatchSize(batchSize),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	)

	cases := []struct {
		name  string
		node  *schema.PrimitiveNode
		write func(cw file.ColumnChunkWriter) (int64, error)
		read  func(t *testing.T, ccr file.ColumnChunkReader) []string
	}{
		{
			name: "ByteArray/WriteBatch",
			node: schema.NewByteArrayNode("v", parquet.Repetitions.Repeated, -1),
			write: func(cw file.ColumnChunkWriter) (int64, error) {
				return cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(byteArrayVals, defLevels, repLevels)
			},
			read: readByteArrayValues,
		},
		{
			name: "ByteArray/WriteBatchSpaced",
			node: schema.NewByteArrayNode("v", parquet.Repetitions.Repeated, -1),
			write: func(cw file.ColumnChunkWriter) (int64, error) {
				return cw.(*file.ByteArrayColumnChunkWriter).WriteBatchSpacedWithError(byteArrayVals, defLevels, repLevels, validBits, 0)
			},
			read: readByteArrayValues,
		},
		{
			name: "FixedLenByteArray/WriteBatch",
			node: schema.NewFixedLenByteArrayNode("v", parquet.Repetitions.Repeated, 1, -1),
			write: func(cw file.ColumnChunkWriter) (int64, error) {
				return cw.(*file.FixedLenByteArrayColumnChunkWriter).WriteBatch(flbaVals, defLevels, repLevels)
			},
			read: readFixedLenByteArrayValues,
		},
		{
			name: "FixedLenByteArray/WriteBatchSpaced",
			node: schema.NewFixedLenByteArrayNode("v", parquet.Repetitions.Repeated, 1, -1),
			write: func(cw file.ColumnChunkWriter) (int64, error) {
				return cw.(*file.FixedLenByteArrayColumnChunkWriter).WriteBatchSpacedWithError(flbaVals, defLevels, repLevels, validBits, 0)
			},
			read: readFixedLenByteArrayValues,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{tc.node}, -1)
			require.NoError(t, err)

			var buf bytes.Buffer
			w := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
			rgw := w.AppendRowGroup()
			cw, err := rgw.NextColumn()
			require.NoError(t, err)

			valueOffset, err := tc.write(cw)
			require.NoError(t, err, "oversized repLevels must not fail the write")
			require.EqualValues(t, len(wantVals), valueOffset, "must write exactly the requested values, no spill past n")
			require.NoError(t, cw.Close())
			require.NoError(t, rgw.Close())
			require.NoError(t, w.Close())

			r, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			defer r.Close()

			require.EqualValues(t, wantRows, r.NumRows())
			cc, err := r.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			require.EqualValues(t, len(wantVals), cc.NumValues(), "column must hold exactly the requested values, no spill")

			ccr, err := r.RowGroup(0).Column(0)
			require.NoError(t, err)
			require.Equal(t, wantVals, tc.read(t, ccr))
		})
	}
}

func readByteArrayValues(t *testing.T, ccr file.ColumnChunkReader) []string {
	t.Helper()
	r := ccr.(*file.ByteArrayColumnChunkReader)
	vals := make([]parquet.ByteArray, 16)
	_, read, err := r.ReadBatch(int64(len(vals)), vals, nil, nil)
	require.NoError(t, err)
	out := make([]string, read)
	for i := 0; i < read; i++ {
		out[i] = string(vals[i])
	}
	return out
}

func readFixedLenByteArrayValues(t *testing.T, ccr file.ColumnChunkReader) []string {
	t.Helper()
	r := ccr.(*file.FixedLenByteArrayColumnChunkReader)
	vals := make([]parquet.FixedLenByteArray, 16)
	_, read, err := r.ReadBatch(int64(len(vals)), vals, nil, nil)
	require.NoError(t, err)
	out := make([]string, read)
	for i := 0; i < read; i++ {
		out[i] = string(vals[i])
	}
	return out
}
