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
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

// TestWriteSpacedV2NullableListWideRow covers punkeel's long-list case: a single
// nullable list whose length (20001) exceeds the write-batch size (20000). There
// is no row boundary before the requested split, so the fix must grow the batch
// forward to keep the whole row in one page rather than shrinking to zero and
// looping without progress. A fixed-width element type is used because it makes
// the pre-fix failure deterministic: the batch shrinks to zero and the writer
// hangs (a byte-array element would instead silently clamp to one, which the
// timeout would not catch). Only one row exists, so it can never be split: it
// must round-trip and occupy exactly one DataPageV2 that starts at a row
// boundary.
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
// fails the test instead of hanging the whole suite.
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
