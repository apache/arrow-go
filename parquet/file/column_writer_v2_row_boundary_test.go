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
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
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
