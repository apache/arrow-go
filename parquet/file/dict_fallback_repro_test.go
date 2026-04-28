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

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// TestDictFallbackDiscardsOrphanDict verifies parquet-mr parity: when dict
// encoding overflows before any dict-encoded data page has been flushed, the
// dictionary is discarded and the buffered indices are re-encoded as PLAIN.
// The resulting chunk must therefore contain zero dict bytes and match a
// dict-disabled baseline to within the tiny delta from differences in how
// rows are batched across pages.
func TestDictFallbackDiscardsOrphanDict(t *testing.T) {
	cases := []struct {
		name    string
		version parquet.Version
		codec   compress.Compression
	}{
		{"v1/uncompressed", parquet.V1_0, compress.Codecs.Uncompressed},
		{"v2/uncompressed", parquet.V2_LATEST, compress.Codecs.Uncompressed},
		{"v1/snappy", parquet.V1_0, compress.Codecs.Snappy},
		{"v2/snappy", parquet.V2_LATEST, compress.Codecs.Snappy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runDictFallbackNoDictPageCheck(t, tc.version, tc.codec)
		})
	}
}

func runDictFallbackNoDictPageCheck(t *testing.T, version parquet.Version, codec compress.Compression) {
	const (
		numValues         = 30000
		valueWidth        = 32
		dictPageSizeLimit = 256 * 1024
		dataPageSize      = 128 * 1024
	)

	values := highCardinalityStrings(numValues, valueWidth)
	knobs := writerKnobs{
		dictPageSizeLimit: dictPageSizeLimit,
		dataPageSize:      dataPageSize,
		codec:             codec,
	}

	knobsOn := knobs
	knobsOn.dictEnabled = true
	_, dictOn := writeByteArrayColumn(t, values, version, knobsOn)

	knobsOff := knobs
	knobsOff.dictEnabled = false
	_, dictOff := writeByteArrayColumn(t, values, version, knobsOff)

	t.Logf("version=%s codec=%s numValues=%d valueWidth=%d (raw≈%d KiB)",
		version, codec, numValues, valueWidth, numValues*valueWidth/1024)
	t.Logf("dict=ON  chunk.compressed=%d  chunk.uncompressed=%d", dictOn.totalCompressed, dictOn.totalUncompressed)
	t.Logf("dict=OFF chunk.compressed=%d  chunk.uncompressed=%d", dictOff.totalCompressed, dictOff.totalUncompressed)
	t.Logf("dict=ON encodings: %v", dictOn.encodings)
	t.Logf("dict=ON per-page breakdown:")
	for _, p := range dictOn.pages {
		t.Logf("  %-18s encoding=%-16s numValues=%-6d compressed=%d B",
			p.pageType, p.encoding, p.numValues, p.compressed)
	}

	require.Falsef(t, dictOn.hasDictionaryPage,
		"dictionary page should have been discarded; chunk=%d", dictOn.totalCompressed)
	require.Zero(t,
		dictOn.countDataPagesByEncoding(parquet.Encodings.PlainDict)+
			dictOn.countDataPagesByEncoding(parquet.Encodings.RLEDict),
		"no dict-encoded data pages expected after discard; encodings=%v", dictOn.encodings)
	require.NotContains(t, dictOn.encodings, parquet.Encodings.PlainDict,
		"encoding list should not advertise PLAIN_DICTIONARY when no dict page exists")
	require.NotContains(t, dictOn.encodings, parquet.Encodings.RLEDict,
		"encoding list should not advertise RLE_DICTIONARY when no dict page exists")

	// With the fix, dict=on should match dict=off to within the rounding from
	// first-page size differences (FallbackToPlain re-seeds the plain encoder
	// with all buffered values, producing one oversized first page).
	diff := dictOn.totalCompressed - dictOff.totalCompressed
	t.Logf("size delta dict=on vs dict=off: %d B", diff)
	require.LessOrEqualf(t, absInt64(diff), int64(valueWidth*2048),
		"post-fix: dict=on should closely match dict=off, got delta=%d", diff)
}

// TestDictFallbackKeepsDictWhenAlreadyFlushed covers the case where some
// dict-encoded data pages have already been emitted before the overflow —
// those must be preserved alongside the dict page, and the remainder of the
// column must switch to PLAIN. Matches parquet-mr's "initialUsedAndHadDictionary"
// path.
func TestDictFallbackKeepsDictWhenAlreadyFlushed(t *testing.T) {
	const (
		valueWidth        = 32
		dictPageSizeLimit = 8 * 1024
		dataPageSize      = 4 * 1024
	)

	// Mixed-cardinality data: a low-card prefix big enough that several
	// dict-encoded data pages are flushed (and pass the first-page
	// compression check) before a high-card tail overflows the dict and
	// triggers fallback. The committed dict pages must be kept and the
	// dictionary page preserved.
	lowCard := make([]parquet.ByteArray, 5000)
	for i := range lowCard {
		lowCard[i] = parquet.ByteArray(fmt.Sprintf("cat_%02d%*s", i%16, valueWidth-8, ""))
	}
	highCard := highCardinalityStrings(5000, valueWidth)
	values := append(lowCard, highCard...)

	_, chunk := writeByteArrayColumn(t, values, parquet.V1_0, writerKnobs{
		dictEnabled:       true,
		dictPageSizeLimit: dictPageSizeLimit,
		dataPageSize:      dataPageSize,
		codec:             compress.Codecs.Uncompressed,
	})

	t.Logf("encodings: %v  hasDictPage=%v  totalCompressed=%d",
		chunk.encodings, chunk.hasDictionaryPage, chunk.totalCompressed)

	require.True(t, chunk.hasDictionaryPage,
		"dict page must be kept when dict-encoded data pages were already flushed")

	dictPages := chunk.countDataPagesByEncoding(parquet.Encodings.PlainDict) +
		chunk.countDataPagesByEncoding(parquet.Encodings.RLEDict)
	plainPages := chunk.countDataPagesByEncoding(parquet.Encodings.Plain)
	require.Greater(t, dictPages, 0, "expected dict-encoded data pages to survive fallback")
	require.Greater(t, plainPages, 0, "expected PLAIN data pages after fallback")

	// All dict-encoded data pages must come before any PLAIN page — otherwise
	// the dictionary offset in the file footer would reference data pages
	// that can't be decoded against it.
	sawPlain := false
	for _, p := range chunk.pages {
		if p.pageType != format.PageType_DATA_PAGE && p.pageType != format.PageType_DATA_PAGE_V2 {
			continue
		}
		switch p.encoding {
		case parquet.Encodings.Plain:
			sawPlain = true
		case parquet.Encodings.PlainDict, parquet.Encodings.RLEDict:
			require.False(t, sawPlain,
				"dict-encoded data page appeared after a PLAIN page — wrong ordering")
		}
	}
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// TestDictFallbackMidCardinality exercises the case the iceberg-go TPC-DS
// benchmark flagged: mid-cardinality columns (ss_list_price etc.) where the
// dictionary grows slowly enough to pass the first-page compression check,
// eventually overflows, and leaves a dict page + dict-encoded pages + PLAIN
// pages stranded in one chunk. With dict-mode pages sized by raw bytes
// (matching parquet-mr), overflow happens at a similar cadence to plain
// encoding, so dict=on either stays competitive with dict=off or the
// committed dict pages are genuinely earning their keep.
func TestDictFallbackMidCardinality(t *testing.T) {
	const (
		numRows            = 200000
		valueWidth         = 8
		distinctValueCount = 20000
		dictPageSizeLimit  = 128 * 1024
		dataPageSize       = 128 * 1024
	)

	values := midCardinalityStrings(numRows, distinctValueCount, valueWidth)

	_, dictOn := writeByteArrayColumn(t, values, parquet.V1_0, writerKnobs{
		dictEnabled:       true,
		dictPageSizeLimit: dictPageSizeLimit,
		dataPageSize:      dataPageSize,
		codec:             compress.Codecs.Snappy,
	})
	_, dictOff := writeByteArrayColumn(t, values, parquet.V1_0, writerKnobs{
		dictEnabled:  false,
		dataPageSize: dataPageSize,
		codec:        compress.Codecs.Snappy,
	})

	t.Logf("mid-card: distinct=%d rows=%d width=%d", distinctValueCount, numRows, valueWidth)
	t.Logf("dict=ON  compressed=%d encodings=%v", dictOn.totalCompressed, dictOn.encodings)
	t.Logf("dict=OFF compressed=%d encodings=%v", dictOff.totalCompressed, dictOff.encodings)

	// The specific assertion: dict=on must not regress against dict=off by
	// more than a small constant. Before the raw-byte page cadence, this
	// scenario produced the 4-entry encoding layout and 20-30% bloat.
	require.LessOrEqualf(t,
		dictOn.totalCompressed, dictOff.totalCompressed+int64(numRows)/10,
		"dict=on (%d) must not balloon against dict=off (%d) on mid-card data",
		dictOn.totalCompressed, dictOff.totalCompressed)
}

func midCardinalityStrings(n, distinct, width int) []parquet.ByteArray {
	pool := make([]parquet.ByteArray, distinct)
	for i := range pool {
		b := make([]byte, width)
		for j := range width {
			b[j] = byte('A' + (i+j*7)%26)
		}
		tail := fmt.Appendf(nil, "-%05d", i)
		copy(b[width-len(tail):], tail)
		pool[i] = parquet.ByteArray(b)
	}
	out := make([]parquet.ByteArray, n)
	for i := range out {
		// Deterministic, roughly uniform distribution across the pool.
		out[i] = pool[(i*2654435761)%distinct]
	}
	return out
}

type writerKnobs struct {
	dictEnabled       bool
	dictPageSizeLimit int64
	dataPageSize      int64
	codec             compress.Compression
}

type pageInfo struct {
	pageType   format.PageType
	encoding   parquet.Encoding
	numValues  int32
	compressed int
}

type chunkSummary struct {
	totalCompressed   int64
	totalUncompressed int64
	encodings         []parquet.Encoding
	encodingStats     []pageEncodingStatsEntry
	pages             []pageInfo
	hasDictionaryPage bool
}

type pageEncodingStatsEntry struct {
	PageType format.PageType
	Encoding parquet.Encoding
}

func (c *chunkSummary) countDataPagesByEncoding(enc parquet.Encoding) int {
	n := 0
	for _, p := range c.pages {
		if p.encoding == enc && (p.pageType == format.PageType_DATA_PAGE || p.pageType == format.PageType_DATA_PAGE_V2) {
			n++
		}
	}
	return n
}

func writeByteArrayColumn(t *testing.T, values []parquet.ByteArray, version parquet.Version, knobs writerKnobs) ([]byte, *chunkSummary) {
	t.Helper()

	root := mustByteArraySchema(t)
	opts := []parquet.WriterProperty{
		parquet.WithVersion(version),
		parquet.WithDictionaryDefault(knobs.dictEnabled),
		parquet.WithCompression(knobs.codec),
	}
	if knobs.dictPageSizeLimit > 0 {
		opts = append(opts, parquet.WithDictionaryPageSizeLimit(knobs.dictPageSizeLimit))
	}
	if knobs.dataPageSize > 0 {
		opts = append(opts, parquet.WithDataPageSize(knobs.dataPageSize))
	}
	if version == parquet.V2_LATEST {
		opts = append(opts, parquet.WithDataPageVersion(parquet.DataPageV2))
	}
	props := parquet.NewWriterProperties(opts...)

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	return buf.Bytes(), summarizeFirstColumnChunk(t, buf.Bytes())
}

func summarizeFirstColumnChunk(t *testing.T, raw []byte) *chunkSummary {
	t.Helper()

	r, err := file.NewParquetReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer r.Close()

	md, err := r.MetaData().RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)
	s := &chunkSummary{
		totalCompressed:   md.TotalCompressedSize(),
		totalUncompressed: md.TotalUncompressedSize(),
		encodings:         md.Encodings(),
		hasDictionaryPage: md.HasDictionaryPage(),
	}
	for _, es := range md.EncodingStats() {
		s.encodingStats = append(s.encodingStats, pageEncodingStatsEntry{
			PageType: es.PageType,
			Encoding: es.Encoding,
		})
	}

	rg := r.RowGroup(0)
	pr, err := rg.GetColumnPageReader(0)
	require.NoError(t, err)
	for pr.Next() {
		page := pr.Page()
		s.pages = append(s.pages, pageInfo{
			pageType:   format.PageType(page.Type()),
			encoding:   parquet.Encoding(page.Encoding()),
			numValues:  page.NumValues(),
			compressed: len(page.Data()),
		})
	}
	require.NoError(t, pr.Err())
	return s
}

func mustByteArraySchema(t *testing.T) *schema.GroupNode {
	t.Helper()
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewByteArrayNode("v", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	return root
}

func highCardinalityStrings(n, width int) []parquet.ByteArray {
	out := make([]parquet.ByteArray, n)
	for i := range out {
		b := make([]byte, width)
		for j := range width {
			b[j] = byte('A' + (i+j*31)%26)
		}
		tail := fmt.Appendf(nil, "-%07d", i)
		copy(b[width-len(tail):], tail)
		out[i] = parquet.ByteArray(b)
	}
	return out
}
