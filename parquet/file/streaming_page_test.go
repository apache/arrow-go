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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func makeStreamTestValues(sizes []int) []parquet.ByteArray {
	values := make([]parquet.ByteArray, len(sizes))
	for i, sz := range sizes {
		buf := make([]byte, sz)
		buf[0] = byte(i)
		for j := 1; j < sz; j++ {
			buf[j] = byte(i*31 + j)
		}
		values[i] = buf
	}
	return values
}

func writeStreamTestColumn(t *testing.T, values []parquet.ByteArray, ver parquet.DataPageVersion, codec compress.Compression) []byte {
	t.Helper()
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false), // ensure PLAIN data pages
		parquet.WithDataPageVersion(ver),
		parquet.WithCompression(codec),
		parquet.WithDataPageSize(1024), // small pages -> multiple data pages
	)

	out := &bytes.Buffer{}
	w := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	return out.Bytes()
}

func readStreamTestColumn(t *testing.T, data []byte, streaming bool) []parquet.ByteArray {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.EnablePageStreaming = streaming

	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	n := int(rdr.NumRows())
	out := make([]parquet.ByteArray, n)
	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	total := 0
	for total < n {
		_, nv, err := bar.ReadBatch(int64(n-total), out[total:], nil, nil)
		require.NoError(t, err)
		if nv == 0 {
			break
		}
		total += nv
	}
	require.Equal(t, n, total)
	return out
}

// TestPageStreamingByteArrayRoundTrip verifies that reading a PLAIN BYTE_ARRAY
// column with EnablePageStreaming produces byte-identical values to the
// materialized path, across V1/V2 data pages and the streaming-eligible codecs,
// including values larger than the stream buffer and larger than a data page.
func TestPageStreamingByteArrayRoundTrip(t *testing.T) {
	// Varied sizes: several small, some > 4096 (stream buffer), some > 1024 (page).
	values := makeStreamTestValues([]int{65, 100, 5000, 200, 9000, 50, 4096, 1})

	cases := []struct {
		name  string
		ver   parquet.DataPageVersion
		codec compress.Compression
	}{
		{"v1-uncompressed", parquet.DataPageV1, compress.Codecs.Uncompressed},
		{"v1-zstd", parquet.DataPageV1, compress.Codecs.Zstd},
		{"v1-gzip", parquet.DataPageV1, compress.Codecs.Gzip},
		{"v2-uncompressed", parquet.DataPageV2, compress.Codecs.Uncompressed},
		{"v2-zstd", parquet.DataPageV2, compress.Codecs.Zstd},
		{"v2-brotli", parquet.DataPageV2, compress.Codecs.Brotli},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := writeStreamTestColumn(t, values, tc.ver, tc.codec)

			materialized := readStreamTestColumn(t, data, false)
			streamed := readStreamTestColumn(t, data, true)

			require.Len(t, materialized, len(values))
			for i := range values {
				require.Truef(t, bytes.Equal(values[i], materialized[i]), "materialized value %d mismatch", i)
				require.Truef(t, bytes.Equal(values[i], streamed[i]), "streamed value %d mismatch", i)
			}
		})
	}
}

// TestPageStreamingPublicPageReaderMaterializes verifies that the public
// GetColumnPageReader path never produces streaming pages even when
// EnablePageStreaming is set, so Page.Data() keeps returning the whole page.
func TestPageStreamingPublicPageReaderMaterializes(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000})
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.EnablePageStreaming = true
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)

	sawData := false
	for pr.Next() {
		dp, ok := pr.Page().(file.DataPage)
		if !ok {
			continue
		}
		sawData = true
		// A materialized page's Data() is the whole page; a streaming page would
		// only hold the (empty, required-column) level region.
		require.Equal(t, int(dp.UncompressedSize()), len(dp.Data()),
			"public GetColumnPageReader must materialize the full page")
	}
	require.NoError(t, pr.Err())
	require.True(t, sawData, "expected at least one data page")
}

func writeNullableStreamColumn(t *testing.T, values []parquet.ByteArray, defLevels []int16, ver parquet.DataPageVersion, codec compress.Compression) []byte {
	t.Helper()
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageVersion(ver),
		parquet.WithCompression(codec),
		parquet.WithDataPageSize(1024),
	)
	out := &bytes.Buffer{}
	w := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, defLevels, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	return out.Bytes()
}

func readNullableStreamColumn(t *testing.T, data []byte, streaming bool, numRows int) (defLevels []int16, values []parquet.ByteArray) {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.EnablePageStreaming = streaming
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	defLevels = make([]int16, 0, numRows)
	values = make([]parquet.ByteArray, 0, numRows)
	for len(defLevels) < numRows {
		vbuf := make([]parquet.ByteArray, numRows)
		dbuf := make([]int16, numRows)
		total, nv, err := bar.ReadBatch(int64(numRows-len(defLevels)), vbuf, dbuf, nil)
		require.NoError(t, err)
		if total == 0 {
			break
		}
		defLevels = append(defLevels, dbuf[:total]...)
		values = append(values, vbuf[:nv]...)
	}
	return defLevels, values
}

// TestPageStreamingByteArrayNullable exercises the V1 definition-level peel
// (readLevelData / RLE levels) and the streaming DecodeSpaced path for a nullable
// column, asserting streaming and materialized reads agree.
func TestPageStreamingByteArrayNullable(t *testing.T) {
	const numRows = 40
	defLevels := make([]int16, numRows)
	var values []parquet.ByteArray
	for i := 0; i < numRows; i++ {
		if i%3 == 0 {
			defLevels[i] = 0 // null
			continue
		}
		defLevels[i] = 1
		sz := 40 + i*120 // varied sizes, some > page / stream buffer
		buf := make([]byte, sz)
		buf[0] = byte(i)
		for j := 1; j < sz; j++ {
			buf[j] = byte(i*7 + j)
		}
		values = append(values, buf)
	}

	cases := []struct {
		name  string
		ver   parquet.DataPageVersion
		codec compress.Compression
	}{
		{"v1-zstd", parquet.DataPageV1, compress.Codecs.Zstd},
		{"v2-gzip", parquet.DataPageV2, compress.Codecs.Gzip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := writeNullableStreamColumn(t, values, defLevels, tc.ver, tc.codec)

			mDef, mVals := readNullableStreamColumn(t, data, false, numRows)
			sDef, sVals := readNullableStreamColumn(t, data, true, numRows)

			require.Equal(t, mDef, sDef, "def levels differ")
			require.Equal(t, defLevels, sDef, "streamed def levels wrong")
			require.Equal(t, len(values), len(sVals))
			for i := range values {
				require.Truef(t, bytes.Equal(mVals[i], sVals[i]), "value %d differs streaming vs materialized", i)
				require.Truef(t, bytes.Equal(values[i], sVals[i]), "streamed value %d wrong", i)
			}
		})
	}
}
