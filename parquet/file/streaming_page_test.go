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

func TestPageStreamingAllocatorBalance(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000, 50})
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)

	props := parquet.NewReaderProperties(checked)
	props.EnablePageStreaming = true
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	n := int(rdr.NumRows())
	out := make([]parquet.ByteArray, n)
	for total := 0; total < n; {
		_, nv, err := cr.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(n-total), out[total:], nil, nil)
		require.NoError(t, err)
		if nv == 0 {
			break
		}
		total += nv
	}
	require.NoError(t, cr.Close())
}

func TestPageStreamingIneligibleCodecFallback(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000, 50})

	for _, codec := range []compress.Compression{compress.Codecs.Snappy, compress.Codecs.Lz4Raw} {
		for _, ver := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			data := writeStreamTestColumn(t, values, ver, codec)

			// EnablePageStreaming is set, but Snappy/LZ4_RAW are not in the allowlist,
			// so pages must fall back to whole-page reads and match materialized.
			materialized := readStreamTestColumn(t, data, false)
			streamed := readStreamTestColumn(t, data, true)

			require.Len(t, streamed, len(values))
			for i := range values {
				require.Truef(t, bytes.Equal(values[i], streamed[i]), "codec %v ver %v value %d", codec, ver, i)
				require.Truef(t, bytes.Equal(materialized[i], streamed[i]), "codec %v ver %v value %d: streamed != materialized", codec, ver, i)
			}
		}
	}
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

func readStreamTestColumnSkip(t *testing.T, data []byte, streaming bool, skip int64) []parquet.ByteArray {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.EnablePageStreaming = streaming

	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	skipped, err := bar.Skip(skip)
	require.NoError(t, err)
	require.Equal(t, skip, skipped)

	n := int(rdr.NumRows()) - int(skip)
	out := make([]parquet.ByteArray, n)
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

// TestPageStreamingByteArraySkip exercises the streaming skip path (discardStreaming
// via Skip), asserting it agrees with the materialized read.
func TestPageStreamingByteArraySkip(t *testing.T) {
	// several small values share a page so the skip discards within a streaming page,
	// with some large values spanning pages too.
	values := makeStreamTestValues([]int{40, 50, 60, 70, 80, 90, 5000, 100, 9000, 40, 55, 66})
	const skip = int64(3)

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
			data := writeStreamTestColumn(t, values, tc.ver, tc.codec)

			materialized := readStreamTestColumnSkip(t, data, false, skip)
			streamed := readStreamTestColumnSkip(t, data, true, skip)

			require.Len(t, streamed, len(values)-int(skip))
			for i := range streamed {
				require.Truef(t, bytes.Equal(values[int(skip)+i], streamed[i]), "value %d after skip", i)
				require.Truef(t, bytes.Equal(materialized[i], streamed[i]), "value %d streamed != materialized", i)
			}
		})
	}
}

func writeRepeatedStreamColumn(t *testing.T, values []parquet.ByteArray, defLevels, repLevels []int16, ver parquet.DataPageVersion, codec compress.Compression) []byte {
	t.Helper()
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Repeated, parquet.Types.ByteArray, -1, -1)),
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
	_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, defLevels, repLevels)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	return out.Bytes()
}

func readRepeatedStreamColumn(t *testing.T, data []byte, streaming bool) (defLevels, repLevels []int16, values []parquet.ByteArray) {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.EnablePageStreaming = streaming
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	const cap = 64
	for {
		vbuf := make([]parquet.ByteArray, cap)
		dbuf := make([]int16, cap)
		rbuf := make([]int16, cap)
		total, nv, err := bar.ReadBatch(cap, vbuf, dbuf, rbuf)
		require.NoError(t, err)
		if total == 0 {
			break
		}
		defLevels = append(defLevels, dbuf[:total]...)
		repLevels = append(repLevels, rbuf[:total]...)
		values = append(values, vbuf[:nv]...)
	}
	return defLevels, repLevels, values
}

// TestPageStreamingRepeated covers a repeated column (maxRepLevel > 0), exercising
// the V1 rep-level peel in readLevelData.
func TestPageStreamingRepeated(t *testing.T) {
	// lists: [v0 v1 v2] [v3] [v4 v5]; rep levels mark list boundaries.
	values := makeStreamTestValues([]int{40, 5000, 60, 9000, 70, 80})
	repLevels := []int16{0, 1, 1, 0, 0, 1}
	defLevels := []int16{1, 1, 1, 1, 1, 1}

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
			data := writeRepeatedStreamColumn(t, values, defLevels, repLevels, tc.ver, tc.codec)

			mDef, mRep, mVals := readRepeatedStreamColumn(t, data, false)
			sDef, sRep, sVals := readRepeatedStreamColumn(t, data, true)

			require.Equal(t, defLevels, sDef)
			require.Equal(t, repLevels, sRep)
			require.Equal(t, mDef, sDef)
			require.Equal(t, mRep, sRep)
			require.Len(t, sVals, len(values))
			for i := range values {
				require.Truef(t, bytes.Equal(values[i], sVals[i]), "value %d", i)
				require.Truef(t, bytes.Equal(mVals[i], sVals[i]), "value %d streamed != materialized", i)
			}
		})
	}
}
