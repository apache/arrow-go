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
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// TestMain lowers the streaming page-size threshold so the streaming tests exercise
// the streaming path on their small (fast) pages.
func TestMain(m *testing.M) {
	*file.StreamingThreshold = 0
	os.Exit(m.Run())
}

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

func writeByteArrayStream(t *testing.T, values []parquet.ByteArray, defLevels, repLevels []int16, rep parquet.Repetition, ver parquet.DataPageVersion, codec compress.Compression, pageSize int64) []byte {
	t.Helper()
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", rep, parquet.Types.ByteArray, -1, -1)),
	}, -1)))
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false), // ensure PLAIN data pages
		parquet.WithDataPageVersion(ver),
		parquet.WithCompression(codec),
		parquet.WithDataPageSize(pageSize),
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

// writeStreamTestColumn writes a Required byte-array column with small (1 KiB) pages.
func writeStreamTestColumn(t *testing.T, values []parquet.ByteArray, ver parquet.DataPageVersion, codec compress.Compression) []byte {
	return writeByteArrayStream(t, values, nil, nil, parquet.Repetitions.Required, ver, codec, 1024)
}

// collectByteArray reads exactly n values from bar via repeated ReadBatch calls.
func collectByteArray(t *testing.T, bar *file.ByteArrayColumnChunkReader, n int) []parquet.ByteArray {
	t.Helper()
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

func readStreamTestColumn(t *testing.T, data []byte, streaming bool) []parquet.ByteArray {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = streaming

	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)
	return collectByteArray(t, bar, int(rdr.NumRows()))
}

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

// TestPageStreamingEngaged guards against eligibility silently falling back to the
// whole-page read: a required column's streaming page holds only the (empty) level
// region, while the materialized page's Data() is the full uncompressed page.
func TestPageStreamingEngaged(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000, 50})
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	firstPageDataLen := func(streaming bool) int {
		props := parquet.NewReaderProperties(memory.DefaultAllocator)
		props.PageStreamingEnabled = streaming
		rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
		require.NoError(t, err)
		defer rdr.Close()

		pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
		require.NoError(t, err)
		defer pr.Close()
		require.True(t, pr.Next())
		return len(pr.Page().Data())
	}

	require.Zero(t, firstPageDataLen(true), "streaming page Data() should hold only the empty level region")
	require.NotZero(t, firstPageDataLen(false), "materialized page Data() should be the whole page")
}

func TestPageStreamingAllocatorBalance(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000, 50})
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)

	props := parquet.NewReaderProperties(checked)
	props.PageStreamingEnabled = true
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

func TestPageStreamingLeveledAllocatorBalance(t *testing.T) {
	nullDef := make([]int16, 40)
	var nullVals []parquet.ByteArray
	for i := 0; i < 40; i++ {
		if i%3 == 0 {
			continue
		}
		nullDef[i] = 1
		buf := make([]byte, 40+i*120)
		for j := range buf {
			buf[j] = byte(i*7 + j)
		}
		nullVals = append(nullVals, buf)
	}

	cases := []struct {
		name      string
		rep       parquet.Repetition
		values    []parquet.ByteArray
		defLevels []int16
		repLevels []int16
	}{
		{"nullable", parquet.Repetitions.Optional, nullVals, nullDef, nil},
		{"repeated", parquet.Repetitions.Repeated,
			makeStreamTestValues([]int{40, 5000, 60, 9000, 70, 80}),
			[]int16{1, 1, 1, 1, 1, 1}, []int16{0, 1, 1, 0, 0, 1}},
	}
	for _, tc := range cases {
		for _, ver := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			t.Run(fmt.Sprintf("%s/v%d", tc.name, ver+1), func(t *testing.T) {
				data := writeByteArrayStream(t, tc.values, tc.defLevels, tc.repLevels, tc.rep, ver, compress.Codecs.Zstd, 1024)

				checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer checked.AssertSize(t, 0)

				props := parquet.NewReaderProperties(checked)
				props.PageStreamingEnabled = true
				rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
				require.NoError(t, err)
				defer rdr.Close()

				cr, err := rdr.RowGroup(0).Column(0)
				require.NoError(t, err)
				bar := cr.(*file.ByteArrayColumnChunkReader)
				vbuf := make([]parquet.ByteArray, 64)
				dbuf := make([]int16, 64)
				rbuf := make([]int16, 64)
				for {
					total, _, err := bar.ReadBatch(64, vbuf, dbuf, rbuf)
					require.NoError(t, err)
					if total == 0 {
						break
					}
				}
				require.NoError(t, cr.Close())
			})
		}
	}
}

func TestPageStreamingIneligibleCodecFallback(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200, 9000, 50})

	for _, codec := range []compress.Compression{compress.Codecs.Snappy, compress.Codecs.Lz4Raw} {
		for _, ver := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			data := writeStreamTestColumn(t, values, ver, codec)

			// PageStreamingEnabled is set, but Snappy/LZ4_RAW are not in the allowlist,
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

func readNullableStreamColumn(t *testing.T, data []byte, streaming bool, numRows int) (defLevels []int16, values []parquet.ByteArray) {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = streaming
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
			data := writeByteArrayStream(t, values, defLevels, nil, parquet.Repetitions.Optional, tc.ver, tc.codec, 1024)

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
	props.PageStreamingEnabled = streaming

	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	skipped, err := bar.Skip(skip)
	require.NoError(t, err)
	require.Equal(t, skip, skipped)

	return collectByteArray(t, bar, int(rdr.NumRows())-int(skip))
}

func TestPageStreamingByteArraySkip(t *testing.T) {
	// several small values share a page so the skip discards within a streaming page,
	// with some large values spanning pages too.
	values := makeStreamTestValues([]int{40, 50, 60, 70, 80, 90, 5000, 100, 9000, 40, 55, 66})

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

			for _, skip := range []int64{3, 8} { // 3 within page 1; 8 crosses page boundaries
				materialized := readStreamTestColumnSkip(t, data, false, skip)
				streamed := readStreamTestColumnSkip(t, data, true, skip)

				require.Len(t, streamed, len(values)-int(skip))
				for i := range streamed {
					require.Truef(t, bytes.Equal(values[int(skip)+i], streamed[i]), "skip %d value %d", skip, i)
					require.Truef(t, bytes.Equal(materialized[i], streamed[i]), "skip %d value %d streamed != materialized", skip, i)
				}
			}
		})
	}
}

func readRepeatedStreamColumn(t *testing.T, data []byte, streaming bool) (defLevels, repLevels []int16, values []parquet.ByteArray) {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = streaming
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	const batchSize = 64
	for {
		vbuf := make([]parquet.ByteArray, batchSize)
		dbuf := make([]int16, batchSize)
		rbuf := make([]int16, batchSize)
		total, nv, err := bar.ReadBatch(batchSize, vbuf, dbuf, rbuf)
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
			data := writeByteArrayStream(t, values, defLevels, repLevels, parquet.Repetitions.Repeated, tc.ver, tc.codec, 1024)

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

func readStreamTestColumnSeekAfterRead(t *testing.T, data []byte, streaming bool, preRead int, seekRow int64) []parquet.ByteArray {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = streaming

	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	// Read a few values first so a streaming page is current and partially consumed
	// when the seek repositions the underlying reader.
	if preRead > 0 {
		pre := make([]parquet.ByteArray, preRead)
		_, _, err := bar.ReadBatch(int64(preRead), pre, nil, nil)
		require.NoError(t, err)
	}
	require.NoError(t, bar.SeekToRow(seekRow))

	return collectByteArray(t, bar, int(rdr.NumRows())-int(seekRow))
}

func TestPageStreamingSeekAfterRead(t *testing.T) {
	values := makeStreamTestValues([]int{40, 50, 60, 70, 5000, 90, 9000, 40, 55, 66, 77, 88})
	const (
		preRead = 2
		seekRow = int64(8)
	)

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

			mat := readStreamTestColumnSeekAfterRead(t, data, false, preRead, seekRow)
			str := readStreamTestColumnSeekAfterRead(t, data, true, preRead, seekRow)

			require.Len(t, str, len(values)-int(seekRow))
			for i := range str {
				require.Truef(t, bytes.Equal(values[int(seekRow)+i], str[i]), "value %d after seek", i)
				require.Truef(t, bytes.Equal(mat[i], str[i]), "value %d streamed != materialized", i)
			}
		})
	}
}

// Large pages (> stream buffer) partially read leave undrained bytes at SeekToRow —
// the case that actually triggers the seek drain fix (unlike the small-page test).
func TestPageStreamingSeekLargePagePartial(t *testing.T) {
	sizes := make([]int, 40)
	for i := range sizes {
		sizes[i] = 64 << 10 // 64 KiB -> ~16 values per >1 MiB page
	}
	values := makeStreamTestValues(sizes)
	data := writeByteArrayStream(t, values, nil, nil, parquet.Repetitions.Required, parquet.DataPageV1, compress.Codecs.Uncompressed, 1<<20)

	const (
		preRead = 20 // into page 2
		seekRow = int64(35)
	)
	mat := readStreamTestColumnSeekAfterRead(t, data, false, preRead, seekRow)
	str := readStreamTestColumnSeekAfterRead(t, data, true, preRead, seekRow)

	require.Len(t, str, len(values)-int(seekRow))
	for i := range str {
		require.Truef(t, bytes.Equal(values[int(seekRow)+i], str[i]), "value %d after seek", i)
		require.Truef(t, bytes.Equal(mat[i], str[i]), "value %d streamed != materialized", i)
	}
}

func TestPageStreamingPageReaderCloseFreesStream(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200})
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)

	props := parquet.NewReaderProperties(checked)
	props.PageStreamingEnabled = true
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	require.True(t, pr.Next()) // advance to a streaming data page
	require.NoError(t, pr.Close())
}

func makeFLBAValues(count, width int) []parquet.FixedLenByteArray {
	vals := make([]parquet.FixedLenByteArray, count)
	for i := range vals {
		b := make([]byte, width)
		b[0] = byte(i)
		for j := 1; j < width; j++ {
			b[j] = byte(i*13 + j)
		}
		vals[i] = b
	}
	return vals
}

func writeFLBAStreamColumn(t *testing.T, values []parquet.FixedLenByteArray, width int, ver parquet.DataPageVersion, codec compress.Compression) []byte {
	t.Helper()
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("data", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, -1, int32(width))),
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
	_, err = cw.(*file.FixedLenByteArrayColumnChunkWriter).WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	return out.Bytes()
}

func readFLBAStreamColumnSkip(t *testing.T, data []byte, streaming bool, skip int64) []parquet.FixedLenByteArray {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = streaming
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.FixedLenByteArrayColumnChunkReader)
	if skip > 0 {
		skipped, err := bar.Skip(skip)
		require.NoError(t, err)
		require.Equal(t, skip, skipped)
	}
	n := int(rdr.NumRows()) - int(skip)
	out := make([]parquet.FixedLenByteArray, n)
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

func TestPageStreamingFLBASkip(t *testing.T) {
	const width = 24
	values := makeFLBAValues(80, width) // multiple 1 KiB pages

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
			data := writeFLBAStreamColumn(t, values, width, tc.ver, tc.codec)

			full := readFLBAStreamColumnSkip(t, data, true, 0)
			require.Len(t, full, len(values))
			for i := range values {
				require.Truef(t, bytes.Equal(values[i], full[i]), "roundtrip value %d", i)
			}

			for _, skip := range []int64{5, 50} { // 50 crosses a page boundary
				mat := readFLBAStreamColumnSkip(t, data, false, skip)
				str := readFLBAStreamColumnSkip(t, data, true, skip)
				require.Len(t, str, len(values)-int(skip))
				for i := range str {
					require.Truef(t, bytes.Equal(values[int(skip)+i], str[i]), "skip %d value %d", skip, i)
					require.Truef(t, bytes.Equal(mat[i], str[i]), "skip %d value %d streamed != materialized", skip, i)
				}
			}
		})
	}
}

// TestPageStreamingPeakMemoryBounded is the core memory-goal check: a single page
// far larger than the stream buffer, read in small batches, must keep the
// allocator footprint near the buffer, not grow to the whole page.
func TestPageStreamingPeakMemoryBounded(t *testing.T) {
	sizes := make([]int, 2000)
	for i := range sizes {
		sizes[i] = 2048 // ~4 MiB of values in one page
	}
	values := makeStreamTestValues(sizes)
	data := writeByteArrayStream(t, values, nil, nil, parquet.Repetitions.Required, parquet.DataPageV1, compress.Codecs.Zstd, 8<<20)

	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)
	props := parquet.NewReaderProperties(checked)
	props.PageStreamingEnabled = true
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	const batch = 64
	out := make([]parquet.ByteArray, batch)
	peak, total := 0, 0
	for {
		_, nv, err := bar.ReadBatch(batch, out, nil, nil)
		require.NoError(t, err)
		if nv == 0 {
			break
		}
		total += nv
		if a := checked.CurrentAlloc(); a > peak {
			peak = a
		}
	}
	require.NoError(t, bar.Close())
	require.Equal(t, len(values), total)

	t.Logf("peak allocator use: %d bytes (page ~%d bytes)", peak, len(values)*2048)
	require.Less(t, peak, 2<<20, "streaming peak should stay near the buffer, not the page")
}

func readInPage(t *testing.T, data []byte, batch int64) (defLevels, repLevels []int16, values []parquet.ByteArray) {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.PageStreamingEnabled = true
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	defer rdr.Close()

	cr, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	bar := cr.(*file.ByteArrayColumnChunkReader)

	dbuf := make([]int16, batch)
	rbuf := make([]int16, batch)
	vbuf := make([]parquet.ByteArray, batch)
	for {
		total, nv, err := bar.ReadBatchInPage(batch, vbuf, dbuf, rbuf)
		require.NoError(t, err)
		if total == 0 {
			break
		}
		defLevels = append(defLevels, dbuf[:total]...)
		repLevels = append(repLevels, rbuf[:total]...)
		for i := 0; i < nv; i++ {
			values = append(values, append([]byte(nil), vbuf[i]...)) // clone the alias
		}
	}
	return defLevels, repLevels, values
}

// Large values force per-page batch clipping (multiple Decodes) on a nullable column.
func TestPageStreamingReadBatchInPageNullable(t *testing.T) {
	const numRows = 60
	defLevels := make([]int16, numRows)
	var values []parquet.ByteArray
	for i := 0; i < numRows; i++ {
		if i%3 == 0 {
			continue // null
		}
		defLevels[i] = 1
		sz := 80 << 10 // 80 KiB: several per 1 MiB window, so DecodeStreaming returns short
		b := make([]byte, sz)
		b[0] = byte(i)
		for j := 1; j < sz; j++ {
			b[j] = byte(i*7 + j)
		}
		values = append(values, b)
	}

	for _, tc := range []struct {
		name  string
		ver   parquet.DataPageVersion
		codec compress.Compression
	}{
		{"v1-zstd", parquet.DataPageV1, compress.Codecs.Zstd},
		{"v2-gzip", parquet.DataPageV2, compress.Codecs.Gzip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := writeByteArrayStream(t, values, defLevels, nil, parquet.Repetitions.Optional, tc.ver, tc.codec, 8<<20)
			for _, batch := range []int64{7, 64} {
				gotDef, _, gotVals := readInPage(t, data, batch)
				require.Equalf(t, defLevels, gotDef, "batch %d def levels", batch)
				require.Lenf(t, gotVals, len(values), "batch %d value count", batch)
				for i := range values {
					require.Truef(t, bytes.Equal(values[i], gotVals[i]), "batch %d value %d", batch, i)
				}
			}
		})
	}
}

func TestPageStreamingReadBatchInPageRequired(t *testing.T) {
	sizes := make([]int, 40)
	for i := range sizes {
		sizes[i] = 80 << 10 // large, so DecodeStreaming returns short against the window
	}
	values := makeStreamTestValues(sizes)
	data := writeByteArrayStream(t, values, nil, nil, parquet.Repetitions.Required, parquet.DataPageV1, compress.Codecs.Zstd, 8<<20)

	for _, batch := range []int64{5, 64} {
		_, _, got := readInPage(t, data, batch)
		require.Lenf(t, got, len(values), "batch %d value count", batch)
		for i := range values {
			require.Truef(t, bytes.Equal(values[i], got[i]), "batch %d value %d", batch, i)
		}
	}
}

func TestPageStreamingReadBatchInPageRepeated(t *testing.T) {
	sizes := make([]int, 30)
	for i := range sizes {
		sizes[i] = 80 << 10
	}
	values := makeStreamTestValues(sizes)
	defLevels := make([]int16, len(values))
	repLevels := make([]int16, len(values))
	for i := range values {
		defLevels[i] = 1
		if i%3 != 0 {
			repLevels[i] = 1 // 0 starts a new list, 1 continues it
		}
	}
	data := writeByteArrayStream(t, values, defLevels, repLevels, parquet.Repetitions.Repeated, parquet.DataPageV1, compress.Codecs.Zstd, 8<<20)

	for _, batch := range []int64{7, 64} {
		gotDef, gotRep, gotVals := readInPage(t, data, batch)
		require.Equalf(t, defLevels, gotDef, "batch %d def levels", batch)
		require.Equalf(t, repLevels, gotRep, "batch %d rep levels", batch)
		require.Lenf(t, gotVals, len(values), "batch %d value count", batch)
		for i := range values {
			require.Truef(t, bytes.Equal(values[i], gotVals[i]), "batch %d value %d", batch, i)
		}
	}
}

// TestPageStreamingThreshold verifies streamingThreshold gates streaming: a page below
// the threshold falls back to a whole-page read.
func TestPageStreamingThreshold(t *testing.T) {
	values := makeStreamTestValues([]int{65, 5000, 200}) // small page, well under 1 MiB
	data := writeStreamTestColumn(t, values, parquet.DataPageV1, compress.Codecs.Zstd)

	firstPageDataLen := func(threshold int64) int {
		defer func(old int64) { *file.StreamingThreshold = old }(*file.StreamingThreshold)
		*file.StreamingThreshold = threshold

		props := parquet.NewReaderProperties(memory.DefaultAllocator)
		props.PageStreamingEnabled = true
		rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
		require.NoError(t, err)
		defer rdr.Close()

		pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
		require.NoError(t, err)
		defer pr.Close()
		require.True(t, pr.Next())
		return len(pr.Page().Data())
	}

	require.NotZero(t, firstPageDataLen(1<<20), "small page must not stream at the 1 MiB threshold")
	require.Zero(t, firstPageDataLen(0), "with threshold 0 the small page streams (empty level region)")
}

// TestPageStreamingRecordReader drives the record reader (the pqarrow read path) over a
// streaming column in small batches, so a Decode's aliased values must survive until the
// builder copies them and be reclaimed by the next Decode's Recycle. Large values force
// chunk rotation, so a broken alias contract would corrupt the output.
func TestPageStreamingRecordReader(t *testing.T) {
	cases := []struct {
		name     string
		rep      parquet.Repetition
		defLevel int16
	}{
		{"required", parquet.Repetitions.Required, 0},
		{"optional", parquet.Repetitions.Optional, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const nrows = 300
			var (
				sizes     []int
				defLevels []int16
				wantNull  []bool
			)
			for i := 0; i < nrows; i++ {
				if tc.defLevel == 1 && i%5 == 0 {
					defLevels = append(defLevels, 0)
					wantNull = append(wantNull, true)
					continue
				}
				sizes = append(sizes, 3000) // large enough to force chunk rotation
				defLevels = append(defLevels, tc.defLevel)
				wantNull = append(wantNull, false)
			}
			values := makeStreamTestValues(sizes)
			var dl []int16
			if tc.defLevel == 1 {
				dl = defLevels
			}
			data := writeByteArrayStream(t, values, dl, nil, tc.rep, parquet.DataPageV1, compress.Codecs.Zstd, 8<<20)

			props := parquet.NewReaderProperties(memory.DefaultAllocator)
			props.PageStreamingEnabled = true
			rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
			require.NoError(t, err)
			defer rdr.Close()

			pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
			require.NoError(t, err)
			descr := rdr.MetaData().Schema.Column(0)

			rr := file.NewRecordReader(descr, file.LevelInfo{NullSlotUsage: 1, DefLevel: tc.defLevel}, arrow.BinaryTypes.Binary, memory.DefaultAllocator, rdr.BufferPool())
			defer rr.Release()
			rr.SetPageReader(pr)
			rr.Reset()
			require.NoError(t, rr.Reserve(nrows))
			for rr.HasMore() {
				read, err := rr.ReadRecords(37) // small batch => many Decode/Recycle boundaries
				require.NoError(t, err)
				if read == 0 {
					break
				}
			}

			var got []parquet.ByteArray
			var gotNull []bool
			for _, chunk := range rr.(file.BinaryRecordReader).GetBuilderChunks() {
				bin := chunk.(*array.Binary)
				for i := 0; i < bin.Len(); i++ {
					gotNull = append(gotNull, bin.IsNull(i))
					got = append(got, bin.Value(i))
				}
				chunk.Release()
			}

			require.Equal(t, nrows, len(got))
			require.Equal(t, wantNull, gotNull)
			vi := 0
			for i := 0; i < nrows; i++ {
				if wantNull[i] {
					continue
				}
				require.Truef(t, bytes.Equal(values[vi], got[i]), "value at row %d corrupted", i)
				vi++
			}
		})
	}
}
