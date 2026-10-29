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
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// writeALPFile writes one row group and returns a reader owned by the caller.
// The callback writes each column; options can override the ALP defaults.
func writeALPFile(t *testing.T, mem memory.Allocator, fields schema.FieldList, writeColumn func(file.ColumnChunkWriter), opts ...parquet.WriterProperty) *file.Reader {
	t.Helper()

	opts = append([]parquet.WriterProperty{
		parquet.WithAllocator(mem),
		parquet.WithEncoding(parquet.Encodings.ALP),
		parquet.WithALPEncoding(true),
		parquet.WithDictionaryDefault(false),
	}, opts...)
	sc, err := schema.NewGroupNode("test", parquet.Repetitions.Required, fields, -1)
	require.NoError(t, err)

	var sink bytes.Buffer
	writer := file.NewParquetWriter(&sink, sc, file.WithWriterProps(parquet.NewWriterProperties(opts...)))
	defer writer.Close()
	rgw := writer.AppendRowGroup()
	for range fields {
		cw, err := rgw.NextColumn()
		require.NoError(t, err)
		writeColumn(cw)
		require.NoError(t, cw.Close())
	}
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	require.Equal(t, 1, reader.NumRowGroups())
	return reader
}

// readALPDoubleColumn returns packed non-null values and, for optional columns,
// definition levels. Row and value offsets advance independently across batches.
func readALPDoubleColumn(t *testing.T, reader *file.Reader, col, batchSize int) ([]float64, []int16) {
	t.Helper()

	cr, err := reader.RowGroup(0).Column(col)
	require.NoError(t, err)
	defer cr.Close()
	r := cr.(*file.Float64ColumnChunkReader)

	rows := int(reader.NumRows())
	out := make([]float64, rows)
	var outDef []int16
	if cr.Descriptor().MaxDefinitionLevel() > 0 {
		outDef = make([]int16, rows)
		for i := range outDef {
			outDef[i] = -1
		}
	}
	valuesRead := 0
	for rowsRead := 0; rowsRead < rows; {
		var def []int16
		if outDef != nil {
			def = outDef[rowsRead:]
		}
		batch := min(batchSize, rows-rowsRead)
		total, n, err := r.ReadBatch(int64(batch), out[valuesRead:], def, nil)
		require.NoError(t, err)
		require.NoError(t, r.Err())
		require.EqualValues(t, batch, total, "read stalled at row %d", rowsRead)
		rowsRead += int(total)
		valuesRead += n
	}
	require.False(t, r.HasNext())
	require.NoError(t, r.Err())
	return out[:valuesRead], outDef
}

func assertALPDataPageEncoding(t *testing.T, chunk *metadata.ColumnChunkMetaData) {
	t.Helper()

	dataPageEntries := 0
	for _, stat := range chunk.EncodingStats() {
		if stat.PageType == file.PageTypeDataPage || stat.PageType == file.PageTypeDataPageV2 {
			dataPageEntries++
			require.Equal(t, parquet.Encodings.ALP, stat.Encoding)
		}
	}
	require.Positive(t, dataPageEntries, "missing data-page encoding statistics")
}

// An all-null page contains levels but no encoded values. PLAIN is the control.
func TestALPAllNullPage(t *testing.T) {
	for _, enc := range []parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.ALP} {
		t.Run(enc.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			const rows = 8
			defLevels := make([]int16, rows)
			reader := writeALPFile(t, mem, schema.FieldList{
				schema.NewFloat64Node("value", parquet.Repetitions.Optional, -1),
			}, func(cw file.ColumnChunkWriter) {
				n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(nil, defLevels, nil)
				require.NoError(t, err)
				require.Zero(t, n)
			}, parquet.WithEncoding(enc))
			defer reader.Close()
			require.EqualValues(t, rows, reader.NumRows())
			if enc == parquet.Encodings.ALP {
				chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
				require.NoError(t, err)
				assertALPDataPageEncoding(t, chunk)
			}

			out, def := readALPDoubleColumn(t, reader, 0, rows)
			require.Empty(t, out)
			require.Equal(t, defLevels, def)
		})
	}
}

// Keep the packed path covered alongside PutSpaced and DecodeSpaced. The latter
// is reached through RecordReader; column ReadBatch only returns packed values.
func TestALPNullsScattered(t *testing.T) {
	const (
		rows            = 3000
		validBitsOffset = 3
	)
	values := make([]float64, rows)
	packed := make([]float64, 0, rows)
	defLevels := make([]int16, rows)
	validBits := make([]byte, bitutil.BytesForBits(rows+validBitsOffset))
	for i := range rows {
		if i%3 == 0 {
			values[i] = -9999 // A null slot must not enter the encoded values.
			continue
		}
		defLevels[i] = 1
		bitutil.SetBit(validBits, i+validBitsOffset)
		values[i] = float64(i) / 100
		switch i {
		case 1:
			values[i] = math.Copysign(0, -1)
		case 2:
			values[i] = math.Float64frombits(0x7ff8000000001234)
		case 4:
			values[i] = math.Inf(1)
		case 5:
			values[i] = math.SmallestNonzeroFloat64
		}
		packed = append(packed, values[i])
	}

	for _, tc := range []struct {
		name   string
		spaced bool
	}{
		{"packed", false},
		{"spaced", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			reader := writeALPFile(t, mem, schema.FieldList{
				schema.NewFloat64Node("value", parquet.Repetitions.Optional, -1),
			}, func(cw file.ColumnChunkWriter) {
				w := cw.(*file.Float64ColumnChunkWriter)
				if tc.spaced {
					n, err := w.WriteBatchSpacedWithError(values, defLevels, nil, validBits, validBitsOffset)
					require.NoError(t, err)
					require.EqualValues(t, rows, n)
				} else {
					n, err := w.WriteBatch(packed, defLevels, nil)
					require.NoError(t, err)
					require.EqualValues(t, len(packed), n)
				}
			})
			defer reader.Close()
			require.EqualValues(t, rows, reader.NumRows())
			chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
			require.NoError(t, err)
			assertALPDataPageEncoding(t, chunk)

			gotPacked, gotDef := readALPDoubleColumn(t, reader, 0, 513)
			require.Equal(t, defLevels, gotDef)
			require.Len(t, gotPacked, len(packed))
			for i := range packed {
				require.Equal(t, math.Float64bits(packed[i]), math.Float64bits(gotPacked[i]), "value %d", i)
			}
			if !tc.spaced {
				return
			}

			pages, err := reader.RowGroup(0).GetColumnPageReader(0)
			require.NoError(t, err)
			rr := file.NewRecordReader(reader.MetaData().Schema.Column(0),
				file.LevelInfo{NullSlotUsage: 1, DefLevel: 1}, arrow.PrimitiveTypes.Float64, mem, reader.BufferPool())
			defer rr.Release()
			rr.SetPageReader(pages)
			require.NoError(t, rr.Reserve(rows))
			n, err := rr.ReadRecords(rows)
			require.NoError(t, err)
			require.EqualValues(t, rows, n)
			require.Equal(t, rows, rr.ValuesWritten())
			require.EqualValues(t, rows-len(packed), rr.NullCount())
			require.EqualValues(t, rows, rr.LevelsPos())
			require.Equal(t, defLevels, rr.DefLevels()[:rows])

			got := arrow.Float64Traits.CastFromBytes(rr.Values())[:rows]
			valid := rr.ReleaseValidBits()
			defer valid.Release()
			for i, def := range defLevels {
				require.Equal(t, def == 1, bitutil.BitIsSet(valid.Bytes(), i), "row %d validity", i)
				if def == 1 {
					require.Equal(t, math.Float64bits(values[i]), math.Float64bits(got[i]), "row %d", i)
				}
			}
		})
	}
}

// These distributions cover exact decimals, integer overflow, NaNs,
// infinities, and subnormals.
func TestALPRandomRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	pools := []struct {
		name string
		draw func() float64
	}{
		{"normal", func() float64 { return rng.NormFloat64() }},
		{"two decimal places", func() float64 { return float64(rng.Intn(1_000_000)) / 100 }},
		{"very large", func() float64 { return rng.NormFloat64() * 1e300 }},
		{"very small", func() float64 { return rng.NormFloat64() * 1e-300 }},
		{"random bits", func() float64 { return math.Float64frombits(rng.Uint64()) }},
		{"whole numbers", func() float64 { return float64(rng.Int63()) }},
	}

	for _, pool := range pools {
		t.Run(pool.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			values := make([]float64, 5000)
			for i := range values {
				values[i] = pool.draw()
			}

			reader := writeALPFile(t, mem, schema.FieldList{
				schema.NewFloat64Node("value", parquet.Repetitions.Required, -1),
			}, func(cw file.ColumnChunkWriter) {
				n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
				require.NoError(t, err)
				require.EqualValues(t, len(values), n)
			})
			defer reader.Close()
			require.EqualValues(t, len(values), reader.NumRows())
			chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
			require.NoError(t, err)
			assertALPDataPageEncoding(t, chunk)

			got, _ := readALPDoubleColumn(t, reader, 0, len(values))
			require.Len(t, got, len(values))
			for i := range values {
				// Compare the bit patterns, so that a NaN or a negative zero has
				// to come back exactly as it went in.
				if math.Float64bits(got[i]) != math.Float64bits(values[i]) {
					t.Fatalf("index %d: got %v (%016x), want %v (%016x)",
						i, got[i], math.Float64bits(got[i]), values[i], math.Float64bits(values[i]))
				}
			}
		})
	}
}

// Data page v2 stores levels separately from encoded values.
func TestALPDataPageV2(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []float64{1.25, 2.5, 3.75, 100, 0.001}
	defLevels := []int16{1, 0, 1, 1, 0, 1, 0, 1}
	reader := writeALPFile(t, mem, schema.FieldList{
		schema.NewFloat64Node("value", parquet.Repetitions.Optional, -1),
	}, func(cw file.ColumnChunkWriter) {
		n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, defLevels, nil)
		require.NoError(t, err)
		require.EqualValues(t, len(values), n)
	}, parquet.WithDataPageVersion(parquet.DataPageV2), parquet.WithCompression(compress.Codecs.Zstd))
	defer reader.Close()
	require.EqualValues(t, len(defLevels), reader.NumRows())
	chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
	require.NoError(t, err)
	assertALPDataPageEncoding(t, chunk)
	require.Equal(t, compress.Codecs.Zstd, chunk.Compression())

	pages, err := reader.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	defer pages.Close()
	require.True(t, pages.Next())
	require.Equal(t, file.PageTypeDataPageV2, pages.Page().Type())
	page := pages.Page().(*file.DataPageV2)
	require.Equal(t, parquet.Encodings.ALP, parquet.Encoding(page.Encoding()))
	require.True(t, page.IsCompressed())
	require.EqualValues(t, len(defLevels), page.NumRows())
	require.EqualValues(t, len(defLevels)-len(values), page.NumNulls())
	require.Positive(t, page.DefinitionLevelByteLen())
	require.False(t, pages.Next())
	require.NoError(t, pages.Err())

	got, gotDef := readALPDoubleColumn(t, reader, 0, len(defLevels))
	require.Equal(t, defLevels, gotDef)
	require.Equal(t, values, got)
}

func TestALPFileRoundtrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []float64
	}{
		{
			name:   "monetary",
			values: []float64{1.23, 4.56, 7.89, 10.11, 12.13, 14.15, 16.17, 18.19},
		},
		{
			name: "large_vector",
			values: func() []float64 {
				v := make([]float64, 2048)
				for i := range v {
					v[i] = 0.125 + float64(i)*0.25
				}
				return v
			}(),
		},
		{
			name:   "integers",
			values: []float64{1, 2, 3, 100, 200, 300, 1000, 2000},
		},
		{
			name:   "special_values",
			values: []float64{0, math.Copysign(0, -1), 1.5, -1.5, math.Inf(1), math.Inf(-1), math.NaN()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			reader := writeALPFile(t, mem, schema.FieldList{
				schema.NewFloat64Node("value", parquet.Repetitions.Required, -1),
			}, func(cw file.ColumnChunkWriter) {
				n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(tc.values, nil, nil)
				require.NoError(t, err)
				require.EqualValues(t, len(tc.values), n)
			})
			defer reader.Close()
			require.EqualValues(t, len(tc.values), reader.NumRows())
			chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
			require.NoError(t, err)
			assertALPDataPageEncoding(t, chunk)

			output, _ := readALPDoubleColumn(t, reader, 0, len(tc.values))
			require.Len(t, output, len(tc.values))

			// Compare the bit patterns, so that a NaN or a negative zero has to come
			// back exactly as it went in.
			for i := range tc.values {
				require.Equal(t, math.Float64bits(tc.values[i]), math.Float64bits(output[i]),
					"index %d: got %v (bits %016x), want %v (bits %016x)",
					i, output[i], math.Float64bits(output[i]), tc.values[i], math.Float64bits(tc.values[i]))
			}
		})
	}
}

// A column that asks for ALP but keeps its dictionary falls back to PLAIN when
// the dictionary outgrows its limit before the first page. The footer must then
// list PLAIN, not ALP: a reader without ALP support may reject a file whose
// footer names it, though no page uses it.
func TestALPRequestedButDictionaryDroppedListsPlain(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := make([]float64, 20000)
	for i := range values {
		values[i] = float64(i) / 7
	}
	reader := writeALPFile(t, mem, schema.FieldList{
		schema.NewFloat64Node("value", parquet.Repetitions.Required, -1),
	}, func(cw file.ColumnChunkWriter) {
		n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, len(values), n)
	}, parquet.WithDictionaryDefault(true),
		parquet.WithEncodingFor("value", parquet.Encodings.ALP),
		parquet.WithDictionaryPageSizeLimit(1024))
	defer reader.Close()
	chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
	require.NoError(t, err)
	dataPageEntries := 0
	for _, s := range chunk.EncodingStats() {
		require.NotEqual(t, parquet.Encodings.ALP, s.Encoding, "no page should be ALP")
		if s.PageType == file.PageTypeDataPage || s.PageType == file.PageTypeDataPageV2 {
			dataPageEntries++
			require.Equal(t, parquet.Encodings.Plain, s.Encoding)
		}
	}
	require.Positive(t, dataPageEntries)
	// This test also checks that the footer does not advertise an unused codec.
	require.NotContains(t, chunk.Encodings(), parquet.Encodings.ALP)
	require.Contains(t, chunk.Encodings(), parquet.Encodings.Plain)

	got, _ := readALPDoubleColumn(t, reader, 0, len(values))
	require.Equal(t, values, got)
}

// The vector size is a writer property, so it has to reach each column's
// encoder: the pages of a column must declare the size set for it, and still
// read back.
func TestALPFileVectorSize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := make([]float64, 5000)
	for i := range values {
		values[i] = float64(i%1234) / 100
	}

	var fields schema.FieldList
	for _, name := range []string{"small", "default", "large"} {
		fields = append(fields, schema.NewFloat64Node(name, parquet.Repetitions.Required, -1))
	}
	reader := writeALPFile(t, mem, fields, func(cw file.ColumnChunkWriter) {
		n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, len(values), n)
	}, parquet.WithALPVectorSizeFor("small", 32),
		parquet.WithALPVectorSizeFor("large", 4096))
	defer reader.Close()

	for col, logSize := range []byte{5, 10, 12} {
		chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(col)
		require.NoError(t, err)
		assertALPDataPageEncoding(t, chunk)

		func() {
			pages, err := reader.RowGroup(0).GetColumnPageReader(col)
			require.NoError(t, err)
			defer pages.Close()
			pageCount := 0
			for pages.Next() {
				page := pages.Page()
				require.Equal(t, file.PageTypeDataPage, page.Type())
				require.Equal(t, parquet.Encodings.ALP, parquet.Encoding(page.Encoding()))
				// Required columns have no levels before the ALP header.
				require.GreaterOrEqual(t, len(page.Data()), 3)
				require.Equal(t, logSize, page.Data()[2], "column %d", col)
				pageCount++
			}
			require.NoError(t, pages.Err())
			require.Positive(t, pageCount)
		}()

		got, _ := readALPDoubleColumn(t, reader, col, len(values))
		require.Equal(t, values, got, "column %d", col)
	}
}

// A column whose values outgrow the data page size is written as several pages.
// Each one carries its own ALP header, offset array and vectors, so a page that
// repeated an earlier page's values would decode to the wrong numbers here.
func TestALPFileRoundtripManyPages(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	const numValues = 40000

	values := make([]float64, numValues)
	for i := range values {
		values[i] = float64(i) / 100
	}

	reader := writeALPFile(t, mem, schema.FieldList{
		schema.NewFloat64Node("value", parquet.Repetitions.Required, -1),
	}, func(cw file.ColumnChunkWriter) {
		n, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, numValues, n)
	}, parquet.WithDataPageSize(16*1024))
	defer reader.Close()

	require.EqualValues(t, numValues, reader.NumRows())
	chunk, err := reader.RowGroup(0).MetaData().ColumnChunk(0)
	require.NoError(t, err)
	assertALPDataPageEncoding(t, chunk)

	pages, err := reader.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	defer pages.Close()
	dataPages := 0
	for pages.Next() {
		require.Equal(t, file.PageTypeDataPage, pages.Page().Type())
		require.Equal(t, parquet.Encodings.ALP, parquet.Encoding(pages.Page().Encoding()))
		dataPages++
	}
	require.NoError(t, pages.Err())
	require.Greater(t, dataPages, 1, "the roundtrip must span multiple data pages")

	// Read in batches that do not divide the vector size, so a batch boundary
	// lands inside a vector and inside a page.
	output, _ := readALPDoubleColumn(t, reader, 0, 700)
	require.Equal(t, values, output)
}

func TestALPFloat32FileRoundtrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []float32{1.23, 4.56, 7.89, 0, -1.5, 100.0, 0.001}

	reader := writeALPFile(t, mem, schema.FieldList{
		schema.NewFloat32Node("value", parquet.Repetitions.Required, -1),
	}, func(cw file.ColumnChunkWriter) {
		n, err := cw.(*file.Float32ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, len(values), n)
	})
	defer reader.Close()
	require.EqualValues(t, len(values), reader.NumRows())

	rgr := reader.RowGroup(0)
	chunk, err := rgr.MetaData().ColumnChunk(0)
	require.NoError(t, err)
	assertALPDataPageEncoding(t, chunk)
	cr, err := rgr.Column(0)
	require.NoError(t, err)
	defer cr.Close()

	f32Reader, ok := cr.(*file.Float32ColumnChunkReader)
	require.True(t, ok)

	output := make([]float32, len(values))
	total, read, err := f32Reader.ReadBatch(int64(len(values)), output, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(values), total)
	require.EqualValues(t, len(values), read)
	require.False(t, f32Reader.HasNext())
	require.NoError(t, f32Reader.Err())

	for i := range values {
		require.Equal(t, math.Float32bits(values[i]), math.Float32bits(output[i]),
			"index %d: got %v (bits %08x), want %v (bits %08x)",
			i, output[i], math.Float32bits(output[i]), values[i], math.Float32bits(values[i]))
	}
}
