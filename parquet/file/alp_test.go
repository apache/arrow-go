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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

// alpWriteReadDouble writes one optional or required double column with ALP and
// reads it back. A nil defLevels writes a required column.
func alpWriteReadDouble(t *testing.T, values []float64, defLevels []int16, opts ...parquet.WriterProperty) ([]float64, []int16) {
	t.Helper()

	rep := parquet.Repetitions.Required
	if defLevels != nil {
		rep = parquet.Repetitions.Optional
	}
	opts = append([]parquet.WriterProperty{
		parquet.WithEncoding(parquet.Encodings.ALP),
		parquet.WithAlpEncoding(true),
		parquet.WithDictionaryDefault(false),
	}, opts...)

	field, err := schema.NewPrimitiveNode("value", rep, parquet.Types.Double, -1, -1)
	require.NoError(t, err)
	sc, err := schema.NewGroupNode("test", parquet.Repetitions.Required, schema.FieldList{field}, 0)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(parquet.NewWriterProperties(opts...)))
	rgw := writer.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, defLevels, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, writer.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	cr, err := reader.RowGroup(0).Column(0)
	require.NoError(t, err)
	r := cr.(*file.Float64ColumnChunkReader)

	rows := int(reader.NumRows())
	out := make([]float64, rows)
	var outDef []int16
	if defLevels != nil {
		outDef = make([]int16, rows)
	}
	for read := 0; read < rows; {
		_, n, err := r.ReadBatch(int64(rows), out[read:], outDef, nil)
		require.NoError(t, err)
		require.NoError(t, r.Err())
		if n == 0 {
			break
		}
		read += n
	}
	return out, outDef
}

// TestAlpAllNullPage covers a page that holds no values, which a column of
// optional values produces when every row in the page is null. PLAIN is the
// control: whatever it does with such a page, ALP has to do as well.
func TestAlpAllNullPage(t *testing.T) {
	for _, enc := range []parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.ALP} {
		t.Run(enc.String(), func(t *testing.T) {
			const rows = 8
			out, def := alpWriteReadDouble(t, nil, make([]int16, rows),
				parquet.WithEncoding(enc))
			require.Len(t, out, rows)
			for i, d := range def {
				require.Zero(t, d, "row %d should be null", i)
			}
		})
	}
}

// TestAlpNullsScattered spreads the nulls through the page, so that the values
// travel through PutSpaced and DecodeSpaced rather than the dense path.
func TestAlpNullsScattered(t *testing.T) {
	const rows = 3000
	values := make([]float64, 0, rows)
	defLevels := make([]int16, rows)
	for i := range rows {
		if i%3 == 0 {
			continue // definition level 0: this row is null
		}
		defLevels[i] = 1
		values = append(values, float64(i)/100)
	}

	got, _ := alpWriteReadDouble(t, values, defLevels)
	require.Equal(t, values, got[:len(values)])
}

// TestAlpRandomRoundtrip draws from distributions that stress different parts of
// the encoder: decimals it can represent exactly, magnitudes that overflow the
// integer form, and random bit patterns, which supply the NaNs, the infinities
// and the subnormals.
func TestAlpRandomRoundtrip(t *testing.T) {
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
			values := make([]float64, 5000)
			for i := range values {
				values[i] = pool.draw()
			}

			got, _ := alpWriteReadDouble(t, values, nil)
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

// TestAlpDataPageV2 covers the second page format, which writes the levels
// outside the encoded values.
func TestAlpDataPageV2(t *testing.T) {
	values := []float64{1.25, 2.5, 3.75, 100, 0.001}
	got, _ := alpWriteReadDouble(t, values, nil, parquet.WithDataPageVersion(parquet.DataPageV2))
	require.Equal(t, values, got)
}

// alpSinglePageReader hands the column reader one page and then reports the end
// of the chunk. It hands it over once, so that a reader which asks for another
// page ends rather than sees the same one again.
type alpSinglePageReader struct {
	page   file.Page
	handed bool
}

func (r *alpSinglePageReader) SetMaxPageHeaderSize(int) {}
func (r *alpSinglePageReader) Page() file.Page          { return r.page }
func (r *alpSinglePageReader) Next() bool {
	if r.handed {
		return false
	}
	r.handed = true
	return r.page != nil
}
func (r *alpSinglePageReader) Err() error { return nil }
func (r *alpSinglePageReader) Reset(parquet.BufferedReader, int64, compress.Compression, *file.CryptoContext) {
}
func (r *alpSinglePageReader) GetDictionaryPage() (*file.DictionaryPage, error) { return nil, nil }
func (r *alpSinglePageReader) SeekToPageWithRow(int64) error                    { return nil }
func (r *alpSinglePageReader) Close() error                                     { return nil }

// TestAlpOnIntegerColumnIsRejected covers a file that claims ALP for a column
// ALP cannot encode. Nothing this library writes produces one, so the page comes
// from another writer or from corruption; either way the reader has to report it
// rather than fail some other way.
func TestAlpOnIntegerColumnIsRejected(t *testing.T) {
	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.Int32, -1, -1)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	body := memory.NewBufferBytes(make([]byte, 64))
	page := file.NewDataPageV1(body, 4, parquet.Encodings.ALP,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))

	r := file.NewColumnReader(descr, &alpSinglePageReader{page: page}, memory.DefaultAllocator, nil)
	total, read, err := r.(*file.Int32ColumnChunkReader).ReadBatch(4, make([]int32, 4), nil, nil)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Zero(t, read)
	// ReadBatch stops rather than returning the failure, as it does for any page
	// it cannot open, so the reason is on the reader.
	require.ErrorContains(t, r.Err(), "only float and double support ALP encoding")
}

// TestAlpPageClaimingTooManyValues covers a page header that claims more values
// than the ALP body describes. The reader has to report the disagreement: a read
// that returns nothing and no error leaves the column reader where it was, so it
// asks again, and the read never ends.
func TestAlpPageClaimingTooManyValues(t *testing.T) {
	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.Double, -1, -1)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	enc := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP, false, descr, memory.DefaultAllocator)
	enc.(encoding.Float64Encoder).Put([]float64{1.5, 2.5})
	buf, err := enc.FlushValues()
	require.NoError(t, err)

	body := memory.NewBufferBytes(buf.Bytes())
	page := file.NewDataPageV1(body, 8, parquet.Encodings.ALP,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))

	r := file.NewColumnReader(descr, &alpSinglePageReader{page: page}, memory.DefaultAllocator, nil)
	_, read, err := r.(*file.Float64ColumnChunkReader).ReadBatch(8, make([]float64, 8), nil, nil)
	require.ErrorContains(t, err, "ALP page holds 2 values")
	require.Equal(t, 2, read, "the values the page does hold still come back")
}
