// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

const shortPageMessage = "the page holds fewer values than its header claims"

// maxDecoderCalls bounds how often a reader may call its decoder. A reader
// that spins on a decoder returning nothing exceeds it and fails the test
// instead of hanging it.
const maxDecoderCalls = 16

type countingInt32Decoder struct {
	encoding.Int32Decoder
	t     *testing.T
	calls int
}

func (d *countingInt32Decoder) tick() {
	d.t.Helper()
	if d.calls++; d.calls > maxDecoderCalls {
		d.t.Fatalf("decoder called more than %d times: the reader is not making progress", maxDecoderCalls)
	}
}

func (d *countingInt32Decoder) Decode(out []int32) (int, error) {
	d.tick()
	return d.Int32Decoder.Decode(out)
}

func (d *countingInt32Decoder) Discard(n int) (int, error) {
	d.tick()
	return d.Int32Decoder.Discard(n)
}

type onePagePager struct {
	page Page
	read bool
}

func (p *onePagePager) SetMaxPageHeaderSize(int) {}
func (p *onePagePager) Page() Page {
	if !p.read {
		return nil
	}
	return p.page
}
func (p *onePagePager) Next() bool {
	was := p.read
	p.read = true
	return !was
}
func (p *onePagePager) Err() error { return nil }
func (p *onePagePager) Reset(parquet.BufferedReader, int64, compress.Compression, *CryptoContext) {
}
func (p *onePagePager) GetDictionaryPage() (*DictionaryPage, error) { return nil, nil }
func (p *onePagePager) SeekToPageWithRow(int64) error {
	p.read = true
	return nil
}
func (p *onePagePager) Close() error { return nil }

func shortPageDescr(t *testing.T, rep parquet.Repetition) *schema.Column {
	t.Helper()
	node, err := schema.NewPrimitiveNode("v", rep, parquet.Types.Int32, -1, -1)
	require.NoError(t, err)
	maxDef := int16(0)
	if rep == parquet.Repetitions.Optional {
		maxDef = 1
	}
	return schema.NewColumn(node, maxDef, 0)
}

// shortPage returns a v1 data page whose header claims numValues values while
// its payload encodes only vals. A non-nil defLevels is written as an RLE level block.
func shortPage(t *testing.T, descr *schema.Column, enc parquet.Encoding, numValues int, defLevels []int16, vals []int32) *DataPageV1 {
	t.Helper()
	mem := memory.DefaultAllocator
	var body []byte
	if defLevels != nil {
		buf := encoding.NewBufferWriter(encoding.LevelEncodingMaxBufferSize(parquet.Encodings.RLE, 1, len(defLevels)), mem)
		var lvlEnc encoding.LevelEncoder
		lvlEnc.Init(parquet.Encodings.RLE, 1, buf)
		_, err := lvlEnc.Encode(defLevels)
		require.NoError(t, err)
		body = binary.LittleEndian.AppendUint32(nil, uint32(lvlEnc.Len()))
		body = append(body, buf.Bytes()[:lvlEnc.Len()]...)
	}

	valEnc := encoding.NewEncoder(parquet.Types.Int32, enc, false, descr, mem)
	defer valEnc.Release()
	valEnc.(encoding.Int32Encoder).Put(vals)
	encoded, err := valEnc.FlushValues()
	require.NoError(t, err)
	defer encoded.Release()
	body = append(body, encoded.Bytes()...)

	return NewDataPageV1WithConfig(memory.NewBufferBytes(body), parquet.Encodings.RLE, parquet.Encodings.RLE,
		DataPageConfig{Num: int32(numValues), Encoding: enc, UncompressedSize: int32(len(body))})
}

func newInt32Reader(t *testing.T, descr *schema.Column, enc parquet.Encoding, page Page) *Int32ColumnChunkReader {
	t.Helper()
	r := NewColumnReader(descr, &onePagePager{page: page}, memory.DefaultAllocator, nil).(*Int32ColumnChunkReader)
	r.decoders[format.Encoding(enc)] = &countingInt32Decoder{
		Int32Decoder: encoding.NewDecoder(parquet.Types.Int32, enc, descr, memory.DefaultAllocator).(encoding.Int32Decoder),
		t:            t,
	}
	return r
}

func TestReadBatchPageHoldsFewerValuesThanHeader(t *testing.T) {
	for name, enc := range map[string]parquet.Encoding{
		"delta binary packed": parquet.Encodings.DeltaBinaryPacked,
		"byte stream split":   parquet.Encodings.ByteStreamSplit,
	} {
		t.Run(name, func(t *testing.T) {
			descr := shortPageDescr(t, parquet.Repetitions.Required)
			r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{7, 8}))
			out := make([]int32, 10)
			total, read, err := r.ReadBatch(10, out, nil, nil)
			require.ErrorContains(t, err, shortPageMessage)
			require.Equal(t, int64(2), total)
			require.Equal(t, []int32{7, 8}, out[:read])
		})
	}
}

func TestReadBatchInPageReportsNoProgress(t *testing.T) {
	descr := shortPageDescr(t, parquet.Repetitions.Required)
	enc := parquet.Encodings.DeltaBinaryPacked
	r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{7, 8}))
	out := make([]int32, 10)

	total, read, err := r.ReadBatchInPage(10, out, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Equal(t, []int32{7, 8}, out[:read])

	total, read, err = r.ReadBatchInPage(10, out, nil, nil)
	require.ErrorContains(t, err, shortPageMessage)
	require.ErrorContains(t, err, "expected 8")
	require.Zero(t, total)
	require.Zero(t, read)
}

func TestSkipPageHoldsFewerValuesThanHeader(t *testing.T) {
	descr := shortPageDescr(t, parquet.Repetitions.Required)
	enc := parquet.Encodings.DeltaBinaryPacked
	r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{7, 8}))

	_, err := r.Skip(5)
	require.ErrorContains(t, err, shortPageMessage)
	require.ErrorContains(t, err, "expected 3")
	require.ErrorContains(t, r.Err(), shortPageMessage)
}

func TestSeekToRowPageHoldsFewerValuesThanHeader(t *testing.T) {
	descr := shortPageDescr(t, parquet.Repetitions.Required)
	enc := parquet.Encodings.DeltaBinaryPacked
	r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{7, 8}))

	err := r.SeekToRow(5)
	require.ErrorContains(t, err, shortPageMessage)
	require.ErrorContains(t, err, "expected 3")
	require.ErrorContains(t, r.Err(), shortPageMessage)
}

func TestShortPageGuardKeepsDecoderError(t *testing.T) {
	descr := shortPageDescr(t, parquet.Repetitions.Required)
	enc := parquet.Encodings.Plain
	r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{7, 8}))

	_, _, err := r.ReadBatch(10, make([]int32, 10), nil, nil)
	require.ErrorContains(t, err, "eof exception")
	require.NotContains(t, err.Error(), shortPageMessage)
}

func TestShortPageGuardLeavesConsistentPagesAlone(t *testing.T) {
	enc := parquet.Encodings.DeltaBinaryPacked

	t.Run("optional page of only nulls", func(t *testing.T) {
		descr := shortPageDescr(t, parquet.Repetitions.Optional)
		r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, make([]int16, 10), nil))
		_, err := r.Skip(3)
		require.NoError(t, err)
		total, read, err := r.ReadBatch(10, make([]int32, 10), make([]int16, 10), nil)
		require.NoError(t, err)
		require.Equal(t, int64(7), total)
		require.Zero(t, read)
	})

	t.Run("zero sized batch", func(t *testing.T) {
		descr := shortPageDescr(t, parquet.Repetitions.Required)
		r := newInt32Reader(t, descr, enc, shortPage(t, descr, enc, 10, nil, []int32{0, 3, 6, 9, 12, 15, 18, 21, 24, 27}))
		total, read, err := r.ReadBatchInPage(0, nil, nil, nil)
		require.NoError(t, err)
		require.Zero(t, total)
		require.Zero(t, read)
	})
}
