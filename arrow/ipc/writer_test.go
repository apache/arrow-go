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

package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type failingPayloadWriter struct {
	err       error
	closeErr  error
	failAfter int
	payloads  int
	closeCall int
}

type shortWriteWriter struct{}

type failingCompressor struct {
	err      error
	closeErr error
}

func (failingCompressor) MaxCompressedLen(n int) int { return n }
func (failingCompressor) Reset(io.Writer)            {}
func (f failingCompressor) Write(p []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return len(p), nil
}
func (f failingCompressor) Close() error { return f.closeErr }
func (failingCompressor) Type() flatbuf.CompressionType {
	return flatbuf.CompressionTypeZSTD
}

type failingWriter struct {
	err error
}

type closeFailWriter struct {
	bytes.Buffer
	err  error
	fail bool
}

func (w *closeFailWriter) Write(p []byte) (int, error) {
	if w.fail {
		return 0, w.err
	}

	return w.Buffer.Write(p)
}

func (shortWriteWriter) Write(p []byte) (int, error) {
	return len(p) - 1, io.ErrShortWrite
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestPayloadWriteRejectsShortWrites(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil))
	bldr.Field(0).(*array.Int8Builder).Append(1)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	payload, err := GetRecordBatchPayload(rec, WithAllocator(mem))
	require.NoError(t, err)
	defer payload.Release()

	_, err = payload.WritePayload(shortWriteWriter{})
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func (w *failingPayloadWriter) Start() error { return nil }
func (w *failingPayloadWriter) WritePayload(Payload) error {
	w.payloads++
	if w.failAfter == 0 || w.payloads >= w.failAfter {
		return w.err
	}
	return nil
}
func (w *failingPayloadWriter) Close() error {
	w.closeCall++
	return w.closeErr
}

func TestWriterCloseFailureIsTerminal(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	want := errors.New("close failed")
	payloadWriter := &failingPayloadWriter{closeErr: want}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))

	require.ErrorIs(t, writer.Close(), want)
	require.ErrorIs(t, writer.Close(), want)
	require.Equal(t, 1, payloadWriter.closeCall)
}

func TestWriterCloseReleasesRecordEncoder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	var output bytes.Buffer
	writer := NewWriter(&output, WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NotNil(t, writer.encoder)

	require.NoError(t, writer.Close())
	require.Nil(t, writer.encoder)
}

func TestWriterCloseAfterFailureReleasesRecordEncoder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	want := errors.New("payload failed")
	payloadWriter := &failingPayloadWriter{err: want, failAfter: 2}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))
	require.ErrorIs(t, writer.Write(record), want)
	require.NotNil(t, writer.encoder)

	require.ErrorIs(t, writer.Close(), want)
	require.Nil(t, writer.encoder)
}

func TestFileWriterCloseFailureIsTerminal(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	want := errors.New("write failed")
	writer, err := NewFileWriter(failingWriter{err: want}, WithSchema(schema))
	require.NoError(t, err)

	require.ErrorIs(t, writer.Close(), want)
	require.ErrorIs(t, writer.Close(), want)
}

func TestFileWriterCloseReleasesRecordEncoder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	var output bytes.Buffer
	writer, err := NewFileWriter(&output, WithSchema(schema))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NotNil(t, writer.encoder)

	require.NoError(t, writer.Close())
	require.Nil(t, writer.encoder)
}

func TestFileWriterCloseFailureReleasesRecordEncoder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	want := errors.New("close failed")
	output := &closeFailWriter{err: want}
	writer, err := NewFileWriter(output, WithSchema(schema))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NotNil(t, writer.encoder)

	output.fail = true
	require.ErrorIs(t, writer.Close(), want)
	require.Nil(t, writer.encoder)
}

func TestWriterSchemaFailureIsTerminal(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	want := errors.New("schema write failed")
	payloadWriter := &failingPayloadWriter{err: want}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))

	require.ErrorIs(t, writer.Write(record), want)
	require.ErrorIs(t, writer.Write(record), want)
	require.Equal(t, 1, payloadWriter.payloads)
	require.ErrorIs(t, writer.Close(), want)
	require.Equal(t, 1, payloadWriter.closeCall)
}

func TestWriterCloseSchemaFailureClosesStartedPayloadWriter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	want := errors.New("schema write failed")
	payloadWriter := &failingPayloadWriter{err: want}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))

	require.ErrorIs(t, writer.Close(), want)
	require.Equal(t, 1, payloadWriter.payloads)
	require.Equal(t, 1, payloadWriter.closeCall)
}

func TestWriterRecordEncodingFailureIsTerminal(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	deepType := arrow.PrimitiveTypes.Int32
	for i := 0; i < kMaxNestingDepth+1; i++ {
		deepType = arrow.ListOf(deepType)
	}
	jsonValue := strings.Repeat("[", kMaxNestingDepth+2) + "1" + strings.Repeat("]", kMaxNestingDepth+2)
	deepArray, _, err := array.FromJSON(mem, deepType, strings.NewReader(jsonValue))
	require.NoError(t, err)
	defer deepArray.Release()

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dictArray, _, err := array.FromJSON(mem, dictType, strings.NewReader(`["value"]`))
	require.NoError(t, err)
	defer dictArray.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "dict", Type: dictType},
		{Name: "deep", Type: deepType},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{dictArray, deepArray}, 1)
	defer record.Release()

	payloadWriter := &failingPayloadWriter{}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))

	firstErr := writer.Write(record)
	require.Error(t, firstErr)
	require.Equal(t, 2, payloadWriter.payloads)

	secondErr := writer.Write(record)
	require.EqualError(t, secondErr, firstErr.Error())
	require.Equal(t, 2, payloadWriter.payloads)
	require.Error(t, writer.Close())
}

func TestWriterPayloadFailureClosesStartedPayloadWriter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()

	payloadErr := errors.New("payload failed")
	closeErr := errors.New("close failed")
	payloadWriter := &failingPayloadWriter{err: payloadErr, closeErr: closeErr, failAfter: 2}
	writer := NewWriterWithPayloadWriter(payloadWriter, WithSchema(schema))

	require.ErrorIs(t, writer.Write(record), payloadErr)
	err := writer.Close()
	require.ErrorIs(t, err, payloadErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, 2, payloadWriter.payloads)
	require.Equal(t, 1, payloadWriter.closeCall)
	require.ErrorIs(t, writer.Close(), payloadErr)
	require.Equal(t, 1, payloadWriter.closeCall)
}

func TestWriterCloseWithoutSchemaReturnsError(t *testing.T) {
	payloadWriter := &failingPayloadWriter{}
	writer := NewWriterWithPayloadWriter(payloadWriter)

	require.ErrorIs(t, writer.Close(), arrow.ErrInvalid)
	require.Zero(t, payloadWriter.payloads)
	require.Zero(t, payloadWriter.closeCall)
}

// reproducer from ARROW-13529
func TestSliceAndWrite(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	sliceAndWrite := func(rec arrow.RecordBatch, schema *arrow.Schema) {
		slice := rec.NewSlice(1, 2)
		defer slice.Release()

		fmt.Println(slice.Columns()[0].(*array.String).Value(0))

		var buf bytes.Buffer
		w := NewWriter(&buf, WithSchema(schema))
		w.Write(slice)
		w.Close()
	}

	assert.NotPanics(t, func() {
		for i := 0; i < 2; i++ {
			sliceAndWrite(rec, schema)
		}
	})
}

func TestNewTruncatedBitmap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	assert.Nil(t, newTruncatedBitmap(alloc, 0, 0, nil), "input bitmap is null")

	buf := memory.NewBufferBytes(make([]byte, bitutil.BytesForBits(8)))
	defer buf.Release()

	bitutil.SetBit(buf.Bytes(), 0)
	bitutil.SetBit(buf.Bytes(), 2)
	bitutil.SetBit(buf.Bytes(), 4)
	bitutil.SetBit(buf.Bytes(), 6)

	assert.Same(t, buf, newTruncatedBitmap(alloc, 0, 8, buf), "no truncation necessary")

	result := newTruncatedBitmap(alloc, 1, 7, buf)
	defer result.Release()
	for i, exp := range []bool{false, true, false, true, false, true, false} {
		assert.Equal(t, exp, bitutil.BitIsSet(result.Bytes(), i), "truncate for offset")
	}

	buf = memory.NewBufferBytes(make([]byte, 128))
	defer buf.Release()
	bitutil.SetBitsTo(buf.Bytes(), 0, 128*8, true)

	result = newTruncatedBitmap(alloc, 0, 8, buf)
	defer result.Release()
	assert.Equal(t, 64, result.Len(), "truncate to smaller buffer")
	assert.Equal(t, 8, bitutil.CountSetBits(result.Bytes(), 0, 8))
}

func TestGetZeroBasedValueOffsets(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	vals := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.AppendValues(vals, nil)

	arr := b.NewArray()
	defer arr.Release()

	env := &recordEncoder{mem: alloc}

	offsets := env.getZeroBasedValueOffsets(arr)
	defer offsets.Release()
	assert.Equal(t, 44, offsets.Len(), "include all offsets if array is not sliced")
	assert.Same(t, arr.Data().Buffers()[1], offsets)

	sl := array.NewSlice(arr, 0, 4)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Equal(t, 20, offsets.Len(), "trim trailing offsets after slice")
	assert.Same(t, sl.Data().Buffers()[1], offsets.Parent())

	sl = array.NewSlice(arr, 2, 6)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Nil(t, offsets.Parent(), "rebase offsets when the first logical offset is non-zero")
	assert.Equal(t, []int32{0, 1, 2, 3, 4}, arrow.Int32Traits.CastFromBytes(offsets.Bytes()))

	emptyPrefixBuilder := array.NewStringBuilder(alloc)
	emptyPrefixBuilder.AppendValues([]string{"", "", "a"}, nil)
	emptyPrefix := emptyPrefixBuilder.NewArray()
	emptyPrefixBuilder.Release()
	defer emptyPrefix.Release()

	sl = array.NewSlice(emptyPrefix, 1, 3)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Same(t, sl.Data().Buffers()[1], offsets.Parent())
	assert.Equal(t, []int32{0, 0, 1}, arrow.Int32Traits.CastFromBytes(offsets.Bytes()))

	largeBuilder := array.NewLargeStringBuilder(alloc)
	largeBuilder.AppendValues(vals, nil)
	large := largeBuilder.NewArray()
	largeBuilder.Release()
	defer large.Release()

	sl = array.NewSlice(large, 0, 4)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Same(t, sl.Data().Buffers()[1], offsets.Parent())
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, arrow.Int64Traits.CastFromBytes(offsets.Bytes()))

	sl = array.NewSlice(large, 2, 6)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Nil(t, offsets.Parent(), "rebase int64 offsets when the first logical offset is non-zero")
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, arrow.Int64Traits.CastFromBytes(offsets.Bytes()))

	largeEmptyPrefixBuilder := array.NewLargeStringBuilder(alloc)
	largeEmptyPrefixBuilder.AppendValues([]string{"", "", "a"}, nil)
	largeEmptyPrefix := largeEmptyPrefixBuilder.NewArray()
	largeEmptyPrefixBuilder.Release()
	defer largeEmptyPrefix.Release()

	sl = array.NewSlice(largeEmptyPrefix, 1, 3)
	defer sl.Release()

	offsets = env.getZeroBasedValueOffsets(sl)
	defer offsets.Release()
	assert.Same(t, sl.Data().Buffers()[1], offsets.Parent())
	assert.Equal(t, []int64{0, 0, 1}, arrow.Int64Traits.CastFromBytes(offsets.Bytes()))
}

func BenchmarkGetZeroBasedValueOffsets(b *testing.B) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(b, 0)

	const n = 1 << 20
	values := make([]string, n)
	for i := range values {
		values[i] = "x"
	}

	stringBuilder := array.NewStringBuilder(alloc)
	stringBuilder.AppendValues(values, nil)
	strings := stringBuilder.NewArray()
	stringBuilder.Release()
	defer strings.Release()

	largeStringBuilder := array.NewLargeStringBuilder(alloc)
	largeStringBuilder.AppendValues(values, nil)
	largeStrings := largeStringBuilder.NewArray()
	largeStringBuilder.Release()
	defer largeStrings.Release()

	env := &recordEncoder{mem: alloc}
	for _, tc := range []struct {
		name string
		arr  arrow.Array
	}{
		{name: "int32", arr: strings},
		{name: "int64", arr: largeStrings},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, slice := range []struct {
				name       string
				begin, end int64
			}{
				{name: "full", begin: 0, end: n},
				{name: "prefix", begin: 0, end: n / 2},
				{name: "middle", begin: n / 4, end: 3 * n / 4},
			} {
				b.Run(slice.name, func(b *testing.B) {
					arr := array.NewSlice(tc.arr, slice.begin, slice.end)
					defer arr.Release()

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						offsets := env.getZeroBasedValueOffsets(arr)
						offsets.Release()
					}
				})
			}
		})
	}
}

func TestWriterCatchPanic(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// mess up the first offset for the string column
	offsetBuf := rec.Column(0).Data().Buffers()[1]
	bitutil.SetBitsTo(offsetBuf.Bytes(), 0, 32, true)

	buf := new(bytes.Buffer)

	writer := NewWriter(buf, WithSchema(schema))
	assert.EqualError(t, writer.Write(rec), "arrow/ipc: unknown error while writing: runtime error: slice bounds out of range [-1:]")
}

func TestWriterMemCompression(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	w := NewWriter(&buf, WithAllocator(mem), WithSchema(schema), WithZstd())
	defer w.Close()

	require.NoError(t, w.Write(rec))
}

func TestNewWriterWithMinSpaceSavings(t *testing.T) {
	const minSpaceSavings = 0.5

	writer := NewWriter(io.Discard, WithMinSpaceSavings(minSpaceSavings))

	assert.Equal(t, minSpaceSavings, writer.minSpaceSavings)
}

func TestRecordEncoderCompressionErrorDoesNotDeadlock(t *testing.T) {
	want := errors.New("compression failed")
	body := make([]*memory.Buffer, 64)
	for i := range body {
		body[i] = memory.NewBufferBytes([]byte("payload"))
	}
	payload := Payload{body: body}
	defer payload.Release()

	encoder := newRecordEncoder(memory.DefaultAllocator, 0, kMaxNestingDepth, true,
		flatbuf.CompressionTypeZSTD, 2, 0, []compressor{
			failingCompressor{err: want},
			failingCompressor{err: want},
		})

	result := make(chan error, 1)
	go func() {
		result <- encoder.compressBodyBuffers(&payload)
	}()

	select {
	case err := <-result:
		require.ErrorIs(t, err, want)
	case <-time.After(time.Second):
		t.Fatal("compression did not return after timeout")
	}
}

func TestRecordEncoderReturnsCompressionError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int8Builder).Append(1)
	record := builder.NewRecordBatch()
	defer record.Release()

	want := errors.New("compression failed")
	encoder := newRecordEncoder(mem, 0, kMaxNestingDepth, true,
		flatbuf.CompressionTypeZSTD, 1, 0, []compressor{failingCompressor{err: want}})
	var payload Payload
	defer payload.Release()

	require.ErrorIs(t, encoder.Encode(&payload, record), want)
}

func TestGetRecordBatchPayloadReturnsCompressionErrorOnClose(t *testing.T) {
	want := errors.New("compression failed")
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int8Builder).Append(1)
	record := builder.NewRecordBatch()
	defer record.Release()

	_, err := GetRecordBatchPayload(record, WithAllocator(mem), WithZstd(),
		withCompressors(failingCompressor{closeErr: want}))
	require.ErrorIs(t, err, want)
}

func TestWriteWithCompressionAndMinSavings(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// a small batch that is known to be compressible
	batch, _, err := array.RecordFromJSON(mem, arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil),
		strings.NewReader(`[
			{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4},
			{"n": 5}, {"n": 6}, {"n": 7}, {"n": 8}, {"n": 9}]`))
	require.NoError(t, err)
	defer batch.Release()

	prefixedSize := func(buf *memory.Buffer) int64 {
		if buf.Len() < arrow.Int64SizeBytes {
			return 0
		}
		return int64(binary.LittleEndian.Uint64(buf.Bytes()))
	}
	contentSize := func(buf *memory.Buffer) int64 {
		return int64(buf.Len()) - int64(arrow.Int64SizeBytes)
	}

	for _, codec := range []flatbuf.CompressionType{flatbuf.CompressionTypeLZ4_FRAME, flatbuf.CompressionTypeZSTD} {
		compressors := []compressor{getCompressor(codec)}
		enc := newRecordEncoder(mem, 0, 5, true, codec, 1, 0, compressors)
		var payload Payload
		require.NoError(t, enc.encode(&payload, batch))
		assert.Len(t, payload.body, 2)

		// compute the savings when body buffers are compressed unconditionally.
		// We also validate that our test batch is indeed compressible.
		uncompressedSize, compressedSize := prefixedSize(payload.body[1]), contentSize(payload.body[1])
		assert.Less(t, compressedSize, uncompressedSize)
		assert.Greater(t, compressedSize, int64(0))
		expectedSavings := 1.0 - float64(compressedSize)/float64(uncompressedSize)

		compressEncoder := newRecordEncoder(mem, 0, 5, true, codec, 1, expectedSavings, compressors)
		payload.Release()
		payload.body = payload.body[:0]
		require.NoError(t, compressEncoder.encode(&payload, batch))
		assert.Len(t, payload.body, 2)
		assert.Equal(t, uncompressedSize, prefixedSize(payload.body[1]))
		assert.Equal(t, compressedSize, contentSize(payload.body[1]))

		payload.Release()
		payload.body = payload.body[:0]
		// slightly bump the threshold. the body buffer should now be prefixed
		// with -1 and its content left uncompressed
		minSavings := math.Nextafter(expectedSavings, 1.0)
		compressEncoder.minSpaceSavings = minSavings
		require.NoError(t, compressEncoder.encode(&payload, batch))
		assert.Len(t, payload.body, 2)
		assert.EqualValues(t, -1, prefixedSize(payload.body[1]))
		assert.Equal(t, uncompressedSize, contentSize(payload.body[1]))
		payload.Release()
		payload.body = payload.body[:0]

		for _, outOfRange := range []float64{math.Nextafter(1.0, 2.0), math.Nextafter(0, -1), math.NaN()} {
			compressEncoder.minSpaceSavings = outOfRange
			err := compressEncoder.encode(&payload, batch)
			assert.ErrorIs(t, err, arrow.ErrInvalid)
			assert.ErrorContains(t, err, "minSpaceSavings not in range [0,1]")
		}
	}
}

func TestWriterInferSchema(t *testing.T) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil))
	bldr.Field(0).(*array.Int8Builder).AppendValues([]int8{1, 2, 3, 4, 5}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	w := NewWriter(&buf)

	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	r, err := NewReader(&buf)
	require.NoError(t, err)
	defer r.Release()

	require.True(t, r.Schema().Equal(rec.Schema()))
}

type testMsgReader struct {
	messages []*Message

	curmsg *Message
}

func (r *testMsgReader) Message() (*Message, error) {
	if r.curmsg != nil {
		r.curmsg.Release()
		r.curmsg = nil
	}

	if len(r.messages) == 0 {
		return nil, io.EOF
	}

	r.curmsg = r.messages[0]
	r.messages = r.messages[1:]
	return r.curmsg, nil
}

func (r *testMsgReader) Release() {
	if r.curmsg != nil {
		r.curmsg.Release()
		r.curmsg = nil
	}
	for _, m := range r.messages {
		m.Release()
	}
	r.messages = nil
}

func (r *testMsgReader) Retain() {}

func TestGetPayloads(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	schemaPayload := GetSchemaPayload(rec.Schema(), mem)
	defer schemaPayload.Release()
	dataPayload, err := GetRecordBatchPayload(rec, WithAllocator(mem))
	require.NoError(t, err)
	defer dataPayload.Release()

	var schemaBuf, dataBuf bytes.Buffer
	schemaPayload.SerializeBody(&schemaBuf)
	dataPayload.SerializeBody(&dataBuf)

	msgrdr := &testMsgReader{
		messages: []*Message{
			NewMessage(schemaPayload.meta, memory.NewBufferBytes(schemaBuf.Bytes())),
			NewMessage(dataPayload.meta, memory.NewBufferBytes(dataBuf.Bytes())),
		},
	}

	rdr, err := NewReaderFromMessageReader(msgrdr, WithAllocator(mem))
	require.NoError(t, err)
	defer rdr.Release()

	assert.Truef(t, rdr.Schema().Equal(rec.Schema()), "expected: %s\ngot: %s", rec.Schema(), rdr.Schema())
	got, err := rdr.Read()
	require.NoError(t, err)

	assert.Truef(t, array.RecordEqual(rec, got), "expected: %s\ngot: %s", rec, got)
}

func TestWritePayload(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil))
	bldr.Field(0).(*array.Int8Builder).AppendValues([]int8{1, 2, 3, 4, 5}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	p, err := GetRecordBatchPayload(rec, WithAllocator(mem))
	defer p.Release()
	require.NoError(t, err)

	_, err = p.WritePayload(&buf)
	require.NoError(t, err)

	r := NewMessageReader(&buf, WithAllocator(mem))
	defer r.Release()

	msg, err := r.Message()
	require.NoError(t, err)
	require.True(t, msg.Type() == MessageRecordBatch)
}

func TestReaderRejectsRecordBatchBeforeInitialDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "dict", Type: dictType}}, nil)
	bldr := array.NewBuilder(mem, dictType)
	defer bldr.Release()
	require.NoError(t, bldr.UnmarshalJSON([]byte(`["value"]`)))
	arr := bldr.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var stream bytes.Buffer
	schemaPayload := GetSchemaPayload(schema, mem)
	defer schemaPayload.Release()
	_, err := schemaPayload.WritePayload(&stream)
	require.NoError(t, err)
	recordPayload, err := GetRecordBatchPayload(rec, WithAllocator(mem))
	require.NoError(t, err)
	defer recordPayload.Release()
	_, err = recordPayload.WritePayload(&stream)
	require.NoError(t, err)
	streamBytes := append([]byte(nil), stream.Bytes()...)

	rdr, err := NewReader(bytes.NewReader(streamBytes), WithAllocator(mem))
	require.NoError(t, err)
	defer rdr.Release()
	require.False(t, rdr.Next())
	require.EqualError(t, rdr.Err(), "arrow/ipc: IPC stream did not have the expected (1) dictionaries at the start of the stream")

	rdr, err = NewReader(bytes.NewReader(streamBytes), WithAllocator(mem))
	require.NoError(t, err)
	defer rdr.Release()
	_, err = rdr.Read()
	require.EqualError(t, err, "arrow/ipc: IPC stream did not have the expected (1) dictionaries at the start of the stream")
	_, err = rdr.Read()
	require.EqualError(t, err, "arrow/ipc: IPC stream did not have the expected (1) dictionaries at the start of the stream")
}

// TestVariadicCountsNotAccumulatedAcrossEncode verifies that variadicCounts
// does not accumulate across encode calls separated by reset(). Without this,
// each batch's variadic counts would include counts from previous batches,
// producing malformed IPC that other implementations (e.g., arrow-rs) cannot
// read.
func TestVariadicCountsNotAccumulatedAcrossEncode(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc := newRecordEncoder(
		mem, 0,
		kMaxNestingDepth,
		false,
		-1,
		1,
		0,
		nil,
	)

	// Create a StringView array with a long string (>12 bytes uses out-of-line
	// storage, which adds to variadicCounts).
	bldr := array.NewStringViewBuilder(mem)
	bldr.Append("this_is_a_long_string_value")
	arr := bldr.NewArray()
	bldr.Release()
	defer arr.Release()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "sv", Type: arrow.BinaryTypes.StringView},
		}, nil,
	)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	expectedCounts := []int64{1}
	for range 2 {
		enc.reset()

		var p Payload
		require.NoError(t, enc.encode(&p, rec))
		require.Equal(t, expectedCounts, enc.variadicCounts)
		p.Release()
	}
}

func TestRecordEncoderResetRestoresDepth(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	childBuilder := array.NewInt32Builder(mem)
	childBuilder.Append(1)
	child := childBuilder.NewArray()
	childBuilder.Release()
	defer child.Release()

	structArr, err := array.NewStructArrayWithFields(
		[]arrow.Array{child},
		[]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}},
	)
	require.NoError(t, err)
	defer structArr.Release()

	structSchema := arrow.NewSchema([]arrow.Field{{
		Name: "struct",
		Type: structArr.DataType(),
	}}, nil)
	structRecord := array.NewRecordBatch(structSchema, []arrow.Array{structArr}, 1)
	defer structRecord.Release()

	enc := newRecordEncoder(mem, 0, 1, true, -1, 1, 0, nil)
	var nestedPayload Payload
	require.ErrorIs(t, enc.encode(&nestedPayload, structRecord), errMaxRecursion)
	nestedPayload.Release()

	enc.reset()

	simpleSchema := arrow.NewSchema([]arrow.Field{{
		Name: "value",
		Type: arrow.PrimitiveTypes.Int32,
	}}, nil)
	simpleBuilder := array.NewInt32Builder(mem)
	simpleBuilder.Append(1)
	simple := simpleBuilder.NewArray()
	simpleBuilder.Release()
	defer simple.Release()
	simpleRecord := array.NewRecordBatch(simpleSchema, []arrow.Array{simple}, 1)
	defer simpleRecord.Release()

	var simplePayload Payload
	require.NoError(t, enc.encode(&simplePayload, simpleRecord))
	simplePayload.Release()
}
