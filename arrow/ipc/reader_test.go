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
	"fmt"
	"io"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderCatchPanic(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	buf := new(bytes.Buffer)
	writer := NewWriter(buf, WithSchema(schema))
	require.NoError(t, writer.Write(rec))

	for i := buf.Len() - 100; i < buf.Len(); i++ {
		buf.Bytes()[i] = 0
	}

	reader, err := NewReader(buf)
	require.NoError(t, err)

	_, err = reader.Read()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "arrow/ipc: unknown error while reading")
	}
}

func TestReaderCheckedAllocator(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "s",
			Type: &arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.String,
				IndexType: arrow.PrimitiveTypes.Int32,
			},
		},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	bldr := b.Field(0).(*array.BinaryDictionaryBuilder)
	bldr.Append([]byte("foo"))
	bldr.Append([]byte("bar"))
	bldr.Append([]byte("baz"))

	rec := b.NewRecordBatch()
	defer rec.Release()

	buf := new(bytes.Buffer)
	writer := NewWriter(buf, WithSchema(schema), WithAllocator(alloc))
	defer writer.Close()
	require.NoError(t, writer.Write(rec))

	reader, err := NewReader(buf, WithAllocator(alloc))
	require.NoError(t, err)
	defer reader.Release()

	_, err = reader.Read()
	require.NoError(t, err)
}

func TestMappedReader(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4}, []bool{true, true, false, true})

	rec1 := b.NewRecordBatch()
	defer rec1.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec1})
	defer tbl.Release()

	var buf bytes.Buffer
	ipcWriter, err := NewFileWriter(&buf, WithAllocator(pool), WithSchema(schema))
	require.NoError(t, err)

	t.Log("Reading data before")
	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	n := 0
	for tr.Next() {
		rec := tr.RecordBatch()
		for i, col := range rec.Columns() {
			t.Logf("rec[%d][%q]: %v nulls:%v\n", n,
				rec.ColumnName(i), col, col.NullBitmapBytes())
		}
		n++
		err := ipcWriter.Write(rec)
		if err != nil {
			panic(err)
		}
	}
	require.NoError(t, ipcWriter.Close())

	t.Log("Reading data after")
	rdr, err := NewMappedFileReader(buf.Bytes(), WithAllocator(pool))
	require.NoError(t, err)
	defer rdr.Close()

	rec, err := rdr.RecordAt(0)
	require.NoError(t, err)
	defer rec.Release()

	// get offset and block info into the buffer bytes
	blk, err := rdr.r.block(nil, &rdr.footer, 0)
	require.NoError(t, err)

	// determine pointer location of bytes for the first buffer
	// no nulls, so only one buffer
	start := unsafe.Pointer(unsafe.SliceData(buf.Bytes()))
	loc := unsafe.Add(unsafe.Add(start, blk.Offset()), blk.Meta())
	// ensure our buffer pointer matches the calculated pointer
	assert.Equal(t, (*byte)(loc), unsafe.SliceData(rec.Column(0).Data().Buffers()[1].Bytes()))

	rec, err = rdr.RecordAt(1)
	require.NoError(t, err)
	defer rec.Release()

	blk, err = rdr.r.block(nil, &rdr.footer, 1)
	require.NoError(t, err)

	start = unsafe.Pointer(unsafe.SliceData(buf.Bytes()))
	loc = unsafe.Add(unsafe.Add(start, blk.Offset()), blk.Meta())
	// check pointer of validity bitmap location
	assert.Equal(t, (*byte)(loc), unsafe.SliceData(rec.Column(0).Data().Buffers()[0].Bytes()))
	// calculate and check pointer of data buffer
	loc = unsafe.Add(loc, rec.Column(0).Data().Buffers()[0].Len())
	assert.Equal(t, (*byte)(loc), unsafe.SliceData(rec.Column(0).Data().Buffers()[1].Bytes()))
}

func TestMappedReaderDictionary(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value",
		Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		},
	}}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	col := b.Field(0).(*array.BinaryDictionaryBuilder)
	for _, value := range []string{"alpha", "beta", "alpha"} {
		require.NoError(t, col.AppendString(value))
	}
	record := b.NewRecordBatch()
	defer record.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(&buf, WithAllocator(pool), WithSchema(schema))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	rdr, err := NewMappedFileReader(buf.Bytes(), WithAllocator(pool))
	require.NoError(t, err)
	defer rdr.Close()

	got, err := rdr.RecordBatchAt(0)
	require.NoError(t, err)
	defer got.Release()
	require.True(t, array.RecordEqual(record, got))
}

func BenchmarkIPC(b *testing.B) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(b, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "s",
			Type: &arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.String,
				IndexType: arrow.PrimitiveTypes.Int32,
			},
		},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	bldr := rb.Field(0).(*array.BinaryDictionaryBuilder)
	bldr.Append([]byte("foo"))
	bldr.Append([]byte("bar"))
	bldr.Append([]byte("baz"))

	rec := rb.NewRecordBatch()
	defer rec.Release()

	for _, codec := range []struct {
		name        string
		codecOption Option
	}{
		{
			name: "plain",
		},
		{
			name:        "zstd",
			codecOption: WithZstd(),
		},
		{
			name:        "lz4",
			codecOption: WithLZ4(),
		},
	} {
		options := []Option{WithSchema(schema), WithAllocator(alloc)}
		if codec.codecOption != nil {
			options = append(options, codec.codecOption)
		}
		b.Run(fmt.Sprintf("Writer/codec=%s", codec.name), func(b *testing.B) {
			buf := new(bytes.Buffer)
			for i := 0; i < b.N; i++ {
				func() {
					buf.Reset()
					writer := NewWriter(buf, options...)
					defer writer.Close()
					if err := writer.Write(rec); err != nil {
						b.Fatal(err)
					}
				}()
			}
		})

		b.Run(fmt.Sprintf("Reader/codec=%s", codec.name), func(b *testing.B) {
			buf := new(bytes.Buffer)
			writer := NewWriter(buf, options...)
			defer writer.Close()
			require.NoError(b, writer.Write(rec))
			bufBytes := buf.Bytes()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					reader, err := NewReader(bytes.NewReader(bufBytes), WithAllocator(alloc))
					if err != nil {
						b.Fatal(err)
					}
					defer reader.Release()
					for {
						if _, err := reader.Read(); err != nil {
							if err == io.EOF {
								break
							}
							b.Fatal(err)
						}
					}
				}()
			}
		})
	}
}

// writeSplitStream writes recs with a single Writer and returns the bytes it
// produced for each record as a separate blob: the first blob carries the
// schema and dictionaries, and the last one also carries the end-of-stream
// marker.
func writeSplitStream(t *testing.T, mem memory.Allocator, schema *arrow.Schema, recs []arrow.RecordBatch) [][]byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, WithSchema(schema), WithAllocator(mem))
	blobs := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		require.NoError(t, w.Write(rec))
		blobs = append(blobs, bytes.Clone(buf.Bytes()))
		buf.Reset()
	}
	require.NoError(t, w.Close())
	blobs[len(blobs)-1] = append(blobs[len(blobs)-1], buf.Bytes()...)
	return blobs
}

func TestReaderContinueFrom(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "d", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}},
	}, nil)

	recs := make([]arrow.RecordBatch, 3)
	for n := range recs {
		b := array.NewRecordBuilder(mem, schema)
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(2 * n), int64(2*n + 1)}, nil)
		require.NoError(t, b.Field(1).(*array.BinaryDictionaryBuilder).AppendString("foo"))
		require.NoError(t, b.Field(1).(*array.BinaryDictionaryBuilder).AppendString("bar"))
		recs[n] = b.NewRecordBatch()
		b.Release()
		defer recs[n].Release()
	}
	blobs := writeSplitStream(t, mem, schema, recs)

	readAll := func(t *testing.T, useRead bool) {
		rdr, err := NewReader(bytes.NewReader(blobs[0]), WithAllocator(mem))
		require.NoError(t, err)
		defer rdr.Release()

		var got []arrow.RecordBatch
		for i, blob := range blobs {
			if i > 0 {
				require.NoError(t, rdr.ContinueFrom(bytes.NewReader(blob)))
			}
			for {
				var rec arrow.RecordBatch
				if useRead {
					rec, err = rdr.Read()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
				} else {
					if !rdr.Next() {
						require.NoError(t, rdr.Err())
						break
					}
					rec = rdr.RecordBatch()
				}
				rec.Retain()
				got = append(got, rec)
			}
		}

		require.Len(t, got, len(recs))
		for i, rec := range got {
			assert.Truef(t, array.RecordEqual(recs[i], rec), "batch %d: got %v, want %v", i, rec, recs[i])
			rec.Release()
		}
	}

	t.Run("Next", func(t *testing.T) { readAll(t, false) })
	t.Run("Read", func(t *testing.T) { readAll(t, true) })

	t.Run("SchemaMessageInContinuation", func(t *testing.T) {
		rdr, err := NewReader(bytes.NewReader(blobs[0]), WithAllocator(mem))
		require.NoError(t, err)
		defer rdr.Release()
		for rdr.Next() {
		}
		require.NoError(t, rdr.Err())

		// a whole new stream, schema message included, is not a continuation
		require.NoError(t, rdr.ContinueFrom(bytes.NewReader(bytes.Join(blobs, nil))))
		assert.False(t, rdr.Next())
		assert.ErrorContains(t, rdr.Err(), "unexpected schema message")

		_, err = rdr.Read()
		assert.ErrorContains(t, err, "unexpected schema message")

		// the failure is sticky: continuing again must not hide it
		assert.ErrorContains(t, rdr.ContinueFrom(bytes.NewReader(blobs[1])), "unexpected schema message")
	})

	t.Run("SchemaNotRead", func(t *testing.T) {
		rdr, err := NewReader(bytes.NewReader(blobs[0]), WithAllocator(mem), WithDelayReadSchema(true))
		require.NoError(t, err)
		defer rdr.Release()

		assert.ErrorContains(t, rdr.ContinueFrom(bytes.NewReader(blobs[1])), "has not read its schema")
		// the reader is untouched and still reads its original source
		assert.True(t, rdr.Next())
		assert.EqualValues(t, 2, rdr.RecordBatch().NumRows())
	})
}

func TestReaderSizeLimits(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	// a small batch whose body fits under the limit, then one whose body
	// (1000 int64 values) does not
	recs := make([]arrow.RecordBatch, 2)
	for n, rows := range []int{2, 1000} {
		b := array.NewRecordBuilder(mem, schema)
		b.Field(0).(*array.Int64Builder).AppendValues(make([]int64, rows), nil)
		recs[n] = b.NewRecordBatch()
		b.Release()
		defer recs[n].Release()
	}
	blobs := writeSplitStream(t, mem, schema, recs)
	const bodyLimit = 1024

	t.Run("NewReader", func(t *testing.T) {
		rdr, err := NewReader(bytes.NewReader(bytes.Join(blobs, nil)), WithAllocator(mem), WithBodySizeLimit(bodyLimit))
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, rdr.Next())
		assert.False(t, rdr.Next())
		assert.ErrorContains(t, rdr.Err(), "exceeds limit 1024")
	})

	t.Run("ContinueFrom", func(t *testing.T) {
		rdr, err := NewReader(bytes.NewReader(blobs[0]), WithAllocator(mem), WithBodySizeLimit(bodyLimit))
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, rdr.Next())
		require.False(t, rdr.Next())
		require.NoError(t, rdr.Err())

		require.NoError(t, rdr.ContinueFrom(bytes.NewReader(blobs[1])))
		assert.False(t, rdr.Next())
		assert.ErrorContains(t, rdr.Err(), "exceeds limit 1024")
	})
}
