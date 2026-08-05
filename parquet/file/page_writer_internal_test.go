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

package file

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/stretchr/testify/require"
)

type shortPageSink struct {
	data        []byte
	pos         int64
	headerShort bool
}

func (s *shortPageSink) Tell() int64 { return s.pos }

func (s *shortPageSink) Write(p []byte) (int, error) {
	n := len(p)
	if s.headerShort || bytes.Equal(p, s.data) {
		n--
	}
	s.pos += int64(n)
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func TestSerializedPageWriterRejectsShortHeaderWrites(t *testing.T) {
	sink := &shortPageSink{headerShort: true, pos: 17}
	writer, err := NewPageWriter(sink, compress.Codecs.Uncompressed,
		compress.DefaultCompressionLevel, nil, -1, -1, memory.DefaultAllocator, false, nil, nil)
	require.NoError(t, err)

	buf := memory.NewBufferBytes([]byte("page body"))
	defer buf.Release()
	_, err = writer.WriteDictionaryPage(NewDictionaryPage(buf, 1, parquet.Encodings.Plain))
	require.ErrorIs(t, err, io.ErrShortWrite)
	serialized := writer.(*serializedPageWriter)
	require.Zero(t, serialized.NumValues())
	require.Zero(t, serialized.DictionaryPageOffset())
}

func TestSerializedPageWriterRejectsShortWrites(t *testing.T) {
	data := []byte("page body")

	tests := []struct {
		name        string
		wantWritten int64
		write       func(PageWriter, *memory.Buffer) (int64, error)
	}{
		{
			name:        "dictionary page",
			wantWritten: 0,
			write: func(writer PageWriter, data *memory.Buffer) (int64, error) {
				return writer.WriteDictionaryPage(NewDictionaryPage(data, 1, parquet.Encodings.Plain))
			},
		},
		{
			name:        "data page",
			wantWritten: int64(len(data) - 1),
			write: func(writer PageWriter, data *memory.Buffer) (int64, error) {
				return writer.WriteDataPage(NewDataPageV1(data, 1, parquet.Encodings.Plain,
					parquet.Encodings.RLE, parquet.Encodings.RLE, int32(data.Len())))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &shortPageSink{data: data, pos: 17}
			writer, err := NewPageWriter(sink, compress.Codecs.Uncompressed,
				compress.DefaultCompressionLevel, nil, -1, -1, memory.DefaultAllocator, false, nil, nil)
			require.NoError(t, err)

			buf := memory.NewBufferBytes(data)
			defer buf.Release()
			written, err := tt.write(writer, buf)
			require.True(t, errors.Is(err, io.ErrShortWrite))
			require.Equal(t, tt.wantWritten, written)
			serialized := writer.(*serializedPageWriter)
			require.Zero(t, serialized.NumValues())
			require.Zero(t, serialized.DictionaryPageOffset())
			require.Zero(t, serialized.DataPageoffset())
		})
	}
}
