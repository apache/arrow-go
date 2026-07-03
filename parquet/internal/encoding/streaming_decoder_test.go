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

package encoding_test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"testing/iotest"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding/streaming"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func plainByteArrayBytes(vals []parquet.ByteArray) []byte {
	var raw bytes.Buffer
	var l [4]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint32(l[:], uint32(len(v)))
		raw.Write(l[:])
		raw.Write(v)
	}
	return raw.Bytes()
}

func TestPlainByteArrayDecoderSetSource(t *testing.T) {
	vals := []parquet.ByteArray{[]byte("hello"), []byte(""), []byte("world!!"), bytes.Repeat([]byte("x"), 5000)}

	dec := encoding.NewDecoder(parquet.Types.ByteArray, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	sd, ok := dec.(streaming.Decoder)
	require.True(t, ok, "PlainByteArrayDecoder must implement streaming.Decoder")
	sd.SetSource(len(vals), streaming.NewStreamBuffer(memory.DefaultAllocator, iotest.OneByteReader(bytes.NewReader(plainByteArrayBytes(vals))), 0, nil))

	out := make([]parquet.ByteArray, len(vals))
	n, err := dec.(encoding.ByteArrayDecoder).Decode(out)
	require.NoError(t, err)
	require.Equal(t, len(vals), n)
	for i := range vals {
		assert.Equal(t, []byte(vals[i]), []byte(out[i]))
	}
}

func TestPlainFixedLenByteArrayDecoderSetSource(t *testing.T) {
	const w = 3
	descr := schema.NewColumn(schema.NewFixedLenByteArrayNode("f", parquet.Repetitions.Required, w, -1), 0, 0)
	vals := []parquet.FixedLenByteArray{[]byte("abc"), []byte("def"), bytes.Repeat([]byte("z"), w)}
	raw := make([]byte, 0, len(vals)*w)
	for _, v := range vals {
		raw = append(raw, v...)
	}

	dec := encoding.NewDecoder(parquet.Types.FixedLenByteArray, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	sd, ok := dec.(streaming.Decoder)
	require.True(t, ok, "PlainFixedLenByteArrayDecoder must implement streaming.Decoder")
	sd.SetSource(len(vals), streaming.NewStreamBuffer(memory.DefaultAllocator, iotest.OneByteReader(bytes.NewReader(raw)), 0, nil))

	out := make([]parquet.FixedLenByteArray, len(vals))
	n, err := dec.(encoding.FixedLenByteArrayDecoder).Decode(out)
	require.NoError(t, err)
	require.Equal(t, len(vals), n)
	for i := range vals {
		assert.Equal(t, []byte(vals[i]), []byte(out[i]))
	}
}

func TestFixedWidthDecoderNotStreaming(t *testing.T) {
	dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	_, ok := dec.(streaming.Decoder)
	assert.False(t, ok)
}
