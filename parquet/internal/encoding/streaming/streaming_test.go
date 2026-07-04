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

package streaming_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamBuffer(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vb := streaming.NewStreamBuffer(memory.DefaultAllocator, bytes.NewReader(data), len(data), nil)

	buf, err := vb.Fill(4)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), 4)
	assert.Equal(t, []byte{0, 1, 2, 3}, buf[:4])

	vb.Advance(4)
	buf, err = vb.Fill(4)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), 4)
	assert.Equal(t, []byte{4, 5, 6, 7}, buf[:4])

	vb.Advance(4)
	_, err = vb.Fill(4)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestClipBatch(t *testing.T) {
	const buf = streaming.DefaultBufferSize
	// Value region larger than the buffer: clip so rows*avgWidth stays near the buffer.
	assert.Equal(t, int64(buf/1024), streaming.ClipBatch(4*buf, 4096)) // avg 1024 bytes
	assert.Equal(t, int64(1), streaming.ClipBatch(4*buf, 2))           // huge values: one per decode
	// Whole region fits one buffer, or no values: no clip (returns nvals).
	assert.Equal(t, int64(1_000_000), streaming.ClipBatch(buf/2, 1_000_000)) // small values
	assert.Equal(t, int64(1_000_000), streaming.ClipBatch(0, 1_000_000))     // all-null page
	assert.Equal(t, int64(0), streaming.ClipBatch(0, 0))
}

func TestStreamBufferGrowsForOversizedValue(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, (1<<20)+1024) // larger than the default buffer
	vb := streaming.NewStreamBuffer(memory.DefaultAllocator, bytes.NewReader(data), len(data), nil)

	buf, err := vb.Fill(len(data))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), len(data))
	assert.Equal(t, data, buf[:len(data)])
	require.NoError(t, vb.Close())
}
