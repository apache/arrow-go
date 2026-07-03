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
	vb := streaming.NewStreamBuffer(memory.DefaultAllocator, bytes.NewReader(data), 0, nil)

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

func TestStreamBufferGrowsForOversizedValue(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 1<<20) // 1 MiB, larger than the cap below
	vb := streaming.NewStreamBuffer(memory.DefaultAllocator, bytes.NewReader(data), 1024, nil)

	buf, err := vb.Fill(len(data))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), len(data))
	assert.Equal(t, data, buf[:len(data)])
}
