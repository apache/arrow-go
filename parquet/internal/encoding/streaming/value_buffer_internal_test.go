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

package streaming

import (
	"bytes"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestBuffer builds a streamBuffer with a small chunk so a few reads force the
// rotate/oversized paths.
func newTestBuffer(mem memory.Allocator, data []byte, chunkSize int) *streamBuffer {
	return &streamBuffer{mem: mem, r: bytes.NewReader(data), chunkSize: chunkSize, cur: mem.Allocate(chunkSize), remaining: len(data)}
}

// fillValue reads the next need bytes as a value: Fill then Advance, returning the
// aliased slice.
func fillValue(t *testing.T, s *streamBuffer, need int) []byte {
	t.Helper()
	b, err := s.Fill(need)
	require.NoError(t, err)
	v := b[:need:need]
	s.Advance(need)
	return v
}

// TestStreamBufferAliasesStableUntilRecycle checks that values aliased across a chunk
// rotation stay valid until Recycle: the filled chunk is set aside, not overwritten.
func TestStreamBufferAliasesStableUntilRecycle(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	s := newTestBuffer(memory.DefaultAllocator, data, 8)

	v1 := fillValue(t, s, 4) // [0..3]
	v2 := fillValue(t, s, 4) // [4..7], fills the chunk
	v3 := fillValue(t, s, 4) // [8..11], forces a fresh chunk

	assert.Equal(t, []byte{0, 1, 2, 3}, v1, "aliased value corrupted by rotation")
	assert.Equal(t, []byte{4, 5, 6, 7}, v2, "aliased value corrupted by rotation")
	assert.Equal(t, []byte{8, 9, 10, 11}, v3)

	require.NoError(t, s.Close())
}

// TestStreamBufferOversizedValue checks that a value larger than the chunk allocates a
// fresh chunk to hold it, and Recycle swaps back to the steady-state size.
func TestStreamBufferOversizedValue(t *testing.T) {
	big := bytes.Repeat([]byte{0xAB}, 20)
	data := append([]byte{1, 2, 3, 4}, big...)
	s := newTestBuffer(memory.DefaultAllocator, data, 8)

	assert.Equal(t, []byte{1, 2, 3, 4}, fillValue(t, s, 4))
	assert.Equal(t, big, fillValue(t, s, 20))
	assert.Greater(t, len(s.cur), 8, "buffer should grow for the oversized value")

	s.Recycle()
	assert.Equal(t, 8, len(s.cur), "buffer should shrink back to the steady-state size")

	require.NoError(t, s.Close())
}

// TestStreamBufferSkip checks Skip discards both buffered and not-yet-read bytes.
func TestStreamBufferSkip(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	s := newTestBuffer(memory.DefaultAllocator, data, 8)

	assert.Equal(t, []byte{0, 1}, fillValue(t, s, 2))
	require.NoError(t, s.Skip(6)) // 2 buffered + 4 straight from the reader
	assert.Equal(t, []byte{8, 9}, fillValue(t, s, 2))

	require.NoError(t, s.Close())
}

// TestStreamBufferRejectsOversizedNeed checks a need past the value region (a corrupt
// length prefix) is rejected before allocating.
func TestStreamBufferRejectsOversizedNeed(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	s := newTestBuffer(checked, make([]byte, 10), 8)

	_, err := s.Fill(1 << 30)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	require.NoError(t, s.Close())
	checked.AssertSize(t, 0) // the 1 GiB was never allocated
}

// TestStreamBufferRecycleReclaims checks that repeated Recycle keeps memory bounded and
// frees the extra chunks a rotation created, so nothing leaks across Decodes.
func TestStreamBufferRecycleReclaims(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	s := newTestBuffer(checked, bytes.Repeat([]byte{1}, 1000), 8)

	for i := 0; i < 50; i++ {
		fillValue(t, s, 5) // off=5
		fillValue(t, s, 5) // forces a rotation: old chunk goes to live
		require.NotEmpty(t, s.live)
		s.Recycle() // must free the retired chunk
		require.Empty(t, s.live)
	}

	require.NoError(t, s.Close())
	checked.AssertSize(t, 0)
}
