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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestBuffer builds a streamBuffer with an undersized backing buffer so a
// few small values force the slide/fresh-buffer paths.
func newTestBuffer(data []byte, size int) *streamBuffer {
	buf := memory.DefaultAllocator.Allocate(size)
	return &streamBuffer{mem: memory.DefaultAllocator, r: bytes.NewReader(data), buf: buf, bufs: [][]byte{buf}}
}

// TestFillOwnedStaysValidAcrossRefill checks that bytes handed out by FillOwned
// survive a later refill: the refill must move to a fresh buffer rather than
// slide the one the caller still aliases.
func TestFillOwnedStaysValidAcrossRefill(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	s := newTestBuffer(data, 8)

	b, err := s.FillOwned(4)
	require.NoError(t, err)
	v1 := b[0:4]
	assert.Equal(t, []byte{0, 1, 2, 3}, v1)
	s.Advance(4)

	b, err = s.FillOwned(4)
	require.NoError(t, err)
	v2 := b[0:4]
	assert.Equal(t, []byte{4, 5, 6, 7}, v2)
	s.Advance(4)

	// Buffer is exhausted and shared: this refill must allocate a fresh buffer.
	b, err = s.FillOwned(4)
	require.NoError(t, err)
	assert.Equal(t, []byte{8, 9, 10, 11}, b[0:4])

	assert.Equal(t, []byte{0, 1, 2, 3}, v1, "aliased value corrupted by refill")
	assert.Equal(t, []byte{4, 5, 6, 7}, v2, "aliased value corrupted by refill")
}

// TestFillAfterFillOwnedPreservesAliases checks that a plain (reuse) Fill after a
// FillOwned does not overwrite the shared buffer.
func TestFillAfterFillOwnedPreservesAliases(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	s := newTestBuffer(data, 8)

	b, err := s.FillOwned(4)
	require.NoError(t, err)
	v1 := b[0:4]
	s.Advance(8) // consume the whole shared window

	_, err = s.Fill(4) // reuse mode, but must not clobber the shared buffer
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 1, 2, 3}, v1, "Fill overwrote FillOwned-aliased bytes")
}

func TestStreamBufferFreesBuffersOnClose(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	data := bytes.Repeat([]byte{1}, 64)

	buf := checked.Allocate(8) // tiny buffer forces fresh allocations on each refill
	s := &streamBuffer{mem: checked, r: bytes.NewReader(data), buf: buf, bufs: [][]byte{buf}}
	for {
		b, err := s.FillOwned(8)
		if err != nil {
			break
		}
		s.Advance(len(b)) // consume fully so the next FillOwned allocates fresh
	}
	require.NoError(t, s.Close())

	checked.AssertSize(t, 0)
}

// TestFillReusesBufferWhenNotShared checks the fast path: without an outstanding
// FillOwned, a refill slides in place rather than allocating.
func TestFillReusesBufferWhenNotShared(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	s := newTestBuffer(data, 8)

	_, err := s.Fill(4)
	require.NoError(t, err)
	require.False(t, s.shared)
	backing := &s.buf[0]
	s.Advance(8)

	_, err = s.Fill(4)
	require.NoError(t, err)
	assert.Same(t, backing, &s.buf[0], "expected in-place reuse, got a fresh buffer")
}
