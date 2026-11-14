//go:build darwin || linux

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

package array

import (
	"bytes"
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	viewTailLen    = 48
	viewTailOffset = math.MaxInt32 + 1 - viewTailLen
)

// mappedBlock returns size bytes of address space that the system commits
// only for the pages a test touches.
func mappedBlock(t *testing.T, size int64) []byte {
	t.Helper()
	if strconv.IntSize < 64 {
		t.Skip("blocks above the int32 range require a 64-bit int")
	}
	b, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syscall.Munmap(b)) })
	return b
}

type mappedAllocator struct {
	memory.GoAllocator
	block []byte
}

func (a *mappedAllocator) Allocate(size int) []byte {
	if size < 1<<30 {
		return a.GoAllocator.Allocate(size)
	}
	return a.block[:size:size]
}

// tailViewParts returns the headers and data buffer of a one element view
// array whose value ends at the last byte of a 2 GiB block.
func tailViewParts(t *testing.T, tail []byte) ([]byte, *memory.Buffer) {
	t.Helper()
	block := mappedBlock(t, math.MaxInt32+1)
	copy(block[viewTailOffset:], tail)

	var headers [1]arrow.ViewHeader
	headers[0].SetBytes(tail)
	headers[0].SetIndexOffset(0, viewTailOffset)
	return append([]byte(nil), arrow.ViewHeaderTraits.CastToBytes(headers[:])...), memory.NewBufferBytes(block)
}

func TestBinaryViewBuilderKeepsOffsetsWithinInt32(t *testing.T) {
	// The first value, 2147483600 bytes, is not appended: its block is
	// reserved and marked full. The block capacity rounds up to 2^31, which
	// leaves 48 bytes of slack. Only the last of them ends past MaxInt32.
	tests := []struct {
		name       string
		valueLen   int
		wantOffset int32
	}{
		{"value ending past MaxInt32 starts a new block", viewTailLen, 0},
		{"value ending at MaxInt32 stays in the block", viewTailLen - 1, viewTailOffset},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(&mappedAllocator{block: mappedBlock(t, math.MaxInt32+1)})
			defer mem.AssertSize(t, 0)

			b := NewBinaryViewBuilder(mem)
			defer b.Release()
			b.ReserveData(viewTailOffset)
			b.blockBuilder.blocks[0].ResizeNoShrink(viewTailOffset)
			b.Append(bytes.Repeat([]byte("v"), tc.valueLen))

			arr := b.NewBinaryViewArray()
			defer arr.Release()
			assert.Equal(t, tc.wantOffset, arr.ValueHeader(0).BufferOffset())
		})
	}
}

func TestBinaryViewBuilderBlockSizeAboveInt32(t *testing.T) {
	const blockSize = math.MaxInt32 + 1 + 1<<20
	mem := memory.NewCheckedAllocator(&mappedAllocator{block: mappedBlock(t, blockSize)})
	defer mem.AssertSize(t, 0)

	b := NewBinaryViewBuilder(mem)
	defer b.Release()
	b.SetBlockSize(blockSize)
	b.ReserveData(1)
	pastLimit := int64(math.MaxInt32) + 64
	b.blockBuilder.blocks[0].ResizeNoShrink(int(pastLimit))
	b.Append(bytes.Repeat([]byte("tail"), viewTailLen/4))

	arr := b.NewBinaryViewArray()
	defer arr.Release()
	assert.Equal(t, int32(0), arr.ValueHeader(0).BufferOffset())
}

func TestBinaryViewValueEndingAtInt32Limit(t *testing.T) {
	tail := bytes.Repeat([]byte("tail"), viewTailLen/4)
	headers, block := tailViewParts(t, tail)
	arr := makeBinaryViewArrayRaw(t, headers, []*memory.Buffer{block}, nil, 0, 1, 0)
	defer arr.Release()

	require.NoError(t, arr.ValidateFull())
	require.NotPanics(t, func() { assert.Equal(t, tail, arr.Value(0)) })
}

func TestStringViewValueEndingAtInt32Limit(t *testing.T) {
	tail := bytes.Repeat([]byte("tail"), viewTailLen/4)
	headers, block := tailViewParts(t, tail)
	arr := makeStringViewArrayRaw(t, headers, []*memory.Buffer{block}, nil, 0, 1, 0)
	defer arr.Release()

	require.NoError(t, arr.ValidateFull())
	require.NotPanics(t, func() { assert.Equal(t, string(tail), arr.Value(0)) })
}

func TestBinaryViewEqualWithValueEndingAtInt32Limit(t *testing.T) {
	tail := bytes.Repeat([]byte("tail"), viewTailLen/4)
	headers, block := tailViewParts(t, tail)
	arr := makeBinaryViewArrayRaw(t, headers, []*memory.Buffer{block}, nil, 0, 1, 0)
	defer arr.Release()

	other := append([]byte(nil), tail...)
	other[len(other)-1] = 'x'

	b := NewBinaryViewBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(tail)
	same := b.NewArray()
	defer same.Release()
	b.Append(other)
	different := b.NewArray()
	defer different.Release()

	require.NotPanics(t, func() {
		assert.True(t, Equal(arr, same))
		assert.False(t, Equal(arr, different))
	})
}
