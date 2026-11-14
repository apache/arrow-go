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

package compute_test

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCastBinaryViewToStringViewChecksUTF8OfValueEndingAtInt32Limit(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("blocks above the int32 range require a 64-bit int")
	}
	const (
		tailLen    = 48
		tailOffset = math.MaxInt32 + 1 - tailLen
	)
	var size int64 = math.MaxInt32 + 1
	// The system commits mapped pages only when they are touched.
	block, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	defer func() { require.NoError(t, syscall.Munmap(block)) }()

	tail := bytes.Repeat([]byte("tail"), tailLen/4)
	tail[tailLen-1] = 0xff
	copy(block[tailOffset:], tail)

	var headers [1]arrow.ViewHeader
	headers[0].SetBytes(tail)
	headers[0].SetIndexOffset(0, tailOffset)
	viewBuf := memory.NewBufferBytes(append([]byte(nil), arrow.ViewHeaderTraits.CastToBytes(headers[:])...))
	data := array.NewData(arrow.BinaryTypes.BinaryView, 1,
		[]*memory.Buffer{nil, viewBuf, memory.NewBufferBytes(block)}, nil, 0, 0)
	defer data.Release()
	in := array.NewBinaryViewData(data)
	defer in.Release()

	_, err = compute.CastArray(context.Background(), in, compute.SafeCastOptions(arrow.BinaryTypes.StringView))
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}
