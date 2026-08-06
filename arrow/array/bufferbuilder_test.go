// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License");  you may not use this file except in compliance
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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestMultiBufferBuilderUnsafeAppendPanicsOnTruncatedCopy(t *testing.T) {
	builder := multiBufferBuilder{mem: memory.NewGoAllocator(), blockSize: 8}
	builder.Retain()
	defer builder.Release()

	buf := memory.NewResizableBuffer(builder.mem)
	buf.ResizeNoShrink(1)
	builder.blocks = []*memory.Buffer{buf}
	builder.currentOutBuffer = 0

	var hdr arrow.ViewHeader
	assert.Panics(t, func() {
		builder.UnsafeAppend(&hdr, make([]byte, 64))
	})
}

func TestMultiBufferCheckpointRestoresTouchedBlocks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := multiBufferBuilder{mem: mem, blockSize: 4}
	builder.refCount.Add(1)
	defer builder.Release()

	first := memory.NewResizableBuffer(mem)
	first.ResizeNoShrink(4)
	first.Resize(2)
	second := memory.NewResizableBuffer(mem)
	second.ResizeNoShrink(4)
	builder.blocks = []*memory.Buffer{first, second}
	builder.currentOutBuffer = 1

	var hdr arrow.ViewHeader

	checkpoint := builder.newCheckpoint()
	checkpoint.capture()

	builder.Reserve(2)
	builder.UnsafeAppend(&hdr, []byte("gh"))
	builder.Reserve(1)
	builder.UnsafeAppend(&hdr, []byte("i"))

	checkpoint.restore()
	assert.Len(t, builder.blocks, 2)
	assert.Equal(t, 2, builder.blocks[0].Len())
	assert.Equal(t, 4, builder.blocks[1].Len())
	assert.Equal(t, 1, builder.currentOutBuffer)
}
