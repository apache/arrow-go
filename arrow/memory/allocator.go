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

package memory

const (
	alignment = 64
)

type Allocator interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}

// AlignedAllocator is an Allocator that returns buffers aligned to 64-byte boundaries.
type AlignedAllocator interface {
	Allocator
	// AllocateAligned returns a buffer of the requested size aligned to 64-byte boundaries. buf is the aligned buffer, and alloc is the actual allocation that should be passed to Free.  If alloc is nil, then it is the same as buf.
	AllocateAligned(size int) (buf []byte, alloc []byte)
	ReallocateAligned(size int, buf []byte, alloc []byte) (newBuf []byte, newAlloc []byte)
}

// MakeAlignedAllocator makes an AlignedAllocator for the given Allocator.  If the Allocator already implements AlignedAllocator, it is returned as-is.  There is usually no need to call this directly as Buffer will call this for you.
func MakeAlignedAllocator(mem Allocator) AlignedAllocator {
	if aligned, ok := mem.(AlignedAllocator); ok {
		return aligned
	}
	return &alignedAllocator{mem}
}

type alignedAllocator struct {
	mem Allocator
}

func (a *alignedAllocator) Allocate(size int) []byte {
	return a.mem.Allocate(size)
}

func (a *alignedAllocator) Reallocate(size int, b []byte) []byte {
	return a.mem.Reallocate(size, b)
}

func (a *alignedAllocator) Free(b []byte) {
	a.mem.Free(b)
}

func (a *alignedAllocator) AllocateAligned(size int) ([]byte, []byte) {
	buf := a.mem.Allocate(size + alignment)
	addr := int(addressOf(buf))
	next := roundUpToMultipleOf64(addr)
	if addr != next {
		shift := next - addr
		return buf[shift : size+shift : size+shift], buf
	}
	return buf[:size:size], buf
}

func (a *alignedAllocator) ReallocateAligned(size int, buf []byte, alloc []byte) (newBuf []byte, newAlloc []byte) {
	if cap(buf) >= size {
		return buf[:size], alloc
	}
	newBuf, newAlloc = a.AllocateAligned(size)
	copy(newBuf, buf)
	if alloc != nil {
		a.Free(alloc)
	}
	return
}
