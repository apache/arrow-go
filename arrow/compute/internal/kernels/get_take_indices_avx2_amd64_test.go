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

//go:build go1.18 && amd64 && !noasm && !appengine

package kernels

import (
	"io"
	"runtime/trace"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

func TestGetTakeIndicesUint32AVX2(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 is not available")
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const length = 256*8 + 3
	values := make([]bool, length)
	want := make([]uint32, 0, length/2)
	for i := range values {
		mask := byte((i/8 + 1) % 256)
		values[i] = mask&(1<<uint(i%8)) != 0
		if values[i] {
			want = append(want, uint32(i))
		}
	}

	t.Run("fragmented_aligned_tail", func(t *testing.T) {
		filter := makeSlicedBooleanFilter(t, values, nil, 8, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.True(t, ok)
		defer result.Release()
		assertTakeIndices[uint32](t, result, want, nil)
	})

	t.Run("runtime_trace", func(t *testing.T) {
		filter := makeSlicedBooleanFilter(t, values, nil, 8, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		startedTrace := !trace.IsEnabled()
		if startedTrace {
			require.NoError(t, trace.Start(io.Discard))
			defer trace.Stop()
		}

		for i := 0; i < 1024; i++ {
			result, ok := getTakeIndicesUint32AVX2(mem, &span)
			require.True(t, ok)
			result.Release()
		}
	})

	t.Run("dirty_tail_padding", func(t *testing.T) {
		const tailLength = 67
		filterBytes := []byte{0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0xff}
		want := make([]uint32, 0, tailLength/2)
		for i := 0; i < tailLength; i++ {
			if filterBytes[i/8]&(1<<uint(i%8)) != 0 {
				want = append(want, uint32(i))
			}
		}

		valuesBuffer := memory.NewBufferBytes(filterBytes)
		defer valuesBuffer.Release()
		data := array.NewData(arrow.FixedWidthTypes.Boolean, tailLength, []*memory.Buffer{nil, valuesBuffer}, nil, 0, 0)
		defer data.Release()
		var span exec.ArraySpan
		span.SetMembers(data)

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.True(t, ok)
		defer result.Release()
		assertTakeIndices[uint32](t, result, want, nil)
	})

	t.Run("unaligned_offset_falls_back", func(t *testing.T) {
		filter := makeSlicedBooleanFilter(t, values, nil, 1, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.False(t, ok)
		require.Nil(t, result)
	})

	t.Run("long_run_falls_back", func(t *testing.T) {
		longRuns := make([]bool, length)
		for i := range longRuns {
			longRuns[i] = i%1024 < 900
		}
		filter := makeBooleanFilter(t, longRuns, nil, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.False(t, ok)
		require.Nil(t, result)
	})

	t.Run("nullable_filter_falls_back", func(t *testing.T) {
		valid := make([]bool, length)
		for i := range valid {
			valid[i] = true
		}
		valid[64] = false
		filter := makeBooleanFilter(t, values, valid, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.False(t, ok)
		require.Nil(t, result)
	})

	t.Run("short_filter_falls_back", func(t *testing.T) {
		filter := makeBooleanFilter(t, values[:63], nil, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		result, ok := getTakeIndicesUint32AVX2(mem, &span)
		require.False(t, ok)
		require.Nil(t, result)
	})
}

func TestGetTakeIndicesUint32AVX2MaskedStores(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 is not available")
	}

	const sentinel = ^uint32(0)
	for _, nbytes := range []int{1, 9} {
		filter := make([]byte, nbytes)
		for i := range filter {
			filter[i] = 0x55
		}
		// Guard both ends of the output and every lane after the selected values.
		got := make([]uint32, nbytes*8+2)
		want := make([]uint32, len(got))
		for tailBits := 1; tailBits <= 8; tailBits++ {
			for mask := 0; mask < 256; mask++ {
				filter[nbytes-1] = byte(mask)
				for i := range got {
					got[i], want[i] = sentinel, sentinel
				}
				pos := 1
				for i := 0; i < (nbytes-1)*8+tailBits; i++ {
					if filter[i/8]&(1<<uint(i%8)) != 0 {
						want[pos] = uint32(i)
						pos++
					}
				}

				_get_take_indices_uint32_avx2(
					unsafe.Pointer(&filter[0]),
					unsafe.Pointer(&got[1]),
					unsafe.Pointer(&getTakeIndicesUint32AVX2Tables[0]),
					int64(nbytes),
					int64(1<<uint(tailBits))-1,
				)
				require.Equal(t, want, got, "nbytes=%d tailBits=%d mask=%#02x", nbytes, tailBits, mask)
			}
		}
	}
}
