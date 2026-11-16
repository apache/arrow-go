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

//go:build linux || darwin

package encoding

import (
	"math"
	"math/rand/v2"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// The unpacking loops load no further than alpUnpackSize bytes: placed right
// before a page that cannot be read, the packed deltas decode without a fault.
func TestALPDecodePackedGuardPage(t *testing.T) {
	pageSize := syscall.Getpagesize()
	mapped, err := syscall.Mmap(-1, 0, 2*pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syscall.Munmap(mapped)) })
	require.NoError(t, syscall.Mprotect(mapped[pageSize:], syscall.PROT_NONE))

	rng := rand.New(rand.NewPCG(5, 6))
	scratch := make([]uint32, 1024)
	for bitWidth := 1; bitWidth <= 64; bitWidth++ {
		for _, count := range []int{8, 9, 64, 65, 1024} {
			size := alpUnpackSize(count, bitWidth)
			if size > pageSize {
				continue
			}
			deltas := alpTestDeltas(rng, count, bitWidth)
			src := mapped[pageSize-size : pageSize]
			clear(src)
			copy(src, alpTestPack(deltas, bitWidth))
			ref := rng.Uint64() >> 20

			got := make([]float64, count)
			alpDecodePacked64(got, src, uint(bitWidth), ref, 1e3, 1e-7, scratch)
			for i, d := range deltas {
				want := float64(int64(d+ref)) * 1e3 * 1e-7
				require.Equal(t, math.Float64bits(want), math.Float64bits(got[i]),
					"double bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}

			if bitWidth > 32 {
				continue
			}
			got32 := make([]float32, count)
			alpDecodePacked32(got32, src, uint(bitWidth), uint32(ref), 1e3, 1e-7, scratch)
			for i, d := range deltas {
				want := float32(int32(uint32(d)+uint32(ref))) * float32(1e3) * float32(1e-7)
				require.Equal(t, math.Float32bits(want), math.Float32bits(got32[i]),
					"float bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}
		}
	}
}
