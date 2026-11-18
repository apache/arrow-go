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

//go:build !noasm

package encoding

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		alpDecodeGroups32 = alpDecodeGroups32AVX2
		alpDecodeGroups64 = alpDecodeGroups64AVX2
	}
}

// alpAVX2MaxBitWidth is the widest delta the AVX2 kernels decode. Wider, a
// delta can span 5 bytes, more than its 32-bit lane holds.
const alpAVX2MaxBitWidth = 25

// alpAVX2Table is what the kernels need to unpack one width: the shuffle that
// moves the 4 bytes holding each of a group's 8 deltas into its own 32-bit
// lane, the shift that brings the delta down to bit 0, and its mask.
type alpAVX2Table struct {
	shuffle [32]byte
	shifts  [8]uint32
	masks   [8]uint32
}

var alpAVX2Tables = func() (tables [alpAVX2MaxBitWidth + 1]alpAVX2Table) {
	for w := 1; w <= alpAVX2MaxBitWidth; w++ {
		t := &tables[w]
		for j := range 8 {
			// The upper 16 bytes of the register are loaded from w/2 bytes
			// into the group, and a shuffle cannot cross into the other half.
			bit := j * w
			if j >= 4 {
				bit -= 8 * (w / 2)
			}
			for b := range 4 {
				t.shuffle[4*j+b] = byte(bit/8 + b)
			}
			t.shifts[j] = uint32(bit % 8)
			t.masks[j] = 1<<w - 1
		}
	}
	return
}()

// alpAVX2Groups returns how many whole groups of 8 deltas of bitWidth bits the
// kernels can decode into count values from src without loading past its end.
// A group's loads reach 16 bytes past w/2 bytes into it.
func alpAVX2Groups(count, srcLen int, bitWidth uint) int {
	w := int(bitWidth)
	reach := w/2 + 16
	if w > alpAVX2MaxBitWidth || srcLen < reach {
		return 0
	}
	return min(count/8, (srcLen-reach)/w+1)
}

//go:noescape
func _alpDecodeGroups32AVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint32, fac, exp float32)

//go:noescape
func _alpDecodeGroups64AVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint64, fac, exp float64)

//go:noescape
func _alpDecodeGroups64SmallAVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint64, fac, exp float64)

func alpDecodeGroups32AVX2(out []float32, src []byte, bitWidth uint, ref uint32, fac, exp float32) int {
	groups := alpAVX2Groups(len(out), len(src), bitWidth)
	if groups == 0 {
		return 0
	}
	_alpDecodeGroups32AVX2(unsafe.Pointer(&src[0]), unsafe.Pointer(&out[0]), groups, int(bitWidth),
		&alpAVX2Tables[bitWidth], ref, fac, exp)
	return groups * 8
}

// alpSmallLimit bounds the integers that the short conversion handles: those
// in [-2^51, 2^51).
const alpSmallLimit = 1 << 51

func alpDecodeGroups64AVX2(out []float64, src []byte, bitWidth uint, ref uint64, fac, exp float64) int {
	groups := alpAVX2Groups(len(out), len(src), bitWidth)
	if groups == 0 {
		return 0
	}
	kernel := _alpDecodeGroups64AVX2
	// The vector's integers run from ref to ref + 2^bitWidth - 1. When all of
	// them are small enough, a shorter conversion is exact.
	if r := int64(ref); r >= -alpSmallLimit && r <= alpSmallLimit-1<<bitWidth {
		kernel = _alpDecodeGroups64SmallAVX2
	}
	kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&out[0]), groups, int(bitWidth),
		&alpAVX2Tables[bitWidth], ref, fac, exp)
	return groups * 8
}
