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

package encoding

// The loops that turn one ALP vector back into values, written once for each
// physical type. A loop generic over the type looks the type up for every
// value; these do not.

import (
	"encoding/binary"
	"math"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
)

// alpUnpackSlack is how many bytes past the first byte of a vector's last
// packed delta the unpacking loops may load. A delta starts at most 7 bits
// into a byte, so up to 57 bits wide it lies inside one 8-byte load, and a
// wider one takes a ninth byte.
const alpUnpackSlack = 9

// alpUnpackSize returns how many bytes of packed deltas the unpacking loops
// load for count values of bitWidth bits. That is more than the deltas occupy:
// a load takes in whatever follows them, and a mask discards it.
func alpUnpackSize(count, bitWidth int) int {
	return (count-1)*bitWidth/8 + alpUnpackSlack
}

// alpDecodePacked64 unpacks len(out) deltas of bitWidth bits from the front of
// src, adds the frame of reference to each and scales it. bitWidth is 1 to 64,
// and src holds alpUnpackSize bytes.
//
// Deltas of up to 32 bits are unpacked 32 at a time into scratch, which holds
// len(out) values, and scaled in a second pass. Where the unpacking is
// vectorized, that is faster than doing both in one pass.
//
// Wider deltas, the few left of a short vector, and every delta on a platform
// without a vectorized unpacker are unpacked and scaled one at a time.
func alpDecodePacked64(out []float64, src []byte, bitWidth uint, ref uint64, fac, exp float64, scratch []uint32) {
	if bitWidth <= 32 {
		n := utils.Unpack32Vectorized(src, scratch[:len(out)], int(bitWidth))
		head := out[:n]
		for i, d := range scratch[:n] {
			head[i] = float64(int64(uint64(d)+ref)) * fac * exp
		}
		// A group of 32 values ends on a byte boundary at any width.
		out, src = out[n:], src[n*int(bitWidth)/8:]
	}

	mask := ^uint64(0) >> (64 - bitWidth)
	bitPos := uint(0)
	if bitWidth > 57 {
		for i := range out {
			offset, shift := bitPos>>3, bitPos&7
			lo := binary.LittleEndian.Uint64(src[offset:])
			hi := uint64(src[offset+8])
			// Shifting by 64 gives zero, which is what a delta that starts
			// on a byte boundary needs of the ninth byte.
			d := (lo>>shift | hi<<(64-shift)) & mask
			out[i] = float64(int64(d+ref)) * fac * exp
			bitPos += bitWidth
		}
		return
	}

	// Four at a time: the loads do not depend on one another, and the loop's
	// own bookkeeping is otherwise a large share of the work.
	i := 0
	for ; i+4 <= len(out); i += 4 {
		o := out[i : i+4 : i+4]
		p1, p2, p3 := bitPos+bitWidth, bitPos+2*bitWidth, bitPos+3*bitWidth
		d0 := binary.LittleEndian.Uint64(src[bitPos>>3:]) >> (bitPos & 7) & mask
		d1 := binary.LittleEndian.Uint64(src[p1>>3:]) >> (p1 & 7) & mask
		d2 := binary.LittleEndian.Uint64(src[p2>>3:]) >> (p2 & 7) & mask
		d3 := binary.LittleEndian.Uint64(src[p3>>3:]) >> (p3 & 7) & mask
		o[0] = float64(int64(d0+ref)) * fac * exp
		o[1] = float64(int64(d1+ref)) * fac * exp
		o[2] = float64(int64(d2+ref)) * fac * exp
		o[3] = float64(int64(d3+ref)) * fac * exp
		bitPos += 4 * bitWidth
	}
	for ; i < len(out); i++ {
		d := binary.LittleEndian.Uint64(src[bitPos>>3:]) >> (bitPos & 7) & mask
		out[i] = float64(int64(d+ref)) * fac * exp
		bitPos += bitWidth
	}
}

// alpDecodePacked32 is the FLOAT form of alpDecodePacked64, with bitWidth from
// 1 to 32. The frame of reference is added in 32 bits, as it was subtracted.
func alpDecodePacked32(out []float32, src []byte, bitWidth uint, ref uint32, fac, exp float32, scratch []uint32) {
	n := utils.Unpack32Vectorized(src, scratch[:len(out)], int(bitWidth))
	head := out[:n]
	for i, d := range scratch[:n] {
		head[i] = float32(int32(d+ref)) * fac * exp
	}
	out, src = out[n:], src[n*int(bitWidth)/8:]

	mask := ^uint64(0) >> (64 - bitWidth)
	bitPos := uint(0)
	i := 0
	for ; i+4 <= len(out); i += 4 {
		o := out[i : i+4 : i+4]
		p1, p2, p3 := bitPos+bitWidth, bitPos+2*bitWidth, bitPos+3*bitWidth
		d0 := binary.LittleEndian.Uint64(src[bitPos>>3:]) >> (bitPos & 7) & mask
		d1 := binary.LittleEndian.Uint64(src[p1>>3:]) >> (p1 & 7) & mask
		d2 := binary.LittleEndian.Uint64(src[p2>>3:]) >> (p2 & 7) & mask
		d3 := binary.LittleEndian.Uint64(src[p3>>3:]) >> (p3 & 7) & mask
		o[0] = float32(int32(uint32(d0)+ref)) * fac * exp
		o[1] = float32(int32(uint32(d1)+ref)) * fac * exp
		o[2] = float32(int32(uint32(d2)+ref)) * fac * exp
		o[3] = float32(int32(uint32(d3)+ref)) * fac * exp
		bitPos += 4 * bitWidth
	}
	for ; i < len(out); i++ {
		d := binary.LittleEndian.Uint64(src[bitPos>>3:]) >> (bitPos & 7) & mask
		out[i] = float32(int32(uint32(d)+ref)) * fac * exp
		bitPos += bitWidth
	}
}

// alpPatch64 overwrites out at each exception's position with the value stored
// for it, and returns the first position outside out, or -1. positions holds
// two bytes for each exception and values eight.
func alpPatch64(out []float64, positions, values []byte) int {
	values = values[:len(positions)*4]
	// Four at a time, for the reason alpDecodePacked64 gives. A group with a
	// position outside out is left to the loop below, which reports it.
	for len(positions) >= 8 && len(values) >= 32 {
		p, v := positions[:8], values[:32]
		i0 := int(binary.LittleEndian.Uint16(p[0:]))
		i1 := int(binary.LittleEndian.Uint16(p[2:]))
		i2 := int(binary.LittleEndian.Uint16(p[4:]))
		i3 := int(binary.LittleEndian.Uint16(p[6:]))
		if i0 >= len(out) || i1 >= len(out) || i2 >= len(out) || i3 >= len(out) {
			break
		}
		out[i0] = math.Float64frombits(binary.LittleEndian.Uint64(v[0:]))
		out[i1] = math.Float64frombits(binary.LittleEndian.Uint64(v[8:]))
		out[i2] = math.Float64frombits(binary.LittleEndian.Uint64(v[16:]))
		out[i3] = math.Float64frombits(binary.LittleEndian.Uint64(v[24:]))
		positions, values = positions[8:], values[32:]
	}
	for len(positions) >= 2 && len(values) >= 8 {
		idx := int(binary.LittleEndian.Uint16(positions))
		if idx >= len(out) {
			return idx
		}
		out[idx] = math.Float64frombits(binary.LittleEndian.Uint64(values))
		positions, values = positions[2:], values[8:]
	}
	return -1
}

// alpPatch32 is the FLOAT form of alpPatch64, with four bytes to a value.
func alpPatch32(out []float32, positions, values []byte) int {
	values = values[:len(positions)*2]
	for len(positions) >= 8 && len(values) >= 16 {
		p, v := positions[:8], values[:16]
		i0 := int(binary.LittleEndian.Uint16(p[0:]))
		i1 := int(binary.LittleEndian.Uint16(p[2:]))
		i2 := int(binary.LittleEndian.Uint16(p[4:]))
		i3 := int(binary.LittleEndian.Uint16(p[6:]))
		if i0 >= len(out) || i1 >= len(out) || i2 >= len(out) || i3 >= len(out) {
			break
		}
		out[i0] = math.Float32frombits(binary.LittleEndian.Uint32(v[0:]))
		out[i1] = math.Float32frombits(binary.LittleEndian.Uint32(v[4:]))
		out[i2] = math.Float32frombits(binary.LittleEndian.Uint32(v[8:]))
		out[i3] = math.Float32frombits(binary.LittleEndian.Uint32(v[12:]))
		positions, values = positions[8:], values[16:]
	}
	for len(positions) >= 2 && len(values) >= 4 {
		idx := int(binary.LittleEndian.Uint16(positions))
		if idx >= len(out) {
			return idx
		}
		out[idx] = math.Float32frombits(binary.LittleEndian.Uint32(values))
		positions, values = positions[2:], values[4:]
	}
	return -1
}
