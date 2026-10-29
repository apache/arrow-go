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

import "math/bits"

// nibbleBitDeposit deposits up to four packed bits into the set positions of
// a four-bit mask. The table is only 256 bytes and works on every architecture.
var nibbleBitDeposit = func() [256]byte {
	var table [256]byte
	for mask := 0; mask < 16; mask++ {
		for packed := 0; packed < 16; packed++ {
			source := byte(packed)
			var deposited byte
			for bit := uint(0); bit < 4; bit++ {
				if mask&(1<<bit) != 0 {
					if source&1 != 0 {
						deposited |= 1 << bit
					}
					source >>= 1
				}
			}
			table[mask<<4|packed] = deposited
		}
	}
	return table
}()

func depositBitmapByte(packed, valid byte) byte {
	low := valid & 15
	high := valid >> 4
	return nibbleBitDeposit[int(low)<<4|int(packed&15)] |
		nibbleBitDeposit[int(high)<<4|int((packed>>bits.OnesCount8(low))&15)]<<4
}

// expandSpacedBitmapByByte expands compacted boolean values in-place.
// Only valid output bits are touched; nulls and bits outside the range are
// left unchanged. The compressed bits start at outOffset+nullCount, so source
// bits for the current output byte are always at or after its destination.
func expandSpacedBitmapByByte(out []byte, outOffset int64, length, nullCount int,
	validBits []byte, validBitsOffset int64) {
	outputPos := int(outOffset)
	validPos := int(validBitsOffset)
	physicalPos := outputPos + nullCount

	for remaining := length; remaining > 0; {
		destShift := outputPos & 7
		count := min(remaining, 8-destShift)

		validByte := validBits[validPos>>3] >> (validPos & 7)
		if (validPos&7)+count > 8 {
			validByte |= validBits[(validPos>>3)+1] << (8 - (validPos & 7))
		}
		validByte &= byte((1 << count) - 1)

		setBits := bits.OnesCount8(validByte)
		if setBits != 0 {
			// Read the full packed source segment before writing the output
			// byte, which also makes overlapping source/destination safe.
			packed := out[physicalPos>>3] >> (physicalPos & 7)
			if (physicalPos&7)+setBits > 8 {
				packed |= out[(physicalPos>>3)+1] << (8 - (physicalPos & 7))
			}

			deposited := depositBitmapByte(packed, validByte)
			mask := validByte << destShift
			byteIndex := outputPos >> 3
			out[byteIndex] = (out[byteIndex] &^ mask) | (deposited << destShift)
			physicalPos += setBits
		}

		outputPos += count
		validPos += count
		remaining -= count
	}
}
