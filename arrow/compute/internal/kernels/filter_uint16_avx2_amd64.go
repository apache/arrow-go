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
	"math/bits"
	"unsafe"

	"golang.org/x/sys/cpu"
)

var filterUint16Avx2Tables = makeFilterUint16Tables()

func makeFilterUint16Tables() (tables [4352]byte) {
	for mask := 0; mask < 256; mask++ {
		pos := 0
		for lane := 0; lane < 8; lane++ {
			if mask&(1<<uint(lane)) == 0 {
				continue
			}
			tables[mask*16+pos] = byte(lane * 2)
			tables[mask*16+pos+1] = byte(lane*2 + 1)
			pos += 2
		}
		for ; pos < 16; pos++ {
			tables[mask*16+pos] = 0x80
		}
		tables[4096+mask] = byte(bits.OnesCount8(uint8(mask)))
	}
	return tables
}

//go:noescape
func _filter_uint16_avx2(values, filter, output, tables unsafe.Pointer, length int64)

func filterUint16Avx2(values []uint16, output []uint16, filterData []byte, filterOffset, length int64) bool {
	if !cpu.X86.HasAVX2 || length < 64 || length%8 != 0 || filterOffset%8 != 0 {
		return false
	}

	numBytes := length / 8
	filterByteOffset := filterOffset / 8
	if filterByteOffset < 0 || filterByteOffset+numBytes > int64(len(filterData)) {
		return false
	}

	mixedBytes := 0
	const sampleBytes = 64
	for i := int64(0); i < numBytes && i < sampleBytes; i++ {
		mask := filterData[filterByteOffset+i]
		if mask != 0 && mask != 0xff {
			mixedBytes++
			if mixedBytes == 4 {
				break
			}
		}
	}
	if mixedBytes < 4 || len(output) == 0 {
		return false
	}

	_filter_uint16_avx2(
		unsafe.Pointer(&values[0]),
		unsafe.Pointer(&filterData[filterByteOffset]),
		unsafe.Pointer(&output[0]),
		unsafe.Pointer(&filterUint16Avx2Tables[0]),
		length,
	)
	return true
}
