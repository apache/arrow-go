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

package bitutil

import (
	"bytes"
	"fmt"
	"testing"
	"unsafe"
)

// legacyBitmapTransfer is the former transferBitmap implementation, retained
// only in this benchmark to compare before/after with the same Go compiler.
func legacyBitmapTransfer(mode transferMode, src []byte, srcOffset, length int, dst []byte, dstOffset int) {
	if length == 0 {
		return
	}
	srcStart, dstStart := srcOffset/8, dstOffset/8
	srcBytes := src[srcStart : srcStart+int(BytesForBits(int64(srcOffset%8+length)))]
	dstBytes := dst[dstStart : dstStart+int(BytesForBits(int64(dstOffset%8+length)))]
	srcPtr, dstPtr := uintptr(unsafe.Pointer(&srcBytes[0])), uintptr(unsafe.Pointer(&dstBytes[0]))
	var overlaps bool
	if srcPtr < dstPtr {
		overlaps = dstPtr-srcPtr < uintptr(len(srcBytes))
	} else {
		overlaps = srcPtr-dstPtr < uintptr(len(dstBytes))
	}
	if overlaps {
		src = bytes.Clone(srcBytes)
		srcOffset %= 8
	}
	if srcOffset%8 != 0 || dstOffset%8 != 0 {
		rdr := NewBitmapWordReader(src, srcOffset, length)
		wr := NewBitmapWordWriter(dst, dstOffset, length)
		for n := rdr.Words(); n > 0; n-- {
			if mode == transferInvert {
				wr.PutNextWord(^rdr.NextWord())
			} else {
				wr.PutNextWord(rdr.NextWord())
			}
		}
		for n := rdr.TrailingBytes(); n > 0; n-- {
			bt, validBits := rdr.NextTrailingByte()
			if mode == transferInvert {
				bt = ^bt
			}
			wr.PutNextTrailingByte(bt, validBits)
		}
		return
	}
	nbytes := int(BytesForBits(int64(length)))
	src = src[srcOffset/8:]
	dst = dst[dstOffset/8:]
	trailingBits := nbytes*8 - length
	trailMask := byte(uint(1)<<(8-trailingBits)) - 1
	var lastData byte
	if mode == transferInvert {
		for i, bt := range src[:nbytes-1] {
			dst[i] = ^bt
		}
		lastData = ^src[nbytes-1]
	} else {
		copy(dst, src[:nbytes-1])
		lastData = src[nbytes-1]
	}
	dst[nbytes-1] &^= trailMask
	dst[nbytes-1] |= lastData & trailMask
}

var bitmapTransferBenchmarkSink byte

func BenchmarkBitmapTransferOffsets(b *testing.B) {
	for _, mode := range []struct {
		name string
		kind transferMode
	}{{"copy", transferCopy}, {"invert", transferInvert}} {
		for _, length := range []int{64, 1024, 65536, 1048576} {
			for _, offsets := range [][2]int{{0, 0}, {3, 3}, {3, 6}, {0, 5}, {7, 0}} {
				for _, impl := range []string{"before", "after"} {
					name := fmt.Sprintf("%s/%dbits/%d-%d/%s", mode.name, length, offsets[0], offsets[1], impl)
					b.Run(name, func(b *testing.B) {
						src := make([]byte, (length+64)/8+16)
						dst := make([]byte, len(src))
						for i := range src {
							src[i] = byte(i*31 + 7)
						}
						b.ReportAllocs()
						b.SetBytes(int64(length / 8))
						for b.Loop() {
							if impl == "before" {
								legacyBitmapTransfer(mode.kind, src, offsets[0], length, dst, offsets[1])
							} else if mode.kind == transferCopy {
								CopyBitmap(src, offsets[0], length, dst, offsets[1])
							} else {
								InvertBitmap(src, offsets[0], length, dst, offsets[1])
							}
						}
						bitmapTransferBenchmarkSink = dst[10]
					})
				}
			}
		}
	}
}
