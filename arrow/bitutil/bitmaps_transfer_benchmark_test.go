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

package bitutil_test

import (
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

// BenchmarkTransferBitmapAliasing measures the overlap snapshot path and the
// allocation-free path, including disjoint ranges in the same backing buffer.
func BenchmarkTransferBitmapAliasing(b *testing.B) {
	operations := []struct {
		name     string
		transfer func([]byte, int, int, []byte, int)
	}{
		{name: "copy", transfer: bitutil.CopyBitmap},
		{name: "invert", transfer: bitutil.InvertBitmap},
	}
	offsets := []struct {
		name     string
		src, dst int
	}{
		{name: "aligned"},
		{name: "unaligned", src: 3, dst: 5},
	}
	layouts := []struct {
		name               string
		separate, disjoint bool
		srcByte, dstByte   int
	}{
		{name: "separate", separate: true},
		{name: "disjoint-same-buffer", disjoint: true},
		{name: "overlap-forward", dstByte: 1},
		{name: "overlap-backward", srcByte: 1},
		{name: "same-range"},
	}

	for _, operation := range operations {
		b.Run(operation.name, func(b *testing.B) {
			for _, nbytes := range []int{32, 1024, 64 * 1024} {
				b.Run(strconv.Itoa(nbytes), func(b *testing.B) {
					for _, offset := range offsets {
						b.Run(offset.name, func(b *testing.B) {
							for _, layout := range layouts {
								b.Run(layout.name, func(b *testing.B) {
									var src, dst []byte
									if layout.separate {
										src = make([]byte, nbytes+8)
										dst = make([]byte, nbytes+8)
									} else {
										backing := make([]byte, 2*nbytes+32)
										srcByte, dstByte := layout.srcByte, layout.dstByte
										if layout.disjoint {
											dstByte = nbytes + 16
										}
										src, dst = backing[srcByte:], backing[dstByte:]
									}
									for i := range src {
										src[i] = byte(i*37 + 0x5b)
									}

									b.SetBytes(int64(nbytes))
									b.ReportAllocs()
									for b.Loop() {
										operation.transfer(src, offset.src, nbytes*8, dst, offset.dst)
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
