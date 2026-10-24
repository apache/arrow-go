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
	"math/rand"
	"testing"
)

func transferScalarReference(mode transferMode, src []byte, srcOffset, length int, dst []byte, dstOffset int) {
	snapshot := bytes.Clone(src)
	for i := 0; i < length; i++ {
		bit := (snapshot[(srcOffset+i)/8] >> uint((srcOffset+i)%8)) & 1
		if mode == transferInvert {
			bit ^= 1
		}
		at := (dstOffset + i) / 8
		mask := byte(1 << uint((dstOffset+i)%8))
		if bit == 0 {
			dst[at] &^= mask
		} else {
			dst[at] |= mask
		}
	}
}

func TestBitmapTransferOffsetsParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20261024))
	lengths := []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 511, 512, 513, 1024}
	for _, mode := range []transferMode{transferCopy, transferInvert} {
		for srcOffset := 0; srcOffset < 16; srcOffset++ {
			for dstOffset := 0; dstOffset < 16; dstOffset++ {
				for _, n := range lengths {
					src := make([]byte, (srcOffset+n+7)/8)
					dst := make([]byte, (dstOffset+n+7)/8)
					rng.Read(src)
					rng.Read(dst)
					want := bytes.Clone(dst)
					transferScalarReference(mode, src, srcOffset, n, want, dstOffset)
					if mode == transferCopy {
						CopyBitmap(src, srcOffset, n, dst, dstOffset)
					} else {
						InvertBitmap(src, srcOffset, n, dst, dstOffset)
					}
					if !bytes.Equal(dst, want) {
						t.Fatalf("mode=%d srcOffset=%d dstOffset=%d len=%d", mode, srcOffset, dstOffset, n)
					}
				}
			}
		}
	}
}

func TestBitmapTransferSubsliceOverlap(t *testing.T) {
	rng := rand.New(rand.NewSource(20261025))
	for _, mode := range []transferMode{transferCopy, transferInvert} {
		for srcByte := 0; srcByte < 6; srcByte++ {
			for dstByte := 0; dstByte < 6; dstByte++ {
				for srcOffset := 0; srcOffset < 8; srcOffset++ {
					for dstOffset := 0; dstOffset < 8; dstOffset++ {
						for _, n := range []int{0, 1, 5, 8, 31, 64, 65, 127, 512} {
							backing := make([]byte, 128)
							rng.Read(backing)
							want := bytes.Clone(backing)
							transferScalarReference(mode, backing[srcByte:], srcOffset, n, want[dstByte:], dstOffset)
							if mode == transferCopy {
								CopyBitmap(backing[srcByte:], srcOffset, n, backing[dstByte:], dstOffset)
							} else {
								InvertBitmap(backing[srcByte:], srcOffset, n, backing[dstByte:], dstOffset)
							}
							if !bytes.Equal(backing, want) {
								t.Fatalf("mode=%d srcByte=%d dstByte=%d srcBit=%d dstBit=%d len=%d", mode, srcByte, dstByte, srcOffset, dstOffset, n)
							}
						}
					}
				}
			}
		}
	}
}

func TestBitmapTransferZeroLengthNil(t *testing.T) {
	CopyBitmap(nil, 0, 0, nil, 0)
	InvertBitmap(nil, 0, 0, nil, 0)
}
