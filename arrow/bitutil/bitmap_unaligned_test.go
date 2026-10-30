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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

type bitmapBinaryOp struct {
	name string
	run  func([]byte, []byte, int64, int64, []byte, int64, int64)
	want func(bool, bool) bool
}

var unalignedBitmapOps = []bitmapBinaryOp{
	{"and", bitutil.BitmapAnd, func(a, b bool) bool { return a && b }},
	{"or", bitutil.BitmapOr, func(a, b bool) bool { return a || b }},
	{"and-not", bitutil.BitmapAndNot, func(a, b bool) bool { return a && !b }},
	{"xor", bitutil.BitmapXor, func(a, b bool) bool { return a != b }},
	{"xnor", bitutil.BitmapXnor, func(a, b bool) bool { return a == b }},
}

// Use reproducible mixed bit patterns without adding a random source dependency.
func fillUnalignedBitmapPattern(data []byte, seed uint32) {
	x := seed | 1
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x)
	}
}

func TestBitmapUnalignedOpsDirectWords(t *testing.T) {
	lengths := []int64{0, 1, 2, 7, 8, 9, 15, 16, 31, 32, 63, 64, 65,
		127, 128, 129, 191, 192, 193, 255, 256, 257, 511, 512, 513, 1025}

	for _, length := range lengths {
		for leftOffset := int64(0); leftOffset < 8; leftOffset++ {
			for rightOffset := int64(0); rightOffset < 8; rightOffset++ {
				left := make([]byte, (leftOffset+length+7)/8+1)
				right := make([]byte, (rightOffset+length+7)/8+1)
				fillUnalignedBitmapPattern(left, uint32(123+length+leftOffset))
				fillUnalignedBitmapPattern(right, uint32(456+length+rightOffset))
				for _, outOffset := range []int64{0, 1, 7} {
					for _, op := range unalignedBitmapOps {
						out := make([]byte, (outOffset+length+7)/8+1)
						fillUnalignedBitmapPattern(out, uint32(789+length+outOffset))
						want := bytes.Clone(out)
						for i := int64(0); i < length; i++ {
							a := bitutil.BitIsSet(left, int(leftOffset+i))
							b := bitutil.BitIsSet(right, int(rightOffset+i))
							bitutil.SetBitTo(want, int(outOffset+i), op.want(a, b))
						}
						op.run(left, right, leftOffset, rightOffset, out, outOffset, length)
						if !bytes.Equal(out, want) {
							t.Fatalf("%s length=%d offsets=%d/%d/%d: got %x, want %x",
								op.name, length, leftOffset, rightOffset, outOffset, out, want)
						}
					}
				}
			}
		}
	}
}

func TestBitmapUnalignedOpsAliasedOutput(t *testing.T) {
	const length int64 = 263
	// The direct path must fall back when the output aliases either input.
	for _, op := range unalignedBitmapOps {
		for _, aliasLeft := range []bool{true, false} {
			left := make([]byte, (length+7)/8+1)
			right := make([]byte, (length+3+7)/8+1)
			fillUnalignedBitmapPattern(left, 12345)
			fillUnalignedBitmapPattern(right, 67890)

			var out []byte
			var leftOffset, rightOffset int64
			if aliasLeft {
				out, leftOffset, rightOffset = left, 0, 3
			} else {
				out, leftOffset, rightOffset = right, 3, 0
				// Both source ranges are large enough for the chosen offsets.
				left = make([]byte, (length+leftOffset+7)/8+1)
				fillUnalignedBitmapPattern(left, 12345)
			}
			beforeLeft := bytes.Clone(left)
			beforeRight := bytes.Clone(right)
			want := bytes.Clone(out)
			for i := int64(0); i < length; i++ {
				a := bitutil.BitIsSet(beforeLeft, int(leftOffset+i))
				b := bitutil.BitIsSet(beforeRight, int(rightOffset+i))
				bitutil.SetBitTo(want, int(i), op.want(a, b))
			}
			op.run(left, right, leftOffset, rightOffset, out, 0, length)
			if !bytes.Equal(out, want) {
				t.Fatalf("%s aliasLeft=%v: got %x, want %x", op.name, aliasLeft, out, want)
			}
		}
	}
}

var bitmapUnalignedBenchmarkSink byte

func BenchmarkBitmapUnalignedOps(b *testing.B) {
	for _, nbytes := range []int{32, 512, 8192, 131072} {
		for _, offsets := range []struct{ left, right, out int64 }{
			{0, 1, 0}, {1, 3, 0}, {7, 4, 0}, {1, 3, 1},
		} {
			left := make([]byte, int64(nbytes)+2)
			right := make([]byte, int64(nbytes)+2)
			out := make([]byte, int64(nbytes)+2)
			fillUnalignedBitmapPattern(left, 13579)
			fillUnalignedBitmapPattern(right, 24680)
			for _, tailBits := range []int64{0, 3} {
				for _, op := range unalignedBitmapOps {
					b.Run(fmt.Sprintf("%dB/left%d-right%d-out%d/tail%d/%s",
						nbytes, offsets.left, offsets.right, offsets.out, tailBits, op.name), func(b *testing.B) {
						b.SetBytes(int64(2 * nbytes))
						b.ReportAllocs()
						for b.Loop() {
							op.run(left, right, offsets.left, offsets.right, out, offsets.out, int64(nbytes*8)-tailBits)
						}
						bitmapUnalignedBenchmarkSink ^= out[0]
					})
				}
			}
		}
	}
}

func TestBitmapUnalignedExactBuffers(t *testing.T) {
	for _, length := range []int64{128, 129, 135, 191, 192, 193, 255, 256, 257} {
		for leftOffset := int64(0); leftOffset < 16; leftOffset++ {
			for rightOffset := int64(0); rightOffset < 16; rightOffset++ {
				left := make([]byte, (leftOffset+length+7)/8)
				right := make([]byte, (rightOffset+length+7)/8)
				fillUnalignedBitmapPattern(left, uint32(123+length+leftOffset))
				fillUnalignedBitmapPattern(right, uint32(456+length+rightOffset))
				for _, outOffset := range []int64{0, 8, 16} {
					for _, op := range unalignedBitmapOps {
						out := bytes.Repeat([]byte{0xa5}, int((outOffset+length+7)/8))
						want := bytes.Clone(out)
						for i := int64(0); i < length; i++ {
							bitutil.SetBitTo(want, int(outOffset+i), op.want(bitutil.BitIsSet(left, int(leftOffset+i)), bitutil.BitIsSet(right, int(rightOffset+i))))
						}
						op.run(left, right, leftOffset, rightOffset, out, outOffset, length)
						if !bytes.Equal(out, want) {
							t.Fatalf("%s length=%d offsets=%d/%d/%d: got %x, want %x", op.name, length, leftOffset, rightOffset, outOffset, out, want)
						}
					}
				}
			}
		}
	}
}
