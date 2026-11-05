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

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func makeSpacedBitmapCase(length, outOffset, validOffset, nullEvery int) ([]byte, []byte, int) {
	valid := bytes.Repeat([]byte{0xa5}, (validOffset+length+15)/8)
	out := bytes.Repeat([]byte{0x3c}, (outOffset+length+15)/8)
	nulls := 0
	rng := rand.New(rand.NewSource(23))
	for i := 0; i < length; i++ {
		isValid := nullEvery == 0 || (nullEvery > 0 && i%nullEvery != 0) ||
			(nullEvery == -1 && rng.Intn(2) == 0)
		bitutil.SetBitTo(valid, validOffset+i, isValid)
		if !isValid {
			nulls++
		}
	}
	for i := 0; i < length-nulls; i++ {
		bitutil.SetBitTo(out, outOffset+nulls+i, rng.Intn(2) == 0)
	}
	return out, valid, nulls
}

func TestExpandSpacedBitmapByByteParity(t *testing.T) {
	for _, length := range []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 4096} {
		for outOffset := 0; outOffset < 8; outOffset++ {
			for validOffset := 0; validOffset < 8; validOffset++ {
				for _, nullEvery := range []int{0, 1, 2, 3, 7, 9, -1} {
					out, valid, nulls := makeSpacedBitmapCase(length, outOffset, validOffset, nullEvery)
					want := bytes.Clone(out)
					got := bytes.Clone(out)
					expandSpacedBitmapPerBit(want, int64(outOffset), length, nulls, valid, int64(validOffset))
					expandSpacedBitmapByByte(got, int64(outOffset), length, nulls, valid, int64(validOffset))
					if !bytes.Equal(got, want) {
						t.Fatalf("length=%d outOffset=%d validOffset=%d nullEvery=%d nulls=%d: got %x, want %x",
							length, outOffset, validOffset, nullEvery, nulls, got, want)
					}
				}
			}
		}
	}
}

var spacedBitmapBenchmarkSink []byte

func BenchmarkSpacedBitmapExpansion(b *testing.B) {
	for _, length := range []int{64 * 1024, 1024 * 1024} {
		for _, nullEvery := range []int{2, 7} {
			for _, offset := range []int{0, 5} {
				source, valid, nulls := makeSpacedBitmapCase(length, offset, offset+1, nullEvery)
				for _, implementation := range []struct {
					name string
					fn   func([]byte, int64, int, int, []byte, int64)
				}{
					{"baseline_per_bit", expandSpacedBitmapPerBit},
					{"byte_deposit", expandSpacedBitmapByByte},
				} {
					b.Run(fmt.Sprintf("length_%d/null_every_%d/offset_%d/%s", length, nullEvery, offset, implementation.name), func(b *testing.B) {
						out := make([]byte, len(source))
						b.ReportAllocs()
						b.SetBytes(int64(length))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							copy(out, source)
							implementation.fn(out, int64(offset), length, nulls, valid, int64(offset+1))
						}
						spacedBitmapBenchmarkSink = out
					})
				}
			}
		}
	}
}
