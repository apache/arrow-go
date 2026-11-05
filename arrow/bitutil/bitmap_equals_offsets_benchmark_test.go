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
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

var bitmapEqualsOffsetBenchmarkResult bool

func BenchmarkBitmapEqualsOffsets(b *testing.B) {
	for _, nbits := range []int{64, 1024, 65536, 1048576} {
		for _, pair := range []struct {
			name   string
			lo, ro int
		}{
			{name: "aligned", lo: 0, ro: 0},
			{name: "same3", lo: 3, ro: 11},
			{name: "same7", lo: 7, ro: 15},
			{name: "different", lo: 3, ro: 6},
		} {
			for _, lastMismatch := range []bool{false, true} {
				name := "equal"
				if lastMismatch {
					name = "lastMismatch"
				}
				left := make([]byte, bitutil.BytesForBits(int64(nbits+pair.lo)))
				right := make([]byte, bitutil.BytesForBits(int64(nbits+pair.ro)))
				rng := rand.New(rand.NewSource(55))
				_, _ = rng.Read(left)
				for i := 0; i < nbits; i++ {
					if bitutil.BitIsSet(left, pair.lo+i) {
						bitutil.SetBit(right, pair.ro+i)
					}
				}
				if lastMismatch {
					i := pair.ro + nbits - 1
					right[i/8] ^= 1 << uint(i%8)
				}
				b.Run(fmt.Sprintf("bits=%d/%s/%s", nbits, pair.name, name), func(b *testing.B) {
					b.SetBytes(int64((nbits + 7) / 8 * 2))
					b.ReportAllocs()
					for b.Loop() {
						bitmapEqualsOffsetBenchmarkResult = bitutil.BitmapEquals(
							left, right, int64(pair.lo), int64(pair.ro), int64(nbits),
						)
					}
				})
			}
		}
	}
}
