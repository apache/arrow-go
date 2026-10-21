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

package bitutils_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/internal/bitutils"
)

var fourWordsBenchmarkSink int64

func BenchmarkFourWordsUnaligned(b *testing.B) {
	for _, length := range []int64{512, 1024, 4096} {
		for _, offset := range []int64{1, 3, 7} {
			bitmap := make([]byte, bitutil.BytesForBits(offset+length))
			rng := &rand.ChaCha8{}
			_, _ = rng.Read(bitmap)

			b.Run(fmt.Sprintf("%dbits/offset%d", length, offset), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(bitutil.BytesForBits(length)))

				var totalPopcnt int64
				for b.Loop() {
					counter := bitutils.NewBitBlockCounter(bitmap, offset, length)
					for pos := int64(0); pos < length; {
						block := counter.NextFourWords()
						pos += int64(block.Len)
						totalPopcnt += int64(block.Popcnt)
					}
				}
				fourWordsBenchmarkSink = totalPopcnt
			})
		}
	}
}
