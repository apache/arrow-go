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

func BenchmarkBitmapAlignedOps(b *testing.B) {
	for _, nbytes := range []int{8, 32, 64, 128, 1024, bufferSize * 4, bufferSize * 16} {
		b.Run(strconv.Itoa(nbytes), func(b *testing.B) {
			left := randomBuffer(int64(nbytes))
			right := randomBuffer(int64(nbytes))
			out := make([]byte, nbytes)
			length := int64(nbytes * 8)

			for _, op := range []struct {
				name string
				fn   noAllocFn
			}{
				{name: "and", fn: bitutil.BitmapAnd},
				{name: "or", fn: bitutil.BitmapOr},
				{name: "and-not", fn: bitutil.BitmapAndNot},
				{name: "xor", fn: bitutil.BitmapXor},
				{name: "xnor", fn: bitutil.BitmapXnor},
			} {
				b.Run(op.name, func(b *testing.B) {
					b.SetBytes(int64(2 * nbytes))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						op.fn(left, right, 0, 0, out, 0, length)
					}
				})
			}
		})
	}
}
