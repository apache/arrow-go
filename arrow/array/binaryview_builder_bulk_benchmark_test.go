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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package array

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkBinaryViewBuilderAppendNulls(b *testing.B) {
	for _, count := range []int{1024, 65536} {
		for _, offset := range []int{0, 1, 7} {
			b.Run(fmt.Sprintf("count_%d/offset_%d", count, offset), func(b *testing.B) {
				builder := NewBinaryViewBuilder(memory.DefaultAllocator)
				builder.Resize(count + offset)
				builder.length = offset
				defer builder.Release()

				b.ReportAllocs()
				b.SetBytes(int64(count))
				b.ResetTimer()
				for b.Loop() {
					builder.AppendNulls(count)
					builder.length = offset
					builder.nulls = 0
				}
			})
		}
	}
}
