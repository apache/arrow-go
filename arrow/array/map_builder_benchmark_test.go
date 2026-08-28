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

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkMapBuilderNewArrayAfterBulkChildren(b *testing.B) {
	for _, entries := range []int{32, 1024, 65536} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			builder := array.NewMapBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, false)
			defer builder.Release()

			offsets := []int32{0, int32(entries)}
			valid := []bool{true}
			keys := make([]int32, entries)
			items := make([]int32, entries)
			for i := range keys {
				keys[i] = int32(i)
				items[i] = int32(i)
			}

			keyBuilder := builder.KeyBuilder().(*array.Int32Builder)
			itemBuilder := builder.ItemBuilder().(*array.Int32Builder)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				builder.AppendValues(offsets, valid)
				keyBuilder.AppendValues(keys, nil)
				itemBuilder.AppendValues(items, nil)
				b.StartTimer()
				arr := builder.NewMapArray()
				arr.Release()
			}
		})
	}
}
