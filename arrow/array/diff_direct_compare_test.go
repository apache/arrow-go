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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDiff_DirectComparisonsWithOffsets(t *testing.T) {
	cases := []struct {
		name  string
		build func(memory.Allocator) (arrow.Array, arrow.Array)
	}{
		{
			name: "int64",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewInt64Builder(mem)
				baseBuilder.AppendValues([]int64{0, 1, 2, 3, 5, 6}, valid)
				base := baseBuilder.NewInt64Array()
				baseBuilder.Release()

				targetBuilder := array.NewInt64Builder(mem)
				targetBuilder.AppendValues([]int64{10, 1, 2, 4, 5, 60}, valid)
				target := targetBuilder.NewInt64Array()
				targetBuilder.Release()
				return base, target
			},
		},
		{
			name: "boolean",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewBooleanBuilder(mem)
				baseBuilder.AppendValues([]bool{false, true, false, true, false, true}, valid)
				base := baseBuilder.NewBooleanArray()
				baseBuilder.Release()

				targetBuilder := array.NewBooleanBuilder(mem)
				targetBuilder.AppendValues([]bool{true, true, true, false, false, false}, valid)
				target := targetBuilder.NewBooleanArray()
				targetBuilder.Release()
				return base, target
			},
		},
		{
			name: "string",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewStringBuilder(mem)
				baseBuilder.AppendValues([]string{"before", "one", "ignored", "three", "five", "after"}, valid)
				base := baseBuilder.NewStringArray()
				baseBuilder.Release()

				targetBuilder := array.NewStringBuilder(mem)
				targetBuilder.AppendValues([]string{"before", "one", "unused", "four", "five", "after"}, valid)
				target := targetBuilder.NewStringArray()
				targetBuilder.Release()
				return base, target
			},
		},
		{
			name: "binary",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				baseBuilder.AppendValues([][]byte{
					[]byte("before"), []byte("one"), []byte("ignored"), []byte("three"), []byte("five"), []byte("after"),
				}, valid)
				base := baseBuilder.NewBinaryArray()
				baseBuilder.Release()

				targetBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				targetBuilder.AppendValues([][]byte{
					[]byte("before"), []byte("one"), []byte("unused"), []byte("four"), []byte("five"), []byte("after"),
				}, valid)
				target := targetBuilder.NewBinaryArray()
				targetBuilder.Release()
				return base, target
			},
		},
		{
			name: "large_string",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewLargeStringBuilder(mem)
				baseBuilder.AppendValues([]string{"before", "one", "ignored", "three", "five", "after"}, valid)
				base := baseBuilder.NewLargeStringArray()
				baseBuilder.Release()

				targetBuilder := array.NewLargeStringBuilder(mem)
				targetBuilder.AppendValues([]string{"before", "one", "unused", "four", "five", "after"}, valid)
				target := targetBuilder.NewLargeStringArray()
				targetBuilder.Release()
				return base, target
			},
		},
		{
			name: "large_binary",
			build: func(mem memory.Allocator) (arrow.Array, arrow.Array) {
				valid := []bool{true, true, false, true, true, true}
				baseBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
				baseBuilder.AppendValues([][]byte{
					[]byte("before"), []byte("one"), []byte("ignored"), []byte("three"), []byte("five"), []byte("after"),
				}, valid)
				base := baseBuilder.NewLargeBinaryArray()
				baseBuilder.Release()

				targetBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
				targetBuilder.AppendValues([][]byte{
					[]byte("before"), []byte("one"), []byte("unused"), []byte("four"), []byte("five"), []byte("after"),
				}, valid)
				target := targetBuilder.NewLargeBinaryArray()
				targetBuilder.Release()
				return base, target
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			baseFull, targetFull := tc.build(mem)
			defer baseFull.Release()
			defer targetFull.Release()

			base := array.NewSlice(baseFull, 1, 5)
			defer base.Release()
			target := array.NewSlice(targetFull, 1, 5)
			defer target.Release()

			edits, err := array.Diff(base, target)
			if err != nil {
				t.Fatal(err)
			}
			validateEditScript(t, edits, base, target)
		})
	}
}
