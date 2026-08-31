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
	"github.com/stretchr/testify/require"
)

func TestNewChunkedSliceRetainsFullChunks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		begin, end int64
		want       []int32
		valid      []bool
		fullChunks []int
	}{
		{"all", 0, 6, []int32{1, 2, 3, 4, 5, 6}, []bool{true, false, true, true, false, true}, []int{0, 1, 2}},
		{"middle", 2, 4, []int32{3, 4}, []bool{true, true}, []int{1}},
		{"partial ends", 1, 5, []int32{2, 3, 4, 5}, []bool{false, true, true, false}, []int{-1, 1, -1}},
		{"empty", 6, 6, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			builder := array.NewInt32Builder(mem)
			builder.AppendValues([]int32{0, 1, 2, 3, 4, 5, 6, 7}, []bool{true, true, false, true, true, false, true, true})
			backing := builder.NewInt32Array()
			builder.Release()
			chunks := []arrow.Array{
				array.NewSlice(backing, 1, 3),
				array.NewSlice(backing, 3, 5),
				array.NewSlice(backing, 5, 7),
			}
			backing.Release()
			input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, chunks)
			for _, chunk := range chunks {
				chunk.Release()
			}
			result := array.NewChunkedSlice(input, tc.begin, tc.end)
			defer result.Release()
			input.Release()

			require.Equal(t, len(tc.want), result.Len())
			require.Len(t, result.Chunks(), len(tc.fullChunks))
			position := 0
			for i, chunk := range result.Chunks() {
				if full := tc.fullChunks[i]; full >= 0 {
					require.Same(t, chunks[full], chunk)
				}
				values := chunk.(*array.Int32)
				for j := range values.Len() {
					require.Equal(t, tc.valid[position], values.IsValid(j))
					if tc.valid[position] {
						require.Equal(t, tc.want[position], values.Value(j))
					}
					position++
				}
			}
		})
	}
}
