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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestBinaryBuilderDirectBulkOffsetsAcrossBatches(t *testing.T) {
	for _, factory := range binaryBuilderFactories {
		t.Run(factory.name, func(t *testing.T) {
			for _, prefix := range []int{0, 1, 7, 8, 9, 31, 64} {
				t.Run(fmt.Sprintf("prefix_%d", prefix), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
					defer mem.AssertSize(t, 0)
					builder := factory.new(mem)
					defer builder.Release()

					var expectedPayload []byte
					var expectedOffsets []int64
					var expectedValid []bool
					appendExpected := func(s string, valid bool) {
						expectedOffsets = append(expectedOffsets, int64(len(expectedPayload)))
						expectedPayload = append(expectedPayload, s...)
						expectedValid = append(expectedValid, valid)
					}
					for i := 0; i < prefix; i++ {
						s := fmt.Sprintf("prefix_%d", i)
						appendBinaryBuilderValue(builder, s)
						appendExpected(s, true)
					}

					for batch := 0; batch < 5; batch++ {
						vals := make([]string, 17+batch)
						valid := make([]bool, len(vals))
						for i := range vals {
							if i%7 != 0 {
								vals[i] = strings.Repeat("a", (i+batch*3)%71)
							}
							valid[i] = (i+batch)%4 != 0
							appendExpected(vals[i], valid[i])
						}
						appendBinaryBuilderValues(builder, vals, valid)
					}

					builder.AppendNulls(3)
					for range 3 {
						appendExpected("", false)
					}
					builder.AppendEmptyValues(2)
					for range 2 {
						appendExpected("", true)
					}
					appendBinaryBuilderValues(builder, []string{"tail", ""}, nil)
					appendExpected("tail", true)
					appendExpected("", true)

					arr := builder.NewArray().(array.BinaryLike)
					defer arr.Release()
					require.NoError(t, arr.(interface{ ValidateFull() error }).ValidateFull())
					require.Equal(t, len(expectedOffsets), arr.Len())
					require.True(t, bytes.Equal(expectedPayload, arr.ValueBytes()))
					nulls := 0
					for i := range expectedOffsets {
						require.Equal(t, expectedOffsets[i], arr.ValueOffset64(i), "offset %d", i)
						require.Equal(t, expectedValid[i], arr.IsValid(i), "valid %d", i)
						if !expectedValid[i] {
							nulls++
						}
						end := len(expectedPayload)
						if i+1 < len(expectedOffsets) {
							end = int(expectedOffsets[i+1])
						}
						require.Equal(t, end-int(expectedOffsets[i]), arr.ValueLen(i), "length %d", i)
					}
					require.Equal(t, nulls, arr.NullN())
				})
			}
		})
	}
}
