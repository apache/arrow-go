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
	"github.com/stretchr/testify/assert"
)

func TestSliceApproxEqualBinaryBuilderLogicalType(t *testing.T) {
	for _, dtype := range []arrow.BinaryDataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.LargeString,
	} {
		t.Run(dtype.Name(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			builder := array.NewBinaryBuilder(mem, dtype)
			defer builder.Release()
			builder.AppendValues([][]byte{[]byte("a\x00"), []byte("same"), []byte("x\x00")}, nil)
			left := builder.NewArray()
			defer left.Release()
			builder.AppendValues([][]byte{[]byte("a"), []byte("same"), []byte("x")}, nil)
			right := builder.NewArray()
			defer right.Release()

			want := dtype.ID() == arrow.STRING || dtype.ID() == arrow.LARGE_STRING
			assert.Equal(t, want, array.SliceApproxEqual(left, 0, 3, right, 0, 3))
			assert.Equal(t, want, array.SliceApproxEqual(right, 0, 3, left, 0, 3))
			assert.Equal(t, want, array.SliceApproxEqual(left, 0, 1, right, 0, 1))
			assert.True(t, array.SliceApproxEqual(left, 1, 2, right, 1, 2))
			assert.False(t, array.SliceEqual(left, 0, 3, right, 0, 3))

			normalized := array.MakeFromData(right.Data())
			defer normalized.Release()
			assert.Equal(t, want, array.SliceApproxEqual(left, 0, 3, normalized, 0, 3))
			assert.Equal(t, want, array.SliceApproxEqual(normalized, 0, 3, left, 0, 3))
		})
	}
}
