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

package array

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestConcatMissingOffsetsDiagnostic(t *testing.T) {
	for _, dtype := range []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int8),
		arrow.LargeListOf(arrow.PrimitiveTypes.Int8),
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary,
	} {
		t.Run(dtype.Name(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			data := NewData(dtype, 1, make([]*memory.Buffer, len(dtype.Layout().Buffers)), nil, 0, 0)
			defer data.Release()

			result, err := concat([]arrow.ArrayData{data}, mem)
			if result != nil {
				defer result.Release()
			}
			require.EqualError(t, err, "array/concat: array is missing an offset buffer")
			require.Nil(t, result)
		})
	}
}
