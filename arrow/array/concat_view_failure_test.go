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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestConcatViewShortHeaderReleasesDestination(t *testing.T) {
	for _, dtype := range []arrow.DataType{&arrow.BinaryViewType{}, &arrow.StringViewType{}} {
		for _, tc := range []struct {
			headerBytes int
			offset      int
		}{
			{0, 0},
			{1, 0},
			{arrow.ViewHeaderSizeBytes - 1, 0},
			{arrow.ViewHeaderSizeBytes, 1},
		} {
			t.Run(fmt.Sprintf("%s/bytes=%d/offset=%d", dtype.Name(), tc.headerBytes, tc.offset), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				header := memory.NewBufferBytes(make([]byte, tc.headerBytes))
				defer header.Release()
				variadic := memory.NewResizableBuffer(mem)
				variadic.Resize(32)
				defer variadic.Release()
				data := NewData(dtype, 1, []*memory.Buffer{nil, header, variadic}, nil, 0, tc.offset)
				defer data.Release()

				result, err := concat([]arrow.ArrayData{data}, mem)
				if result != nil {
					defer result.Release()
				}
				require.Error(t, err)
				require.Nil(t, result)
			})
		}
	}
}
