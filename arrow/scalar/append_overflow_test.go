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

package scalar_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendListOverflowReturnsInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bigNull := array.NewNull(math.MaxInt32)
	defer bigNull.Release()
	oneNull := array.NewNull(1)
	defer oneNull.Release()

	t.Run("ListBuilder", func(t *testing.T) {
		bldr := array.NewListBuilder(mem, arrow.Null)
		defer bldr.Release()

		sBig := scalar.NewListScalar(bigNull)
		sOne := scalar.NewListScalar(oneNull)

		err := scalar.AppendSlice(bldr, []scalar.Scalar{sBig, sOne})
		require.ErrorIs(t, err, arrow.ErrInvalid)
		assert.Equal(t, 0, bldr.Len())
		assert.Equal(t, 0, bldr.ValueBuilder().Len())
	})

	t.Run("ListViewBuilder", func(t *testing.T) {
		bldr := array.NewListViewBuilder(mem, arrow.Null)
		defer bldr.Release()

		sBig := scalar.NewListViewScalar(bigNull)
		sOne := scalar.NewListViewScalar(oneNull)

		err := scalar.AppendSlice(bldr, []scalar.Scalar{sBig, sOne})
		require.ErrorIs(t, err, arrow.ErrInvalid)
		assert.Equal(t, 0, bldr.Len())
		assert.Equal(t, 0, bldr.ValueBuilder().Len())
	})

	t.Run("MapBuilder", func(t *testing.T) {
		bldr := array.NewMapBuilder(mem, arrow.Null, arrow.Null, false)
		defer bldr.Release()

		structType := bldr.Type().(*arrow.MapType).Elem().(*arrow.StructType)
		bigData := array.NewData(structType, math.MaxInt32, []*memory.Buffer{nil}, []arrow.ArrayData{bigNull.Data(), bigNull.Data()}, 0, 0)
		defer bigData.Release()
		bigEntries := array.NewStructData(bigData)
		defer bigEntries.Release()

		oneData := array.NewData(structType, 1, []*memory.Buffer{nil}, []arrow.ArrayData{oneNull.Data(), oneNull.Data()}, 0, 0)
		defer oneData.Release()
		oneEntries := array.NewStructData(oneData)
		defer oneEntries.Release()

		sBig := scalar.NewMapScalar(bigEntries)
		defer sBig.Release()
		sOne := scalar.NewMapScalar(oneEntries)
		defer sOne.Release()

		err := scalar.AppendSlice(bldr, []scalar.Scalar{sBig, sOne})
		require.ErrorIs(t, err, arrow.ErrInvalid)
		assert.Equal(t, 0, bldr.Len())
		assert.Equal(t, 0, bldr.ValueBuilder().Len())
		assert.Equal(t, 0, bldr.KeyBuilder().Len())
		assert.Equal(t, 0, bldr.ItemBuilder().Len())
	})
}
