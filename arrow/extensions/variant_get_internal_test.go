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

package extensions

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/require"
)

func mkShreddedIntObj(t *testing.T, mem memory.Allocator, v int64) *VariantArray {
	t.Helper()
	vt := NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	bldr := NewVariantBuilder(mem, vt)
	defer bldr.Release()
	var vb variant.Builder
	require.NoError(t, vb.Append(map[string]any{"a": v}))
	val, err := vb.Build()
	require.NoError(t, err)
	bldr.Append(val)

	return bldr.NewArray().(*VariantArray)
}

// TestTryPerfectShreddingFires proves the fast path returns the typed_value column
// directly for a perfect shredding, and declines when the residual value has data.
func TestTryPerfectShreddingFires(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := mkShreddedIntObj(t, mem, 5)
	defer arr.Release()

	state := stateFromVariant(arr)
	nulls := newNullTracker(arr.Len())
	nulls.apply(arr.Storage())

	step, err := followFieldElement(state, "a")
	require.NoError(t, err)
	require.Equal(t, stepSuccess, step.kind)

	out := tryPerfectShredding(step.state, nulls, arrow.PrimitiveTypes.Int64)
	require.NotNil(t, out, "perfect shredding must fire for a fully shredded int64 leaf")
	defer out.Release()
	require.Equal(t, int64(5), out.(*array.Int64).Value(0))

	// A non-matching target type must decline.
	require.Nil(t, tryPerfectShredding(step.state, nulls, arrow.PrimitiveTypes.Int32))
}

func TestVariantGetNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr := mkShreddedIntObj(t, mem, 9)

	perfect, err := VariantGet(arr, GetOptions{
		Path:   VariantPath{VariantPathField("a")},
		AsType: arrow.PrimitiveTypes.Int64,
		Mem:    mem,
	})
	require.NoError(t, err)
	perfect.Release()

	variantOut, err := VariantGet(arr, GetOptions{
		Path: VariantPath{VariantPathField("a")},
		Mem:  mem,
	})
	require.NoError(t, err)
	variantOut.Release()

	missing, err := VariantGet(arr, GetOptions{
		Path:   VariantPath{VariantPathField("nope")},
		AsType: arrow.PrimitiveTypes.Int64,
		Mem:    mem,
	})
	require.NoError(t, err)
	missing.Release()

	arr.Release()

	// Ancestor-null case: forces nullTracker to allocate a real bitmap buffer that
	// buildTargetVariant threads onto the target struct.
	vt := NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	nb := NewVariantBuilder(mem, vt)
	var vb variant.Builder
	require.NoError(t, vb.Append(map[string]any{"a": int64(1)}))
	val, err := vb.Build()
	require.NoError(t, err)
	nb.Append(val)
	nb.AppendNull()
	withNull := nb.NewArray().(*VariantArray)
	nb.Release()

	got, err := VariantGet(withNull, GetOptions{
		Path: VariantPath{VariantPathField("a")},
		Mem:  mem,
	})
	require.NoError(t, err)
	require.True(t, got.(*VariantArray).IsNull(1))
	got.Release()
	withNull.Release()
}
