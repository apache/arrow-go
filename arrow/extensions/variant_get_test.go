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

package extensions_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkVariant(t *testing.T, v any) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, b.Append(v))
	val, err := b.Build()
	require.NoError(t, err)

	return val
}

// nonShreddedVariants builds a plain (metadata, value) VariantArray from Go values.
func nonShreddedVariants(t *testing.T, mem memory.Allocator, vals ...any) *extensions.VariantArray {
	t.Helper()
	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	for _, v := range vals {
		if v == nil {
			bldr.AppendNull()

			continue
		}
		bldr.Append(mkVariant(t, v))
	}

	return bldr.NewArray().(*extensions.VariantArray)
}

// shreddedIntObjects builds a VariantArray shredding an object with a single int64 field "a".
func shreddedIntObjects(t *testing.T, mem memory.Allocator, vals ...int64) *extensions.VariantArray {
	t.Helper()
	vt := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	for _, v := range vals {
		bldr.Append(mkVariant(t, map[string]any{"a": v}))
	}

	return bldr.NewArray().(*extensions.VariantArray)
}

func TestVariantGetTypedOutput(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := nonShreddedVariants(t, mem,
		map[string]any{"a": int64(1), "b": "x"},
		map[string]any{"a": int64(2), "b": "y"},
		nil,
	)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("a")},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	ints := out.(*array.Int64)
	require.Equal(t, 3, ints.Len())
	assert.EqualValues(t, 1, ints.Value(0))
	assert.EqualValues(t, 2, ints.Value(1))
	assert.True(t, ints.IsNull(2))
}

func TestVariantGetVariantOutput(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := nonShreddedVariants(t, mem,
		map[string]any{"a": int64(7)},
		map[string]any{"b": int64(9)}, // no "a" -> null
	)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path: extensions.VariantPath{extensions.VariantPathField("a")},
	})
	require.NoError(t, err)
	defer out.Release()

	varr := out.(*extensions.VariantArray)
	require.Equal(t, 2, varr.Len())

	v, err := varr.Value(0)
	require.NoError(t, err)
	assert.EqualValues(t, 7, v.Value())
	assert.True(t, varr.IsNull(1))
}

func TestVariantGetNestedPath(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := nonShreddedVariants(t, mem, map[string]any{"a": map[string]any{"b": int64(5)}})
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("a"), extensions.VariantPathField("b")},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	assert.EqualValues(t, 5, out.(*array.Int64).Value(0))
}

func TestVariantGetIndex(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := nonShreddedVariants(t, mem, []any{int64(10), int64(20), int64(30)})
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathIndex(1)},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	assert.EqualValues(t, 20, out.(*array.Int64).Value(0))

	oob, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathIndex(9)},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer oob.Release()
	assert.True(t, oob.(*array.Int64).IsNull(0))
}

func TestVariantGetPerfectShredding(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := shreddedIntObjects(t, mem, 11, 22, 33)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("a")},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	ints := out.(*array.Int64)
	require.Equal(t, 3, ints.Len())
	assert.EqualValues(t, 11, ints.Value(0))
	assert.EqualValues(t, 22, ints.Value(1))
	assert.EqualValues(t, 33, ints.Value(2))
}

func TestVariantGetShreddedVariantOutput(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := shreddedIntObjects(t, mem, 100, 200)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path: extensions.VariantPath{extensions.VariantPathField("a")},
	})
	require.NoError(t, err)
	defer out.Release()

	varr := out.(*extensions.VariantArray)
	v, err := varr.Value(0)
	require.NoError(t, err)
	assert.EqualValues(t, 100, v.Value())
	v, err = varr.Value(1)
	require.NoError(t, err)
	assert.EqualValues(t, 200, v.Value())
}

func TestVariantGetMissingField(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := shreddedIntObjects(t, mem, 1, 2)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("missing")},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	ints := out.(*array.Int64)
	require.Equal(t, 2, ints.Len())
	assert.True(t, ints.IsNull(0))
	assert.True(t, ints.IsNull(1))
}

// TestVariantGetNotShreddedFallback covers a field present only in the residual value
// of a shredded object: the columnar walk stops and the per-row fallback recovers it.
func TestVariantGetNotShreddedFallback(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	// "b" is not in the shredding schema, so it lands in the residual value column.
	bldr.Append(mkVariant(t, map[string]any{"a": int64(1), "b": int64(42)}))
	arr := bldr.NewArray().(*extensions.VariantArray)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("b")},
		AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()

	assert.EqualValues(t, 42, out.(*array.Int64).Value(0))
}

func TestVariantGetNestedTypeUnsupported(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := nonShreddedVariants(t, mem, map[string]any{"a": int64(1)})
	defer arr.Release()

	_, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path:   extensions.VariantPath{extensions.VariantPathField("a")},
		AsType: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}),
	})
	require.ErrorIs(t, err, arrow.ErrNotImplemented)
}

func TestVariantGetRejectsNonVariant(t *testing.T) {
	mem := memory.DefaultAllocator
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.Append(1)
	arr := bldr.NewArray()
	defer arr.Release()

	_, err := extensions.VariantGet(arr, extensions.GetOptions{})
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

// TestVariantGetDictMetadata guards against the panic from asserting TypedArray[[]byte]
// on a dictionary-encoded metadata column, which is spec-legal.
func TestVariantGetDictMetadata(t *testing.T) {
	mem := memory.DefaultAllocator
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.Binary}},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			)},
		), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	bldr := vt.NewBuilder(mem).(*extensions.VariantBuilder)
	defer bldr.Release()
	// "b" is not shredded, so extracting it takes the NotShredded -> buildTargetVariant path.
	bldr.Append(mkVariant(t, map[string]any{"a": int64(5), "b": "resid"}))
	arr := bldr.NewArray().(*extensions.VariantArray)
	defer arr.Release()

	out, err := extensions.VariantGet(arr, extensions.GetOptions{
		Path: extensions.VariantPath{extensions.VariantPathField("b")},
	})
	require.NoError(t, err)
	defer out.Release()

	v, err := out.(*extensions.VariantArray).Value(0)
	require.NoError(t, err)
	assert.Equal(t, "resid", v.Value())
}
