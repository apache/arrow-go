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

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vgVariant(t *testing.T, v any) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, b.Append(v))
	val, err := b.Build()
	require.NoError(t, err)

	return val
}

func vgNonShredded(t *testing.T, mem memory.Allocator, vals ...any) *extensions.VariantArray {
	t.Helper()
	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	for _, v := range vals {
		if v == nil {
			bldr.AppendNull()

			continue
		}
		bldr.Append(vgVariant(t, v))
	}

	return bldr.NewArray().(*extensions.VariantArray)
}

func vgShreddedInt(t *testing.T, mem memory.Allocator, vals ...int64) *extensions.VariantArray {
	t.Helper()
	vt := extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64)
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	for _, v := range vals {
		bldr.Append(vgVariant(t, v))
	}

	return bldr.NewArray().(*extensions.VariantArray)
}

func field(name string) variant.VariantPath { return variant.VariantPath{}.Field(name) }

func TestVariantGetTyped(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem,
		map[string]any{"a": int64(1)},
		map[string]any{"a": int64(2)},
		nil,
	)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: arrow.PrimitiveTypes.Int64})
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
	arr := vgNonShredded(t, mem, map[string]any{"a": int64(7)}, map[string]any{"b": int64(9)})
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a")})
	require.NoError(t, err)
	defer out.Release()

	varr := out.(*extensions.VariantArray)
	v, err := varr.Value(0)
	require.NoError(t, err)
	assert.EqualValues(t, 7, v.Value())
	assert.True(t, varr.IsNull(1))
}

func TestVariantGetNestedAndIndex(t *testing.T) {
	mem := memory.DefaultAllocator
	nested := vgNonShredded(t, mem, map[string]any{"a": map[string]any{"b": int64(5)}})
	defer nested.Release()
	out, err := compute.VariantGet(context.Background(), nested, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("a").Field("b"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	assert.EqualValues(t, 5, out.(*array.Int64).Value(0))

	arrs := vgNonShredded(t, mem, []any{int64(10), int64(20), int64(30)})
	defer arrs.Release()
	got, err := compute.VariantGet(context.Background(), arrs, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(1), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer got.Release()
	assert.EqualValues(t, 20, got.(*array.Int64).Value(0))

	oob, err := compute.VariantGet(context.Background(), arrs, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(9), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer oob.Release()
	assert.True(t, oob.(*array.Int64).IsNull(0))
}

// TestVariantGetMixedShreddedRows reproduces zeroshade's [1,2] case: row 0 is in
// typed_value, row 1 is in the residual value. Both must come back, not [1,null].
func TestVariantGetMixedShreddedRows(t *testing.T) {
	mem := memory.DefaultAllocator
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	b := array.NewStructBuilder(mem, s)
	defer b.Release()
	mb := b.FieldBuilder(0).(*array.BinaryBuilder)
	vb := b.FieldBuilder(1).(*array.BinaryBuilder)
	tb := b.FieldBuilder(2).(*array.Int64Builder)

	b.Append(true)
	mb.Append(variant.EmptyMetadataBytes[:])
	vb.AppendNull()
	tb.Append(1)

	b.Append(true)
	mb.Append(variant.EmptyMetadataBytes[:])
	enc, err := variant.Encode(int64(2))
	require.NoError(t, err)
	vb.Append(enc)
	tb.AppendNull()

	st := b.NewArray()
	defer st.Release()
	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	arr := array.NewExtensionArrayWithStorage(vt, st).(*extensions.VariantArray)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	require.Equal(t, 2, ints.Len())
	assert.EqualValues(t, 1, ints.Value(0))
	assert.EqualValues(t, 2, ints.Value(1), "residual-value row must be reconstructed, not null")
	assert.False(t, ints.IsNull(1))
}

// TestVariantGetLenientCast covers zeroshade's :269 examples: ordinary widening
// casts succeed under the default (non-strict) mode.
func TestVariantGetLenientCast(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgShreddedInt(t, mem, 3, 5)
	defer arr.Release()

	f64, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)
	defer f64.Release()
	assert.EqualValues(t, 3, f64.(*array.Float64).Value(0))
	assert.EqualValues(t, 5, f64.(*array.Float64).Value(1))

	dec, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: &arrow.Decimal128Type{Precision: 10, Scale: 0}})
	require.NoError(t, err)
	defer dec.Release()
	assert.Equal(t, 2, dec.Len())
}

func TestVariantGetStrictCastErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgShreddedInt(t, mem, 5_000_000_000) // overflows int8
	defer arr.Release()

	_, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		AsType: arrow.PrimitiveTypes.Int8, Strict: true,
	})
	require.Error(t, err, "strict cast of an overflowing value must error")
}

// TestVariantGetFieldOnScalarErrors covers :323: a field step into a scalar is a
// type error, not a silent null.
func TestVariantGetFieldOnScalarErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, int64(1))
	defer arr.Release()

	_, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a")})
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

// TestVariantGetHugeIndex covers :307: a huge index must not wrap to a valid one.
func TestVariantGetHugeIndex(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, []any{int64(10), int64(20)})
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(1 << 40), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	assert.True(t, out.(*array.Int64).IsNull(0))
}

func TestVariantGetEmptyPath(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, int64(1), int64(2))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{})
	require.NoError(t, err)
	defer out.Release()
	assert.Equal(t, 2, out.Len())
}

// TestVariantGetShreddedFieldPushdown drives nested field steps through the shredded
// typed_value columns and the perfect-shredding fast path.
func TestVariantGetShreddedFieldPushdown(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.StructOf(
			arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64})}))
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	bldr.Append(vgVariant(t, map[string]any{"a": map[string]any{"b": int64(5)}}))
	bldr.Append(vgVariant(t, map[string]any{"a": map[string]any{"b": int64(6)}}))
	arr := bldr.NewArray().(*extensions.VariantArray)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("a").Field("b"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	assert.EqualValues(t, 5, ints.Value(0))
	assert.EqualValues(t, 6, ints.Value(1))
}

// TestVariantGetShreddedListIndex drives an index step over a shredded list, which
// gathers elements with the take kernel.
func TestVariantGetShreddedListIndex(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewShreddedVariantType(arrow.ListOf(arrow.PrimitiveTypes.Int64))
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	bldr.Append(vgVariant(t, []any{int64(10), int64(20), int64(30)}))
	bldr.Append(vgVariant(t, []any{int64(40), int64(50)}))
	arr := bldr.NewArray().(*extensions.VariantArray)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(1), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	assert.EqualValues(t, 20, ints.Value(0))
	assert.EqualValues(t, 50, ints.Value(1))
}

func TestVariantGetNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := exec.WithAllocator(context.Background(), mem)

	vt := extensions.NewShreddedVariantType(arrow.ListOf(arrow.PrimitiveTypes.Int64))
	bldr := extensions.NewVariantBuilder(mem, vt)
	bldr.Append(vgVariant(t, []any{int64(10), int64(20)}))
	bldr.AppendNull()
	arr := bldr.NewArray().(*extensions.VariantArray)
	bldr.Release()

	idx, err := compute.VariantGet(ctx, arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(0), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	idx.Release()

	missing, err := compute.VariantGet(ctx, arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("nope"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	missing.Release()

	arr.Release()
}

// TestVariantGetDictMetadata guards against a panic when the metadata column is
// dictionary-encoded (spec-legal): buildTargetVariant must read the raw column,
// not the plain-binary accessor.
func TestVariantGetDictMetadata(t *testing.T) {
	mem := memory.DefaultAllocator
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.Binary}},
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
	bldr.Append(vgVariant(t, map[string]any{"a": int64(5), "b": "resid"}))
	arr := bldr.NewArray().(*extensions.VariantArray)
	defer arr.Release()

	// "b" is not shredded, so this takes the NotShredded -> buildTargetVariant path.
	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: variant.VariantPath{}.Field("b")})
	require.NoError(t, err)
	defer out.Release()
	v, err := out.(*extensions.VariantArray).Value(0)
	require.NoError(t, err)
	assert.Equal(t, "resid", v.Value())
}
