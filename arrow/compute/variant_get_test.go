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
	"github.com/apache/arrow-go/v18/arrow/decimal"
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
	arr.Release()

	// A missing field on an object-shredded variant exercises the all-null path.
	objVT := extensions.NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	ob := extensions.NewVariantBuilder(mem, objVT)
	ob.Append(vgVariant(t, map[string]any{"a": int64(1)}))
	objArr := ob.NewArray().(*extensions.VariantArray)
	ob.Release()

	missing, err := compute.VariantGet(ctx, objArr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("nope"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	missing.Release()
	objArr.Release()
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

// vgMixedResidual builds a two-row shredded array: row 0 shredded (fill populates typed_value, top value null),
// row 1 residual (top typed_value null, top value = resid). Storage comes from NewShreddedVariantType.
func vgMixedResidual(t *testing.T, mem memory.Allocator, shredType arrow.DataType, fill func(b array.Builder), resid variant.Value) *extensions.VariantArray {
	t.Helper()
	vt := extensions.NewShreddedVariantType(shredType)
	s := vt.StorageType().(*arrow.StructType)
	mIdx, _ := s.FieldIdx("metadata")
	vIdx, _ := s.FieldIdx("value")
	tIdx, _ := s.FieldIdx("typed_value")

	b := array.NewStructBuilder(mem, s)
	defer b.Release()
	mb := b.FieldBuilder(mIdx).(*array.BinaryBuilder)
	vb := b.FieldBuilder(vIdx).(*array.BinaryBuilder)

	b.Append(true)
	mb.Append(variant.EmptyMetadataBytes[:])
	vb.AppendNull()
	fill(b.FieldBuilder(tIdx))

	b.Append(true)
	mb.Append(resid.Metadata().Bytes())
	vb.Append(resid.Bytes())
	b.FieldBuilder(tIdx).AppendNull()

	st := b.NewArray()
	defer st.Release()

	return array.NewExtensionArrayWithStorage(vt, st).(*extensions.VariantArray)
}

// TestVariantGetResidualBackedField covers zeroshade's blocking :233 case for a root
// field path: row 1's {"a":2} lives in the top residual, so $.a must return 2 not null.
func TestVariantGetResidualBackedField(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgMixedResidual(t, mem, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}),
		func(b array.Builder) {
			tv := b.(*array.StructBuilder)
			tv.Append(true)
			a := tv.FieldBuilder(0).(*array.StructBuilder)
			a.Append(true)
			a.FieldBuilder(0).(*array.BinaryBuilder).AppendNull()
			a.FieldBuilder(1).(*array.Int64Builder).Append(1)
		}, vgVariant(t, map[string]any{"a": int64(2)}))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	require.Equal(t, 2, ints.Len())
	assert.EqualValues(t, 1, ints.Value(0))
	assert.EqualValues(t, 2, ints.Value(1), "residual-backed row must be reassembled, not nulled")
}

// TestVariantGetResidualBackedNestedField covers the nested-field case: $.a.b where
// row 1's whole {"a":{"b":6}} lives in the top residual.
func TestVariantGetResidualBackedNestedField(t *testing.T) {
	mem := memory.DefaultAllocator
	shred := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.StructOf(arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64})})
	arr := vgMixedResidual(t, mem, shred,
		func(b array.Builder) {
			tv := b.(*array.StructBuilder)
			tv.Append(true)
			a := tv.FieldBuilder(0).(*array.StructBuilder)
			a.Append(true)
			a.FieldBuilder(0).(*array.BinaryBuilder).AppendNull()
			aTV := a.FieldBuilder(1).(*array.StructBuilder)
			aTV.Append(true)
			bf := aTV.FieldBuilder(0).(*array.StructBuilder)
			bf.Append(true)
			bf.FieldBuilder(0).(*array.BinaryBuilder).AppendNull()
			bf.FieldBuilder(1).(*array.Int64Builder).Append(5)
		}, vgVariant(t, map[string]any{"a": map[string]any{"b": int64(6)}}))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("a").Field("b"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	assert.EqualValues(t, 5, ints.Value(0))
	assert.EqualValues(t, 6, ints.Value(1), "residual-backed row must be reassembled, not nulled")
}

// TestVariantGetResidualBackedListIndex covers the list-index case: [0] where row 1's
// whole [30,40] lives in the top residual.
func TestVariantGetResidualBackedListIndex(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgMixedResidual(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int64),
		func(b array.Builder) {
			lb := b.(*array.ListBuilder)
			lb.Append(true)
			el := lb.ValueBuilder().(*array.StructBuilder)
			for _, v := range []int64{10, 20} {
				el.Append(true)
				el.FieldBuilder(0).(*array.BinaryBuilder).AppendNull()
				el.FieldBuilder(1).(*array.Int64Builder).Append(v)
			}
		}, vgVariant(t, []any{int64(30), int64(40)}))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(0), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	assert.EqualValues(t, 10, ints.Value(0))
	assert.EqualValues(t, 30, ints.Value(1), "residual-backed row must be reassembled, not nulled")
}

// TestVariantGetMixedWidthIntegers covers zeroshade :411: variant ints encode at their
// natural width, so a wider AsType must not drop rows whose leaf shredded narrower.
func TestVariantGetMixedWidthIntegers(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem,
		map[string]any{"a": int64(1)},                // int8
		map[string]any{"a": int64(1000)},             // int16
		map[string]any{"a": int64(5_000_000_000)},    // int64
		map[string]any{"a": int64(9007199254740993)}) // int64 > 2^53, must stay exact (no float intermediate)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	require.Equal(t, 4, ints.Len())
	assert.EqualValues(t, 1, ints.Value(0))
	assert.EqualValues(t, 1000, ints.Value(1), "narrower-width leaf must not be dropped")
	assert.EqualValues(t, 5_000_000_000, ints.Value(2))
	assert.EqualValues(t, 9007199254740993, ints.Value(3), "value > 2^53 must be exact, not routed through float64")
}

// TestVariantGetHeterogeneousLeaves (arrow-rs parity): a valid int64 survives a
// narrower-typed sibling; only a non-numeric string nulls.
func TestVariantGetHeterogeneousLeaves(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem,
		map[string]any{"a": int64(1)},             // int8 encoding
		map[string]any{"a": int64(5_000_000_000)}, // int64, does not fit int8
		map[string]any{"a": "x"})                  // non-numeric -> null
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	assert.EqualValues(t, 1, ints.Value(0))
	assert.EqualValues(t, 5_000_000_000, ints.Value(1), "valid int64 must survive a narrower-typed sibling")
	assert.True(t, ints.IsNull(2), "non-numeric string must be null")
}

// TestVariantGetEmptyKey covers zeroshade's blocking path.go:42 case: an empty-string
// object key is a field step, not array index 0.
func TestVariantGetEmptyKey(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, map[string]any{"": int64(42)})
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field(""), AsType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)
	defer out.Release()
	assert.EqualValues(t, 42, out.(*array.Int64).Value(0))
}

// TestVariantGetResidualBackedDeepField exercises the residual break after a successful
// columnar descent: row 0 is shredded through a.b, row 1's {"b":6} sits in a's residual.
func TestVariantGetResidualBackedDeepField(t *testing.T) {
	mem := memory.DefaultAllocator
	shred := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.StructOf(arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64})})
	vt := extensions.NewShreddedVariantType(shred)
	s := vt.StorageType().(*arrow.StructType)
	mIdx, _ := s.FieldIdx("metadata")
	vIdx, _ := s.FieldIdx("value")
	tIdx, _ := s.FieldIdx("typed_value")

	b := array.NewStructBuilder(mem, s)
	defer b.Release()
	mb := b.FieldBuilder(mIdx).(*array.BinaryBuilder)
	vb := b.FieldBuilder(vIdx).(*array.BinaryBuilder)
	tvb := b.FieldBuilder(tIdx).(*array.StructBuilder) // struct{a}
	aField := tvb.FieldBuilder(0).(*array.StructBuilder)
	aVal := aField.FieldBuilder(0).(*array.BinaryBuilder)
	aTyped := aField.FieldBuilder(1).(*array.StructBuilder) // struct{b}
	bField := aTyped.FieldBuilder(0).(*array.StructBuilder)
	bVal := bField.FieldBuilder(0).(*array.BinaryBuilder)
	bTyped := bField.FieldBuilder(1).(*array.Int64Builder)

	// row 0: fully shredded a.b = 5
	b.Append(true)
	mb.Append(variant.EmptyMetadataBytes[:])
	vb.AppendNull()
	tvb.Append(true)
	aField.Append(true)
	aVal.AppendNull()
	aTyped.Append(true)
	bField.Append(true)
	bVal.AppendNull()
	bTyped.Append(5)

	// row 1: a is residual-backed with {"b":6} (top typed_value present, a.value set)
	resid := vgVariant(t, map[string]any{"b": int64(6)})
	b.Append(true)
	mb.Append(resid.Metadata().Bytes())
	vb.AppendNull()
	tvb.Append(true)
	aField.Append(true)
	aVal.Append(resid.Bytes())
	aTyped.AppendNull() // a.typed_value null -> recurses bField/bVal/bTyped null

	st := b.NewArray()
	defer st.Release()
	arr := array.NewExtensionArrayWithStorage(vt, st).(*extensions.VariantArray)
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Field("a").Field("b"), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	defer out.Release()
	ints := out.(*array.Int64)
	require.Equal(t, 2, ints.Len())
	assert.EqualValues(t, 5, ints.Value(0))
	assert.EqualValues(t, 6, ints.Value(1), "mid-level residual row must be reassembled, not nulled")
}

// TestVariantGetResidualNoLeak guards the residual break path (buildTargetVariant +
// per-row reassembly) against leaks, which TestVariantGetNoLeak does not reach.
func TestVariantGetResidualNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := exec.WithAllocator(context.Background(), mem)

	arr := vgMixedResidual(t, mem, arrow.ListOf(arrow.PrimitiveTypes.Int64),
		func(bld array.Builder) {
			lb := bld.(*array.ListBuilder)
			lb.Append(true)
			el := lb.ValueBuilder().(*array.StructBuilder)
			for _, v := range []int64{10, 20} {
				el.Append(true)
				el.FieldBuilder(0).(*array.BinaryBuilder).AppendNull()
				el.FieldBuilder(1).(*array.Int64Builder).Append(v)
			}
		}, vgVariant(t, []any{int64(30), int64(40)}))

	out, err := compute.VariantGet(ctx, arr, compute.VariantGetOptions{
		Path: variant.VariantPath{}.Index(0), AsType: arrow.PrimitiveTypes.Int64,
	})
	require.NoError(t, err)
	out.Release()
	arr.Release()
}

// vgNonShreddedVals builds a non-shredded array from pre-built values, so a test can
// control per-value encoding (e.g. timestamp unit) that vgNonShredded cannot.
func vgNonShreddedVals(t *testing.T, mem memory.Allocator, vals ...variant.Value) *extensions.VariantArray {
	t.Helper()
	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	for _, v := range vals {
		bldr.Append(v)
	}

	return bldr.NewArray().(*extensions.VariantArray)
}

func vgTimestamp(t *testing.T, ts arrow.Timestamp, nano bool) variant.Value {
	t.Helper()
	var b variant.Builder
	opts := []variant.AppendOpt{variant.OptTimestampUTC}
	if nano {
		opts = append(opts, variant.OptTimestampNano)
	}
	require.NoError(t, b.Append(ts, opts...))
	val, err := b.Build()
	require.NoError(t, err)

	return val
}

// TestVariantGetMixedFloatWidths (:411, floats): a Float(32) and a Double(64) leaf
// both survive a Float64 request, independent of order.
func TestVariantGetMixedFloatWidths(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, float32(1.5), float64(2.5))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)
	defer out.Release()
	f := out.(*array.Float64)
	require.Equal(t, 2, f.Len())
	assert.InDelta(t, 1.5, f.Value(0), 1e-9)
	assert.InDelta(t, 2.5, f.Value(1), 1e-9, "Double leaf must not be dropped by a Float first leaf")
}

// TestVariantGetIntPlusFloat: a mixed int/float column cast to Float64 widens the int
// through the cast kernels rather than nulling it.
func TestVariantGetIntPlusFloat(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, int64(3), float64(2.5))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)
	defer out.Release()
	f := out.(*array.Float64)
	assert.InDelta(t, 3.0, f.Value(0), 1e-9)
	assert.InDelta(t, 2.5, f.Value(1), 1e-9)
}

// TestVariantGetTypeOrderIndependent (:411): the same two values give the same result
// regardless of which row comes first.
func TestVariantGetTypeOrderIndependent(t *testing.T) {
	mem := memory.DefaultAllocator
	forward := vgNonShredded(t, mem, int64(3), float64(2.5))
	defer forward.Release()
	reverse := vgNonShredded(t, mem, float64(2.5), int64(3))
	defer reverse.Release()

	optsF := compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64}
	fwd, err := compute.VariantGet(context.Background(), forward, optsF)
	require.NoError(t, err)
	defer fwd.Release()
	rev, err := compute.VariantGet(context.Background(), reverse, optsF)
	require.NoError(t, err)
	defer rev.Release()

	fa, ra := fwd.(*array.Float64), rev.(*array.Float64)
	assert.InDelta(t, fa.Value(0), ra.Value(1), 1e-9)
	assert.InDelta(t, fa.Value(1), ra.Value(0), 1e-9)
	assert.False(t, fa.IsNull(0) || fa.IsNull(1) || ra.IsNull(0) || ra.IsNull(1), "no leaf dropped in either order")
}

// TestVariantGetMixedTimestampUnits: a micros leaf and a nanos leaf of the same instant
// both land on it when cast to a nanos target (units are converted, not reinterpreted).
func TestVariantGetMixedTimestampUnits(t *testing.T) {
	mem := memory.DefaultAllocator
	const micros = arrow.Timestamp(1_600_000_000_000_000)
	const nanos = arrow.Timestamp(1_600_000_000_000_000_000)
	arr := vgNonShreddedVals(t, mem, vgTimestamp(t, micros, false), vgTimestamp(t, nanos, true))
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		AsType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
	})
	require.NoError(t, err)
	defer out.Release()
	ts := out.(*array.Timestamp)
	require.Equal(t, 2, ts.Len())
	assert.EqualValues(t, nanos, ts.Value(0), "micros leaf must be scaled to nanos, not copied raw")
	assert.EqualValues(t, nanos, ts.Value(1))
}

// TestVariantGetMixedDecimalScales: leaves shredded at different scales both rescale to
// the requested target scale.
func TestVariantGetMixedDecimalScales(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShreddedVals(t, mem,
		vgVariant(t, variant.DecimalValue[decimal.Decimal32]{Scale: 1, Value: decimal.Decimal32(15)}),  // 1.5
		vgVariant(t, variant.DecimalValue[decimal.Decimal32]{Scale: 2, Value: decimal.Decimal32(225)})) // 2.25
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		AsType: &arrow.Decimal128Type{Precision: 38, Scale: 2},
	})
	require.NoError(t, err)
	defer out.Release()
	d := out.(*array.Decimal128)
	require.Equal(t, 2, d.Len())
	assert.InDelta(t, 1.5, d.Value(0).ToFloat64(2), 1e-9, "scale-1 leaf must rescale to scale-2, not drop")
	assert.InDelta(t, 2.25, d.Value(1).ToFloat64(2), 1e-9)
}

// TestVariantGetStrictSlowPathErrors (:411/:269): on the reassembly path a lossy cast
// errors under Strict rather than nulling.
func TestVariantGetStrictSlowPathErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, map[string]any{"a": int64(5_000_000_000)}) // overflows int32
	defer arr.Release()

	_, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
		Path: field("a"), AsType: arrow.PrimitiveTypes.Int32, Strict: true,
	})
	require.Error(t, err, "Strict must error on an overflowing cast, not null it")
}

// TestVariantGetMixedTypeNoLeak guards the multi-group scatter path (cast + Concatenate
// + Take), which the single-type leak tests do not reach.
// TestVariantGetNestedTypeNotImplemented pins that a nested AsType is rejected with
// ErrNotImplemented rather than silently producing an all-null array.
func TestVariantGetNestedTypeNotImplemented(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, map[string]any{"a": map[string]any{"x": int64(1)}})
	defer arr.Release()

	for _, nested := range []arrow.DataType{
		arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}),
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
	} {
		out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: nested})
		if out != nil {
			out.Release()
		}
		require.ErrorIs(t, err, arrow.ErrNotImplemented, "nested AsType %s must error, not null", nested)
	}
}

func TestVariantGetMixedTypeNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := exec.WithAllocator(context.Background(), mem)

	arr := vgNonShredded(t, mem, int64(3), float64(2.5), nil, "x")
	out, err := compute.VariantGet(ctx, arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)
	out.Release()
	arr.Release()
}

// TestVariantGetInterleavedScatter exercises the multi-group scatter (Concatenate +
// TakeArray) with a NON-IDENTITY permutation: two same-typed leaves straddle a
// differently-typed one, so group order [3,7,2.5] must scatter back to row order
// [3,2.5,7]. Every other multi-group test lands in identity order.
func TestVariantGetInterleavedScatter(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgNonShredded(t, mem, int64(3), float64(2.5), int64(7)) // Int8{0,2}, Float64{1}
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{AsType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)
	defer out.Release()
	f := out.(*array.Float64)
	require.Equal(t, 3, f.Len())
	assert.InDelta(t, 3.0, f.Value(0), 1e-9)
	assert.InDelta(t, 2.5, f.Value(1), 1e-9, "interleaved leaf must scatter back to its row, not stay in group order")
	assert.InDelta(t, 7.0, f.Value(2), 1e-9)
}

// TestVariantGetStrictObjectLeafErrors pins that under Strict an object/array leaf cast
// to a primitive errors (impossible cast) rather than silently nulling.
func TestVariantGetStrictObjectLeafErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	for _, v := range []any{
		map[string]any{"a": map[string]any{"x": int64(1)}}, // object leaf at $.a
		map[string]any{"a": []any{int64(1), int64(2)}},     // array leaf at $.a
	} {
		arr := vgNonShredded(t, mem, v)
		out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{
			Path: field("a"), AsType: arrow.PrimitiveTypes.Int64, Strict: true,
		})
		if out != nil {
			out.Release()
		}
		arr.Release()
		require.ErrorIs(t, err, arrow.ErrInvalid, "strict cast of a non-primitive leaf to Int64 must error")
	}
}

// TestVariantGetShreddedFieldOnScalarErrors pins that a field step into a shredded
// scalar column errors on the columnar path, matching the per-row GetByPath path.
func TestVariantGetShreddedFieldOnScalarErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := vgShreddedInt(t, mem, 1, 2, 3) // typed_value is a scalar Int64 column
	defer arr.Release()

	out, err := compute.VariantGet(context.Background(), arr, compute.VariantGetOptions{Path: field("a"), AsType: arrow.PrimitiveTypes.Int64})
	if out != nil {
		out.Release()
	}
	require.ErrorIs(t, err, arrow.ErrInvalid, "field access on a shredded scalar must error, not return null")
}
