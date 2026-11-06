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

//go:build go1.18

package exprs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	substraitext "github.com/substrait-io/substrait-go/v8/extensions"
	"github.com/substrait-io/substrait-go/v8/types"
	proto "github.com/substrait-io/substrait-protobuf/go/substraitpb"
)

var (
	extSet           = exprs.NewDefaultExtensionSet()
	_, u32TypeRef, _ = extSet.EncodeTypeVariation(arrow.PrimitiveTypes.Uint32)

	boringSchema = types.NamedStruct{
		Names: []string{
			"bool", "i8", "i32", "i32_req",
			"u32", "i64", "f32", "f32_req",
			"f64", "date32", "str", "bin"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types: []types.Type{
				&types.BooleanType{},
				&types.Int8Type{},
				&types.Int32Type{},
				&types.Int32Type{Nullability: types.NullabilityRequired},
				&types.Int32Type{
					TypeVariationRef: u32TypeRef,
				},
				&types.Int64Type{},
				&types.Float32Type{},
				&types.Float32Type{Nullability: types.NullabilityRequired},
				&types.Float64Type{},
				&types.DateType{},
				&types.StringType{},
				&types.BinaryType{},
			},
		},
	}

	boringArrowSchema = arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
)

func TestToArrowSchema(t *testing.T) {
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i32_req", Type: arrow.PrimitiveTypes.Int32},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f32_req", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	sc, err := exprs.ToArrowSchema(boringSchema, extSet)
	assert.NoError(t, err)

	assert.Truef(t, expectedSchema.Equal(sc), "expected: %s\ngot: %s", expectedSchema, sc)
}

func assertEqual(t *testing.T, expected, actual any) bool {
	switch e := expected.(type) {
	case compute.Datum:
		return assert.Truef(t, e.Equals(compute.NewDatumWithoutOwning(actual)),
			"expected: %s\ngot: %s", e, actual)
	case arrow.Array:
		switch a := actual.(type) {
		case compute.Datum:
			if a.Kind() == compute.KindArray {
				actual := a.(*compute.ArrayDatum).MakeArray()
				defer actual.Release()
				return assert.Truef(t, array.Equal(e, actual), "expected: %s\ngot: %s",
					e, actual)
			}
		case arrow.Array:
			return assert.Truef(t, array.Equal(e, a), "expected: %s\ngot: %s",
				e, actual)
		}
		t.Errorf("expected arrow Array, got %s", actual)
		return false
	}
	panic("unimplemented comparison")
}

func TestComparisons(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var (
		ctx  = compute.WithAllocator(context.Background(), mem)
		zero = scalar.MakeScalar(int32(0))
		one  = scalar.MakeScalar(int32(1))
		two  = scalar.MakeScalar(int32(2))

		exampleUUID    = uuid.MustParse("102cb62f-e6f8-4eb0-9973-d9b012ff0967")
		exampleUUID2   = uuid.MustParse("c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b")
		uuidStorage, _ = scalar.MakeScalarParam(exampleUUID[:],
			&arrow.FixedSizeBinaryType{ByteWidth: 16})
		uuidScalar      = scalar.NewExtensionScalar(uuidStorage, extensions.NewUUIDType())
		uuidStorage2, _ = scalar.MakeScalarParam(exampleUUID2[:],
			&arrow.FixedSizeBinaryType{ByteWidth: 16})
		uuidScalar2 = scalar.NewExtensionScalar(uuidStorage2, extensions.NewUUIDType())
	)

	getArgType := func(dt arrow.DataType) types.Type {
		switch dt.ID() {
		case arrow.INT32:
			return &types.Int32Type{}
		case arrow.STRING:
			return &types.StringType{}
		case arrow.BINARY:
			return &types.BinaryType{}
		case arrow.EXTENSION:
			return &types.UUIDType{}
		}
		panic("wtf")
	}

	expect := func(t *testing.T, fn string, arg1, arg2 scalar.Scalar, res bool) {
		baseStruct := types.NamedStruct{
			Names: []string{"arg1", "arg2"},
			Struct: types.StructType{
				Types: []types.Type{getArgType(arg1.DataType()), getArgType(arg2.DataType())},
			},
		}

		ex, err := exprs.NewScalarCall(extSet, fn, nil,
			expr.MustExpr(expr.NewRootFieldRef(expr.NewStructFieldRef(0), types.NewRecordTypeFromStruct(baseStruct.Struct))),
			expr.MustExpr(expr.NewRootFieldRef(expr.NewStructFieldRef(1), types.NewRecordTypeFromStruct(baseStruct.Struct))))
		require.NoError(t, err)

		expression := &expr.Extended{
			Extensions: extSet.GetSubstraitRegistry().Set,
			ReferredExpr: []expr.ExpressionReference{
				expr.NewExpressionReference([]string{"out"}, ex),
			},
			BaseSchema: baseStruct,
		}

		input, _ := scalar.NewStructScalarWithNames([]scalar.Scalar{arg1, arg2}, []string{"arg1", "arg2"})
		out, err := exprs.ExecuteScalarSubstrait(ctx, expression, compute.NewDatum(input))
		require.NoError(t, err)
		require.Equal(t, compute.KindScalar, out.Kind())

		result := out.(*compute.ScalarDatum).Value
		assert.Equal(t, res, result.(*scalar.Boolean).Value)
	}

	expect(t, "equal", one, one, true)
	expect(t, "equal", one, two, false)
	expect(t, "less", one, two, true)
	expect(t, "less", one, zero, false)
	expect(t, "greater", one, zero, true)
	expect(t, "greater", one, two, false)

	// Note: Direct comparison between string and binary types is not supported
	// in substrait-go v7.2.2+ due to stricter type parameter validation.
	// Previously these comparisons were allowed but are now correctly rejected
	// as 'equal(any1, any1)' requires both arguments to be the same type.
	// expect(t, "equal", str, bin, true)
	// expect(t, "equal", bin, str, true)

	expect(t, "equal", uuidScalar, uuidScalar, true)
	expect(t, "equal", uuidScalar, uuidScalar2, false)
	expect(t, "less", uuidScalar, uuidScalar2, true)
	expect(t, "less", uuidScalar2, uuidScalar, false)
	expect(t, "greater", uuidScalar, uuidScalar2, false)
	expect(t, "greater", uuidScalar2, uuidScalar, true)
}

func TestExecuteFieldRef(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	fromJSON := func(ty arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	scalarFromJSON := func(ty arrow.DataType, json string) scalar.Scalar {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		defer arr.Release()
		s, err := scalar.GetScalar(arr, 0)
		require.NoError(t, err)
		return s
	}

	tests := []struct {
		testName string
		ref      compute.FieldRef
		input    compute.Datum
		expected compute.Datum
	}{
		{"basic ref", compute.FieldRefName("a"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
			 	{"a": 6.125},
				{"a": 0.0},
				{"a": -1}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"ref one field", compute.FieldRefName("a"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
				{"a": 6.125, "b": 7.5},
				{"a": 0.0, "b": 2.125},
				{"a": -1, "b": 4.0}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"second field", compute.FieldRefName("b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
					{"a": 6.125, "b": 7.5},
					{"a": 0.0, "b": 2.125},
					{"a": -1, "b": 4.0}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[7.5, 2.125, 4.0]`))},
		{"nested field by path", compute.FieldRefPath(compute.FieldPath{0, 0}), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
				{"a": {"b": 6.125}},
				{"a": {"b": 0.0}},
				{"a": {"b": -1}}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"nested field by name", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
					{"a": {"b": 6.125}},
					{"a": {"b": 0.0}},
					{"a": {"b": -1}}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"nested field with nulls", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
						{"a": {"b": 6.125}},
						{"a": null},
						{"a": {"b": null}}
					 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, null, null]`))},
		{"nested scalar", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": {"b": 64.0}}]`)),
			compute.NewDatum(scalar.NewFloat64Scalar(64.0))},
		{"nested scalar with null", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": {"b": null}}]`)),
			compute.NewDatum(scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64))},
		{"nested scalar null", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": null}]`)),
			compute.NewDatum(scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64))},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			scoped := memory.NewCheckedAllocatorScope(mem)
			defer scoped.CheckSize(t)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			dt := tt.input.(compute.ArrayLikeDatum).Type().(arrow.NestedType)
			schema := arrow.NewSchema(dt.Fields(), nil)
			ref, err := exprs.NewFieldRef(tt.ref, schema, extSet)
			require.NoError(t, err)
			assert.NotNil(t, ref)

			actual, err := exprs.ExecuteScalarExpression(ctx, schema, ref, tt.input)
			require.NoError(t, err)
			defer actual.Release()

			assert.Truef(t, tt.expected.Equals(actual), "expected: %s\ngot: %s", tt.expected, actual)
		})
	}
}

func TestGetRefFieldEmptySchema(t *testing.T) {
	_, err := exprs.GetRefField(expr.NewStructFieldRef(0), nil)
	assert.ErrorIs(t, err, compute.ErrNoChildren)
}

func TestGetRefFieldNestedNoChildren(t *testing.T) {
	ref := &expr.StructFieldRef{
		Field: 0,
		Child: expr.NewStructFieldRef(0),
	}
	fields := []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}

	_, err := exprs.GetRefField(ref, fields)
	assert.ErrorIs(t, err, compute.ErrNoChildren)
	assert.EqualError(t, err, compute.ErrNoChildren.Error()+": int32")
}

func TestExecuteScalarFuncCall(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	fromJSON := func(ty arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	basicSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	nestedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.StructOf(basicSchema.Fields()...), Nullable: false},
	}, nil)

	bldr := exprs.NewExprBuilder(extSet)

	tests := []struct {
		name     string
		ex       exprs.Builder
		sc       *arrow.Schema
		input    compute.Datum
		expected compute.Datum
	}{
		{"add", bldr.MustCallScalar("add", nil, bldr.FieldRef("a"),
			bldr.Literal(expr.NewPrimitiveLiteral(float64(3.5), false))),
			basicSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(basicSchema.Fields()...),
				`[
				{"a": 6.125, "b": 3.375},
				{"a": 0.0, "b": 1},
				{"a": -1, "b": 4.75}
			]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[9.625, 3.5, 2.5]`))},
		{"add sub", bldr.MustCallScalar("add", nil, bldr.FieldRef("a"),
			bldr.MustCallScalar("subtract", nil,
				bldr.WrapLiteral(expr.NewLiteral(float64(3.5), false)),
				bldr.FieldRef("b"))),
			basicSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(basicSchema.Fields()...),
				`[
				{"a": 6.125, "b": 3.375},
				{"a": 0.0, "b": 1},
				{"a": -1, "b": 4.75}
			]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[6.25, 2.5, -2.25]`))},
		{"add nested", bldr.MustCallScalar("add", nil,
			bldr.FieldRefList("a", "a"), bldr.FieldRefList("a", "b")), nestedSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(nestedSchema.Fields()...),
				`[
					{"a": {"a": 6.125, "b": 3.375}},
					{"a": {"a": 0.0, "b": 1}},
					{"a": {"a": -1, "b": 4.75}}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[9.5, 1, 3.75]`))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scoped := memory.NewCheckedAllocatorScope(mem)
			defer scoped.CheckSize(t)

			bldr.SetInputSchema(tt.sc)
			ex, err := tt.ex.BuildExpr()
			require.NoError(t, err)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			dt := tt.input.(compute.ArrayLikeDatum).Type().(arrow.NestedType)
			schema := arrow.NewSchema(dt.Fields(), nil)

			actual, err := exprs.ExecuteScalarExpression(ctx, schema, ex, tt.input)
			require.NoError(t, err)
			defer actual.Release()

			assert.Truef(t, tt.expected.Equals(actual), "expected: %s\ngot: %s", tt.expected, actual)
		})
	}
}

func TestGenerateMask(t *testing.T) {
	sc, err := boringArrowSchema.AddField(0, arrow.Field{
		Name: "in", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
	require.NoError(t, err)

	bldr := exprs.NewExprBuilder(extSet)
	require.NoError(t, bldr.SetInputSchema(sc))

	tests := []struct {
		name   string
		json   string
		filter exprs.Builder
	}{
		{"simple", `[
			{"i32": 0, "f32": -0.1, "in": true},
			{"i32": 0, "f32":  0.3, "in": true},
			{"i32": 1, "f32":  0.2, "in": false},
			{"i32": 2, "f32": -0.1, "in": false},
			{"i32": 0, "f32":  0.1, "in": true},
			{"i32": 0, "f32": null, "in": true},
			{"i32": 0, "f32":  1.0, "in": true}
		]`, bldr.MustCallScalar("equal", nil,
			bldr.FieldRef("i32"), bldr.Literal(expr.NewPrimitiveLiteral(int32(0), false)))},
		{"complex", `[
			{"f64":  0.3, "f32":  0.1, "in": true},
			{"f64": -0.1, "f32":  0.3, "in": false},
			{"f64":  0.1, "f32":  0.2, "in": true},
			{"f64":  0.0, "f32": -0.1, "in": false},
			{"f64":  1.0, "f32":  0.1, "in": true},
			{"f64": -2.0, "f32": null, "in": null},
			{"f64":  3.0, "f32":  1.0, "in": true}
		]`, bldr.MustCallScalar("greater", nil,
			bldr.MustCallScalar("multiply", nil,
				bldr.Must(bldr.Cast(bldr.FieldRef("f32"), arrow.PrimitiveTypes.Float64)),
				bldr.FieldRef("f64")),
			bldr.Literal(expr.NewPrimitiveLiteral(float64(0), false)))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)

			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(tt.json))
			require.NoError(t, err)
			defer rec.Release()

			input := compute.NewDatumWithoutOwning(rec)
			expectedMask := rec.Column(0)

			mask, err := exprs.ExecuteScalarExpression(ctx, sc,
				expr.MustExpr(tt.filter.BuildExpr()), input)
			require.NoError(t, err)
			defer mask.Release()

			assertEqual(t, expectedMask, mask)
		})
	}
}

func Test_Types(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name   string
		schema func() *arrow.Schema
		record func(rq *require.Assertions, schema *arrow.Schema) arrow.RecordBatch
		val    func(rq *require.Assertions) expr.Literal
	}{
		{
			name: "expect arrow.TIME64 (ns) ok",
			schema: func() *arrow.Schema {
				field := arrow.Field{
					Name:     "col",
					Type:     &arrow.Time64Type{Unit: arrow.Nanosecond},
					Nullable: true,
				}

				return arrow.NewSchema([]arrow.Field{field}, nil)
			},
			record: func(rq *require.Assertions, schema *arrow.Schema) arrow.RecordBatch {
				b := array.NewTime64Builder(memory.DefaultAllocator, &arrow.Time64Type{Unit: arrow.Nanosecond})
				defer b.Release()

				t1, err := arrow.Time64FromString("10:00:00.000000", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Time64 value")

				b.AppendValues([]arrow.Time64{t1}, []bool{true})

				return array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
			},
			val: func(rq *require.Assertions) expr.Literal {
				v, err := arrow.Time64FromString("11:00:00.000000", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Time64 value")

				pt := &types.PrecisionTime{
					Precision: int32(arrow.Nanosecond) * 3,
					Value:     int64(v),
				}

				lit, err := expr.NewLiteral(pt, true)
				rq.NoError(err, "Failed to create literal")

				return lit
			},
		},
		{
			name: "expect arrow.TIME64 (ns) ok",
			schema: func() *arrow.Schema {
				field := arrow.Field{
					Name:     "col",
					Type:     &arrow.Time64Type{Unit: arrow.Nanosecond},
					Nullable: true,
				}

				return arrow.NewSchema([]arrow.Field{field}, nil)
			},
			record: func(rq *require.Assertions, schema *arrow.Schema) arrow.RecordBatch {
				b := array.NewTime64Builder(memory.DefaultAllocator, &arrow.Time64Type{Unit: arrow.Nanosecond})
				defer b.Release()

				t1, err := arrow.Time64FromString("10:00:00.000000", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Time64 value")

				b.AppendValues([]arrow.Time64{t1}, []bool{true})

				return array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
			},
			val: func(rq *require.Assertions) expr.Literal {
				v, err := arrow.Time64FromString("11:00:00.000000", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Time64 value")

				pt := &types.PrecisionTime{
					Precision: int32(arrow.Nanosecond) * 3,
					Value:     int64(v),
				}

				lit, err := expr.NewLiteral(pt, true)
				rq.NoError(err, "Failed to create literal")

				return lit
			},
		},
		{
			name: "expect arrow.TIMESTAMP (ns) ok",
			schema: func() *arrow.Schema {
				field := arrow.Field{
					Name:     "col",
					Type:     &arrow.TimestampType{Unit: arrow.Nanosecond},
					Nullable: true,
				}

				return arrow.NewSchema([]arrow.Field{field}, nil)
			},
			record: func(rq *require.Assertions, schema *arrow.Schema) arrow.RecordBatch {
				b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
				defer b.Release()

				t1, err := arrow.TimestampFromString("2021-01-01T10:00:00.000000Z", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Timestamp value")

				b.AppendValues([]arrow.Timestamp{t1}, []bool{true})

				return array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
			},
			val: func(rq *require.Assertions) expr.Literal {
				v, err := arrow.TimestampFromString("2021-01-01T11:00:00.000000Z", arrow.Nanosecond)
				rq.NoError(err, "Failed to create Timestamp value")

				pts := &types.PrecisionTimestamp{
					PrecisionTimestamp: &proto.Expression_Literal_PrecisionTimestamp{
						Precision: int32(arrow.Nanosecond) * 3,
						Value:     int64(v),
					},
				}

				lit, err := expr.NewLiteral(pts, true)
				rq.NoError(err, "Failed to create literal")

				return lit
			},
		},
		{
			name: "expect arrow.DECIMAL128 ok",
			schema: func() *arrow.Schema {
				field := arrow.Field{
					Name:     "col",
					Type:     &arrow.Decimal128Type{Precision: 38, Scale: 10},
					Nullable: true,
				}

				return arrow.NewSchema([]arrow.Field{field}, nil)
			},
			record: func(rq *require.Assertions, schema *arrow.Schema) arrow.RecordBatch {
				b := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 38, Scale: 10})
				defer b.Release()

				d, err := decimal.Decimal128FromFloat(123.456789, 38, 10)
				rq.NoError(err, "Failed to create Decimal128 value")

				b.Append(d)

				return array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
			},
			val: func(rq *require.Assertions) expr.Literal {
				v, _, s, err := expr.DecimalStringToBytes("456.7890123456")
				rq.NoError(err, "Failed to convert decimal string to bytes")

				lit, err := expr.NewLiteral(&types.Decimal{
					Value:     v[:16],
					Precision: 38,
					Scale:     s,
				}, true)
				rq.NoError(err, "Failed to create Decimal128 literal")

				return lit
			},
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			rq := require.New(t)
			schema := tc.schema()
			record := tc.record(rq, schema)

			extSet := exprs.GetExtensionIDSet(ctx)
			builder := exprs.NewExprBuilder(extSet)

			err := builder.SetInputSchema(schema)
			rq.NoError(err, "Failed to set input schema")

			b, err := builder.CallScalar("less", nil,
				builder.FieldRef("col"),
				builder.Literal(tc.val(rq)),
			)

			rq.NoError(err, "Failed to call scalar")

			e, err := b.BuildExpr()
			rq.NoError(err, "Failed to build expression")

			ctx = exprs.WithExtensionIDSet(ctx, extSet)

			dr := compute.NewDatum(record)
			defer dr.Release()

			_, err = exprs.ExecuteScalarExpression(ctx, schema, e, dr)
			rq.NoError(err, "Failed to execute scalar expression")
		})
	}
}

func TestLargeTypes(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "str", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
	}, nil)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[
		{"str": "hello world", "bin": "Zm9vYmFy"}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	dr := compute.NewDatumWithoutOwning(rec)

	t.Run("large_string ref", func(t *testing.T) {
		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))

		e, err := bldr.FieldRef("str").BuildExpr()
		require.NoError(t, err, "Failed to build field reference expression")

		ctx := context.Background()
		result, err := exprs.ExecuteScalarExpression(ctx, sc, e, dr)
		require.NoError(t, err, "Failed to execute scalar expression")
		defer result.Release()
	})

	t.Run("large_binary ref", func(t *testing.T) {
		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))

		e, err := bldr.FieldRef("bin").BuildExpr()
		require.NoError(t, err, "Failed to build field reference expression")

		ctx := context.Background()
		result, err := exprs.ExecuteScalarExpression(ctx, sc, e, dr)
		require.NoError(t, err, "Failed to execute scalar expression")
		defer result.Release()
	})
}

func TestDictionaryTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name      string
		dictType  *arrow.DictionaryType
		dict      string
		valueType arrow.DataType
		lit       expr.Literal
	}{
		{"int8 utf8", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
			`["a", "b"]`, arrow.BinaryTypes.String, expr.NewPrimitiveLiteral("a", false)},
		{"int32 utf8", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
			`["a", "b"]`, arrow.BinaryTypes.String, expr.NewPrimitiveLiteral("a", false)},
		{"int8 large_utf8", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.LargeString},
			`["a", "b"]`, arrow.BinaryTypes.String, expr.NewPrimitiveLiteral("a", false)},
		{"int8 binary", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.Binary},
			`["YQ==", "Yg=="]`, arrow.BinaryTypes.Binary, expr.NewByteSliceLiteral([]byte("a"), false)},
		{"int32 large_binary", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.LargeBinary},
			`["YQ==", "Yg=="]`, arrow.BinaryTypes.Binary, expr.NewByteSliceLiteral([]byte("a"), false)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := array.DictArrayFromJSON(mem, tt.dictType, `[0, 1, null, 0]`, tt.dict)
			require.NoError(t, err)
			defer col.Release()

			dictSchema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tt.dictType, Nullable: true}}, nil)
			rec := array.NewRecordBatch(dictSchema, []arrow.Array{col}, int64(col.Len()))
			defer rec.Release()
			input := compute.NewDatumWithoutOwning(rec)

			bldr := exprs.NewExprBuilder(extSet)
			require.NoError(t, bldr.SetInputSchema(arrow.NewSchema(
				[]arrow.Field{{Name: "col", Type: tt.valueType, Nullable: true}}, nil)))

			cases := []struct {
				name     string
				ex       exprs.Builder
				expected string
			}{
				{"equal", bldr.MustCallScalar("equal", nil, bldr.FieldRef("col"), bldr.Literal(tt.lit)),
					`[true, false, null, true]`},
				{"is_null", bldr.MustCallScalar("is_null", nil, bldr.FieldRef("col")),
					`[false, false, true, false]`},
			}

			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					ex, err := c.ex.BuildExpr()
					require.NoError(t, err)

					ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
					result, err := exprs.ExecuteScalarExpression(ctx, dictSchema, ex, input)
					require.NoError(t, err)
					defer result.Release()

					expected, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(c.expected))
					require.NoError(t, err)
					defer expected.Release()
					assertEqual(t, expected, result)
				})
			}
		})
	}

	rejected := []struct {
		name      string
		dictType  *arrow.DictionaryType
		dict      string
		valueType arrow.DataType
		fn        string
		err       error
	}{
		{"binary dictionary as utf8", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.Binary},
			`["YQ=="]`, arrow.BinaryTypes.String, "", arrow.ErrInvalid},
		{"float64 dictionary", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Float64},
			`[1.5]`, arrow.PrimitiveTypes.Float64, "is_nan", arrow.ErrInvalid},
		{"is_nan utf8 dictionary", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
			`["a"]`, arrow.BinaryTypes.String, "is_nan", arrow.ErrNotImplemented},
		{"is_nan binary dictionary", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.Binary},
			`["YQ=="]`, arrow.BinaryTypes.Binary, "is_nan", arrow.ErrNotImplemented},
	}

	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			col, err := array.DictArrayFromJSON(mem, tt.dictType, `[0]`, tt.dict)
			require.NoError(t, err)
			defer col.Release()

			dictSchema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tt.dictType, Nullable: true}}, nil)
			rec := array.NewRecordBatch(dictSchema, []arrow.Array{col}, int64(col.Len()))
			defer rec.Release()

			bldr := exprs.NewExprBuilder(extSet)
			require.NoError(t, bldr.SetInputSchema(arrow.NewSchema(
				[]arrow.Field{{Name: "col", Type: tt.valueType, Nullable: true}}, nil)))
			ex, err := bldr.FieldRef("col").BuildExpr()
			require.NoError(t, err)
			if tt.fn != "" {
				// Bypass signature validation to exercise kernel dispatch for
				// is_nan on string and binary dictionaries.
				ex, err = expr.NewCustomScalarFunc(extSet.GetSubstraitRegistry(),
					substraitext.NewScalarFuncVariant(substraitext.ID{URN: exprs.SubstraitComparisonFuncsURI, Name: tt.fn}),
					&types.BooleanType{Nullability: types.NullabilityNullable}, nil, ex)
				require.NoError(t, err)
			}

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			_, err = exprs.ExecuteScalarExpression(ctx, dictSchema, ex, compute.NewDatumWithoutOwning(rec))
			assert.ErrorIs(t, err, tt.err)
			assert.ErrorContains(t, err, "dictionary<")
		})
	}
}

func TestFieldRefTypeMismatchReleases(t *testing.T) {
	structOf := func(inner arrow.DataType) arrow.DataType {
		return arrow.StructOf(arrow.Field{Name: "a", Type: inner, Nullable: true})
	}

	tests := []struct {
		name               string
		exprType, dataType arrow.DataType
		json               string
		path               []any
	}{
		{"top level", arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32,
			`[{"col": 1}, {"col": 2}]`, []any{"col"}},
		{"nested", structOf(arrow.BinaryTypes.String), structOf(arrow.PrimitiveTypes.Int32),
			`[{"col": {"a": 1}}, {"col": {"a": 2}}]`, []any{"col", "a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			exprSchema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tt.exprType, Nullable: true}}, nil)
			dataSchema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tt.dataType, Nullable: true}}, nil)

			rec, _, err := array.RecordFromJSON(mem, dataSchema, strings.NewReader(tt.json))
			require.NoError(t, err)
			defer rec.Release()

			bldr := exprs.NewExprBuilder(extSet)
			require.NoError(t, bldr.SetInputSchema(exprSchema))
			e, err := bldr.FieldRefList(tt.path...).BuildExpr()
			require.NoError(t, err)

			ctx := compute.WithAllocator(context.Background(), mem)
			result, err := exprs.ExecuteScalarExpression(ctx, dataSchema, e, compute.NewDatumWithoutOwning(rec))
			assert.ErrorIs(t, err, arrow.ErrInvalid)
			assert.Nil(t, result)
		})
	}
}

func TestNestedFieldRefResultOwnership(t *testing.T) {
	scalarInput := func(t *testing.T, mem memory.Allocator, dt *arrow.StructType, json string) *scalar.Struct {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(json))
		require.NoError(t, err)
		defer arr.Release()
		sc, err := scalar.GetScalar(arr, 0)
		require.NoError(t, err)
		return sc.(*scalar.Struct)
	}

	execute := func(t *testing.T, mem memory.Allocator, sc *arrow.Schema, e expr.Expression, input compute.Datum) compute.Datum {
		ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
		result, err := exprs.ExecuteScalarExpression(ctx, sc, e, input)
		require.NoError(t, err)
		return result
	}

	t.Run("array struct field", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		sc := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), Nullable: true}}, nil)
		rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(`[{"s": {"a": 1}}, {"s": {"a": 2}}]`))
		require.NoError(t, err)

		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))
		e, err := bldr.FieldRefList("s", "a").BuildExpr()
		require.NoError(t, err)

		result := execute(t, mem, sc, e, compute.NewDatumWithoutOwning(rec))
		rec.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")

		expected, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2]`))
		require.NoError(t, err)
		defer expected.Release()
		assertEqual(t, expected, result)
		result.Release()
	})

	t.Run("scalar struct field", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		dt := arrow.StructOf(arrow.Field{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true}), Nullable: true})
		input := scalarInput(t, mem, dt, `[{"s": {"b": "aGVsbG8="}}]`)
		sc := arrow.NewSchema(dt.Fields(), nil)

		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))
		e, err := bldr.FieldRefList("s", "b").BuildExpr()
		require.NoError(t, err)

		result := execute(t, mem, sc, e, compute.NewDatumWithoutOwning(input))
		input.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")

		assert.Equal(t, []byte("hello"), result.(*compute.ScalarDatum).Value.(*scalar.Binary).Data())
		result.Release()
	})

	t.Run("scalar list element", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		dt := arrow.StructOf(arrow.Field{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "l", Type: arrow.ListOf(arrow.BinaryTypes.Binary), Nullable: true}), Nullable: true})
		input := scalarInput(t, mem, dt, `[{"s": {"l": ["aGVsbG8=", "d29ybGQ="]}}]`)
		defer input.Release()
		sc := arrow.NewSchema(dt.Fields(), nil)

		st, err := exprs.ToSubstraitType(dt, false, extSet)
		require.NoError(t, err)
		e, err := expr.NewRootFieldRef(&expr.StructFieldRef{Field: 0, Child: &expr.StructFieldRef{Field: 0,
			Child: &expr.ListElementRef{Offset: 1}}},
			types.NewRecordTypeFromStruct(*st.(*types.StructType)))
		require.NoError(t, err)

		result := execute(t, mem, sc, e, compute.NewDatumWithoutOwning(input))
		defer result.Release()
		assert.Equal(t, []byte("world"), result.(*compute.ScalarDatum).Value.(*scalar.Binary).Data())
	})

	t.Run("scalar list element struct field", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		dt := arrow.StructOf(arrow.Field{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "l", Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "x", Type: arrow.BinaryTypes.Binary, Nullable: true})), Nullable: true}), Nullable: true})
		input := scalarInput(t, mem, dt, `[{"s": {"l": [{"x": "aGVsbG8="}, {"x": "d29ybGQ="}]}}]`)
		sc := arrow.NewSchema(dt.Fields(), nil)

		st, err := exprs.ToSubstraitType(dt, false, extSet)
		require.NoError(t, err)
		e, err := expr.NewRootFieldRef(&expr.StructFieldRef{Field: 0, Child: &expr.StructFieldRef{Field: 0,
			Child: &expr.ListElementRef{Offset: 1, Child: &expr.StructFieldRef{Field: 0}}}},
			types.NewRecordTypeFromStruct(*st.(*types.StructType)))
		require.NoError(t, err)

		result := execute(t, mem, sc, e, compute.NewDatumWithoutOwning(input))
		input.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")
		assert.Equal(t, []byte("world"), result.(*compute.ScalarDatum).Value.(*scalar.Binary).Data())
		result.Release()
	})

	t.Run("scalar struct result", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		inner := arrow.StructOf(arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true})
		dt := arrow.StructOf(arrow.Field{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "t", Type: inner, Nullable: true}), Nullable: true})
		input := scalarInput(t, mem, dt, `[{"s": {"t": {"b": "aGVsbG8="}}}]`)

		result, err := exprs.GetReferencedValue(mem,
			&expr.StructFieldRef{Field: 0, Child: &expr.StructFieldRef{Field: 0}},
			compute.NewDatumWithoutOwning(input), extSet)
		require.NoError(t, err)
		input.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")
		b := result.(*compute.ScalarDatum).Value.(*scalar.Struct).Value[0]
		assert.Equal(t, []byte("hello"), b.(*scalar.Binary).Data())
		result.Release()
	})

	t.Run("array list element", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		lists, _, err := array.FromJSON(mem, arrow.ListOf(arrow.BinaryTypes.Binary),
			strings.NewReader(`[["YQ=="], ["Yg==", "Yw=="]]`))
		require.NoError(t, err)

		result, err := exprs.GetReferencedValue(mem, &expr.ListElementRef{Offset: 1},
			compute.NewDatumWithoutOwning(lists), extSet)
		require.NoError(t, err)
		lists.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")

		expected, _, err := array.FromJSON(mem, arrow.BinaryTypes.Binary, strings.NewReader(`["Yg==", "Yw=="]`))
		require.NoError(t, err)
		defer expected.Release()
		assert.Truef(t, array.Equal(expected, result.(*compute.ScalarDatum).Value.(*scalar.List).Value),
			"got %s", result)
		result.Release()
	})

	t.Run("GetScalar", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		dt := arrow.StructOf(arrow.Field{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true}), Nullable: true})
		input := scalarInput(t, mem, dt, `[{"s": {"b": "aGVsbG8="}}]`)

		result, err := exprs.GetScalar(&expr.StructFieldRef{Field: 0, Child: &expr.StructFieldRef{Field: 0}},
			input, mem, extSet)
		require.NoError(t, err)
		input.Release()
		require.NotZero(t, mem.CurrentAlloc(), "result no longer holds its buffers")
		assert.Equal(t, []byte("hello"), result.(*scalar.Binary).Data())
		result.(scalar.Releasable).Release()
	})
}

func TestNestedFieldRefSlicedStruct(t *testing.T) {
	inner := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	sc := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "t", Type: inner, Nullable: true}), Nullable: true}}, nil)
	const records = `[
		{"s": {"a": 1, "t": {"a": 10}}},
		{"s": {"a": 2, "t": {"a": 20}}},
		{"s": {"a": 3, "t": {"a": 30}}},
		{"s": {"a": 4, "t": {"a": 40}}}
	]`

	tests := []struct {
		name     string
		exprType arrow.DataType
		path     []any
		expected string
	}{
		{"field", arrow.PrimitiveTypes.Int32, []any{"s", "a"}, `[3, 4]`},
		{"nested field", arrow.PrimitiveTypes.Int32, []any{"s", "t", "a"}, `[30, 40]`},
		{"type mismatch", arrow.BinaryTypes.String, []any{"s", "t", "a"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(records))
			require.NoError(t, err)
			defer rec.Release()
			sliced := rec.NewSlice(2, 4)
			defer sliced.Release()

			exprSchema := sc
			if tt.expected == "" {
				exprSchema = arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.StructOf(
					arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					arrow.Field{Name: "t", Type: arrow.StructOf(
						arrow.Field{Name: "a", Type: tt.exprType, Nullable: true}), Nullable: true}), Nullable: true}}, nil)
			}
			bldr := exprs.NewExprBuilder(extSet)
			require.NoError(t, bldr.SetInputSchema(exprSchema))
			e, err := bldr.FieldRefList(tt.path...).BuildExpr()
			require.NoError(t, err)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			result, err := exprs.ExecuteScalarExpression(ctx, sc, e, compute.NewDatumWithoutOwning(sliced))
			if tt.expected == "" {
				assert.ErrorIs(t, err, arrow.ErrInvalid)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err)
			defer result.Release()

			expected, _, err := array.FromJSON(mem, tt.exprType, strings.NewReader(tt.expected))
			require.NoError(t, err)
			defer expected.Release()
			assertEqual(t, expected, result)
		})
	}
}

func TestNestedFieldRefNullStruct(t *testing.T) {
	t.Run("array", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		child, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3, null]`))
		require.NoError(t, err)
		defer child.Release()
		// the struct is null at index 1, where its child holds a valid 2
		validity := memory.NewResizableBuffer(mem)
		defer validity.Release()
		validity.Resize(1)
		validity.Bytes()[0] = 0b1101
		structs, err := array.NewStructArrayWithNulls([]arrow.Array{child}, []string{"a"}, validity, 1, 0)
		require.NoError(t, err)
		defer structs.Release()

		sc := arrow.NewSchema([]arrow.Field{{Name: "s", Type: structs.DataType(), Nullable: true}}, nil)
		rec := array.NewRecordBatch(sc, []arrow.Array{structs}, int64(structs.Len()))
		defer rec.Release()
		sliced := rec.NewSlice(1, 4)
		defer sliced.Release()

		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))
		e, err := bldr.FieldRefList("s", "a").BuildExpr()
		require.NoError(t, err)

		ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
		result, err := exprs.ExecuteScalarExpression(ctx, sc, e, compute.NewDatumWithoutOwning(sliced))
		require.NoError(t, err)
		defer result.Release()

		expected, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[null, 3, null]`))
		require.NoError(t, err)
		defer expected.Release()
		assertEqual(t, expected, result)
	})

	t.Run("scalar", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		innerType := arrow.StructOf(arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true})
		dt := arrow.StructOf(arrow.Field{Name: "s", Type: innerType, Nullable: true})
		// the struct is null, but its child holds a valid value
		inner := scalar.NewStructScalar([]scalar.Scalar{
			scalar.NewBinaryScalar(memory.NewBufferBytes([]byte("hello")), arrow.BinaryTypes.Binary)}, innerType)
		inner.Valid = false
		input := scalar.NewStructScalar([]scalar.Scalar{inner}, dt)
		sc := arrow.NewSchema(dt.Fields(), nil)

		bldr := exprs.NewExprBuilder(extSet)
		require.NoError(t, bldr.SetInputSchema(sc))
		e, err := bldr.FieldRefList("s", "b").BuildExpr()
		require.NoError(t, err)

		ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
		result, err := exprs.ExecuteScalarExpression(ctx, sc, e, compute.NewDatumWithoutOwning(input))
		require.NoError(t, err)
		defer result.Release()
		assert.False(t, result.(*compute.ScalarDatum).Value.IsValid())
	})

	t.Run("scalar list fields", func(t *testing.T) {
		for _, listType := range []arrow.DataType{
			arrow.ListOf(arrow.PrimitiveTypes.Int32),
			arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
		} {
			t.Run(listType.ID().String(), func(t *testing.T) {
				inputType := arrow.StructOf(arrow.Field{Name: "l", Type: listType, Nullable: true})
				input := scalar.MakeNullScalar(inputType)
				defer input.(scalar.Releasable).Release()

				result, err := exprs.GetReferencedValue(memory.DefaultAllocator,
					&expr.StructFieldRef{Field: 0, Child: &expr.ListElementRef{Offset: 0}},
					compute.NewDatumWithoutOwning(input), extSet)
				require.NoError(t, err)
				defer result.Release()
				assert.Equal(t, arrow.PrimitiveTypes.Int32, result.(*compute.ScalarDatum).Value.DataType())
				assert.False(t, result.(*compute.ScalarDatum).Value.IsValid())
			})
		}
	})

	t.Run("scalar map field", func(t *testing.T) {
		mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
		inputType := arrow.StructOf(arrow.Field{Name: "m", Type: mapType, Nullable: true})
		input := scalar.MakeNullScalar(inputType)
		defer input.(scalar.Releasable).Release()

		result, err := exprs.GetReferencedValue(memory.DefaultAllocator,
			&expr.StructFieldRef{Field: 0, Child: &expr.MapKeyRef{
				MapKey: expr.NewPrimitiveLiteral("key", false)}},
			compute.NewDatumWithoutOwning(input), extSet)
		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, arrow.PrimitiveTypes.Int32, result.(*compute.ScalarDatum).Value.DataType())
		assert.False(t, result.(*compute.ScalarDatum).Value.IsValid())
	})

	t.Run("unknown null count without bitmap", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		child, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3]`))
		require.NoError(t, err)
		defer child.Release()
		dt := arrow.StructOf(arrow.Field{Name: "a", Type: child.DataType(), Nullable: true})
		data := array.NewData(dt, child.Len(), []*memory.Buffer{nil}, []arrow.ArrayData{child.Data()}, array.UnknownNullCount, 0)
		defer data.Release()

		result, err := exprs.GetReferencedValue(mem, &expr.StructFieldRef{Field: 0},
			&compute.ArrayDatum{Value: data}, extSet)
		require.NoError(t, err)
		defer result.Release()
		assertEqual(t, child, result)
	})

	t.Run("field without validity bitmap", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		runEnds, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 3]`))
		require.NoError(t, err)
		defer runEnds.Release()
		values, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2]`))
		require.NoError(t, err)
		defer values.Release()
		child := array.NewRunEndEncodedArray(runEnds, values, 3, 0)
		defer child.Release()
		validity := memory.NewBufferBytes([]byte{0b101})
		defer validity.Release()
		parent, err := array.NewStructArrayWithNulls([]arrow.Array{child}, []string{"a"}, validity, 1, 0)
		require.NoError(t, err)
		defer parent.Release()

		result, err := exprs.GetReferencedValue(mem, &expr.StructFieldRef{Field: 0},
			compute.NewDatumWithoutOwning(parent), extSet)
		assert.ErrorIs(t, err, arrow.ErrNotImplemented)
		assert.Nil(t, result)
	})

	t.Run("null field without validity bitmap", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		child := array.NewNull(3)
		defer child.Release()
		validity := memory.NewBufferBytes([]byte{0b101})
		defer validity.Release()
		parent, err := array.NewStructArrayWithNulls([]arrow.Array{child}, []string{"n"}, validity, 1, 0)
		require.NoError(t, err)
		defer parent.Release()

		result, err := exprs.GetReferencedValue(mem, &expr.StructFieldRef{Field: 0},
			compute.NewDatumWithoutOwning(parent), extSet)
		require.NoError(t, err)
		defer result.Release()
		assertEqual(t, child, result)
	})
}

func TestDecimalFilterLarge(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name string
		n    int
	}{
		{
			name: "arrow.DECIMAL128 - number of records < 33 ok",
			n:    32,
		},
		{
			name: "arrow.DECIMAL128 - number of records >= 33 panic",
			n:    33,
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			rq := require.New(t)

			typ := &arrow.Decimal128Type{Precision: 3, Scale: 1}
			field := arrow.Field{
				Name:     "col",
				Type:     typ,
				Nullable: true,
			}
			schema := arrow.NewSchema([]arrow.Field{field}, nil)

			db := array.NewDecimal128Builder(memory.DefaultAllocator, typ)
			defer db.Release()

			for i := 0; i < tc.n; i++ {
				d, err := decimal.Decimal128FromFloat(float64(i), 3, 1)
				rq.NoError(err, "Failed to create Decimal128 value")

				db.Append(d)
			}

			rec := array.NewRecordBatch(schema, []arrow.Array{db.NewArray()}, int64(tc.n))

			extSet := exprs.GetExtensionIDSet(ctx)
			builder := exprs.NewExprBuilder(extSet)

			err := builder.SetInputSchema(schema)
			rq.NoError(err, "Failed to set input schema")

			v, p, s, err := expr.DecimalStringToBytes("10.0")
			rq.NoError(err, "Failed to convert decimal string to bytes")

			lit, err := expr.NewLiteral(&types.Decimal{
				Value:     v[:16],
				Precision: p,
				Scale:     s,
			}, true)
			rq.NoError(err, "Failed to create Decimal128 literal")

			t.Run("array_scalar", func(t *testing.T) {
				b, err := builder.CallScalar("less", nil,
					builder.FieldRef("col"),
					builder.Literal(lit),
				)

				rq.NoError(err, "Failed to call scalar")

				e, err := b.BuildExpr()
				rq.NoError(err, "Failed to build expression")

				ctx = exprs.WithExtensionIDSet(ctx, extSet)

				dr := compute.NewDatum(rec)
				defer dr.Release()

				_, err = exprs.ExecuteScalarExpression(ctx, schema, e, dr)
				rq.NoError(err, "Failed to execute scalar expression")
			})

			t.Run("scalar_array", func(t *testing.T) {
				b, err := builder.CallScalar("less", nil,
					builder.Literal(lit),
					builder.FieldRef("col"),
				)

				rq.NoError(err, "Failed to call scalar")

				e, err := b.BuildExpr()
				rq.NoError(err, "Failed to build expression")

				ctx = exprs.WithExtensionIDSet(ctx, extSet)

				dr := compute.NewDatum(rec)
				defer dr.Release()

				_, err = exprs.ExecuteScalarExpression(ctx, schema, e, dr)
				rq.NoError(err, "Failed to execute scalar expression")
			})
		})
	}
}

func TestStartsWithExpr(t *testing.T) {
	varchar, _, err := exprs.FromSubstraitType(&types.VarCharType{Length: -1}, extSet)
	require.NoError(t, err)
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "v", Type: varchar, Nullable: true},
	}, nil)

	bldr := exprs.NewExprBuilder(extSet)
	require.NoError(t, bldr.SetInputSchema(sc))
	ab := bldr.Literal(expr.NewPrimitiveLiteral("ab", false))
	caseSensitivity := func(pref string) []*types.FunctionOption {
		return []*types.FunctionOption{{Name: "case_sensitivity", Preference: []string{pref}}}
	}

	tests := []struct {
		name     string
		ex       exprs.Builder
		expected string
	}{
		{"large_string", bldr.MustCallScalar("starts_with", nil, bldr.FieldRef("s"), ab), `[true, null, false, false]`},
		{"varchar", bldr.MustCallScalar("starts_with", nil, bldr.FieldRef("v"),
			bldr.WrapLiteral(expr.NewLiteral(&types.VarChar{Value: "ab", Length: 2}, false))),
			`[true, false, null, true]`},
		{"non-literal pattern", bldr.MustCallScalar("starts_with", nil, bldr.FieldRef("s"),
			bldr.Must(bldr.Cast(ab, arrow.BinaryTypes.String))), ""},
		{"null pattern", bldr.MustCallScalar("starts_with", nil, bldr.FieldRef("s"),
			bldr.Literal(expr.NewNullLiteral(&types.StringType{}))), ""},
		{"case insensitive", bldr.MustCallScalar("starts_with", caseSensitivity("CASE_INSENSITIVE"), bldr.FieldRef("s"),
			bldr.Literal(expr.NewPrimitiveLiteral("AB", false))), `[true, null, false, false]`},
		{"case insensitive ascii", bldr.MustCallScalar("starts_with", caseSensitivity("CASE_INSENSITIVE_ASCII"),
			bldr.FieldRef("s"), ab), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)

			ex, err := tt.ex.BuildExpr()
			require.NoError(t, err)

			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(`[
				{"s": "abc", "v": "ab"},
				{"s": null, "v": "b"},
				{"s": "xab", "v": null},
				{"s": "a", "v": "abab"}
			]`))
			require.NoError(t, err)
			defer rec.Release()

			result, err := exprs.ExecuteScalarExpression(ctx, sc, ex, compute.NewDatumWithoutOwning(rec))
			if tt.expected == "" {
				assert.ErrorIs(t, err, arrow.ErrNotImplemented)
				return
			}
			require.NoError(t, err)
			defer result.Release()

			expected, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(tt.expected))
			require.NoError(t, err)
			defer expected.Release()
			assertEqual(t, expected, result)
		})
	}
}
