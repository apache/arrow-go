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

package exprs

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	"github.com/substrait-io/substrait-go/v8/extensions"
	"github.com/substrait-io/substrait-go/v8/types"
)

var (
	boringArrowSchema = arrow.NewSchema([]arrow.Field{
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
)

func TestMakeExecBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const numRows = 3
	var (
		ctx         = compute.WithAllocator(context.Background(), mem)
		i32, _, _   = array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3]`))
		f32, _, _   = array.FromJSON(mem, arrow.PrimitiveTypes.Float32, strings.NewReader(`[1.5, 2.25, 3.125]`))
		empty, _, _ = array.RecordFromJSON(mem, boringArrowSchema, strings.NewReader(`[]`))
	)
	defer i32.Release()
	defer f32.Release()

	getField := func(n string) arrow.Field {
		f, _ := boringArrowSchema.FieldsByName(n)
		return f[0]
	}

	tests := []struct {
		name  string
		batch arrow.RecordBatch
	}{
		{"empty", empty},
		{"subset", array.NewRecordBatch(arrow.NewSchema([]arrow.Field{getField("i32"), getField("f32")}, nil),
			[]arrow.Array{i32, f32}, numRows)},
		{"flipped subset", array.NewRecordBatch(arrow.NewSchema([]arrow.Field{getField("f32"), getField("i32")}, nil),
			[]arrow.Array{f32, i32}, numRows)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.batch.Release()
			batch, err := makeExecBatch(ctx, boringArrowSchema, compute.NewDatumWithoutOwning(tt.batch))
			require.NoError(t, err)
			require.Equal(t, tt.batch.NumRows(), batch.Len)

			defer func() {
				for _, v := range batch.Values {
					v.Release()
				}
			}()

			for i, field := range boringArrowSchema.Fields() {
				typ := batch.Values[i].(compute.ArrayLikeDatum).Type()
				assert.Truef(t, arrow.TypeEqual(typ, field.Type),
					"expected: %s\ngot: %s", field.Type, typ)

				idxes := tt.batch.Schema().FieldIndices(field.Name)
				if batch.Values[i].Kind() == compute.KindScalar {
					assert.False(t, batch.Values[i].(*compute.ScalarDatum).Value.IsValid(),
						"null placeholder should be injected")
					assert.Len(t, idxes, 0, "should only happen when column isn't found")
				} else {
					col := tt.batch.Column(idxes[0])
					val := batch.Values[i].(*compute.ArrayDatum).MakeArray()
					defer val.Release()

					assert.Truef(t, array.Equal(col, val), "expected: %s\ngot: %s", col, val)
				}
			}
		})
	}
}

func TestMakeExecBatchReleasesNullScalars(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: dictType, Nullable: true}}, nil)

	tests := []struct {
		name    string
		partial compute.Datum
	}{
		{"missing record field", compute.NewDatumWithoutOwning(
			array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1))},
		{"missing struct field", compute.NewDatumWithoutOwning(
			scalar.NewStructScalar(nil, arrow.StructOf()))},
		{"null struct field cast", compute.NewDatumWithoutOwning(scalar.NewStructScalar(
			[]scalar.Scalar{scalar.MakeNullScalar(arrow.BinaryTypes.String)},
			arrow.StructOf(arrow.Field{Name: "d", Type: arrow.BinaryTypes.String, Nullable: true})))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := makeExecBatch(context.Background(), schema, tt.partial)
			require.NoError(t, err)
			dict := batch.Values[0].(*compute.ScalarDatum).Value.(*scalar.Dictionary).Value.Dict.Data()
			require.NotEmpty(t, dict.Buffers())
			batch.Values[0].Release()
			assert.Nil(t, dict.Buffers())
		})
	}
}

func TestMakeExecBatchErrorReleases(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	binField := arrow.Field{Name: "a", Type: arrow.BinaryTypes.Binary, Nullable: true}
	strField := arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}
	structType := arrow.StructOf(binField, strField)
	const row = `[{"a": "eA==", "b": "x"}]`

	tests := []struct {
		name    string
		partial func(mem memory.Allocator) compute.Datum
	}{
		{"record cast", func(mem memory.Allocator) compute.Datum {
			rec, _, err := array.RecordFromJSON(mem, arrow.NewSchema(structType.Fields(), nil), strings.NewReader(row))
			require.NoError(t, err)
			defer rec.Release()
			return compute.NewDatum(rec)
		}},
		{"ambiguous record field", func(mem memory.Allocator) compute.Datum {
			a, _, err := array.FromJSON(mem, binField.Type, strings.NewReader(`["eA=="]`))
			require.NoError(t, err)
			defer a.Release()
			bField := arrow.Field{Name: "b", Type: binField.Type, Nullable: true}
			rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{binField, bField, bField}, nil),
				[]arrow.Array{a, a, a}, 1)
			defer rec.Release()
			return compute.NewDatum(rec)
		}},
		{"struct array cast", func(mem memory.Allocator) compute.Datum {
			arr, _, err := array.FromJSON(mem, structType, strings.NewReader(row))
			require.NoError(t, err)
			defer arr.Release()
			return compute.NewDatum(arr)
		}},
		{"struct scalar cast", func(mem memory.Allocator) compute.Datum {
			arr, _, err := array.FromJSON(mem, structType, strings.NewReader(row))
			require.NoError(t, err)
			defer arr.Release()
			sc, err := scalar.GetScalar(arr, 0)
			require.NoError(t, err)
			return &compute.ScalarDatum{Value: sc}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			partial := tt.partial(mem)
			defer partial.Release()

			batch, err := makeExecBatch(compute.WithAllocator(context.Background(), mem), schema, partial)
			require.Error(t, err)
			assert.Empty(t, batch.Values)
		})
	}
}

func TestLiteralToDatumIntervalYearToMonth(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	extSet := NewExtensionSetDefault(
		expr.NewEmptyExtensionRegistry(extensions.GetDefaultCollectionWithNoError()))

	const (
		years  int32 = 3
		months int32 = 7
	)

	protoLitType := types.NewIntervalYearToMonthType()
	protoLit := &expr.ProtoLiteral{
		Value: &types.IntervalYearToMonth{Years: years, Months: months},
		Type:  &protoLitType,
	}
	expected, err := literalToDatum(mem, protoLit, extSet)
	require.NoError(t, err, "ProtoLiteral baseline failed")
	defer expected.Release()

	cases := []struct {
		name string
		lit  expr.Literal
	}{
		{"value", expr.IntervalYearToMonthLiteral{Years: years, Months: months}},
		{"pointer", &expr.IntervalYearToMonthLiteral{Years: years, Months: months}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := literalToDatum(mem, tc.lit, extSet)
			require.NoError(t, err)
			defer got.Release()
			assert.Truef(t, got.Equals(expected),
				"IntervalYearToMonthLiteral (%s) datum did not match ProtoLiteral baseline\nexpected: %s\ngot: %s",
				tc.name, expected, got)
		})
	}
}

func TestLiteralToDatumNullUserDefinedRelease(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	reg := NewExtensionIDRegistry()
	require.NoError(t, reg.RegisterType(extensions.ID{URN: "extension:test:dictionary", Name: "dict"}, dictType))
	extSet := NewExtensionSet(expr.NewEmptyExtensionRegistry(extensions.GetDefaultCollectionWithNoError()), reg)
	_, anchor, ok := extSet.EncodeType(dictType)
	require.True(t, ok)

	lit := expr.NewNullLiteral(&types.UserDefinedType{TypeReference: anchor})
	got, err := literalToDatum(memory.DefaultAllocator, lit, extSet)
	require.NoError(t, err)
	dict := got.(*compute.ScalarDatum).Value.(*scalar.Dictionary).Value.Dict.Data()
	require.NotEmpty(t, dict.Buffers())
	got.Release()
	assert.Nil(t, dict.Buffers())
}
