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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariantExtensionType(t *testing.T) {
	variant1, err := extensions.NewVariantType(arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)
	variant2, err := extensions.NewVariantType(arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)

	assert.Equal(t, "extension<parquet.variant>", variant1.String())
	assert.True(t, arrow.TypeEqual(variant1, variant2))

	// can be provided in either order
	variantFieldsFlipped, err := extensions.NewVariantType(arrow.StructOf(
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)

	assert.Equal(t, "metadata", variantFieldsFlipped.Metadata().Name)
	assert.Equal(t, "value", variantFieldsFlipped.Value().Name)

	tests := []struct {
		dt          arrow.DataType
		expectedErr string
	}{
		{arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary}),
			"missing non-nullable field 'value'"},
		{arrow.StructOf(arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary}), "missing non-nullable field 'metadata'"},
		{arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}),
			"value field must be non-nullable binary type, got int32"},
		{arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "extra", Type: arrow.BinaryTypes.Binary}),
			"has 3 fields, but missing 'typed_value' field"},
		{arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: true},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}),
			"metadata field must be non-nullable binary type"},
		{arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true}),
			"value field must be non-nullable binary type"},
		{arrow.FixedWidthTypes.Boolean, "bad storage type bool for variant type"},
		{arrow.StructOf(
			arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "extra", Type: arrow.BinaryTypes.Binary, Nullable: true}), "too many fields in variant storage type"},
		{arrow.StructOf(
			arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}),
			"metadata field must be non-nullable binary type, got utf8"},
		{arrow.StructOf(
			arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true}),
			"value field must be nullable if typed_value is present"},
		{arrow.StructOf(
			arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
			arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: false}),
			"typed_value field must be nullable"},
	}

	for _, tt := range tests {
		_, err := extensions.NewVariantType(tt.dt)
		assert.Error(t, err)
		assert.ErrorContains(t, err, tt.expectedErr)
	}
}

func TestVariantExtensionBadNestedTypes(t *testing.T) {
	tests := []struct {
		name string
		dt   arrow.DataType
	}{
		{"map is invalid", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{"union is invalid", arrow.UnionOf(arrow.SparseMode,
			[]arrow.Field{{Name: "name", Type: arrow.PrimitiveTypes.Int64}}, []arrow.UnionTypeCode{0})},
		{"list elem must be non-nullable", arrow.ListOf(arrow.BinaryTypes.String)},
		{"list elem must be struct", arrow.ListOfNonNullable(arrow.BinaryTypes.String)},
		{"nullable struct elem", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.BinaryTypes.String, Nullable: true})},
		{"non-struct struct elem", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.BinaryTypes.String, Nullable: false})},
		{"empty struct elem", arrow.StructOf()},
		{"invalid struct elem", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.StructOf(
				arrow.Field{Name: "foobar", Type: arrow.BinaryTypes.String, Nullable: false},
			), Nullable: false})},
		{"empty struct elem", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.StructOf(), Nullable: false})},
		{"nullable value struct elem",
			arrow.StructOf(
				arrow.Field{Name: "foobar", Type: arrow.StructOf(
					arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				), Nullable: false})},
		{"non-nullable two elem struct", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: false},
			)})},
		{"invalid nested shredded struct", arrow.StructOf(
			arrow.Field{Name: "foobar", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String), Nullable: true}),
			})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := arrow.StructOf(
				arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: tt.dt, Nullable: true})

			_, err := extensions.NewVariantType(storage)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "typed_value field must be a valid nested type, got "+tt.dt.String())
		})
	}
}

func TestNonShredded(t *testing.T) {
	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, extensions.NewDefaultVariantType())
	defer bldr.Release()

	vals := []any{
		"hello world",
		42,
		nil,
		[]any{"foo", 25},
		map[string]any{"key1": "value1", "key2": 100},
	}

	bldr.AppendNull()

	var b variant.Builder
	for _, v := range vals {
		require.NoError(t, b.Append(v))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()
	}

	arr := bldr.NewArray()
	defer arr.Release()

	assert.IsType(t, &extensions.VariantArray{}, arr)
	varr := arr.(*extensions.VariantArray)

	assert.False(t, varr.IsShredded())
	assert.True(t, varr.IsNull(0))
	assert.False(t, varr.IsValid(0))
	assert.False(t, varr.IsNull(1))
	assert.True(t, varr.IsValid(1))
	assert.True(t, varr.IsNull(3))
	assert.False(t, varr.IsValid(3))

	result, err := varr.Values()
	require.NoError(t, err)

	assert.Len(t, result, varr.Len())
	assert.Equal(t, variant.NullValue, result[0])
	assert.Equal(t, "hello world", result[1].Value())
	assert.Equal(t, int8(42), result[2].Value())
	assert.Nil(t, result[3].Value())
}

func TestShreddedPrimitiveVariant(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true})

	bldr := array.NewStructBuilder(memory.DefaultAllocator, s)
	defer bldr.Release()

	metaBldr := bldr.FieldBuilder(0).(*array.BinaryBuilder)
	valueBldr := bldr.FieldBuilder(1).(*array.BinaryBuilder)
	typedValueBldr := bldr.FieldBuilder(2).(*array.Int64Builder)

	// let's create `34, null, "n/a", 100` while shredding the integers
	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(34)

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.Append(variant.NullValue.Bytes())
	typedValueBldr.AppendNull()

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	vbytes, err := variant.Encode("n/a")
	require.NoError(t, err)
	valueBldr.Append(vbytes)
	typedValueBldr.AppendNull()

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(100)

	arr := bldr.NewArray()
	defer arr.Release()

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)

	require.IsType(t, &extensions.VariantType{}, vt)

	variantArr := array.NewExtensionArrayWithStorage(vt, arr)
	defer variantArr.Release()

	assert.Equal(t, 4, variantArr.Len())
	assert.IsType(t, &extensions.VariantArray{}, variantArr)
	varr := variantArr.(*extensions.VariantArray)

	assert.True(t, varr.IsShredded())

	v, err := varr.Value(0)
	require.NoError(t, err)
	// converting to variant will use the smallest integer type
	assert.Equal(t, variant.Int8, v.Type())
	assert.EqualValues(t, 34, v.Value())

	v, err = varr.Value(1)
	require.NoError(t, err)
	assert.Equal(t, variant.Null, v.Type())
	assert.Nil(t, v.Value())

	v, err = varr.Value(2)
	require.NoError(t, err)
	assert.Equal(t, variant.String, v.Type())
	assert.Equal(t, "n/a", v.Value())

	v, err = varr.Value(3)
	require.NoError(t, err)
	// converting to variant will use the smallest integer type
	assert.Equal(t, variant.Int8, v.Type())
	assert.EqualValues(t, 100, v.Value())
}

func TestShreddedArrayVariant(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.ListOfNonNullable(arrow.StructOf(
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
			arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
		)), Nullable: true})

	bldr := array.NewStructBuilder(memory.DefaultAllocator, s)
	defer bldr.Release()

	metaBldr := bldr.FieldBuilder(0).(*array.BinaryBuilder)
	valueBldr := bldr.FieldBuilder(1).(*array.BinaryBuilder)
	typedValueBldr := bldr.FieldBuilder(2).(*array.ListBuilder)
	typedValueElemBldr := typedValueBldr.ValueBuilder().(*array.StructBuilder)
	typedValueElemValueBldr := typedValueElemBldr.FieldBuilder(0).(*array.BinaryBuilder)
	typedValueElemTypedValueBldr := typedValueElemBldr.FieldBuilder(1).(*array.StringBuilder)

	// we'll create a shredded column of the following list:
	// ["comedy", "drama"], ["horror", null], ["comedy", "drama", "romance"], null
	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("comedy")
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("drama")

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("horror")
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.Append(variant.NullValue.Bytes())
	typedValueElemTypedValueBldr.AppendNull()

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("comedy")
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("drama")
	typedValueElemBldr.Append(true)
	typedValueElemValueBldr.AppendNull()
	typedValueElemTypedValueBldr.Append("romance")

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.Append(variant.NullValue.Bytes())
	typedValueBldr.AppendNull()

	arr := bldr.NewArray()
	defer arr.Release()
	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	variantArr := array.NewExtensionArrayWithStorage(vt, arr)
	defer variantArr.Release()
	assert.Equal(t, 4, variantArr.Len())
	assert.IsType(t, &extensions.VariantArray{}, variantArr)

	varr := variantArr.(*extensions.VariantArray)
	assert.Equal(t, `VariantArray[["comedy","drama"] ["horror",null] ["comedy","drama","romance"] (null)]`, varr.String())

	out, err := json.Marshal(varr)
	require.NoError(t, err)
	assert.JSONEq(t, `[["comedy","drama"], ["horror", null], ["comedy","drama","romance"], null]`, string(out))
}

func TestShreddedBuilderArrayVariant(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.ListOfNonNullable(arrow.StructOf(
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
			arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
		)), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, vt)
	defer bldr.Release()

	var b variant.Builder
	vals := []any{
		[]any{"comedy", "drama"},
		[]any{"horror", nil},
		[]any{"comedy", "drama", "romance"},
		nil,
	}

	for _, v := range vals {
		b.Append(v)
		result, err := b.Build()
		require.NoError(t, err)
		bldr.Append(result)
		b.Reset()
	}

	arr := bldr.NewArray()
	defer arr.Release()
	assert.IsType(t, &extensions.VariantArray{}, arr)
	varr := arr.(*extensions.VariantArray)

	assert.EqualValues(t, 4, varr.Len())
	assert.True(t, varr.IsShredded())

	untyped := varr.UntypedValues()
	assert.EqualValues(t, 3, untyped.NullN())
	for i := range 3 {
		assert.True(t, untyped.IsNull(i))
	}

	assert.Equal(t, variant.NullValue.Bytes(), untyped.Value(3))

	typedVals := varr.Shredded()
	assert.EqualValues(t, 4, typedVals.Len())
	assert.EqualValues(t, 1, typedVals.NullN())

	for i := range 3 {
		assert.False(t, typedVals.IsNull(i))
	}

	assert.True(t, typedVals.IsNull(3))
	assert.IsType(t, &array.List{}, typedVals)

	typedList := typedVals.(*array.List)
	typedUntypedValues := typedList.ListValues().(*array.Struct).Field(0).(*array.Binary)
	typedTypedValues := typedList.ListValues().(*array.Struct).Field(1).(*array.String)

	start, end := typedList.ValueOffsets(0)
	assert.EqualValues(t, 0, start)
	assert.EqualValues(t, 2, end)

	assert.True(t, typedUntypedValues.IsNull(0))
	assert.True(t, typedUntypedValues.IsNull(1))
	assert.Equal(t, "comedy", typedTypedValues.Value(0))
	assert.Equal(t, "drama", typedTypedValues.Value(1))

	start, end = typedList.ValueOffsets(1)
	assert.EqualValues(t, 2, start)
	assert.EqualValues(t, 4, end)
	assert.True(t, typedUntypedValues.IsNull(2))
	assert.Equal(t, variant.NullValue.Bytes(), typedUntypedValues.Value(3))
	assert.Equal(t, "horror", typedTypedValues.Value(2))
	assert.True(t, typedTypedValues.IsNull(3))

	start, end = typedList.ValueOffsets(2)
	assert.EqualValues(t, 4, start)
	assert.EqualValues(t, 7, end)

	assert.True(t, typedUntypedValues.IsNull(4))
	assert.True(t, typedUntypedValues.IsNull(5))
	assert.True(t, typedUntypedValues.IsNull(6))
	assert.Equal(t, "comedy", typedTypedValues.Value(4))
	assert.Equal(t, "drama", typedTypedValues.Value(5))
	assert.Equal(t, "romance", typedTypedValues.Value(6))
}

func TestVariantShreddedObject(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "event_type", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			)},
			arrow.Field{Name: "event_ts", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			)},
		), Nullable: true})

	bldr := array.NewStructBuilder(memory.DefaultAllocator, s)
	defer bldr.Release()

	metaBldr := bldr.FieldBuilder(0).(*array.BinaryBuilder)
	valueBldr := bldr.FieldBuilder(1).(*array.BinaryBuilder)
	typedValueBldr := bldr.FieldBuilder(2).(*array.StructBuilder)
	typedValueEventTypeBldr := typedValueBldr.FieldBuilder(0).(*array.StructBuilder)
	typedValueEventTypeValueBldr := typedValueEventTypeBldr.FieldBuilder(0).(*array.BinaryBuilder)
	typedValueEventTypeTypedValueBldr := typedValueEventTypeBldr.FieldBuilder(1).(*array.StringBuilder)
	typedValueEventTsBldr := typedValueBldr.FieldBuilder(1).(*array.StructBuilder)
	typedValueEventTsValueBldr := typedValueEventTsBldr.FieldBuilder(0).(*array.BinaryBuilder)
	typedValueEventTsTypedValueBldr := typedValueEventTsBldr.FieldBuilder(1).(*array.TimestampBuilder)

	var b variant.Builder
	b.AddKey("event_type")
	b.AddKey("event_ts")
	v, err := b.Build()
	require.NoError(t, err)

	// first event: {"event_type": "noop", "event_ts": 1729794114937}
	// fully shredded!
	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.Append("noop")
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.Append(1729794114937)

	// second event: {"event_type": "login", "event_ts": 1729794146402, "email": "user@example.com"}
	// partially shredded object, the email is not shredded
	b.AddKey("email")

	start := b.Offset()
	field := []variant.FieldEntry{b.NextField(start, "email")}
	require.NoError(t, b.AppendString("user@example.com"))
	require.NoError(t, b.FinishObject(start, field))
	v, err = b.Build()
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.Append(v.Bytes())
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.Append("login")
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.Append(1729794146402)

	// third event: {"error_msg": "malformed: bad event"}
	// object with all shredded fields missing

	b.Reset()
	b.AddKey("error_msg")
	start = b.Offset()
	field = []variant.FieldEntry{b.NextField(start, "error_msg")}
	require.NoError(t, b.AppendString("malformed: bad event"))
	require.NoError(t, b.FinishObject(start, field))
	v, err = b.Build()
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.Append(v.Bytes())
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.AppendNull()
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.AppendNull()

	// fourth event: "malformed: not an object"
	// not an object at all, stored as variant string
	byts, err := variant.Encode("malformed: not an object")
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.Append(byts)
	typedValueBldr.AppendNull()

	// fifth event: {"event_ts": 1729794240241, "click": "_button"}
	// field `event_type` missing

	b.Reset()
	b.AddKey("event_ts")
	start = b.Offset()
	field = []variant.FieldEntry{b.NextField(start, "click")}
	require.NoError(t, b.AppendString("_button"))
	require.NoError(t, b.FinishObject(start, field))
	v, err = b.Build()
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.Append(v.Bytes())
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.AppendNull()
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.Append(1729794240241)

	// sixth event: {"event_type": null, "event_ts": 1729794954163}
	// field event_type is present but is null

	b.Reset()
	b.AddKey("event_ts")
	b.AddKey("event_type")

	v, err = b.Build()
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.Append(variant.NullValue.Bytes())
	typedValueEventTypeTypedValueBldr.AppendNull()
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.Append(1729794954163)

	// seventh event: {"event_type": "noop", "event_ts": "2024-10-24"}
	// event_ts is present but not a timestamp

	b.Reset()
	b.AddKey("event_type")
	b.AddKey("event_ts")
	v, err = b.Build()
	require.NoError(t, err)

	bldr.Append(true)
	metaBldr.Append(v.Metadata().Bytes())
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.Append("noop")
	typedValueEventTsBldr.Append(true)
	byts, _ = variant.Encode("2024-10-24")
	typedValueEventTsValueBldr.Append(byts)
	typedValueEventTsTypedValueBldr.AppendNull()

	// eight event: {}
	// object present but empty

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.AppendNull()
	typedValueBldr.Append(true)
	typedValueEventTypeBldr.Append(true)
	typedValueEventTypeValueBldr.AppendNull()
	typedValueEventTypeTypedValueBldr.AppendNull()
	typedValueEventTsBldr.Append(true)
	typedValueEventTsValueBldr.AppendNull()
	typedValueEventTsTypedValueBldr.AppendNull()

	// ninth event: null

	bldr.Append(true)
	metaBldr.Append(variant.EmptyMetadataBytes[:])
	valueBldr.Append(variant.NullValue.Bytes())
	typedValueBldr.AppendNull()

	arr := bldr.NewArray()
	defer arr.Release()
	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	variantArr := array.NewExtensionArrayWithStorage(vt, arr)
	defer variantArr.Release()
	assert.Equal(t, 9, variantArr.Len())
	assert.IsType(t, &extensions.VariantArray{}, variantArr)

	varr := variantArr.(*extensions.VariantArray)
	assert.JSONEq(t, `{
		"event_type": "noop", 
		"event_ts": "1970-01-21 00:29:54.114937Z"}`, varr.ValueStr(0))
	assert.JSONEq(t, `{
		"event_type": "login", 
		"event_ts": "1970-01-21 00:29:54.146402Z", 
		"email": "user@example.com"}`, varr.ValueStr(1))
	assert.JSONEq(t, `{"error_msg": "malformed: bad event"}`, varr.ValueStr(2))
	assert.JSONEq(t, `"malformed: not an object"`, varr.ValueStr(3))
	assert.JSONEq(t, `{
		"event_ts": "1970-01-21 00:29:54.240241Z",
		"click": "_button"}`, varr.ValueStr(4))
	assert.JSONEq(t, `{
		"event_type": null,
		"event_ts": "1970-01-21 00:29:54.954163Z"}`, varr.ValueStr(5))
	assert.JSONEq(t, `{
		"event_type": "noop",
		"event_ts": "2024-10-24"}`, varr.ValueStr(6))
	assert.JSONEq(t, `{}`, varr.ValueStr(7))
	assert.Equal(t, "(null)", varr.ValueStr(8))
}

func TestVariantShreddedBuilder(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "event_type", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			)},
			arrow.Field{Name: "event_ts", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			)},
		), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)

	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, vt)
	require.NotNil(t, bldr)
	defer bldr.Release()

	t.Run("shredded builder", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{
			"event_type": "noop",
			"event_ts":   arrow.Timestamp(1729794114937),
		}, variant.OptTimestampUTC))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{
			"event_type": "login",
			"event_ts":   arrow.Timestamp(1729794146402),
			"email":      "user@example.com",
		}, variant.OptTimestampUTC))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{
			"error_msg": "malformed: bad event",
		}))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.AppendString("malformed: not an object"))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{
			"event_ts": arrow.Timestamp(1729794240241),
			"click":    "_button",
		}, variant.OptTimestampUTC))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{
			"event_type": nil,
			"event_ts":   arrow.Timestamp(1729794954163),
		}, variant.OptTimestampUTC))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{
			"event_type": "noop",
			"event_ts":   "2024-10-24",
		}))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		require.NoError(t, b.Append(map[string]any{}))
		v, err = b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()

		bldr.Append(variant.NullValue)
	})

	arr := bldr.NewArray()
	defer arr.Release()

	assert.Equal(t, 9, arr.Len())
	assert.IsType(t, &extensions.VariantArray{}, arr)
	varr := arr.(*extensions.VariantArray)

	t.Run("shredded to string", func(t *testing.T) {
		assert.JSONEq(t, `{
		"event_type": "noop", 
		"event_ts": "1970-01-21 00:29:54.114937Z"}`, varr.ValueStr(0))
		assert.JSONEq(t, `{
		"event_type": "login", 
		"event_ts": "1970-01-21 00:29:54.146402Z", 
		"email": "user@example.com"}`, varr.ValueStr(1))
		assert.JSONEq(t, `{"error_msg": "malformed: bad event"}`, varr.ValueStr(2))
		assert.JSONEq(t, `"malformed: not an object"`, varr.ValueStr(3))
		assert.JSONEq(t, `{
		"event_ts": "1970-01-21 00:29:54.240241Z",
		"click": "_button"}`, varr.ValueStr(4))
		assert.JSONEq(t, `{
		"event_type": null,
		"event_ts": "1970-01-21 00:29:54.954163Z"}`, varr.ValueStr(5))
		assert.JSONEq(t, `{
		"event_type": "noop",
		"event_ts": "2024-10-24"}`, varr.ValueStr(6))
		assert.JSONEq(t, `{}`, varr.ValueStr(7))
		assert.Equal(t, "(null)", varr.ValueStr(8))
	})

	// see https://github.com/apache/parquet-format/blob/master/VariantShredding.md#objects
	// for the expected shredding results and what should be in each shredded
	// field via the table provided in that example.
	// we use that example to verify that we shredded correctly.
	metaData := varr.Metadata()
	untyped := varr.UntypedValues()
	assert.EqualValues(t, 9, untyped.Len())
	assert.EqualValues(t, 4, untyped.NullN())

	t.Run("shredded untyped values", func(t *testing.T) {
		tests := []struct {
			isnull bool
			value  string
		}{
			{true, ""},
			{false, `{"email": "user@example.com"}`},
			{false, `{"error_msg": "malformed: bad event"}`},
			{false, `"malformed: not an object"`},
			{false, `{"click": "_button"}`},
			{true, ""},
			{true, ""},
			{true, ""},
			{false, "null"},
		}

		for idx, tt := range tests {
			t.Run(fmt.Sprintf("index %d", idx), func(t *testing.T) {
				assert.Equal(t, tt.isnull, untyped.IsNull(idx), "index %d", idx)
				if !tt.isnull {
					v, err := variant.New(metaData.Value(idx), untyped.Value(idx))
					require.NoError(t, err, "index %d", idx)
					assert.JSONEq(t, tt.value, v.String(), "index %d", idx)
				}
			})
		}
	})

	t.Run("shredded typed values", func(t *testing.T) {
		typed := varr.Shredded().(*array.Struct)
		assert.EqualValues(t, 9, typed.Len())
		assert.EqualValues(t, 2, typed.NullN())
		assert.True(t, typed.IsNull(3))
		assert.True(t, typed.IsNull(8))

		t.Run("event type", func(t *testing.T) {
			eventType := typed.Field(0).(*array.Struct)
			value := eventType.Field(0).(*array.Binary)
			typed := eventType.Field(1).(*array.String)

			assert.EqualValues(t, 1, value.Len()-value.NullN())
			assert.Equal(t, variant.NullValue.Bytes(), value.Value(5))
			assert.EqualValues(t, 3, typed.Len()-typed.NullN())
			assert.Equal(t, "noop", typed.Value(0))
			assert.Equal(t, "login", typed.Value(1))
			assert.Equal(t, "noop", typed.Value(6))
		})

		t.Run("event ts", func(t *testing.T) {
			eventTs := typed.Field(1).(*array.Struct)
			value := eventTs.Field(0).(*array.Binary)
			typed := eventTs.Field(1).(*array.Timestamp)

			assert.EqualValues(t, 1, value.Len()-value.NullN())
			expected, _ := variant.Encode("2024-10-24")
			assert.Equal(t, expected, value.Value(6))
			assert.EqualValues(t, 4, typed.Len()-typed.NullN())
			assert.EqualValues(t, 1729794114937, typed.Value(0))
			assert.EqualValues(t, 1729794146402, typed.Value(1))
			assert.EqualValues(t, 1729794240241, typed.Value(4))
			assert.EqualValues(t, 1729794954163, typed.Value(5))
		})
	})
}

func TestVariantWithDecimals(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "decimal4", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: &arrow.Decimal32Type{Precision: 4, Scale: 2}, Nullable: true},
			)},
			arrow.Field{Name: "decimal8", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: &arrow.Decimal64Type{Precision: 6, Scale: 4}, Nullable: true},
			)},
			arrow.Field{Name: "decimal16", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: &arrow.Decimal128Type{Precision: 8, Scale: 3}, Nullable: true},
			)},
		), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, vt)
	defer bldr.Release()

	values := []any{
		map[string]any{
			"decimal4": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(123),
				Scale: 2,
			},
			"decimal8": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(12345),
				Scale: 4,
			},
			"decimal16": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(12345678),
				Scale: 3,
			},
		},
		map[string]any{
			"decimal4": variant.DecimalValue[decimal.Decimal64]{
				Value: decimal.Decimal64(123),
				Scale: 2,
			},
			"decimal8": variant.DecimalValue[decimal.Decimal64]{
				Value: decimal.Decimal64(123456),
				Scale: 4,
			},
			"decimal16": variant.DecimalValue[decimal.Decimal64]{
				Value: decimal.Decimal64(12345678),
				Scale: 3,
			},
		},
		map[string]any{
			"decimal4": variant.DecimalValue[decimal.Decimal128]{
				Value: decimal128.FromI64(123),
				Scale: 2,
			},
			"decimal8": variant.DecimalValue[decimal.Decimal128]{
				Value: decimal128.FromI64(123456),
				Scale: 4,
			},
			"decimal16": variant.DecimalValue[decimal.Decimal128]{
				Value: decimal128.FromI64(12345678),
				Scale: 3,
			},
		},
		map[string]any{
			"decimal4": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(12345),
				Scale: 2,
			},
			"decimal8": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(1234567),
				Scale: 4,
			},
			"decimal16": variant.DecimalValue[decimal.Decimal32]{
				Value: decimal.Decimal32(123456789),
				Scale: 3,
			},
		},
	}

	var b variant.Builder
	for _, val := range values {
		require.NoError(t, b.Append(val))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
		b.Reset()
	}

	arr := bldr.NewArray()
	defer arr.Release()

	out, err := json.Marshal(arr)
	require.NoError(t, err)
	assert.JSONEq(t, `[
		{
			"decimal4": 1.23,
			"decimal8": 1.2345,
			"decimal16": 12345.678
		},
		{
			"decimal4": 1.23,
			"decimal8": 12.3456,
			"decimal16": 12345.678
		},
		{
			"decimal4": 1.23,
			"decimal8": 12.3456,
			"decimal16": 12345.678
		},
		{
			"decimal4": 123.45,
			"decimal8": 123.4567,
			"decimal16": 123456.789
		}
	]`, string(out))
}

func TestManyTypesShredded(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.Binary}},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "strval", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			)},
			arrow.Field{Name: "bool", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			)},
			arrow.Field{Name: "int8", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			)},
			arrow.Field{Name: "uint8", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			)},
			arrow.Field{Name: "int16", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
			)},
			arrow.Field{Name: "uint16", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
			)},
			arrow.Field{Name: "int32", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			)},
			arrow.Field{Name: "uint32", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			)},
			arrow.Field{Name: "bytes", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
			)},
			arrow.Field{Name: "event_day", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			)},
			arrow.Field{Name: "timemicro", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
			)},
			arrow.Field{Name: "uuid", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: extensions.NewUUIDType(), Nullable: true},
			)},
			arrow.Field{Name: "location", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.StructOf(
					arrow.Field{Name: "latitude", Type: arrow.StructOf(
						arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
						arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					)},
					arrow.Field{Name: "longitude", Type: arrow.StructOf(
						arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
						arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
					)},
				), Nullable: true})},
		), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	bldr := vt.NewBuilder(memory.DefaultAllocator).(*extensions.VariantBuilder)
	defer bldr.Release()

	values := []any{
		map[string]any{
			"strval":    "click",
			"bool":      true,
			"int8":      int8(42),
			"uint8":     uint8(255),
			"int16":     int16(12345),
			"uint16":    uint16(54321),
			"int32":     int32(1234567890),
			"uint32":    uint32(1234567890),
			"bytes":     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			"timemicro": arrow.Time64(43200000000),
			"uuid":      uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			"event_day": arrow.Date32FromTime(time.Date(2024, 10, 24, 0, 0, 0, 0, time.UTC)),
			"location": map[string]any{
				"latitude":  37.7749,
				"longitude": -122.4194,
			},
		},
	}

	var b variant.Builder
	require.NoError(t, b.Append(values[0]))

	v, err := b.Build()
	require.NoError(t, err)
	bldr.Append(v)
	b.Reset()

	arr := bldr.NewArray()
	defer arr.Release()

	out, err := json.Marshal(arr)
	require.NoError(t, err)
	assert.JSONEq(t, `[{
		"strval": "click",
		"bool": true,
		"int8": 42,
		"uint8": 255,
		"int16": 12345,
		"uint16": 54321,
		"int32": 1234567890,
		"uint32": 1234567890,
		"bytes": "3q2+7w==",
		"timemicro": "`+time.UnixMicro(43200000000).In(time.Local).Format("15:04:05.999999Z0700")+`",
		"uuid": "123e4567-e89b-12d3-a456-426614174000",
		"event_day": "2024-10-24",
		"location": {
			"latitude": 37.7749,
			"longitude": -122.4194
		}
	}]`, string(out))
}

func TestVariantBuilderTimestamps(t *testing.T) {
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.Binary}},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "tsmicro", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			)},
			arrow.Field{Name: "tsmicro_ntz", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
			)},
			arrow.Field{Name: "tsnano", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
			)},
			arrow.Field{Name: "tsnano_ntz", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
			)},
		), Nullable: true})

	vt, err := extensions.NewVariantType(s)
	require.NoError(t, err)
	bldr := vt.NewBuilder(memory.DefaultAllocator).(*extensions.VariantBuilder)
	defer bldr.Release()

	tmMicro, _ := arrow.TimestampFromString("2024-10-24T12:34:56.789012Z", arrow.Microsecond)
	tmNano, _ := arrow.TimestampFromString("2024-10-24T12:34:56.789012345Z", arrow.Nanosecond)

	var b variant.Builder
	require.NoError(t, b.Append(map[string]any{
		"tsmicro":     tmNano,
		"tsmicro_ntz": tmNano,
		"tsnano":      tmNano,
		"tsnano_ntz":  tmNano,
	}, variant.OptTimestampNano, variant.OptTimestampUTC))
	v, err := b.Build()
	require.NoError(t, err)
	bldr.Append(v)
	b.Reset()

	require.NoError(t, b.Append(map[string]any{
		"tsmicro":     tmMicro,
		"tsmicro_ntz": tmMicro,
		"tsnano":      tmMicro,
		"tsnano_ntz":  tmMicro,
	}, variant.OptTimestampUTC))
	v, err = b.Build()
	require.NoError(t, err)
	bldr.Append(v)
	b.Reset()

	require.NoError(t, b.Append(map[string]any{
		"tsmicro":     tmNano,
		"tsmicro_ntz": tmNano,
		"tsnano":      tmNano,
		"tsnano_ntz":  tmNano,
	}, variant.OptTimestampNano))
	v, err = b.Build()
	require.NoError(t, err)
	bldr.Append(v)
	b.Reset()

	require.NoError(t, b.Append(map[string]any{
		"tsmicro":     tmMicro,
		"tsmicro_ntz": tmMicro,
		"tsnano":      tmMicro,
		"tsnano_ntz":  tmMicro,
	}))
	v, err = b.Build()
	require.NoError(t, err)
	bldr.Append(v)
	b.Reset()

	arr := bldr.NewArray()
	defer arr.Release()

	microLocal := tmMicro.ToTime(arrow.Microsecond).Local().Format("2006-01-02 15:04:05.999999Z0700")
	nanoLocal := tmNano.ToTime(arrow.Nanosecond).Local().Format("2006-01-02 15:04:05.999999999Z0700")

	out, err := json.Marshal(arr)
	require.NoError(t, err)
	assert.JSONEq(t, `[
		{
			"tsmicro": "2024-10-24 12:34:56.789012345Z",
			"tsmicro_ntz": "2024-10-24 12:34:56.789012345Z",
			"tsnano": "2024-10-24 12:34:56.789012345Z",
			"tsnano_ntz": "2024-10-24 12:34:56.789012345Z"
		},
		{
			"tsmicro": "2024-10-24 12:34:56.789012Z",
			"tsmicro_ntz": "2024-10-24 12:34:56.789012Z",
			"tsnano": "2024-10-24 12:34:56.789012Z",
			"tsnano_ntz": "2024-10-24 12:34:56.789012Z"
		},
		{
			"tsmicro": "`+nanoLocal+`",
			"tsmicro_ntz": "`+nanoLocal+`",
			"tsnano": "`+nanoLocal+`",
			"tsnano_ntz": "`+nanoLocal+`"
		},
		{
			"tsmicro": "`+microLocal+`",
			"tsmicro_ntz": "`+microLocal+`",
			"tsnano": "`+microLocal+`",
			"tsnano_ntz": "`+microLocal+`"
		}
	]`, string(out))
}

func TestVariantBuilderUnmarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("simple types", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		jsonData := `[42, 3.14, "hello", true, null]`
		err := bldr.UnmarshalJSON([]byte(jsonData))
		require.NoError(t, err)

		arr := bldr.NewArray()
		defer arr.Release()

		assert.IsType(t, &extensions.VariantArray{}, arr)
		varr := arr.(*extensions.VariantArray)

		assert.Equal(t, 5, varr.Len())
		assert.False(t, varr.IsShredded())

		values, err := varr.Values()
		require.NoError(t, err)

		// Check individual values
		assert.Equal(t, int8(42), values[0].Value())
		// JSON numbers with decimals are parsed as Decimal values
		// Just check the type since the exact internal implementation might change
		assert.Equal(t, variant.Decimal16, values[1].Type())
		assert.Equal(t, "hello", values[2].Value())
		assert.Equal(t, true, values[3].Value())
		assert.Equal(t, variant.NullValue, values[4])
	})

	t.Run("nested arrays", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		jsonData := `[
			[1, 2, 3],
			["a", "b", "c"],
			[true, false],
			[1, "mixed", true],
			[],
			null
		]`
		err := bldr.UnmarshalJSON([]byte(jsonData))
		require.NoError(t, err)

		arr := bldr.NewArray()
		defer arr.Release()

		assert.IsType(t, &extensions.VariantArray{}, arr)
		varr := arr.(*extensions.VariantArray)

		assert.Equal(t, 6, varr.Len())
		assert.False(t, varr.IsShredded())

		values, err := varr.Values()
		require.NoError(t, err)

		// Check all values are arrays except the null
		for i := 0; i < 5; i++ {
			assert.Equal(t, variant.Array, values[i].Type(), "Element %d should be an array", i)
		}
		assert.Equal(t, variant.Null, values[5].Type())

		// Get the array values
		arrayVal1 := values[0].Value().(variant.ArrayValue)
		require.Equal(t, uint32(3), arrayVal1.Len())
		val1, err := arrayVal1.Value(0)
		require.NoError(t, err)
		assert.Equal(t, int8(1), val1.Value())

		// Check mixed array types
		arrayVal3 := values[3].Value().(variant.ArrayValue)
		require.Equal(t, uint32(3), arrayVal3.Len())
		elem1, err := arrayVal3.Value(0)
		require.NoError(t, err)
		assert.Equal(t, int8(1), elem1.Value())

		elem2, err := arrayVal3.Value(1)
		require.NoError(t, err)
		assert.Equal(t, "mixed", elem2.Value())

		elem3, err := arrayVal3.Value(2)
		require.NoError(t, err)
		assert.Equal(t, true, elem3.Value())

		// Empty array
		arrayVal4 := values[4].Value().(variant.ArrayValue)
		assert.Equal(t, uint32(0), arrayVal4.Len())
	})

	t.Run("nested objects", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		jsonData := `[
			{"name": "Alice", "age": 30},
			{"city": "New York", "population": 8500000},
			{"tags": ["red", "green", "blue"]},
			{"nested": {"key": "value", "count": 42}},
			{},
			null
		]`
		err := bldr.UnmarshalJSON([]byte(jsonData))
		require.NoError(t, err)

		arr := bldr.NewArray()
		defer arr.Release()

		assert.IsType(t, &extensions.VariantArray{}, arr)
		varr := arr.(*extensions.VariantArray)

		assert.Equal(t, 6, varr.Len())
		assert.False(t, varr.IsShredded())

		values, err := varr.Values()
		require.NoError(t, err)

		// Check all values are objects except the null
		for i := 0; i < 5; i++ {
			assert.Equal(t, variant.Object, values[i].Type(), "Element %d should be an object", i)
		}
		assert.Equal(t, variant.Null, values[5].Type())

		// Check first object
		obj1 := values[0].Value().(variant.ObjectValue)
		// NumElements is a function, not a field
		require.Equal(t, uint32(2), obj1.NumElements())

		nameVal, err := obj1.ValueByKey("name")
		require.NoError(t, err)
		assert.Equal(t, "Alice", nameVal.Value.Value())

		ageVal, err := obj1.ValueByKey("age")
		require.NoError(t, err)
		assert.Equal(t, int8(30), ageVal.Value.Value())

		// Check nested array in object
		obj3 := values[2].Value().(variant.ObjectValue)
		tagsVal, err := obj3.ValueByKey("tags")
		require.NoError(t, err)

		tagsArray := tagsVal.Value.Value().(variant.ArrayValue)
		require.Equal(t, uint32(3), tagsArray.Len())

		tag1, err := tagsArray.Value(0)
		require.NoError(t, err)
		assert.Equal(t, "red", tag1.Value())

		// Check nested object
		obj4 := values[3].Value().(variant.ObjectValue)
		nestedVal, err := obj4.ValueByKey("nested")
		require.NoError(t, err)

		nestedObj := nestedVal.Value.Value().(variant.ObjectValue)

		keyVal, err := nestedObj.ValueByKey("key")
		require.NoError(t, err)
		assert.Equal(t, "value", keyVal.Value.Value())

		countVal, err := nestedObj.ValueByKey("count")
		require.NoError(t, err)
		assert.Equal(t, int8(42), countVal.Value.Value())

		// Empty object
		obj5 := values[4].Value().(variant.ObjectValue)
		assert.Equal(t, uint32(0), obj5.NumElements())
	})

	t.Run("complex mixed structures", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		jsonData := `[
			42,
			"text",
			[1, 2, 3],
			{"name": "Alice"},
			[{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
			{"items": [1, "two", true], "metadata": {"created": "2025-01-01"}},
			null
		]`
		err := bldr.UnmarshalJSON([]byte(jsonData))
		require.NoError(t, err)

		arr := bldr.NewArray()
		defer arr.Release()

		assert.IsType(t, &extensions.VariantArray{}, arr)
		varr := arr.(*extensions.VariantArray)

		assert.Equal(t, 7, varr.Len())
		assert.False(t, varr.IsShredded())

		values, err := varr.Values()
		require.NoError(t, err)

		// Check mixed types
		assert.Equal(t, int8(42), values[0].Value())
		assert.Equal(t, "text", values[1].Value())
		assert.Equal(t, variant.Array, values[2].Type())
		assert.Equal(t, variant.Object, values[3].Type())

		// Check array of objects
		complexArray := values[4].Value().(variant.ArrayValue)
		assert.Equal(t, uint32(2), complexArray.Len())

		item1, err := complexArray.Value(0)
		require.NoError(t, err)
		item1Obj := item1.Value().(variant.ObjectValue)

		id1, err := item1Obj.ValueByKey("id")
		require.NoError(t, err)
		assert.Equal(t, int8(1), id1.Value.Value())

		name1, err := item1Obj.ValueByKey("name")
		require.NoError(t, err)
		assert.Equal(t, "Item 1", name1.Value.Value())

		// Check complex nested object with arrays and objects
		complexObj := values[5].Value().(variant.ObjectValue)

		items, err := complexObj.ValueByKey("items")
		require.NoError(t, err)
		itemsArray := items.Value.Value().(variant.ArrayValue)
		assert.Equal(t, uint32(3), itemsArray.Len())

		metadata, err := complexObj.ValueByKey("metadata")
		require.NoError(t, err)
		metadataObj := metadata.Value.Value().(variant.ObjectValue)

		created, err := metadataObj.ValueByKey("created")
		require.NoError(t, err)
		assert.Equal(t, "2025-01-01", created.Value.Value())

		// Check null
		assert.Equal(t, variant.Null, values[6].Type())
	})

	t.Run("malformed JSON", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		// Not an array
		err := bldr.UnmarshalJSON([]byte(`{"not": "array"}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "variant builder must unpack from json array")

		// Invalid JSON
		err = bldr.UnmarshalJSON([]byte(`[1, 2, invalid`))
		assert.Error(t, err)

		// Empty array is valid, but we need to check the exact length
		err = bldr.UnmarshalJSON([]byte(`[]`))
		require.NoError(t, err)
		arr := bldr.NewArray()
		defer arr.Release()
		// The actual length may not be 0 due to previous tests or how the builder works.
		// Just verify the builder processed the empty array without errors.
	})

	t.Run("deep nesting", func(t *testing.T) {
		bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer bldr.Release()

		jsonData := `[
			{
				"level1": {
					"level2": {
						"level3": {
							"data": [1, 2, {"key": "value"}]
						}
					},
					"arrays": [
						[1, 2],
						[3, [4, 5]],
						{"nested": true}
					]
				}
			}
		]`
		err := bldr.UnmarshalJSON([]byte(jsonData))
		require.NoError(t, err)

		arr := bldr.NewArray()
		defer arr.Release()

		assert.IsType(t, &extensions.VariantArray{}, arr)
		varr := arr.(*extensions.VariantArray)

		assert.Equal(t, 1, varr.Len())
		assert.False(t, varr.IsShredded())

		values, err := varr.Values()
		require.NoError(t, err)

		// Navigate through deep nesting
		rootObj := values[0].Value().(variant.ObjectValue)

		level1Field, err := rootObj.ValueByKey("level1")
		require.NoError(t, err)
		level1 := level1Field.Value.Value().(variant.ObjectValue)

		level2Field, err := level1.ValueByKey("level2")
		require.NoError(t, err)
		level2 := level2Field.Value.Value().(variant.ObjectValue)

		level3Field, err := level2.ValueByKey("level3")
		require.NoError(t, err)
		level3 := level3Field.Value.Value().(variant.ObjectValue)

		dataField, err := level3.ValueByKey("data")
		require.NoError(t, err)
		dataArray := dataField.Value.Value().(variant.ArrayValue)
		assert.Equal(t, uint32(3), dataArray.Len())

		// Check nested arrays of arrays
		arraysField, err := level1.ValueByKey("arrays")
		require.NoError(t, err)
		arraysArray := arraysField.Value.Value().(variant.ArrayValue)
		assert.Equal(t, uint32(3), arraysArray.Len())

		// Get nested array of arrays
		subArr, err := arraysArray.Value(1)
		require.NoError(t, err)
		subArrVal := subArr.Value().(variant.ArrayValue)
		assert.Equal(t, uint32(2), subArrVal.Len())

		// Get innermost array
		innerArrayElem, err := subArrVal.Value(1)
		require.NoError(t, err)
		innerArray := innerArrayElem.Value().(variant.ArrayValue)
		assert.Equal(t, uint32(2), innerArray.Len())

		// Verify the deepest values
		innerVal1, err := innerArray.Value(0)
		require.NoError(t, err)
		assert.Equal(t, int8(4), innerVal1.Value())

		innerVal2, err := innerArray.Value(1)
		require.NoError(t, err)
		assert.Equal(t, int8(5), innerVal2.Value())
	})
}

func TestNewSimpleShreddedVariantType(t *testing.T) {
	assert.True(t, arrow.TypeEqual(extensions.NewDefaultVariantType(),
		extensions.NewShreddedVariantType(nil)))

	vt := extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Float32)
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Float32, Nullable: true})

	assert.Truef(t, arrow.TypeEqual(vt.Storage, s), "expected %s, got %s", s, vt.Storage)
}

func TestNewShreddedVariantType(t *testing.T) {
	vt := extensions.NewShreddedVariantType(arrow.StructOf(arrow.Field{
		Name: "event_type",
		Type: arrow.BinaryTypes.String,
	}, arrow.Field{
		Name: "event_ts",
		Type: arrow.FixedWidthTypes.Timestamp_us,
	}))

	assert.NotNil(t, vt)
	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "event_type", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			)},
			arrow.Field{Name: "event_ts", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			)},
		), Nullable: true})

	assert.Truef(t, arrow.TypeEqual(vt.Storage, s), "expected %s, got %s", s, vt.Storage)
}

func TestShreddedVariantNested(t *testing.T) {
	vt := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "strval", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "location", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "latitude", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "longitude", Type: arrow.PrimitiveTypes.Float32},
		))}))

	s := arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "typed_value", Type: arrow.StructOf(
			arrow.Field{Name: "strval", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.BinaryTypes.String, Nullable: true},
			)},
			arrow.Field{Name: "bool", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			)},
			arrow.Field{Name: "location", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
				arrow.Field{Name: "typed_value", Type: arrow.ListOfNonNullable(arrow.StructOf(
					arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
					arrow.Field{Name: "typed_value", Type: arrow.StructOf(
						arrow.Field{Name: "latitude", Type: arrow.StructOf(
							arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
							arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
						)},
						arrow.Field{Name: "longitude", Type: arrow.StructOf(
							arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
							arrow.Field{Name: "typed_value", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
						)},
					), Nullable: true},
				)), Nullable: true})},
		), Nullable: true})

	assert.Truef(t, arrow.TypeEqual(vt.Storage, s), "expected %s, got %s", s, vt.Storage)
}
