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

package variant_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildNullValue(t *testing.T) {
	var b variant.Builder
	b.AppendNull()

	v, err := b.Build()
	require.NoError(t, err)

	assert.Equal(t, variant.Null, v.Type())
	assert.EqualValues(t, 1, v.Metadata().Version())
	assert.Zero(t, v.Metadata().DictionarySize())
}

func TestBuildPrimitive(t *testing.T) {
	tests := []struct {
		name string
		op   func(*variant.Builder) error
	}{
		{"primitive_boolean_true", func(b *variant.Builder) error {
			return b.AppendBool(true)
		}},
		{"primitive_boolean_false", func(b *variant.Builder) error {
			return b.AppendBool(false)
		}},
		// AppendInt will use the smallest possible int type
		{"primitive_int8", func(b *variant.Builder) error { return b.AppendInt(42) }},
		{"primitive_int16", func(b *variant.Builder) error { return b.AppendInt(1234) }},
		{"primitive_int32", func(b *variant.Builder) error { return b.AppendInt(123456) }},
		// FIXME: https://github.com/apache/parquet-testing/issues/82
		// primitive_int64 is an int32 value, but the metadata is int64
		{"primitive_int64", func(b *variant.Builder) error { return b.AppendInt(12345678) }},
		{"primitive_float", func(b *variant.Builder) error { return b.AppendFloat32(1234568000) }},
		{"primitive_double", func(b *variant.Builder) error { return b.AppendFloat64(1234567890.1234) }},
		{"primitive_string", func(b *variant.Builder) error {
			return b.AppendString(`This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!`)
		}},
		{"short_string", func(b *variant.Builder) error { return b.AppendString(`Less than 64 bytes (❤️ with utf8)`) }},
		// 031337deadbeefcafe
		{"primitive_binary", func(b *variant.Builder) error {
			return b.AppendBinary([]byte{0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe})
		}},
		{"primitive_decimal4", func(b *variant.Builder) error { return b.AppendDecimal4(2, 1234) }},
		{"primitive_decimal8", func(b *variant.Builder) error { return b.AppendDecimal8(2, 1234567890) }},
		{"primitive_decimal16", func(b *variant.Builder) error { return b.AppendDecimal16(2, decimal128.FromU64(1234567891234567890)) }},
		{"primitive_date", func(b *variant.Builder) error { return b.AppendDate(20194) }},
		{"primitive_timestamp", func(b *variant.Builder) error { return b.AppendTimestamp(1744821296780000, true, true) }},
		{"primitive_timestampntz", func(b *variant.Builder) error { return b.AppendTimestamp(1744806896780000, true, false) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := loadVariant(t, tt.name)

			var b variant.Builder
			require.NoError(t, tt.op(&b))

			v, err := b.Build()
			require.NoError(t, err)

			assert.Equal(t, expected.Type(), v.Type())
			assert.Equal(t, expected.Bytes(), v.Bytes())
			assert.Equal(t, expected.Metadata().Bytes(), v.Metadata().Bytes())
		})
	}
}

func TestBuildInt64(t *testing.T) {
	var b variant.Builder
	require.NoError(t, b.AppendInt(1234567890987654321))

	v, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, variant.Int64, v.Type())
	assert.Equal(t, []byte{primitiveHeader(variant.PrimitiveInt64),
		0xB1, 0x1C, 0x6C, 0xB1, 0xF4, 0x10, 0x22, 0x11}, v.Bytes())
}

func TestBuildObject(t *testing.T) {
	var b variant.Builder
	start := b.Offset()

	fields := make([]variant.FieldEntry, 0, 7)

	fields = append(fields, b.NextField(start, "int_field"))
	require.NoError(t, b.AppendInt(1))

	fields = append(fields, b.NextField(start, "double_field"))
	require.NoError(t, b.AppendDecimal4(8, 123456789))

	fields = append(fields, b.NextField(start, "boolean_true_field"))
	require.NoError(t, b.AppendBool(true))

	fields = append(fields, b.NextField(start, "boolean_false_field"))
	require.NoError(t, b.AppendBool(false))

	fields = append(fields, b.NextField(start, "string_field"))
	require.NoError(t, b.AppendString("Apache Parquet"))

	fields = append(fields, b.NextField(start, "null_field"))
	require.NoError(t, b.AppendNull())

	fields = append(fields, b.NextField(start, "timestamp_field"))
	require.NoError(t, b.AppendString("2025-04-16T12:34:56.78"))

	require.NoError(t, b.FinishObject(start, fields))
	v, err := b.Build()
	require.NoError(t, err)

	assert.Equal(t, variant.Object, v.Type())
	expected := loadVariant(t, "object_primitive")

	assert.Equal(t, expected.Metadata().DictionarySize(), v.Metadata().DictionarySize())
	assert.Equal(t, expected.Metadata().Bytes(), v.Metadata().Bytes())
	assert.Equal(t, expected.Bytes(), v.Bytes())
}

func TestBuildObjectDuplicateKeys(t *testing.T) {
	t.Run("disallow duplicates", func(t *testing.T) {
		var b variant.Builder
		start := b.Offset()

		fields := make([]variant.FieldEntry, 0, 3)

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(1))

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(2))

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(3))

		require.Error(t, b.FinishObject(start, fields))
	})

	t.Run("allow duplicates", func(t *testing.T) {
		var b variant.Builder
		start := b.Offset()

		fields := make([]variant.FieldEntry, 0, 3)

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(1))

		fields = append(fields, b.NextField(start, "string_field"))
		require.NoError(t, b.AppendString("Apache Parquet"))

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(2))

		fields = append(fields, b.NextField(start, "int_field"))
		require.NoError(t, b.AppendInt(3))

		fields = append(fields, b.NextField(start, "string_field"))
		require.NoError(t, b.AppendString("Apache Arrow"))

		b.SetAllowDuplicates(true)
		require.NoError(t, b.FinishObject(start, fields))

		v, err := b.Build()
		require.NoError(t, err)

		assert.Equal(t, variant.Object, v.Type())
		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"int_field": 3,
			"string_field": "Apache Arrow"
		}`, string(out))
	})
}

func TestBuildObjectNested(t *testing.T) {
	var b variant.Builder

	start := b.Offset()
	topFields := make([]variant.FieldEntry, 0, 3)

	topFields = append(topFields, b.NextField(start, "id"))
	require.NoError(t, b.AppendInt(1))

	topFields = append(topFields, b.NextField(start, "observation"))

	observeFields := make([]variant.FieldEntry, 0, 3)
	observeStart := b.Offset()
	observeFields = append(observeFields, b.NextField(observeStart, "location"))
	require.NoError(t, b.AppendString("In the Volcano"))
	observeFields = append(observeFields, b.NextField(observeStart, "time"))
	require.NoError(t, b.AppendString("12:34:56"))
	observeFields = append(observeFields, b.NextField(observeStart, "value"))

	valueStart := b.Offset()
	valueFields := make([]variant.FieldEntry, 0, 2)
	valueFields = append(valueFields, b.NextField(valueStart, "humidity"))
	require.NoError(t, b.AppendInt(456))
	valueFields = append(valueFields, b.NextField(valueStart, "temperature"))
	require.NoError(t, b.AppendInt(123))

	require.NoError(t, b.FinishObject(valueStart, valueFields))
	require.NoError(t, b.FinishObject(observeStart, observeFields))

	topFields = append(topFields, b.NextField(start, "species"))
	speciesStart := b.Offset()
	speciesFields := make([]variant.FieldEntry, 0, 2)
	speciesFields = append(speciesFields, b.NextField(speciesStart, "name"))
	require.NoError(t, b.AppendString("lava monster"))

	speciesFields = append(speciesFields, b.NextField(speciesStart, "population"))
	require.NoError(t, b.AppendInt(6789))

	require.NoError(t, b.FinishObject(speciesStart, speciesFields))
	require.NoError(t, b.FinishObject(start, topFields))

	v, err := b.Build()
	require.NoError(t, err)

	out, err := json.Marshal(v)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"id": 1,
		"observation": {
			"location": "In the Volcano",
			"time": "12:34:56",
			"value": {
				"humidity": 456,
				"temperature": 123
			}
		},
		"species": {
			"name": "lava monster",
			"population": 6789
		}
	}`, string(out))
}

func TestBuildUUID(t *testing.T) {
	u := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")

	var b variant.Builder
	require.NoError(t, b.AppendUUID(u))
	v, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, variant.UUID, v.Type())
	assert.Equal(t, []byte{primitiveHeader(variant.PrimitiveUUID),
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, v.Bytes())
}

func TestBuildTimestampNanos(t *testing.T) {
	t.Run("ts nanos tz negative", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.AppendTimestamp(-1, false, true))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanos, v.Type())
		assert.Equal(t, []byte{primitiveHeader(variant.PrimitiveTimestampNanos),
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, v.Bytes())
	})

	t.Run("ts nanos tz positive", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.AppendTimestamp(1744877350123456789, false, true))
		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanos, v.Type())
		assert.Equal(t, []byte{primitiveHeader(variant.PrimitiveTimestampNanos),
			0x15, 0xC9, 0xBB, 0x86, 0xB4, 0x0C, 0x37, 0x18}, v.Bytes())
	})

	t.Run("ts nanos ntz positive", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.AppendTimestamp(1744877350123456789, false, false))
		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanosNTZ, v.Type())
		assert.Equal(t, []byte{primitiveHeader(variant.PrimitiveTimestampNanosNTZ),
			0x15, 0xC9, 0xBB, 0x86, 0xB4, 0x0C, 0x37, 0x18}, v.Bytes())
	})
}

func TestBuildArrayValues(t *testing.T) {
	t.Run("array primitive", func(t *testing.T) {
		var b variant.Builder

		start := b.Offset()
		offsets := make([]int, 0, 4)

		offsets = append(offsets, b.NextElement(start))
		require.NoError(t, b.AppendInt(2))

		offsets = append(offsets, b.NextElement(start))
		require.NoError(t, b.AppendInt(1))

		offsets = append(offsets, b.NextElement(start))
		require.NoError(t, b.AppendInt(5))

		offsets = append(offsets, b.NextElement(start))
		require.NoError(t, b.AppendInt(9))

		require.NoError(t, b.FinishArray(start, offsets))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())

		expected := loadVariant(t, "array_primitive")
		assert.Equal(t, expected.Metadata().Bytes(), v.Metadata().Bytes())
		assert.Equal(t, expected.Bytes(), v.Bytes())
	})

	t.Run("array empty", func(t *testing.T) {
		var b variant.Builder

		require.NoError(t, b.FinishArray(b.Offset(), nil))
		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())

		expected := loadVariant(t, "array_empty")
		assert.Equal(t, expected.Metadata().Bytes(), v.Metadata().Bytes())
		assert.Equal(t, expected.Bytes(), v.Bytes())
	})

	t.Run("array nested", func(t *testing.T) {
		var b variant.Builder

		start := b.Offset()
		offsets := make([]int, 0, 3)

		{
			offsets = append(offsets, b.NextElement(start))
			objStart := b.Offset()
			objFields := make([]variant.FieldEntry, 0, 2)
			objFields = append(objFields, b.NextField(objStart, "id"))
			require.NoError(t, b.AppendInt(1))

			objFields = append(objFields, b.NextField(objStart, "thing"))
			thingObjStart := b.Offset()
			thingObjFields := make([]variant.FieldEntry, 0, 1)
			thingObjFields = append(thingObjFields, b.NextField(thingObjStart, "names"))

			namesStart := b.Offset()
			namesOffsets := make([]int, 0, 2)
			namesOffsets = append(namesOffsets, b.NextElement(namesStart))
			require.NoError(t, b.AppendString("Contrarian"))
			namesOffsets = append(namesOffsets, b.NextElement(namesStart))
			require.NoError(t, b.AppendString("Spider"))
			require.NoError(t, b.FinishArray(namesStart, namesOffsets))

			require.NoError(t, b.FinishObject(thingObjStart, thingObjFields))
			require.NoError(t, b.FinishObject(objStart, objFields))
		}
		{
			offsets = append(offsets, b.NextElement(start))
			b.AppendNull()
		}
		{
			offsets = append(offsets, b.NextElement(start))
			objStart := b.Offset()
			objFields := make([]variant.FieldEntry, 0, 3)
			objFields = append(objFields, b.NextField(objStart, "id"))
			require.NoError(t, b.AppendInt(2))

			objFields = append(objFields, b.NextField(objStart, "names"))
			namesStart := b.Offset()
			namesOffsets := make([]int, 0, 3)
			namesOffsets = append(namesOffsets, b.NextElement(namesStart))
			require.NoError(t, b.AppendString("Apple"))
			namesOffsets = append(namesOffsets, b.NextElement(namesStart))
			require.NoError(t, b.AppendString("Ray"))
			namesOffsets = append(namesOffsets, b.NextElement(namesStart))
			require.NoError(t, b.AppendNull())
			require.NoError(t, b.FinishArray(namesStart, namesOffsets))

			objFields = append(objFields, b.NextField(objStart, "type"))
			require.NoError(t, b.AppendString("if"))

			require.NoError(t, b.FinishObject(objStart, objFields))
		}

		require.NoError(t, b.FinishArray(start, offsets))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())
		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `[
			{"id": 1, "thing": {"names": ["Contrarian", "Spider"]}},
			null,
			{"id": 2, "names": ["Apple", "Ray", null], "type": "if"}
		]`, string(out))
	})
}

func TestAppendPrimitives(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		valueOut any
		expected variant.Type
	}{
		{"null", nil, nil, variant.Null},
		{"bool_true", true, true, variant.Bool},
		{"bool_false", false, false, variant.Bool},
		{"int8", int8(42), int8(42), variant.Int8},
		{"uint8", uint8(42), int8(42), variant.Int8},
		{"int16", int16(1234), int16(1234), variant.Int16},
		{"uint16", uint16(1234), int16(1234), variant.Int16},
		{"int32", int32(123456), int32(123456), variant.Int32},
		{"uint32", uint32(123456), int32(123456), variant.Int32},
		{"int64", int64(1234567890123), int64(1234567890123), variant.Int64},
		{"int", int(123456), int32(123456), variant.Int32},
		{"uint", uint(123456), int32(123456), variant.Int32},
		{"float32", float32(123.45), float32(123.45), variant.Float},
		{"float64", float64(123.45), float64(123.45), variant.Double},
		{"string", "test string", "test string", variant.String},
		{"bytes", []byte{1, 2, 3, 4}, []byte{1, 2, 3, 4}, variant.Binary},
		{"date", arrow.Date32(2023), arrow.Date32(2023), variant.Date},
		{"timestamp", arrow.Timestamp(1234567890), arrow.Timestamp(1234567890), variant.TimestampMicrosNTZ},
		{"time", arrow.Time64(123456), arrow.Time64(123456), variant.Time},
		{"uuid", uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"),
			uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), variant.UUID},
		{"decimal4", variant.DecimalValue[decimal.Decimal32]{
			Scale: 2, Value: decimal.Decimal32(1234),
		}, variant.DecimalValue[decimal.Decimal32]{
			Scale: 2, Value: decimal.Decimal32(1234),
		}, variant.Decimal4},
		{"decimal8", variant.DecimalValue[decimal.Decimal64]{
			Scale: 2, Value: decimal.Decimal64(1234),
		}, variant.DecimalValue[decimal.Decimal64]{
			Scale: 2, Value: decimal.Decimal64(1234),
		}, variant.Decimal8},
		{"decimal16", variant.DecimalValue[decimal.Decimal128]{
			Scale: 2, Value: decimal128.FromU64(1234),
		}, variant.DecimalValue[decimal.Decimal128]{
			Scale: 2, Value: decimal128.FromU64(1234),
		}, variant.Decimal16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b variant.Builder
			require.NoError(t, b.Append(tt.value))

			v, err := b.Build()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, v.Type())
			assert.Equal(t, tt.valueOut, v.Value())
		})
	}
}

func TestAppendTimestampOptions(t *testing.T) {
	testTime := time.Date(2023, 5, 15, 14, 30, 0, 123456789, time.UTC)

	tests := []struct {
		name     string
		opts     []variant.AppendOpt
		expected variant.Type
	}{
		{"default_micros", nil, variant.TimestampMicrosNTZ},
		{"nanos", []variant.AppendOpt{variant.OptTimestampNano}, variant.TimestampNanosNTZ},
		{"utc_micros", []variant.AppendOpt{variant.OptTimestampUTC}, variant.TimestampMicros},
		{"utc_nanos", []variant.AppendOpt{variant.OptTimestampUTC, variant.OptTimestampNano}, variant.TimestampNanos},
		{"as_date", []variant.AppendOpt{variant.OptTimeAsDate}, variant.Date},
		{"as_time", []variant.AppendOpt{variant.OptTimeAsTime}, variant.Time},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b variant.Builder
			require.NoError(t, b.Append(testTime, tt.opts...))

			v, err := b.Build()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, v.Type())
		})
	}
}

func TestAppendArrays(t *testing.T) {
	t.Run("slice_of_any", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append([]any{1, "test", true, nil}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `[1, "test", true, null]`, string(out))
	})

	t.Run("slice_of_ints", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append([]int{10, 20, 30}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `[10, 20, 30]`, string(out))
	})

	t.Run("nested_slices", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append([]any{
			[]int{1, 2},
			[]string{"a", "b"},
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Array, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `[[1, 2], ["a", "b"]]`, string(out))
	})
}

func TestAppendMaps(t *testing.T) {
	t.Run("map_string_any", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{
			"int":  123,
			"str":  "test",
			"bool": true,
			"null": nil,
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{"bool": true, "int": 123, "null": null, "str": "test"}`, string(out))
	})

	t.Run("map_string_int", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]int{
			"int":  123,
			"int2": 456,
		}))
		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())
		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{"int": 123, "int2": 456}`, string(out))
	})

	t.Run("map_with_nested_objects", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{
			"metadata": map[string]any{
				"id":   1,
				"name": "test",
			},
			"values": []int{10, 20, 30},
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{"metadata": {"id": 1, "name": "test"}, "values": [10, 20, 30]}`, string(out))
	})

	t.Run("unsupported_map_key", func(t *testing.T) {
		var b variant.Builder
		err := b.Append(map[int]string{1: "test"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported map key type")
	})
}

type SimpleStruct struct {
	ID      int
	Name    string
	IsValid bool
}

type StructWithTags struct {
	ID        int       `variant:"id"`
	Name      string    `variant:"name"`
	Ignored   string    `variant:"-"`
	Timestamp time.Time `variant:"ts,nanos,utc"`
	Date      time.Time `variant:"date,date"`
	TimeOnly  time.Time `variant:",time"`
}

type NestedStruct struct {
	ID       int           `variant:"id"`
	Metadata *SimpleStruct `variant:"meta"`
	Tags     []string      `variant:"tags"`
}

func TestAppendStructs(t *testing.T) {
	t.Run("simple_struct", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(SimpleStruct{
			ID:      123,
			Name:    "test",
			IsValid: true,
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{"ID": 123, "Name": "test", "IsValid": true}`, string(out))
	})

	t.Run("struct_with_tags", func(t *testing.T) {
		testTime := time.Date(2023, 5, 15, 14, 30, 0, 123456789, time.UTC)
		var b variant.Builder
		require.NoError(t, b.Append(StructWithTags{
			ID:        123,
			Name:      "test",
			Ignored:   "should not appear",
			Timestamp: testTime,
			Date:      testTime,
			TimeOnly:  testTime,
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		obj := v.Value().(variant.ObjectValue)
		id, err := obj.ValueByKey("id")
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, id.Value.Type())
		assert.Equal(t, int8(123), id.Value.Value())

		name, err := obj.ValueByKey("name")
		require.NoError(t, err)
		assert.Equal(t, variant.String, name.Value.Type())
		assert.Equal(t, "test", name.Value.Value())

		ignored, err := obj.ValueByKey("Ignored")
		require.ErrorIs(t, err, arrow.ErrNotFound)
		assert.Zero(t, ignored)

		ts, err := obj.ValueByKey("ts")
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanos, ts.Value.Type())
		assert.Equal(t, arrow.Timestamp(testTime.UnixNano()), ts.Value.Value())

		date, err := obj.ValueByKey("date")
		require.NoError(t, err)
		assert.Equal(t, variant.Date, date.Value.Type())
		assert.Equal(t, arrow.Date32FromTime(testTime), date.Value.Value())

		timeOnly, err := obj.ValueByKey("TimeOnly")
		require.NoError(t, err)
		assert.Equal(t, variant.Time, timeOnly.Value.Type())
		assert.Equal(t, arrow.Time64(52200123456), timeOnly.Value.Value())
	})

	t.Run("nested_struct", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(NestedStruct{
			ID: 123,
			Metadata: &SimpleStruct{
				ID:      456,
				Name:    "nested",
				IsValid: true,
			},
			Tags: []string{"tag1", "tag2"},
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"id": 123, 
			"meta": {"ID": 456, "Name": "nested", "IsValid": true}, 
			"tags": ["tag1", "tag2"]
		}`, string(out))
	})

	t.Run("nil_struct_pointer", func(t *testing.T) {
		var b variant.Builder
		require.NoError(t, b.Append(NestedStruct{
			ID:       123,
			Metadata: nil,
			Tags:     []string{"tag1"},
		}))

		v, err := b.Build()
		require.NoError(t, err)
		assert.Equal(t, variant.Object, v.Type())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `{"id": 123, "meta": null, "tags": ["tag1"]}`, string(out))
	})
}

func TestAppendReset(t *testing.T) {
	var b variant.Builder

	// First build
	require.NoError(t, b.Append(map[string]any{"key": "value"}))
	v1, err := b.Build()
	require.NoError(t, err)

	out1, err := json.Marshal(v1)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key": "value"}`, string(out1))
	v1 = v1.Clone()

	// Reset and build again
	b.Reset()
	require.NoError(t, b.Append([]int{1, 2, 3}))
	v2, err := b.Build()
	require.NoError(t, err)

	// First value should still be valid because we cloned it
	// before calling Reset
	assert.Equal(t, variant.Object, v1.Type())
	out1, err = json.Marshal(v1)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key": "value"}`, string(out1))

	// Second value should be different
	assert.Equal(t, variant.Array, v2.Type())
	out2, err := json.Marshal(v2)
	require.NoError(t, err)
	assert.JSONEq(t, `[1, 2, 3]`, string(out2))

	// Without cloning, the first value would be invalidated
	v1Clone := v1.Clone()
	b.Reset()

	out3, err := json.Marshal(v1Clone)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key": "value"}`, string(out3))
}

func TestBuilderFromJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		val   any
	}{
		{"null_value", `null`, nil},
		{"boolean_true", `true`, true},
		{"boolean_false", `false`, false},
		{"int8", `42`, int8(42)},
		{"int16", `1234`, int16(1234)},
		{"int32", `123456`, int32(123456)},
		{"int64", `1234567890123`, int64(1234567890123)},
		{"decimal", `123.456789`, variant.DecimalValue[decimal.Decimal128]{
			Scale: 6, Value: decimal128.FromU64(123456789),
		}},
		{"string", `"test string"`, "test string"},
		{"float64", `1e+20`, float64(1e+20)},
		{"array", `[1, 2, 3]`, []int{1, 2, 3}},
		{"object", `{"key": "value"}`, map[string]any{"key": "value"}},
		{"nested_object", `{"outer": {"inner": 42}}`, map[string]any{
			"outer": map[string]any{"inner": 42},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := variant.ParseJSON(tt.input, false)
			require.NoError(t, err)

			var b variant.Builder
			require.NoError(t, b.Append(tt.val))
			expected, err := b.Build()
			require.NoError(t, err)

			assert.Equal(t, expected.Type(), v.Type())
			assert.Equal(t, expected.Bytes(), v.Bytes())
			assert.Equal(t, expected.Metadata().Bytes(), v.Metadata().Bytes())
		})
	}
}

func TestBuilderJSONErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"empty string", ``, "unexpected end of JSON input"},
		{"invalid token", `a`, "failed to decode JSON token"},
		{"missing array end", `[1, 2, 3`, "failed to decode JSON array end"},
		{"invalid array", `[1, "foo", bar`, "failed to decode JSON token"},
		{"missing elem", `[1,`, "unexpected end of JSON input"},
		{"invalid elem", `[1, 5 }`, "expected end of JSON array, got }"},
		{"extra delimiter", `]`, "unexpected JSON delimiter"},
		{"invalid key", `{"key": "value", 42: "invalid"}`, "expected string key in JSON object"},
		{"eof key", `{"key": 1,`, "unexpected end of JSON input"},
		{"invalid token key", `{ab`, "failed to decode JSON key"},
		{"invalid object", `{"key": foo}`, "failed to decode JSON token"},
		{"invalid object end", `{"key": 123 ]`, "expected end of JSON object"},
		{"missing object end", `{"key": 123`, "failed to decode JSON object end"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := variant.ParseJSON(tt.input, false)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
