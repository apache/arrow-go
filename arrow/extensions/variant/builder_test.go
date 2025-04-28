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

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions/variant"
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

func TestBuildObjec(t *testing.T) {
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

		offsets = append(offsets, b.Offset()-start)
		require.NoError(t, b.AppendInt(2))

		offsets = append(offsets, b.Offset()-start)
		require.NoError(t, b.AppendInt(1))

		offsets = append(offsets, b.Offset()-start)
		require.NoError(t, b.AppendInt(5))

		offsets = append(offsets, b.Offset()-start)
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
}
