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
	"math"
	"os"
	"path/filepath"
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

func getVariantDir() string {
	variantDir := os.Getenv("PARQUET_TEST_DATA")
	if variantDir == "" {
		return ""
	}

	return filepath.Join(variantDir, "..", "variant")
}

func metadataTestFilename(test string) string {
	return test + ".metadata"
}

func valueTestFilename(test string) string {
	return test + ".value"
}

func TestBasicRead(t *testing.T) {
	dir := getVariantDir()
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}

	tests := []string{
		// FIXME: null metadata is corrupt, see
		// https://github.com/apache/parquet-testing/issues/81
		// "primitive_null.metadata",
		"primitive_boolean_true.metadata",
		"primitive_boolean_false.metadata",
		"primitive_int8.metadata",
		"primitive_int16.metadata",
		"primitive_int32.metadata",
		"primitive_int64.metadata",
		"primitive_float.metadata",
		"primitive_double.metadata",
		"primitive_string.metadata",
		"primitive_binary.metadata",
		"primitive_date.metadata",
		"primitive_decimal4.metadata",
		"primitive_decimal8.metadata",
		"primitive_decimal16.metadata",
		"primitive_timestamp.metadata",
		"primitive_timestampntz.metadata",
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			fname := filepath.Join(dir, test)
			require.FileExists(t, fname, "file %s does not exist", fname)

			metadata, err := os.ReadFile(fname)
			require.NoError(t, err)

			m, err := variant.NewMetadata(metadata)
			require.NoError(t, err)
			assert.EqualValues(t, 1, m.Version())
			_, err = m.KeyAt(0)
			assert.Error(t, err)
		})
	}

	t.Run("object_primitive.metadata", func(t *testing.T) {
		fname := filepath.Join(dir, "object_primitive.metadata")
		require.FileExists(t, fname, "file %s does not exist", fname)

		metadata, err := os.ReadFile(fname)
		require.NoError(t, err)

		m, err := variant.NewMetadata(metadata)
		require.NoError(t, err)
		assert.EqualValues(t, 1, m.Version())

		keys := []string{
			"int_field", "double_field", "boolean_true_field",
			"boolean_false_field", "string_field", "null_field",
			"timestamp_field",
		}

		for i, k := range keys {
			key, err := m.KeyAt(uint32(i))
			require.NoError(t, err)
			assert.Equal(t, k, key)
			assert.Equal(t, uint32(i), m.IdFor(k)[0])
		}
	})
}

func loadVariant(t *testing.T, test string) variant.Value {
	dir := getVariantDir()
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}

	fname := filepath.Join(dir, test)
	metadataPath := metadataTestFilename(fname)
	valuePath := valueTestFilename(fname)

	metaBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	valueBytes, err := os.ReadFile(valuePath)
	require.NoError(t, err)

	v, err := variant.New(metaBytes, valueBytes)
	require.NoError(t, err)
	return v
}

func TestPrimitiveVariants(t *testing.T) {
	tests := []struct {
		name        string
		expected    any
		variantType variant.Type
		jsonStr     string
	}{
		{"primitive_boolean_true", true, variant.Bool, "true"},
		{"primitive_boolean_false", false, variant.Bool, "false"},
		{"primitive_int8", int8(42), variant.Int8, "42"},
		{"primitive_int16", int16(1234), variant.Int16, "1234"},
		{"primitive_int32", int32(123456), variant.Int32, "123456"},
		// FIXME: https://github.com/apache/parquet-testing/issues/82
		// primitive_int64 is an int32 value, but the metadata is int64
		{"primitive_int64", int64(1234567890123456789), variant.Int64, "1234567890123456789"},
		{"primitive_float", float32(1234567940.0), variant.Float, "1234568000"},
		{"primitive_double", float64(1234567890.1234), variant.Double, "1234567890.1234"},
		{"primitive_string",
			`This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!`,
			variant.String, `"This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!"`},
		{"short_string", `Less than 64 bytes (❤️ with utf8)`, variant.String, `"Less than 64 bytes (❤️ with utf8)"`},
		// 031337deadbeefcafe
		{"primitive_binary", []byte{0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe}, variant.Binary, `"AxM33q2+78r+"`},
		{"primitive_decimal4", variant.DecimalValue[decimal.Decimal32]{
			Scale: 2,
			Value: decimal.Decimal32(1234),
		}, variant.Decimal4, `12.34`},
		{"primitive_decimal8", variant.DecimalValue[decimal.Decimal64]{
			Scale: 2,
			Value: decimal.Decimal64(1234567890),
		}, variant.Decimal8, `12345678.90`},
		{"primitive_decimal16", variant.DecimalValue[decimal.Decimal128]{
			Scale: 2,
			Value: decimal128.FromU64(1234567891234567890),
		}, variant.Decimal16, `12345678912345678.90`},
		// // 2025-04-16
		{"primitive_date", arrow.Date32(20194), variant.Date, `"2025-04-16"`},
		{"primitive_timestamp", arrow.Timestamp(1744821296780000), variant.TimestampMicros, `"2025-04-16 16:34:56.78Z"`},
		{"primitive_timestampntz", arrow.Timestamp(1744806896780000), variant.TimestampMicrosNTZ, `"` + time.UnixMicro(1744806896780000).UTC().In(time.Local).Format("2006-01-02 15:04:05.999999Z0700") + `"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := loadVariant(t, tt.name)
			assert.Equal(t, tt.expected, v.Value())
			assert.Equal(t, tt.variantType, v.Type())

			out, err := json.Marshal(v)
			require.NoError(t, err)
			assert.Equal(t, tt.jsonStr, string(out))
		})
	}
}

func primitiveHeader(p variant.PrimitiveType) uint8 {
	return (uint8(p) << 2)
}

func TestNullValue(t *testing.T) {
	emptyMeta := variant.EmptyMetadataBytes
	nullChars := []byte{primitiveHeader(variant.PrimitiveNull)}

	v, err := variant.New(emptyMeta[:], nullChars)
	require.NoError(t, err)

	assert.Equal(t, variant.Null, v.Type())

	out, err := json.Marshal(v)
	require.NoError(t, err)
	assert.Equal(t, "null", string(out))
}

func TestSimpleInt64(t *testing.T) {
	metaBytes := variant.EmptyMetadataBytes[:]

	int64Bytes := []byte{primitiveHeader(variant.PrimitiveInt64),
		0xB1, 0x1C, 0x6C, 0xB1, 0xF4, 0x10, 0x22, 0x11}

	v, err := variant.New(metaBytes, int64Bytes)
	require.NoError(t, err)

	assert.Equal(t, variant.Int64, v.Type())
	assert.Equal(t, int64(1234567890987654321), v.Value())

	negInt64Bytes := []byte{primitiveHeader(variant.PrimitiveInt64),
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	v, err = variant.New(metaBytes, negInt64Bytes)
	require.NoError(t, err)

	assert.Equal(t, variant.Int64, v.Type())
	assert.Equal(t, int64(-1), v.Value())
}

func TestObjectValues(t *testing.T) {
	v := loadVariant(t, "object_primitive")
	assert.Equal(t, variant.Object, v.Type())

	obj := v.Value().(variant.ObjectValue)
	assert.EqualValues(t, 7, obj.NumElements())

	tests := []struct {
		field    string
		expected any
		typ      variant.Type
	}{
		{"int_field", int8(1), variant.Int8},
		{"double_field", variant.DecimalValue[decimal.Decimal32]{
			Scale: 8, Value: decimal.Decimal32(123456789)}, variant.Decimal4},
		{"boolean_true_field", true, variant.Bool},
		{"boolean_false_field", false, variant.Bool},
		{"string_field", "Apache Parquet", variant.String},
		{"null_field", nil, variant.Null},
		{"timestamp_field", "2025-04-16T12:34:56.78", variant.String},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			v, err := obj.ValueByKey(tt.field)
			require.NoError(t, err)

			assert.Equal(t, tt.typ, v.Value.Type())
			assert.Equal(t, tt.expected, v.Value.Value())
		})
	}

	t.Run("json", func(t *testing.T) {
		out, err := json.Marshal(v)
		require.NoError(t, err)

		expected := `{
			"boolean_false_field":false,
			"boolean_true_field":true,
			"double_field":1.23456789,
			"int_field":1,
			"null_field":null,
			"string_field":"Apache Parquet",
			"timestamp_field":"2025-04-16T12:34:56.78"}`

		assert.JSONEq(t, expected, string(out))
	})

	t.Run("invalid_key", func(t *testing.T) {
		v, err := obj.ValueByKey("invalid_key")
		require.ErrorIs(t, err, arrow.ErrNotFound)
		assert.Zero(t, v)
	})

	t.Run("field by index", func(t *testing.T) {
		fieldOrder := []string{
			"boolean_false_field",
			"boolean_true_field",
			"double_field",
			"int_field",
			"null_field",
			"string_field",
			"timestamp_field",
		}

		for i := range obj.NumElements() {
			val, err := obj.FieldAt(i)
			require.NoError(t, err)

			assert.Equal(t, fieldOrder[i], val.Key)
		}
	})
}

func TestNestedObjectValues(t *testing.T) {
	v := loadVariant(t, "object_nested")
	assert.Equal(t, variant.Object, v.Type())
	obj := v.Value().(variant.ObjectValue)
	assert.EqualValues(t, 3, obj.NumElements())

	// trying to get the exists key
	id, err := obj.ValueByKey("id")
	require.NoError(t, err)
	assert.Equal(t, variant.Int8, id.Value.Type())
	assert.Equal(t, int8(1), id.Value.Value())

	observation, err := obj.ValueByKey("observation")
	require.NoError(t, err)
	assert.Equal(t, variant.Object, observation.Value.Type())

	species, err := obj.ValueByKey("species")
	require.NoError(t, err)
	assert.Equal(t, variant.Object, species.Value.Type())

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

	t.Run("inner object", func(t *testing.T) {
		speciesObj := species.Value.Value().(variant.ObjectValue)
		assert.EqualValues(t, 2, speciesObj.NumElements())

		name, err := speciesObj.ValueByKey("name")
		require.NoError(t, err)
		assert.Equal(t, variant.String, name.Value.Type())
		assert.Equal(t, "lava monster", name.Value.Value())

		population, err := speciesObj.ValueByKey("population")
		require.NoError(t, err)
		assert.Equal(t, variant.Int16, population.Value.Type())
		assert.Equal(t, int16(6789), population.Value.Value())
	})

	t.Run("inner key outside", func(t *testing.T) {
		// only observation should successfully retrieve key
		observationKeys := []string{"location", "time", "value"}
		observationObj := observation.Value.Value().(variant.ObjectValue)
		speciesObj := species.Value.Value().(variant.ObjectValue)
		for _, k := range observationKeys {
			inner, err := observationObj.ValueByKey(k)
			require.NoError(t, err)
			assert.Equal(t, k, inner.Key)

			_, err = obj.ValueByKey(k)
			require.ErrorIs(t, err, arrow.ErrNotFound)

			_, err = speciesObj.ValueByKey(k)
			require.ErrorIs(t, err, arrow.ErrNotFound)
		}
	})
}

func TestUUID(t *testing.T) {
	emptyMeta := variant.EmptyMetadataBytes[:]
	uuidBytes := []byte{primitiveHeader(variant.PrimitiveUUID),
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}

	v, err := variant.New(emptyMeta, uuidBytes)
	require.NoError(t, err)
	assert.Equal(t, variant.UUID, v.Type())
	assert.Equal(t, uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), v.Value())
}

func TestTimestampNanos(t *testing.T) {
	emptyMeta := variant.EmptyMetadataBytes[:]

	t.Run("ts nanos tz negative", func(t *testing.T) {
		data := []byte{primitiveHeader(variant.PrimitiveTimestampNanos),
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		v, err := variant.New(emptyMeta, data)
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanos, v.Type())
		assert.Equal(t, arrow.Timestamp(-1), v.Value())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `"1969-12-31 23:59:59.999999999Z"`, string(out))
	})

	t.Run("ts nanos tz positive", func(t *testing.T) {
		data := []byte{primitiveHeader(variant.PrimitiveTimestampNanos),
			0x15, 0xC9, 0xBB, 0x86, 0xB4, 0x0C, 0x37, 0x18}
		v, err := variant.New(emptyMeta, data)
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanos, v.Type())
		assert.Equal(t, arrow.Timestamp(1744877350123456789), v.Value())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, `"2025-04-17 08:09:10.123456789Z"`, string(out))
	})

	t.Run("ts nanos ntz positive", func(t *testing.T) {
		data := []byte{primitiveHeader(variant.PrimitiveTimestampNanosNTZ),
			0x15, 0xC9, 0xBB, 0x86, 0xB4, 0x0C, 0x37, 0x18}
		v, err := variant.New(emptyMeta, data)
		require.NoError(t, err)
		assert.Equal(t, variant.TimestampNanosNTZ, v.Type())
		assert.Equal(t, arrow.Timestamp(1744877350123456789), v.Value())

		tm := time.Unix(1744877350123456789/int64(time.Second), 1744877350123456789%int64(time.Second))
		tm = tm.In(time.Local)
		out, err := json.Marshal(v)
		require.NoError(t, err)
		assert.JSONEq(t, tm.Format(`"2006-01-02 15:04:05.999999999Z0700"`), string(out))
	})
}

func TestArrayValues(t *testing.T) {
	t.Run("array primitive", func(t *testing.T) {
		v := loadVariant(t, "array_primitive")
		assert.Equal(t, variant.Array, v.Type())

		arr := v.Value().(variant.ArrayValue)
		assert.EqualValues(t, 4, arr.Len())

		elem0, err := arr.Value(0)
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, elem0.Type())
		assert.Equal(t, int8(2), elem0.Value())

		elem1, err := arr.Value(1)
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, elem1.Type())
		assert.Equal(t, int8(1), elem1.Value())

		elem2, err := arr.Value(2)
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, elem2.Type())
		assert.Equal(t, int8(5), elem2.Value())

		elem3, err := arr.Value(3)
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, elem3.Type())
		assert.Equal(t, int8(9), elem3.Value())

		_, err = arr.Value(4)
		require.ErrorIs(t, err, arrow.ErrIndex)

		out, err := json.Marshal(v)
		require.NoError(t, err)
		expected := `[2,1,5,9]`
		assert.JSONEq(t, expected, string(out))
	})

	t.Run("empty array", func(t *testing.T) {
		v := loadVariant(t, "array_empty")
		assert.Equal(t, variant.Array, v.Type())

		arr := v.Value().(variant.ArrayValue)
		assert.EqualValues(t, 0, arr.Len())
		_, err := arr.Value(0)
		require.ErrorIs(t, err, arrow.ErrIndex)
	})

	t.Run("array nested", func(t *testing.T) {
		v := loadVariant(t, "array_nested")
		assert.Equal(t, variant.Array, v.Type())

		arr := v.Value().(variant.ArrayValue)
		assert.EqualValues(t, 3, arr.Len())

		elem0, err := arr.Value(0)
		require.NoError(t, err)
		assert.Equal(t, variant.Object, elem0.Type())
		elemObj0 := elem0.Value().(variant.ObjectValue)
		assert.EqualValues(t, 2, elemObj0.NumElements())

		id, err := elemObj0.ValueByKey("id")
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, id.Value.Type())
		assert.Equal(t, int8(1), id.Value.Value())

		elem1, err := arr.Value(1)
		require.NoError(t, err)
		assert.Equal(t, variant.Null, elem1.Type())

		elem2, err := arr.Value(2)
		require.NoError(t, err)
		assert.Equal(t, variant.Object, elem2.Type())
		elemObj2 := elem2.Value().(variant.ObjectValue)
		assert.EqualValues(t, 3, elemObj2.NumElements())
		id, err = elemObj2.ValueByKey("id")
		require.NoError(t, err)
		assert.Equal(t, variant.Int8, id.Value.Type())
		assert.Equal(t, int8(2), id.Value.Value())

		out, err := json.Marshal(v)
		require.NoError(t, err)
		expected := `[
			{"id":1, "thing":{"names": ["Contrarian", "Spider"]}},
			null,
			{"id":2, "names": ["Apple", "Ray", null], "type": "if"}
		]`
		assert.JSONEq(t, expected, string(out))
	})
}

func TestInvalidMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
		errMsg   string
	}{
		{
			name:     "empty metadata",
			metadata: []byte{},
			errMsg:   "too short",
		},
		{
			name:     "unsupported version",
			metadata: []byte{0x02, 0x00, 0x00}, // Version != 1 is unsupported
			errMsg:   "unsupported version",
		},
		{
			name:     "truncated metadata",
			metadata: []byte{0x01, 0x05}, // Metadata too short for its header
			errMsg:   "too short",
		},
		{
			name:     "too short for dict size",
			metadata: []byte{0x81, 0x01, 0x00}, // Offset size is 3, not enough bytes
			errMsg:   "too short for dictionary",
		},
		{
			name:     "key count exceeds metadata size",
			metadata: []byte{0x01, 0xFF, 0x00}, // Claims to have many keys but doesn't
			errMsg:   "out of range",
		},
		{
			name:     "string data out of range",
			metadata: []byte{0x01, 0x01, 0x00, 0x05},
			errMsg:   "string data out of range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := variant.NewMetadata(tt.metadata)
			require.Error(t, err)
			assert.ErrorIs(t, err, variant.ErrInvalidMetadata)
			assert.Contains(t, err.Error(), tt.errMsg)

			_, err = variant.New(tt.metadata, []byte{})
			require.Error(t, err)
			assert.ErrorIs(t, err, variant.ErrInvalidMetadata)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestInvalidValue(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
		value    []byte
		errMsg   string
	}{
		{
			name:     "empty value",
			metadata: variant.EmptyMetadataBytes[:],
			value:    []byte{},
			errMsg:   "invalid variant value: empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := variant.New(tt.metadata, tt.value)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestInvalidObjectAccess(t *testing.T) {
	v := loadVariant(t, "object_primitive")
	obj := v.Value().(variant.ObjectValue)

	t.Run("field_at_out_of_bounds", func(t *testing.T) {
		_, err := obj.FieldAt(obj.NumElements())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
		assert.ErrorIs(t, err, arrow.ErrIndex)
	})

	t.Run("corrupt_id", func(t *testing.T) {
		// Create a corrupt variant with invalid field ID
		objBytes := v.Bytes()
		idPosition := 2 // Assumes field ID is at this position - adjust if needed

		// Make a copy so we don't modify the original
		corruptBytes := make([]byte, len(objBytes))
		copy(corruptBytes, objBytes)

		// Set field ID to an invalid value
		corruptBytes[idPosition] = 0xFF

		corrupt, err := variant.NewWithMetadata(v.Metadata(), corruptBytes)
		require.NoError(t, err)

		corruptObj := corrupt.Value().(variant.ObjectValue)
		_, err = corruptObj.FieldAt(0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fieldID")

		_, err = corruptObj.ValueByKey("int_field")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fieldID")
	})
}

func TestInvalidArrayAccess(t *testing.T) {
	v := loadVariant(t, "array_primitive")
	arr := v.Value().(variant.ArrayValue)

	t.Run("out_of_bounds", func(t *testing.T) {
		_, err := arr.Value(arr.Len())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
		assert.ErrorIs(t, err, arrow.ErrIndex)
	})

	t.Run("negative_index", func(t *testing.T) {
		_, err := arr.Value(uint32(math.MaxUint32))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})
}

func TestInvalidBuilderOperations(t *testing.T) {
	t.Run("invalid_object_size", func(t *testing.T) {
		var b variant.Builder
		start := b.Offset()

		// Move offset to before start to create invalid size
		b.AppendInt(123)
		fields := []variant.FieldEntry{{Key: "test", ID: 0, Offset: -10}}

		err := b.FinishObject(start+10, fields)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid object size")
	})

	t.Run("invalid_array_size", func(t *testing.T) {
		var b variant.Builder
		start := b.Offset()

		// Move offset to before start to create invalid size
		b.AppendInt(123)
		offsets := []int{-10}

		err := b.FinishArray(start+10, offsets)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid array size")
	})

}

func TestUnsupportedTypes(t *testing.T) {
	var b variant.Builder

	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name:  "complex number",
			value: complex(1, 2),
		},
		{
			name:  "function",
			value: func() {},
		},
		{
			name:  "channel",
			value: make(chan int),
		},
		{
			name: "map with non-string keys",
			value: map[int]string{
				1: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := b.Append(tt.value)
			require.Error(t, err)
		})
	}
}

func TestDuplicateKeys(t *testing.T) {
	t.Run("disallow_duplicates", func(t *testing.T) {
		var b variant.Builder
		b.SetAllowDuplicates(false) // default, but explicit for test clarity

		start := b.Offset()
		fields := make([]variant.FieldEntry, 0)

		fields = append(fields, b.NextField(start, "key"))
		require.NoError(t, b.AppendInt(1))

		fields = append(fields, b.NextField(start, "key"))
		require.NoError(t, b.AppendInt(2))

		err := b.FinishObject(start, fields)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "disallowed duplicate key")
	})

	t.Run("allow_duplicates", func(t *testing.T) {
		var b variant.Builder
		b.SetAllowDuplicates(true)

		start := b.Offset()
		fields := make([]variant.FieldEntry, 0)

		fields = append(fields, b.NextField(start, "key"))
		require.NoError(t, b.AppendInt(1))

		fields = append(fields, b.NextField(start, "key"))
		require.NoError(t, b.AppendInt(2))

		require.NoError(t, b.FinishObject(start, fields))

		v, err := b.Build()
		require.NoError(t, err)

		obj := v.Value().(variant.ObjectValue)
		field, err := obj.ValueByKey("key")
		require.NoError(t, err)
		assert.Equal(t, int8(2), field.Value.Value())
	})
}

func TestValueCloneConsistency(t *testing.T) {
	var b variant.Builder
	require.NoError(t, b.AppendString("test"))

	v, err := b.Build()
	require.NoError(t, err)

	cloned := v.Clone()

	// Reset should invalidate the original value's buffer
	b.Reset()
	require.NoError(t, b.AppendInt(123))

	// Original value's buffer is now used for something else
	// But the cloned value should still be valid
	assert.Equal(t, variant.String, cloned.Type())
	assert.Equal(t, "test", cloned.Value())
}
