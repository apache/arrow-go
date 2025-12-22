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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTimeUnit = arrow.Microsecond

var testDate1 = time.Date(2025, 01, 01, 00, 00, 00, 00, time.FixedZone("UTC+00:00", 0))

var testZone1 = time.FixedZone("UTC-08:00", -8*60*60)
var testDate2 = testDate1.In(testZone1)

var testZone2 = time.FixedZone("UTC+06:00", +6*60*60)
var testDate3 = testDate1.In(testZone2)

func dict(index arrow.DataType) arrow.DataType {
	return &arrow.DictionaryType{
		IndexType: index,
		ValueType: arrow.PrimitiveTypes.Int16,
		Ordered: false,
	}
}

func ree(runEnds arrow.DataType) arrow.DataType {
	v := arrow.RunEndEncodedOf(runEnds, arrow.PrimitiveTypes.Int16)
	v.ValueNullable = false
	return v
}

// All tests use this in a for loop to make sure everything works for every possible
// encoding of offsets (primitive, dictionary, run-end)
var allAllowedOffsetTypes = []arrow.DataType{
	// primitive offsetType
	arrow.PrimitiveTypes.Int16,

	// dict-encoded offsetType
	dict(arrow.PrimitiveTypes.Uint8),
	dict(arrow.PrimitiveTypes.Uint16),
	dict(arrow.PrimitiveTypes.Uint32),
	dict(arrow.PrimitiveTypes.Uint64),
	dict(arrow.PrimitiveTypes.Int8),
	dict(arrow.PrimitiveTypes.Int16),
	dict(arrow.PrimitiveTypes.Int32),
	dict(arrow.PrimitiveTypes.Int64),

	// run-end encoded offsetType
	ree(arrow.PrimitiveTypes.Int16),
	ree(arrow.PrimitiveTypes.Int32),
	ree(arrow.PrimitiveTypes.Int64),
}

func TestTimestampWithOffsetTypePrimitiveBasics(t *testing.T) {
	typ := extensions.NewTimestampWithOffsetTypePrimitiveEncoded(testTimeUnit)

	assert.Equal(t, "arrow.timestamp_with_offset", typ.ExtensionName())
	assert.True(t, typ.ExtensionEquals(typ))

	assert.True(t, arrow.TypeEqual(typ, typ))
	assert.True(t, arrow.TypeEqual(
		arrow.StructOf(
			arrow.Field{
				Name: "timestamp",
				Type: &arrow.TimestampType{
					Unit:     testTimeUnit,
					TimeZone: "UTC",
				},
				Nullable: false,
			},
			arrow.Field{
				Name:     "offset_minutes",
				Type:     arrow.PrimitiveTypes.Int16,
				Nullable: false,
			},
		),
		typ.StorageType()))

	assert.Equal(t, "extension<arrow.timestamp_with_offset>", typ.String())
}

func TestTimestampWithOffsetTypeDictionaryEncodedBasics(t *testing.T) {
	invalidIndexType := arrow.PrimitiveTypes.Float32
	_, err := extensions.NewTimestampWithOffsetTypeDictionaryEncoded(testTimeUnit, invalidIndexType)
	assert.True(t, err != nil, "Err should not be nil if index type is invalid dict key")

	indexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
	};

	for _, indexType := range indexTypes {
		typ, err := extensions.NewTimestampWithOffsetTypeDictionaryEncoded(testTimeUnit, indexType)
		assert.True(t, err == nil, "Err should be nil")

		assert.Equal(t, "arrow.timestamp_with_offset", typ.ExtensionName())
		assert.True(t, typ.ExtensionEquals(typ))

		assert.True(t, arrow.TypeEqual(typ, typ))
		assert.True(t, arrow.TypeEqual(
			arrow.StructOf(
				arrow.Field{
					Name: "timestamp",
					Type: &arrow.TimestampType{
						Unit:     testTimeUnit,
						TimeZone: "UTC",
					},
					Nullable: false,
				},
				arrow.Field{
					Name: "offset_minutes",
					Type: dict(indexType),
					Nullable: false,
				},
			),
			typ.StorageType()))

		assert.Equal(t, "extension<arrow.timestamp_with_offset>", typ.String())
	}
}

func TestTimestampWithOffsetTypeRunEndEncodedBasics(t *testing.T) {
	invalidRunEndsType := arrow.PrimitiveTypes.Float32
	_, err := extensions.NewTimestampWithOffsetTypeRunEndEncoded(testTimeUnit, invalidRunEndsType)
	assert.True(t, err != nil, "Err should not be nil if run ends type is invalid")

	runEndsTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
	};

	for _, indexType := range runEndsTypes {
		typ, err := extensions.NewTimestampWithOffsetTypeRunEndEncoded(testTimeUnit, indexType)
		assert.True(t, err == nil, "Err should be nil")

		assert.Equal(t, "arrow.timestamp_with_offset", typ.ExtensionName())
		assert.True(t, typ.ExtensionEquals(typ))

		assert.True(t, arrow.TypeEqual(typ, typ))
		assert.True(t, arrow.TypeEqual(
			arrow.StructOf(
				arrow.Field{
					Name: "timestamp",
					Type: &arrow.TimestampType{
						Unit:     testTimeUnit,
						TimeZone: "UTC",
					},
					Nullable: false,
				},
				arrow.Field{
					Name: "offset_minutes",
					Type: ree(indexType),
					Nullable: false,
				},
			),
			typ.StorageType()))

		assert.Equal(t, "extension<arrow.timestamp_with_offset>", typ.String())
	}
}

func TestTimestampWithOffsetExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// NOTE: we need to compare the arrays parsed from JSON with a primitive-encoded array, since that will always
	// use that encoding (there is no way to pass a flag to array.FromJSON to say explicitly what storage type you want)
	primitiveBuilder, err := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, arrow.PrimitiveTypes.Int16)
	assert.NoError(t, err)
	primitiveBuilder.Append(testDate1)
	primitiveBuilder.AppendNull()
	primitiveBuilder.Append(testDate2)
	primitiveBuilder.Append(testDate3)
	jsonComparisonArr := primitiveBuilder.NewArray()
	defer jsonComparisonArr.Release()

	for _, offsetType := range allAllowedOffsetTypes {
		builder, _ := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)

		builder.Append(testDate1)
		builder.AppendNull()
		builder.Append(testDate2)
		builder.Append(testDate3)

		// it should build the array with the correct size
		arr := builder.NewArray()
		typedArr := arr.(*extensions.TimestampWithOffsetArray)
		assert.Equal(t, 4, arr.Data().Len())
		defer arr.Release()

		// typedArr.Value(i) should return values adjusted for their original timezone
		assert.Equal(t, testDate1, typedArr.Value(0))
		assert.Equal(t, testDate2, typedArr.Value(2))
		assert.Equal(t, testDate3, typedArr.Value(3))

		// storage TimeUnit should be the same as we pass in to the builder, and storage timezone should be UTC
		timestampStructField := typedArr.Storage().(*array.Struct).Field(0)
		timestampStructDataType := timestampStructField.DataType().(*arrow.TimestampType)
		assert.Equal(t, timestampStructDataType.Unit, testTimeUnit)
		assert.Equal(t, timestampStructDataType.TimeZone, "UTC")

		// stored values should be equivalent to the raw values in UTC
		timestampsArr := timestampStructField.(*array.Timestamp)
		assert.Equal(t, testDate1.In(time.UTC), timestampsArr.Value(0).ToTime(testTimeUnit))
		assert.Equal(t, testDate2.In(time.UTC), timestampsArr.Value(2).ToTime(testTimeUnit))
		assert.Equal(t, testDate3.In(time.UTC), timestampsArr.Value(3).ToTime(testTimeUnit))

		// the array should encode itself as JSON and string
		arrStr := arr.String()
		assert.Equal(t, fmt.Sprintf(`["%[1]s" (null) "%[2]s" "%[3]s"]`, testDate1, testDate2, testDate3), arrStr)
		jsonStr, err := json.Marshal(arr)
		assert.NoError(t, err)

		// roundtripping from JSON with array.FromJSON should work
		expectedDataType, _ := extensions.NewTimestampWithOffsetType(testTimeUnit, offsetType)
		roundtripped, _, err := array.FromJSON(mem, expectedDataType, bytes.NewReader(jsonStr))
		defer roundtripped.Release()
		assert.NoError(t, err)
		assert.Truef(t, array.Equal(jsonComparisonArr, roundtripped), "expected %s\n\ngot %s", jsonComparisonArr, roundtripped)
	}
}

func TestTimestampWithOffsetExtensionRecordBuilder(t *testing.T) {
	for _, offsetType := range allAllowedOffsetTypes {
		dataType, _ := extensions.NewTimestampWithOffsetType(testTimeUnit, offsetType)
		schema := arrow.NewSchema([]arrow.Field{
			{
				Name:     "timestamp_with_offset",
				Nullable: true,
				Type:     dataType,
			},
		}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()

		fieldBuilder := builder.Field(0).(*extensions.TimestampWithOffsetBuilder)

		// append a simple time.Time
		fieldBuilder.Append(testDate1)

		// append a null and 2 time.Time all at once
		values := []time.Time{
			time.Unix(0, 0).In(time.UTC),
			testDate2,
			testDate3,
		}
		valids := []bool{false, true, true}
		fieldBuilder.AppendValues(values, valids)

		// append a value from RFC3339 string
		fieldBuilder.AppendValueFromString(testDate1.Format(time.RFC3339))

		// append value formatted in a different string layout
		fieldBuilder.Layout = time.RFC3339Nano
		fieldBuilder.AppendValueFromString(testDate2.Format(time.RFC3339Nano))

		record := builder.NewRecordBatch()

		// Record batch should JSON-encode values containing per-row timezone info
		json, err := record.MarshalJSON()
		require.NoError(t, err)
		expect := `[{"timestamp_with_offset":"2025-01-01T00:00:00Z"}
,{"timestamp_with_offset":null}
,{"timestamp_with_offset":"2024-12-31T16:00:00-08:00"}
,{"timestamp_with_offset":"2025-01-01T06:00:00+06:00"}
,{"timestamp_with_offset":"2025-01-01T00:00:00Z"}
,{"timestamp_with_offset":"2024-12-31T16:00:00-08:00"}
]`
		require.Equal(t, expect, string(json))

		// Record batch roundtrip to JSON should work
		roundtripped, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(json))
		require.NoError(t, err)
		defer roundtripped.Release()
		require.Equal(t, schema, roundtripped.Schema())
		assert.Truef(t, array.RecordEqual(record, roundtripped), "expected %s\n\ngot %s", record, roundtripped)
	}
}

func TestTimestampWithOffsetTypeBatchIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for _, offsetType := range allAllowedOffsetTypes {
		builder, _ := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)
		builder.Append(testDate1)
		builder.AppendNull()
		builder.Append(testDate2)
		builder.Append(testDate3)
		arr := builder.NewArray()
		defer arr.Release()

		typ, _ := extensions.NewTimestampWithOffsetType(testTimeUnit, offsetType)

		batch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "timestamp_with_offset", Type: typ, Nullable: true}}, nil), []arrow.Array{arr}, -1)
		defer batch.Release()

		var written arrow.RecordBatch
		{
			var buf bytes.Buffer
			wr := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
			require.NoError(t, wr.Write(batch))
			require.NoError(t, wr.Close())

			rdr, err := ipc.NewReader(&buf)
			require.NoError(t, err)
			written, err = rdr.Read()
			require.NoError(t, err)
			written.Retain()
			defer written.Release()
			rdr.Release()
		}

		assert.Truef(t, batch.Schema().Equal(written.Schema()), "expected: %s\n\ngot: %s",
			batch.Schema(), written.Schema())

		assert.Truef(t, array.RecordEqual(batch, written), "expected: %s\n\ngot: %s",
			batch, written)
	}
}
