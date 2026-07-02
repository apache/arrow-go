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

var epoch = time.Unix(0, 0).In(time.FixedZone("UTC+00:00", 0))

var testDate0 = time.Date(2025, 01, 01, 00, 00, 00, 00, time.FixedZone("UTC+00:00", 0))

var testZone1 = time.FixedZone("UTC-08:30", -8*60*60-30*60)
var testDate1 = testDate0.In(testZone1)

var testZone2 = time.FixedZone("UTC+11:00", +11*60*60)
var testDate2 = testDate0.In(testZone2)

func dict(index arrow.DataType) arrow.DataType {
	return &arrow.DictionaryType{
		IndexType: index,
		ValueType: arrow.PrimitiveTypes.Int16,
		Ordered:   false,
	}
}

func ree(runEnds arrow.DataType) arrow.DataType {
	v := arrow.RunEndEncodedOf(runEnds, arrow.PrimitiveTypes.Int16)
	v.ValueNullable = false
	return v
}

// All tests use this in a for loop to make sure everything works for every possible
// encoding of offsets (primitive, dictionary, run-end)
var allAllowedOffsetTypes = make(map[string]arrow.DataType)

func init() {
	// primitive offsetType
	allAllowedOffsetTypes["primitive-int16"] = arrow.PrimitiveTypes.Int16

	// dict-encoded offsetType
	allAllowedOffsetTypes["dict-Uint8"] = dict(arrow.PrimitiveTypes.Uint8)
	allAllowedOffsetTypes["dict-Uint16"] = dict(arrow.PrimitiveTypes.Uint16)
	allAllowedOffsetTypes["dict-Uint32"] = dict(arrow.PrimitiveTypes.Uint32)
	allAllowedOffsetTypes["dict-Uint64"] = dict(arrow.PrimitiveTypes.Uint64)
	allAllowedOffsetTypes["dict-Int8"] = dict(arrow.PrimitiveTypes.Int8)
	allAllowedOffsetTypes["dict-Int16"] = dict(arrow.PrimitiveTypes.Int16)
	allAllowedOffsetTypes["dict-Int32"] = dict(arrow.PrimitiveTypes.Int32)
	allAllowedOffsetTypes["dict-Int64"] = dict(arrow.PrimitiveTypes.Int64)

	// run-end encoded offsetType
	allAllowedOffsetTypes["ree-Int16"] = ree(arrow.PrimitiveTypes.Int16)
	allAllowedOffsetTypes["ree-Int32"] = ree(arrow.PrimitiveTypes.Int32)
	allAllowedOffsetTypes["ree-Int64"] = ree(arrow.PrimitiveTypes.Int64)
}

func TestTimestampWithOffsetTypePrimitiveBasics(t *testing.T) {
	typ := extensions.NewTimestampWithOffsetType(testTimeUnit)

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

func assertDictBasics[I extensions.DictIndexType](t *testing.T, indexType I) {
	typ := extensions.NewTimestampWithOffsetTypeDictionaryEncoded(testTimeUnit, indexType)

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
				Type:     dict(arrow.DataType(indexType)),
				Nullable: false,
			},
		),
		typ.StorageType()))

	assert.Equal(t, "extension<arrow.timestamp_with_offset>", typ.String())
}

func TestTimestampWithOffsetTypeDictionaryEncodedBasics(t *testing.T) {
	assertDictBasics(t, &arrow.Uint8Type{})
	assertDictBasics(t, &arrow.Uint16Type{})
	assertDictBasics(t, &arrow.Uint32Type{})
	assertDictBasics(t, &arrow.Uint64Type{})
	assertDictBasics(t, &arrow.Int8Type{})
	assertDictBasics(t, &arrow.Int16Type{})
	assertDictBasics(t, &arrow.Int32Type{})
	assertDictBasics(t, &arrow.Int64Type{})
}

func assertReeBasics[E extensions.RunEndsType](t *testing.T, runEndsType E) {
	typ := extensions.NewTimestampWithOffsetTypeRunEndEncoded(testTimeUnit, runEndsType)

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
				Type:     ree(arrow.DataType(runEndsType)),
				Nullable: false,
			},
		),
		typ.StorageType()))

	assert.Equal(t, "extension<arrow.timestamp_with_offset>", typ.String())
}

func TestTimestampWithOffsetTypeRunEndEncodedBasics(t *testing.T) {
	assertReeBasics(t, &arrow.Int16Type{})
	assertReeBasics(t, &arrow.Int32Type{})
	assertReeBasics(t, &arrow.Int64Type{})
}

func TestTimestampWithOffsetEquals(t *testing.T) {
	// Completely different types are not equal
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Nanosecond).ExtensionEquals(extensions.NewBool8Type()))

	// Different time units are not equal
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Nanosecond).ExtensionEquals(extensions.NewTimestampWithOffsetType(arrow.Microsecond)))
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Nanosecond).ExtensionEquals(extensions.NewTimestampWithOffsetType(arrow.Second)))
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Microsecond).ExtensionEquals(extensions.NewTimestampWithOffsetType(arrow.Second)))

	// Different underlying storage type is not equal
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Microsecond).ExtensionEquals(extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Int16Type{})))
	assert.False(t, extensions.NewTimestampWithOffsetType(arrow.Microsecond).ExtensionEquals(extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int16Type{})))
	assert.False(t, extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Int16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int16Type{})))

	// Dict-encoding key type is not equal
	assert.False(t, extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Int16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Uint16Type{})))

	// REE index type is not equal
	assert.False(t, extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int32Type{})))

	// Equals OK
	assert.True(t, extensions.NewTimestampWithOffsetType(arrow.Nanosecond).ExtensionEquals(extensions.NewTimestampWithOffsetType(arrow.Nanosecond)))
	assert.True(t, extensions.NewTimestampWithOffsetType(arrow.Microsecond).ExtensionEquals(extensions.NewTimestampWithOffsetType(arrow.Microsecond)))
	assert.True(t, extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Int16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Int16Type{})))
	assert.True(t, extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Uint16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeDictionaryEncoded(arrow.Microsecond, &arrow.Uint16Type{})))
	assert.True(t, extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int16Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int16Type{})))
	assert.True(t, extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int32Type{}).ExtensionEquals(extensions.NewTimestampWithOffsetTypeRunEndEncoded(arrow.Microsecond, &arrow.Int32Type{})))
}

func TestTimestampWithOffsetExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			builder, _ := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)

			builder.Append(testDate0)
			builder.AppendNull()
			builder.Append(testDate1)
			builder.Append(testDate2)
			builder.Append(epoch)

			// it should build the array with the correct size
			arr := builder.NewArray()
			typedArr := arr.(*extensions.TimestampWithOffsetArray)
			assert.Equal(t, 5, arr.Data().Len())
			defer arr.Release()

			// typedArr.Value(i) should return values adjusted for their original timezone
			assert.Equal(t, testDate0, typedArr.Value(0))
			assert.Equal(t, testDate1, typedArr.Value(2))
			assert.Equal(t, testDate2, typedArr.Value(3))
			assert.Equal(t, epoch, typedArr.Value(4))

			// storage TimeUnit should be the same as we pass in to the builder, and storage timezone should be UTC
			timestampStructField := typedArr.Storage().(*array.Struct).Field(0)
			timestampStructDataType := timestampStructField.DataType().(*arrow.TimestampType)
			assert.Equal(t, timestampStructDataType.Unit, testTimeUnit)
			assert.Equal(t, timestampStructDataType.TimeZone, "UTC")

			// stored values should be equivalent to the raw values in UTC
			timestampsArr := timestampStructField.(*array.Timestamp)
			assert.Equal(t, testDate0.In(time.UTC), timestampsArr.Value(0).ToTime(testTimeUnit))
			assert.Equal(t, testDate1.In(time.UTC), timestampsArr.Value(2).ToTime(testTimeUnit))
			assert.Equal(t, testDate2.In(time.UTC), timestampsArr.Value(3).ToTime(testTimeUnit))
			assert.Equal(t, epoch.In(time.UTC), timestampsArr.Value(4).ToTime(testTimeUnit))

			// the array should encode itself as JSON and string
			arrStr := arr.String()
			assert.Equal(t, fmt.Sprintf(`["%[1]s" (null) "%[2]s" "%[3]s" "%[4]s"]`, testDate0, testDate1, testDate2, epoch), arrStr)
			jsonStr, err := json.Marshal(arr)
			assert.NoError(t, err)

			// roundtripping from JSON with array.FromJSON should work
			expectedDataType, _ := extensions.NewTimestampWithOffsetTypeCustomOffset(testTimeUnit, offsetType)
			roundtripped, _, err := array.FromJSON(mem, expectedDataType, bytes.NewReader(jsonStr))
			defer roundtripped.Release()
			assert.NoError(t, err)
			assert.Truef(t, array.Equal(arr, roundtripped), "expected %s\n\ngot %s", arr, roundtripped)
		})
	}
}

func TestTimestampWithOffsetBuilderAppendValuesLeadingNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			builder, err := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)
			require.NoError(t, err)

			// A leading null as the very first operation on a fresh builder must not
			// corrupt the offsets. Reading every value below would panic on a
			// run-end encoded array whose run-ends and values children fell out of
			// sync (which happened when a run was continued before one existed).
			builder.AppendValues([]time.Time{epoch, testDate1, testDate2}, []bool{false, true, true})

			arr := builder.NewArray()
			defer arr.Release()
			typedArr := arr.(*extensions.TimestampWithOffsetArray)

			require.Equal(t, 3, arr.Data().Len())
			assert.True(t, typedArr.IsNull(0))
			assert.Equal(t, testDate1, typedArr.Value(1))
			assert.Equal(t, testDate2, typedArr.Value(2))
		})
	}
}

func TestTimestampWithOffsetBuilderRunEndEncodedNullContinuesRun(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder, err := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, ree(arrow.PrimitiveTypes.Int16))
	require.NoError(t, err)

	// The same offset surrounds a null. The null must continue the current run
	// rather than reset the tracked offset, otherwise one logical run is split
	// into two adjacent runs that share the same value.
	builder.AppendValues([]time.Time{testDate1, epoch, testDate1}, []bool{true, false, true})

	arr := builder.NewArray()
	defer arr.Release()
	typedArr := arr.(*extensions.TimestampWithOffsetArray)

	offsets := typedArr.Storage().(*array.Struct).Field(1).(*array.RunEndEncoded)
	assert.Equal(t, 1, offsets.Values().Len())

	assert.Equal(t, testDate1, typedArr.Value(0))
	assert.True(t, typedArr.IsNull(1))
	assert.Equal(t, testDate1, typedArr.Value(2))
}

func TestTimestampWithOffsetBuilderAppendValuesNilValids(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			builder, err := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)
			require.NoError(t, err)

			// nil validity must be treated as all-valid so the parent struct and
			// its child builders stay the same length.
			builder.AppendValues([]time.Time{testDate0, testDate1, testDate2}, nil)

			arr := builder.NewArray()
			defer arr.Release()
			typedArr := arr.(*extensions.TimestampWithOffsetArray)

			require.Equal(t, 3, arr.Data().Len())
			assert.Equal(t, testDate0, typedArr.Value(0))
			assert.Equal(t, testDate1, typedArr.Value(1))
			assert.Equal(t, testDate2, typedArr.Value(2))
		})
	}
}

func TestTimestampWithOffsetBuilderReuseAfterNewArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			builder, err := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)
			require.NoError(t, err)

			builder.Append(testDate1)
			arr1 := builder.NewArray()
			defer arr1.Release()

			// Reusing the builder must start a fresh run even when the first value
			// repeats the previous array's offset; otherwise a run-end-encoded
			// offset is continued on a freshly reset (empty) builder.
			builder.Append(testDate1)
			builder.Append(testDate2)
			arr2 := builder.NewArray()
			defer arr2.Release()

			typedArr := arr2.(*extensions.TimestampWithOffsetArray)
			require.Equal(t, 2, arr2.Data().Len())
			assert.Equal(t, testDate1, typedArr.Value(0))
			assert.Equal(t, testDate2, typedArr.Value(1))
		})
	}
}

func TestTimestampWithOffsetExtensionRecordBuilder(t *testing.T) {
	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			dataType, _ := extensions.NewTimestampWithOffsetTypeCustomOffset(testTimeUnit, offsetType)
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
			fieldBuilder.Append(testDate0)

			// append the epoch
			fieldBuilder.Append(epoch)

			// append a null and 2 time.Time all at once
			values := []time.Time{
				time.Unix(0, 0).In(time.UTC),
				testDate1,
				testDate2,
			}
			valids := []bool{false, true, true}
			fieldBuilder.AppendValues(values, valids)

			// append a value from RFC3339 string
			fieldBuilder.AppendValueFromString(testDate0.Format(time.RFC3339))

			// append value formatted in a different string layout
			fieldBuilder.Layout = time.RFC3339Nano
			fieldBuilder.AppendValueFromString(testDate1.Format(time.RFC3339Nano))

			record := builder.NewRecordBatch()

			// Record batch should JSON-encode values containing per-row timezone info
			json, err := record.MarshalJSON()
			require.NoError(t, err)
			expect := `[{"timestamp_with_offset":"2025-01-01T00:00:00Z"}
,{"timestamp_with_offset":"1970-01-01T00:00:00Z"}
,{"timestamp_with_offset":null}
,{"timestamp_with_offset":"2024-12-31T15:30:00-08:30"}
,{"timestamp_with_offset":"2025-01-01T11:00:00+11:00"}
,{"timestamp_with_offset":"2025-01-01T00:00:00Z"}
,{"timestamp_with_offset":"2024-12-31T15:30:00-08:30"}
]`
			require.Equal(t, expect, string(json))

			// Record batch roundtrip to JSON should work
			roundtripped, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(json))
			require.NoError(t, err)
			defer roundtripped.Release()
			require.Equal(t, schema, roundtripped.Schema())
			assert.Truef(t, array.RecordEqual(record, roundtripped), "expected %s\n\ngot %s", record, roundtripped)
		})
	}
}

func TestTimestampWithOffsetTypeBatchIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	for name, offsetType := range allAllowedOffsetTypes {
		t.Run(name, func(t *testing.T) {
			builder, _ := extensions.NewTimestampWithOffsetBuilder(mem, testTimeUnit, offsetType)
			builder.Append(testDate0)
			builder.AppendNull()
			builder.Append(testDate1)
			builder.Append(testDate2)
			builder.Append(epoch)
			arr := builder.NewArray()
			defer arr.Release()

			typ, _ := extensions.NewTimestampWithOffsetTypeCustomOffset(testTimeUnit, offsetType)

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
		})
	}
}
