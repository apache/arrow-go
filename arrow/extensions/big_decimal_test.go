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
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	value = strings.ReplaceAll(value, " ", "")
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	return decoded
}

func ptr[T any](value T) *T { return &value }

func TestBigDecimalEncodingExamples(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		encoded    string
		class      extensions.BigDecimalClass
		scale      int16
		stringForm string
	}{
		{"negative signaling NaN", "-sNaN", "00 00 00", extensions.BigDecimalNegativeSignalingNaN, 0, "-sNaN"},
		{"negative quiet NaN", "-NaN", "01 00 00", extensions.BigDecimalNegativeQuietNaN, 0, "-NaN"},
		{"negative infinity", "-Infinity", "02 00 00", extensions.BigDecimalNegativeInfinity, 0, "-Infinity"},
		{"negative finite", "-123", "03 00 00 7B", extensions.BigDecimalNegativeFinite, 0, "-123"},
		{"zero", "0", "04 00 00 00", extensions.BigDecimalPositiveFinite, 0, "0"},
		{"positive finite", "123", "04 00 00 7B", extensions.BigDecimalPositiveFinite, 0, "123"},
		{"million", "1000000", "04 00 00 40 42 0F", extensions.BigDecimalPositiveFinite, 0, "1000000"},
		{"fraction", "12.34", "04 02 00 D2 04", extensions.BigDecimalPositiveFinite, 2, "12.34"},
		{"small fraction", "0.0012", "04 04 00 0C", extensions.BigDecimalPositiveFinite, 4, "0.0012"},
		{"scale zero", "1200", "04 00 00 B0 04", extensions.BigDecimalPositiveFinite, 0, "1200"},
		{"negative scale", "12E2", "04 FE FF 0C", extensions.BigDecimalPositiveFinite, -2, "12E2"},
		{"positive infinity", "Infinity", "05 00 00", extensions.BigDecimalPositiveInfinity, 0, "Infinity"},
		{"positive quiet NaN", "NaN", "06 00 00", extensions.BigDecimalPositiveQuietNaN, 0, "NaN"},
		{"positive signaling NaN", "sNaN", "07 00 00", extensions.BigDecimalPositiveSignalingNaN, 0, "sNaN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := extensions.ParseBigDecimal(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.class, value.Class())
			assert.Equal(t, tt.scale, value.Scale())

			encoded, err := extensions.EncodeBigDecimal(value)
			require.NoError(t, err)
			assert.Equal(t, decodeHex(t, tt.encoded), encoded)
			require.NoError(t, extensions.ValidateBigDecimalEncoding(encoded))

			decoded, err := extensions.DecodeBigDecimal(encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.stringForm, decoded.String())
			assert.Equal(t, tt.scale, decoded.Scale())
		})
	}
}

func TestParseBigDecimalRejectsNonCanonicalSpellings(t *testing.T) {
	for _, value := range []string{
		"-qNaN", "+qNaN", "Inf", "+Inf", "-Inf", "+Infinity", "+NaN", "qNaN", "+sNaN",
		"+1", "01", ".5", "5.", "1e2", "1E+2", "1E-2",
	} {
		t.Run(value, func(t *testing.T) {
			_, err := extensions.ParseBigDecimal(value)
			assert.Error(t, err)
		})
	}
}

func TestBigDecimalEncodingValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		encoded []byte
	}{
		{"short", []byte{4, 0}},
		{"unknown class", []byte{8, 0, 0}},
		{"nonfinite scale", []byte{5, 1, 0}},
		{"nonfinite magnitude", []byte{6, 0, 0, 0}},
		{"finite no magnitude", []byte{4, 0, 0}},
		{"nonminimal magnitude", []byte{4, 0, 0, 1, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Error(t, extensions.ValidateBigDecimalEncoding(tt.encoded))
			_, err := extensions.DecodeBigDecimal(tt.encoded)
			assert.Error(t, err)
		})
	}
}

func TestBigDecimalNegativeZero(t *testing.T) {
	value, err := extensions.NewFiniteBigDecimal(new(big.Int), 2, true)
	require.NoError(t, err)
	assert.Equal(t, "-0.00", value.String())
	encoded, err := extensions.EncodeBigDecimal(value)
	require.NoError(t, err)
	assert.Equal(t, []byte{3, 2, 0, 0}, encoded)
}

func TestBigDecimalTypeMetadata(t *testing.T) {
	metadata := extensions.BigDecimalMetadata{
		MaxPrecision:     ptr[int32](38),
		MaxScale:         ptr[int16](10),
		SupportsInfinity: ptr(true),
		SupportsNaN:      ptr(false),
	}
	typ, err := extensions.NewBigDecimalTypeWithMetadata(metadata)
	require.NoError(t, err)

	assert.Equal(t, arrow.BinaryTypes.BinaryView, typ.StorageType())
	assert.Equal(t, "arrow.big_decimal", typ.ExtensionName())
	assert.Equal(t, `{"max_precision":38,"max_scale":10,"supports_infinity":true,"supports_nan":false}`, typ.Serialize())

	deserialized, err := typ.Deserialize(arrow.BinaryTypes.BinaryView, typ.Serialize())
	require.NoError(t, err)
	assert.True(t, arrow.TypeEqual(typ, deserialized))
	assert.Equal(t, typ.Fingerprint(), deserialized.Fingerprint())

	returned := typ.Metadata()
	*returned.MaxPrecision = 7
	assert.EqualValues(t, 38, *typ.Metadata().MaxPrecision)

	different, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{MaxPrecision: ptr[int32](39)})
	require.NoError(t, err)
	assert.False(t, arrow.TypeEqual(typ, different))
	assert.NotEqual(t, typ.Fingerprint(), different.Fingerprint())

	explicitV1, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{LayoutVersion: 1})
	require.NoError(t, err)
	assert.True(t, arrow.TypeEqual(extensions.NewBigDecimalType(), explicitV1))
	assert.Empty(t, explicitV1.Serialize())

	_, err = typ.Deserialize(arrow.BinaryTypes.Binary, typ.Serialize())
	assert.ErrorContains(t, err, "invalid storage type")
	_, err = typ.Deserialize(arrow.BinaryTypes.BinaryView, `{"layout_version":2}`)
	assert.ErrorContains(t, err, "unsupported BigDecimal layout version")
	_, err = typ.Deserialize(arrow.BinaryTypes.BinaryView, `{"layout_version":0}`)
	assert.ErrorContains(t, err, "unsupported BigDecimal layout version")
	_, err = typ.Deserialize(arrow.BinaryTypes.BinaryView, `{"unknown":1}`)
	assert.ErrorContains(t, err, "unknown field")
	_, err = extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{MaxPrecision: ptr[int32](0)})
	assert.ErrorContains(t, err, "max_precision must be positive")
}

func TestBigDecimalBuilderAndArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := extensions.NewBigDecimalType()
	genericBuilder := array.NewBuilder(mem, typ)
	builder, ok := genericBuilder.(*extensions.BigDecimalBuilder)
	require.True(t, ok)
	defer builder.Release()

	for _, value := range []string{"12.34", "-123", "12E2", "NaN"} {
		require.NoError(t, builder.AppendValueFromString(value))
	}
	builder.AppendNull()

	largeMagnitude := new(big.Int).Lsh(big.NewInt(1), 72)
	large, err := extensions.NewFiniteBigDecimal(largeMagnitude, 0, false)
	require.NoError(t, err)
	require.NoError(t, builder.Append(large))

	arr := builder.NewBigDecimalArray()
	defer arr.Release()
	require.NoError(t, array.ValidateFull(arr))
	assert.Equal(t, 6, arr.Len())
	assert.Equal(t, 1, arr.NullN())
	assert.Equal(t, "12.34", arr.ValueStr(0))
	assert.Equal(t, "-123", arr.ValueStr(1))
	assert.Equal(t, int16(-2), arr.Value(2).Scale())
	assert.Equal(t, "NaN", arr.ValueStr(3))
	assert.Equal(t, array.NullValueStr, arr.ValueStr(4))

	storage := arr.Storage().(*array.BinaryView)
	assert.True(t, storage.ValueHeader(0).IsInline())
	assert.False(t, storage.ValueHeader(5).IsInline())

	encodedJSON, err := arr.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, `["12.34","-123","12E2","NaN",null,"4722366482869645213696"]`, string(encodedJSON))
	roundTripped, _, err := array.FromJSON(mem, typ, bytes.NewReader(encodedJSON))
	require.NoError(t, err)
	defer roundTripped.Release()
	assert.True(t, array.Equal(arr, roundTripped))
}

func TestBigDecimalArrayValidationAndInformationalMetadata(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{
		MaxPrecision:     ptr[int32](3),
		MaxScale:         ptr[int16](2),
		SupportsInfinity: ptr(false),
		SupportsNaN:      ptr(false),
	})
	require.NoError(t, err)
	builder := extensions.NewBigDecimalBuilder(mem, typ)
	defer builder.Release()

	// Metadata describes the source schema but does not restrict row data.
	for _, value := range []string{"9.99", "10.00", "0.001", "Infinity", "NaN"} {
		assert.NoError(t, builder.AppendValueFromString(value), value)
	}

	storageBuilder := array.NewBinaryViewBuilder(mem)
	storageBuilder.Append([]byte{4, 0, 0})
	storage := storageBuilder.NewBinaryViewArray()
	storageBuilder.Release()
	defer storage.Release()

	invalid := array.NewExtensionArrayWithStorage(typ, storage)
	defer invalid.Release()
	assert.ErrorContains(t, array.ValidateFull(invalid), "finite BigDecimal values must have a magnitude")

	outOfRange, err := extensions.ParseBigDecimal("1001")
	require.NoError(t, err)
	encoded, err := extensions.EncodeBigDecimal(outOfRange)
	require.NoError(t, err)
	metadataStorageBuilder := array.NewBinaryViewBuilder(mem)
	metadataStorageBuilder.Append(encoded)
	metadataStorage := metadataStorageBuilder.NewBinaryViewArray()
	metadataStorageBuilder.Release()
	defer metadataStorage.Release()
	metadataArray := array.NewExtensionArrayWithStorage(typ, metadataStorage)
	defer metadataArray.Release()
	assert.NoError(t, array.ValidateFull(metadataArray))
}

func TestBigDecimalMetadataDoesNotOverrideEquivalentRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{
		MaxPrecision: ptr[int32](3),
		MaxScale:     ptr[int16](1),
	})
	require.NoError(t, err)
	builder := extensions.NewBigDecimalBuilder(mem, typ)
	defer builder.Release()

	for _, value := range []string{"1200", "12E2", "5", "5.00"} {
		assert.NoError(t, builder.AppendValueFromString(value), value)
	}
}

func TestBigDecimalExactDecimalConversions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := extensions.NewBigDecimalBuilder(mem, extensions.NewBigDecimalType())
	defer builder.Release()
	for _, value := range []string{"12.34", "12E2", "-123", "12.340"} {
		require.NoError(t, builder.AppendValueFromString(value))
	}
	builder.AppendNull()
	arr := builder.NewBigDecimalArray()
	defer arr.Release()

	decimal128Arr, err := arr.ToDecimal128(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
	require.NoError(t, err)
	defer decimal128Arr.Release()
	assert.Equal(t, "1234", decimal128Arr.Value(0).BigInt().String())
	assert.Equal(t, "120000", decimal128Arr.Value(1).BigInt().String())
	assert.Equal(t, "-12300", decimal128Arr.Value(2).BigInt().String())
	assert.Equal(t, "1234", decimal128Arr.Value(3).BigInt().String())
	assert.True(t, decimal128Arr.IsNull(4))

	decimal256Arr, err := arr.ToDecimal256(mem, &arrow.Decimal256Type{Precision: 20, Scale: 2})
	require.NoError(t, err)
	defer decimal256Arr.Release()
	assert.Equal(t, "120000", decimal256Arr.Value(1).BigInt().String())

	lossBuilder := extensions.NewBigDecimalBuilder(mem, extensions.NewBigDecimalType())
	defer lossBuilder.Release()
	require.NoError(t, lossBuilder.AppendValueFromString("12.345"))
	lossArr := lossBuilder.NewBigDecimalArray()
	defer lossArr.Release()
	_, err = lossArr.ToDecimal128(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
	assert.ErrorContains(t, err, "would lose data")

	overflowBuilder := extensions.NewBigDecimalBuilder(mem, extensions.NewBigDecimalType())
	defer overflowBuilder.Release()
	require.NoError(t, overflowBuilder.AppendValueFromString("1000"))
	overflowArr := overflowBuilder.NewBigDecimalArray()
	defer overflowArr.Release()
	_, err = overflowArr.ToDecimal128(mem, &arrow.Decimal128Type{Precision: 3, Scale: 0})
	assert.ErrorContains(t, err, "does not fit decimal precision")

	nonfiniteBuilder := extensions.NewBigDecimalBuilder(mem, extensions.NewBigDecimalType())
	defer nonfiniteBuilder.Release()
	require.NoError(t, nonfiniteBuilder.AppendValueFromString("Infinity"))
	nonfiniteArr := nonfiniteBuilder.NewBigDecimalArray()
	defer nonfiniteArr.Release()
	_, err = nonfiniteArr.ToDecimal256(mem, &arrow.Decimal256Type{Precision: 10, Scale: 0})
	assert.ErrorContains(t, err, "cannot convert non-finite")

	largeBuilder := extensions.NewBigDecimalBuilder(mem, extensions.NewBigDecimalType())
	defer largeBuilder.Release()
	largeValue := strings.Repeat("9", 50)
	require.NoError(t, largeBuilder.AppendValueFromString(largeValue))
	largeArr := largeBuilder.NewBigDecimalArray()
	defer largeArr.Release()
	largeDecimal, err := largeArr.ToDecimal256(mem, &arrow.Decimal256Type{Precision: 60, Scale: 0})
	require.NoError(t, err)
	defer largeDecimal.Release()
	assert.Equal(t, largeValue, largeDecimal.Value(0).BigInt().String())
}

func TestBigDecimalIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{
		MaxPrecision: ptr[int32](38),
		MaxScale:     ptr[int16](10),
		SupportsNaN:  ptr(true),
	})
	require.NoError(t, err)
	builder := extensions.NewBigDecimalBuilder(mem, typ)
	require.NoError(t, builder.AppendValueFromString("12.34"))
	builder.AppendNull()
	require.NoError(t, builder.AppendValueFromString("NaN"))
	arr := builder.NewBigDecimalArray()
	builder.Release()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: typ, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{arr}, -1)
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithAllocator(mem), ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader, err := ipc.NewReader(&buf, ipc.WithAllocator(mem))
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	actual := reader.RecordBatch()
	assert.True(t, schema.Equal(actual.Schema()))
	assert.True(t, array.RecordEqual(record, actual))
	assert.IsType(t, &extensions.BigDecimalArray{}, actual.Column(0))
}

func TestBigDecimalRegistered(t *testing.T) {
	registered := arrow.GetExtensionType("arrow.big_decimal")
	require.NotNil(t, registered)
	assert.IsType(t, &extensions.BigDecimalType{}, registered)
}

func TestBigDecimalIPCUnregisteredFallback(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ, err := extensions.NewBigDecimalTypeWithMetadata(extensions.BigDecimalMetadata{
		MaxPrecision: ptr[int32](38),
		MaxScale:     ptr[int16](10),
	})
	require.NoError(t, err)
	builder := extensions.NewBigDecimalBuilder(mem, typ)
	require.NoError(t, builder.AppendValueFromString("12.34"))
	arr := builder.NewBigDecimalArray()
	builder.Release()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: typ}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{arr}, -1)
	defer record.Release()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithAllocator(mem), ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	require.NoError(t, arrow.UnregisterExtensionType(typ.ExtensionName()))
	defer func() { require.NoError(t, arrow.RegisterExtensionType(extensions.NewBigDecimalType())) }()

	reader, err := ipc.NewReader(&buf, ipc.WithAllocator(mem))
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	actual := reader.RecordBatch()
	field := actual.Schema().Field(0)
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.BinaryView, field.Type))
	extensionName, ok := field.Metadata.GetValue(ipc.ExtensionTypeKeyName)
	require.True(t, ok)
	assert.Equal(t, typ.ExtensionName(), extensionName)
	extensionMetadata, ok := field.Metadata.GetValue(ipc.ExtensionMetadataKeyName)
	require.True(t, ok)
	assert.Equal(t, typ.Serialize(), extensionMetadata)
	assert.True(t, array.Equal(arr.Storage(), actual.Column(0)))
}
