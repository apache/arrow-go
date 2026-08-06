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

package pqarrow_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeDecimalArray(t *testing.T, kind, value string) arrow.Array {
	t.Helper()
	n, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok)

	switch kind {
	case "decimal128":
		builder := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 38, Scale: 0})
		defer builder.Release()
		builder.Append(decimal128.FromBigInt(n))
		return builder.NewDecimal128Array()
	case "decimal256":
		builder := array.NewDecimal256Builder(memory.DefaultAllocator, &arrow.Decimal256Type{Precision: 76, Scale: 0})
		defer builder.Release()
		builder.Append(decimal256.FromBigInt(n))
		return builder.NewDecimal256Array()
	default:
		t.Fatalf("unknown decimal kind %q", kind)
		return nil
	}
}

func writeDecimalInteger(arr arrow.Array, physical parquet.Type, precision int32) error {
	_, err := writeDecimalIntegerData(arr, physical, precision, parquet.Repetitions.Required)
	return err
}

func writeDecimalIntegerData(arr arrow.Array, physical parquet.Type, precision int32, repetition parquet.Repetition) ([]byte, error) {
	mem := memory.DefaultAllocator
	primitive := schema.Must(schema.NewPrimitiveNodeLogical(
		"value",
		repetition,
		schema.NewDecimalLogicalType(precision, 0),
		physical,
		-1,
		-1,
	))
	parquetSchema := schema.MustGroup(schema.NewGroupNode(
		"schema",
		parquet.Repetitions.Required,
		schema.FieldList{primitive},
		-1,
	))

	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()
	writer := file.NewParquetWriter(sink, parquetSchema)
	defer writer.Close()

	rowGroup, err := writer.AppendRowGroupChecked()
	if err != nil {
		return nil, err
	}
	defer rowGroup.Close()

	column, err := rowGroup.NextColumn()
	if err != nil {
		return nil, err
	}
	defer column.Close()

	var defLevels []int16
	leafFieldNullable := false
	if repetition == parquet.Repetitions.Optional {
		defLevels = make([]int16, arr.Len())
		leafFieldNullable = true
		for i := range defLevels {
			if !arr.IsNull(i) {
				defLevels[i] = 1
			}
		}
	}

	if err := pqarrow.WriteArrowToColumn(
		pqarrow.NewArrowWriteContext(context.Background(), nil),
		column,
		arr,
		defLevels,
		nil,
		leafFieldNullable,
	); err != nil {
		return nil, err
	}
	if err := column.Close(); err != nil {
		return nil, err
	}
	if err := rowGroup.Close(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return append([]byte(nil), sink.Bytes()...), nil
}

func TestDecimalIntegerOverflow(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		value     string
		physical  parquet.Type
		precision int32
		wantErr   bool
	}{
		{name: "decimal128 int32 precision max", kind: "decimal128", value: "999999999", physical: parquet.Types.Int32, precision: 9},
		{name: "decimal128 int32 negative precision max", kind: "decimal128", value: "-999999999", physical: parquet.Types.Int32, precision: 9},
		{name: "decimal128 int32 precision overflow", kind: "decimal128", value: "1000000000", physical: parquet.Types.Int32, precision: 9, wantErr: true},
		{name: "decimal128 int32 negative precision overflow", kind: "decimal128", value: "-1000000000", physical: parquet.Types.Int32, precision: 9, wantErr: true},
		{name: "decimal256 int32 precision max", kind: "decimal256", value: "999999999", physical: parquet.Types.Int32, precision: 9},
		{name: "decimal256 int32 negative precision max", kind: "decimal256", value: "-999999999", physical: parquet.Types.Int32, precision: 9},
		{name: "decimal128 int32 overflow", kind: "decimal128", value: "2147483648", physical: parquet.Types.Int32, precision: 9, wantErr: true},
		{name: "decimal256 int32 overflow", kind: "decimal256", value: "-2147483649", physical: parquet.Types.Int32, precision: 9, wantErr: true},
		{name: "decimal128 int64 precision max", kind: "decimal128", value: "999999999999999999", physical: parquet.Types.Int64, precision: 18},
		{name: "decimal128 int64 negative precision max", kind: "decimal128", value: "-999999999999999999", physical: parquet.Types.Int64, precision: 18},
		{name: "decimal128 int64 precision overflow", kind: "decimal128", value: "1000000000000000000", physical: parquet.Types.Int64, precision: 18, wantErr: true},
		{name: "decimal128 int64 negative precision overflow", kind: "decimal128", value: "-1000000000000000000", physical: parquet.Types.Int64, precision: 18, wantErr: true},
		{name: "decimal256 int64 precision max", kind: "decimal256", value: "999999999999999999", physical: parquet.Types.Int64, precision: 18},
		{name: "decimal256 int64 negative precision max", kind: "decimal256", value: "-999999999999999999", physical: parquet.Types.Int64, precision: 18},
		{name: "decimal128 int64 physical max outside precision", kind: "decimal128", value: "9223372036854775807", physical: parquet.Types.Int64, precision: 18, wantErr: true},
		{name: "decimal128 int64 overflow", kind: "decimal128", value: "9223372036854775808", physical: parquet.Types.Int64, precision: 18, wantErr: true},
		{name: "decimal256 int64 overflow", kind: "decimal256", value: "-9223372036854775809", physical: parquet.Types.Int64, precision: 18, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arr := makeDecimalArray(t, tc.kind, tc.value)
			defer arr.Release()

			err := writeDecimalInteger(arr, tc.physical, tc.precision)
			if tc.wantErr {
				require.ErrorIs(t, err, arrow.ErrInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func makeNullableDecimalArray(t *testing.T, kind, invalid string) arrow.Array {
	t.Helper()
	invalidValue, ok := new(big.Int).SetString(invalid, 10)
	require.True(t, ok)

	switch kind {
	case "decimal128":
		builder := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 38, Scale: 0})
		defer builder.Release()
		builder.AppendNull()
		builder.Append(decimal128.FromI64(42))
		arr := builder.NewDecimal128Array()
		arr.Values()[0] = decimal128.FromBigInt(invalidValue)
		return arr
	case "decimal256":
		builder := array.NewDecimal256Builder(memory.DefaultAllocator, &arrow.Decimal256Type{Precision: 76, Scale: 0})
		defer builder.Release()
		builder.AppendNull()
		builder.Append(decimal256.FromI64(42))
		arr := builder.NewDecimal256Array()
		arr.Values()[0] = decimal256.FromBigInt(invalidValue)
		return arr
	default:
		t.Fatalf("unknown decimal kind %q", kind)
		return nil
	}
}

func TestDecimalIntegerNullBackingValues(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		physical  parquet.Type
		precision int32
		invalid   string
	}{
		{name: "decimal128 to int32", kind: "decimal128", physical: parquet.Types.Int32, precision: 9, invalid: "2147483648"},
		{name: "decimal256 to int32", kind: "decimal256", physical: parquet.Types.Int32, precision: 9, invalid: "2147483648"},
		{name: "decimal128 to int64", kind: "decimal128", physical: parquet.Types.Int64, precision: 18, invalid: "9223372036854775808"},
		{name: "decimal256 to int64", kind: "decimal256", physical: parquet.Types.Int64, precision: 18, invalid: "9223372036854775808"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arr := makeNullableDecimalArray(t, tc.kind, tc.invalid)
			defer arr.Release()

			_, err := writeDecimalIntegerData(arr, tc.physical, tc.precision, parquet.Repetitions.Optional)
			require.NoError(t, err)
		})
	}
}

func TestDecimalIntegerNullRoundTrip(t *testing.T) {
	arr := makeNullableDecimalArray(t, "decimal128", "2147483648")
	defer arr.Release()

	data, err := writeDecimalIntegerData(arr, parquet.Types.Int32, 9, parquet.Repetitions.Optional)
	require.NoError(t, err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	reader, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := arrowReader.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	result, ok := tbl.Column(0).Data().Chunk(0).(*array.Decimal128)
	require.True(t, ok)
	require.Len(t, result.Values(), 2)
	assert.True(t, result.IsNull(0))
	assert.Equal(t, decimal128.FromI64(42), result.Value(1))
}
