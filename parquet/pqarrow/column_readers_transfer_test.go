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

package pqarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
)

type integerTransferRecordReader struct {
	file.RecordReader
	physicalType parquet.Type
	values       []byte
	length       int
}

func (r *integerTransferRecordReader) ValuesWritten() int { return r.length }
func (r *integerTransferRecordReader) Type() parquet.Type { return r.physicalType }
func (r *integerTransferRecordReader) Values() []byte     { return r.values }
func (r *integerTransferRecordReader) NullCount() int64   { return 0 }
func (r *integerTransferRecordReader) ReleaseValidBits() *memory.Buffer {
	return nil
}

func requireConvertedValues[Out arrowInteger, In parquetInteger](t *testing.T, got []Out, values []In) {
	t.Helper()
	want := make([]Out, len(values))
	convertIntegerValues(want, values)
	require.Equal(t, want, got)
}

func requireIntegerTransfer[In parquetInteger](t *testing.T, rdr file.RecordReader, values []In, dt arrow.DataType) {
	t.Helper()
	data := transferInt(rdr, dt)
	defer data.Release()

	buf := data.Buffers()[1].Bytes()
	switch dt.ID() {
	case arrow.INT8:
		requireConvertedValues(t, arrow.Int8Traits.CastFromBytes(buf), values)
	case arrow.UINT8:
		requireConvertedValues(t, arrow.Uint8Traits.CastFromBytes(buf), values)
	case arrow.INT16:
		requireConvertedValues(t, arrow.Int16Traits.CastFromBytes(buf), values)
	case arrow.UINT16:
		requireConvertedValues(t, arrow.Uint16Traits.CastFromBytes(buf), values)
	case arrow.UINT32:
		requireConvertedValues(t, arrow.Uint32Traits.CastFromBytes(buf), values)
	case arrow.UINT64:
		requireConvertedValues(t, arrow.Uint64Traits.CastFromBytes(buf), values)
	case arrow.DATE32:
		requireConvertedValues(t, arrow.Date32Traits.CastFromBytes(buf), values)
	case arrow.TIME32:
		requireConvertedValues(t, arrow.Time32Traits.CastFromBytes(buf), values)
	case arrow.TIME64:
		requireConvertedValues(t, arrow.Time64Traits.CastFromBytes(buf), values)
	}
}

func TestTransferIntegerValues(t *testing.T) {
	types := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.FixedWidthTypes.Date32,
		arrow.FixedWidthTypes.Time32s,
		arrow.FixedWidthTypes.Time64us,
	}

	t.Run("int32 physical values", func(t *testing.T) {
		values := []int32{-1 << 31, -32769, -1, 0, 1, 255, 1<<31 - 1}
		rdr := &integerTransferRecordReader{
			physicalType: parquet.Types.Int32,
			values:       arrow.Int32Traits.CastToBytes(values),
			length:       len(values),
		}
		for _, dt := range types {
			t.Run(dt.Name(), func(t *testing.T) {
				requireIntegerTransfer(t, rdr, values, dt)
			})
		}
	})

	t.Run("int64 physical values", func(t *testing.T) {
		values := []int64{-1 << 63, -1<<32 - 1, -1, 0, 1, 1<<32 - 1, 1<<63 - 1}
		rdr := &integerTransferRecordReader{
			physicalType: parquet.Types.Int64,
			values:       arrow.Int64Traits.CastToBytes(values),
			length:       len(values),
		}
		for _, dt := range types {
			t.Run(dt.Name(), func(t *testing.T) {
				requireIntegerTransfer(t, rdr, values, dt)
			})
		}
	})
}

func TestTransferDecimalIntegerValues(t *testing.T) {
	t.Run("int32 physical values", func(t *testing.T) {
		values := []int32{-1 << 31, -1, 0, 1, 1<<31 - 1}
		rdr := &integerTransferRecordReader{
			physicalType: parquet.Types.Int32,
			values:       arrow.Int32Traits.CastToBytes(values),
			length:       len(values),
		}

		data128 := transferDecimalInteger(rdr, &arrow.Decimal128Type{Precision: 10})
		defer data128.Release()
		got128 := arrow.Decimal128Traits.CastFromBytes(data128.Buffers()[1].Bytes())
		for i, value := range values {
			require.Equal(t, decimal128.FromI64(int64(value)), got128[i])
		}

		data256 := transferDecimalInteger(rdr, &arrow.Decimal256Type{Precision: 10})
		defer data256.Release()
		got256 := arrow.Decimal256Traits.CastFromBytes(data256.Buffers()[1].Bytes())
		for i, value := range values {
			require.Equal(t, decimal256.FromI64(int64(value)), got256[i])
		}
	})

	t.Run("int64 physical values", func(t *testing.T) {
		values := []int64{-1 << 63, -1, 0, 1, 1<<63 - 1}
		rdr := &integerTransferRecordReader{
			physicalType: parquet.Types.Int64,
			values:       arrow.Int64Traits.CastToBytes(values),
			length:       len(values),
		}

		data128 := transferDecimalInteger(rdr, &arrow.Decimal128Type{Precision: 19})
		defer data128.Release()
		got128 := arrow.Decimal128Traits.CastFromBytes(data128.Buffers()[1].Bytes())
		for i, value := range values {
			require.Equal(t, decimal128.FromI64(value), got128[i])
		}

		data256 := transferDecimalInteger(rdr, &arrow.Decimal256Type{Precision: 19})
		defer data256.Release()
		got256 := arrow.Decimal256Traits.CastFromBytes(data256.Buffers()[1].Bytes())
		for i, value := range values {
			require.Equal(t, decimal256.FromI64(value), got256[i])
		}
	})
}
