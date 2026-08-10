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
	"github.com/apache/arrow-go/v18/parquet"
)

func BenchmarkTransferInteger(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"1K", 1_000}, {"64K", 64_000}, {"1M", 1_000_000}} {
		b.Run("Int32ToInt8/"+tc.name, func(b *testing.B) {
			size := tc.size
			values := make([]int32, size)
			for i := range values {
				values[i] = int32(i*31 - 1_000_000)
			}
			rdr := &integerTransferRecordReader{
				physicalType: parquet.Types.Int32,
				values:       arrow.Int32Traits.CastToBytes(values),
				length:       len(values),
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(values) * arrow.Int32SizeBytes))
			b.ResetTimer()
			for range b.N {
				data := transferInt(rdr, arrow.PrimitiveTypes.Int8)
				data.Release()
			}
		})

		b.Run("Int64ToUint64/"+tc.name, func(b *testing.B) {
			size := tc.size
			values := make([]int64, size)
			for i := range values {
				values[i] = int64(i)*6364136223846793005 - 1
			}
			rdr := &integerTransferRecordReader{
				physicalType: parquet.Types.Int64,
				values:       arrow.Int64Traits.CastToBytes(values),
				length:       len(values),
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(values) * arrow.Int64SizeBytes))
			b.ResetTimer()
			for range b.N {
				data := transferInt(rdr, arrow.PrimitiveTypes.Uint64)
				data.Release()
			}
		})
	}
}

func BenchmarkTransferDecimalInteger(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"1K", 1_000}, {"64K", 64_000}, {"1M", 1_000_000}} {
		size := tc.size
		values := make([]int64, size)
		for i := range values {
			values[i] = int64(i)*6364136223846793005 - 1
		}
		rdr := &integerTransferRecordReader{
			physicalType: parquet.Types.Int64,
			values:       arrow.Int64Traits.CastToBytes(values),
			length:       len(values),
		}

		b.Run("Int64ToDecimal128/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(values) * arrow.Int64SizeBytes))
			b.ResetTimer()
			for range b.N {
				data := transferDecimalInteger(rdr, &arrow.Decimal128Type{Precision: 19})
				data.Release()
			}
		})

		b.Run("Int64ToDecimal256/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(values) * arrow.Int64SizeBytes))
			b.ResetTimer()
			for range b.N {
				data := transferDecimalInteger(rdr, &arrow.Decimal256Type{Precision: 19})
				data.Release()
			}
		})
	}
}
