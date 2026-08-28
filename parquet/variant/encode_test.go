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
	"math"
	"strings"
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

type encodeTestCase struct {
	name   string
	encode func() ([]byte, error)
	of     func() (variant.Value, error)
}

func encodeTestCases() []encodeTestCase {
	testTime := time.Date(2023, 5, 15, 14, 30, 0, 123456789, time.FixedZone("test", 2*60*60))
	shortString := strings.Repeat("a", 63)
	longString := shortString + "b"
	binaryValue := []byte{0, 1, 2, 0xff}
	emptyBinary := []byte{}
	uuidValue := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	decimal4Value := variant.DecimalValue[decimal.Decimal32]{Scale: 2, Value: decimal.Decimal32(1234)}
	decimal8Value := variant.DecimalValue[decimal.Decimal64]{Scale: 2, Value: decimal.Decimal64(1234567890)}
	decimal16Value := variant.DecimalValue[decimal.Decimal128]{
		Scale: 2, Value: decimal128.FromU64(1234567891234567890),
	}

	return []encodeTestCase{
		{
			name:   "bool_true",
			encode: func() ([]byte, error) { return variant.Encode(true) },
			of:     func() (variant.Value, error) { return variant.Of(true) },
		},
		{
			name:   "bool_false",
			encode: func() ([]byte, error) { return variant.Encode(false) },
			of:     func() (variant.Value, error) { return variant.Of(false) },
		},
		{
			name:   "int8_min",
			encode: func() ([]byte, error) { return variant.Encode(int8(-128)) },
			of:     func() (variant.Value, error) { return variant.Of(int8(-128)) },
		},
		{
			name:   "uint8_max",
			encode: func() ([]byte, error) { return variant.Encode(uint8(255)) },
			of:     func() (variant.Value, error) { return variant.Of(uint8(255)) },
		},
		{
			name:   "int16_min",
			encode: func() ([]byte, error) { return variant.Encode(int16(-32768)) },
			of:     func() (variant.Value, error) { return variant.Of(int16(-32768)) },
		},
		{
			name:   "uint16_max",
			encode: func() ([]byte, error) { return variant.Encode(uint16(65535)) },
			of:     func() (variant.Value, error) { return variant.Of(uint16(65535)) },
		},
		{
			name:   "int32_min",
			encode: func() ([]byte, error) { return variant.Encode(int32(-2147483648)) },
			of:     func() (variant.Value, error) { return variant.Of(int32(-2147483648)) },
		},
		{
			name:   "uint32_max",
			encode: func() ([]byte, error) { return variant.Encode(uint32(1<<32 - 1)) },
			of:     func() (variant.Value, error) { return variant.Of(uint32(1<<32 - 1)) },
		},
		{
			name:   "int64_min",
			encode: func() ([]byte, error) { return variant.Encode(int64(-1 << 63)) },
			of:     func() (variant.Value, error) { return variant.Of(int64(-1 << 63)) },
		},
		{
			name:   "int64_max",
			encode: func() ([]byte, error) { return variant.Encode(int64(1<<63 - 1)) },
			of:     func() (variant.Value, error) { return variant.Of(int64(1<<63 - 1)) },
		},
		{
			name:   "int",
			encode: func() ([]byte, error) { return variant.Encode(int(123456)) },
			of:     func() (variant.Value, error) { return variant.Of(int(123456)) },
		},
		{
			name:   "uint",
			encode: func() ([]byte, error) { return variant.Encode(uint(123456)) },
			of:     func() (variant.Value, error) { return variant.Of(uint(123456)) },
		},
		{
			name:   "float32",
			encode: func() ([]byte, error) { return variant.Encode(float32(math.MaxFloat32)) },
			of:     func() (variant.Value, error) { return variant.Of(float32(math.MaxFloat32)) },
		},
		{
			name:   "float64",
			encode: func() ([]byte, error) { return variant.Encode(math.Copysign(0, -1)) },
			of:     func() (variant.Value, error) { return variant.Of(math.Copysign(0, -1)) },
		},
		{
			name:   "date",
			encode: func() ([]byte, error) { return variant.Encode(arrow.Date32(-2147483648)) },
			of:     func() (variant.Value, error) { return variant.Of(arrow.Date32(-2147483648)) },
		},
		{
			name:   "time",
			encode: func() ([]byte, error) { return variant.Encode(arrow.Time64(-123456789)) },
			of:     func() (variant.Value, error) { return variant.Of(arrow.Time64(-123456789)) },
		},
		{
			name: "timestamp",
			encode: func() ([]byte, error) {
				return variant.Encode(arrow.Timestamp(-123456789), variant.OptTimestampNano, variant.OptTimestampUTC)
			},
			of: func() (variant.Value, error) {
				return variant.Of(arrow.Timestamp(-123456789), variant.OptTimestampNano, variant.OptTimestampUTC)
			},
		},
		{
			name:   "short_string",
			encode: func() ([]byte, error) { return variant.Encode(shortString) },
			of:     func() (variant.Value, error) { return variant.Of(shortString) },
		},
		{
			name:   "empty_string",
			encode: func() ([]byte, error) { return variant.Encode("") },
			of:     func() (variant.Value, error) { return variant.Of("") },
		},
		{
			name:   "long_string",
			encode: func() ([]byte, error) { return variant.Encode(longString) },
			of:     func() (variant.Value, error) { return variant.Of(longString) },
		},
		{
			name:   "binary",
			encode: func() ([]byte, error) { return variant.Encode(binaryValue) },
			of:     func() (variant.Value, error) { return variant.Of(binaryValue) },
		},
		{
			name:   "empty_binary",
			encode: func() ([]byte, error) { return variant.Encode(emptyBinary) },
			of:     func() (variant.Value, error) { return variant.Of(emptyBinary) },
		},
		{
			name: "time_default",
			encode: func() ([]byte, error) {
				return variant.Encode(testTime)
			},
			of: func() (variant.Value, error) {
				return variant.Of(testTime)
			},
		},
		{
			name: "time_nanos_utc",
			encode: func() ([]byte, error) {
				return variant.Encode(testTime, variant.OptTimestampNano, variant.OptTimestampUTC)
			},
			of: func() (variant.Value, error) {
				return variant.Of(testTime, variant.OptTimestampNano, variant.OptTimestampUTC)
			},
		},
		{
			name: "time_as_date",
			encode: func() ([]byte, error) {
				return variant.Encode(testTime, variant.OptTimeAsDate)
			},
			of: func() (variant.Value, error) {
				return variant.Of(testTime, variant.OptTimeAsDate)
			},
		},
		{
			name: "time_as_time",
			encode: func() ([]byte, error) {
				return variant.Encode(testTime, variant.OptTimeAsTime)
			},
			of: func() (variant.Value, error) {
				return variant.Of(testTime, variant.OptTimeAsTime)
			},
		},
		{
			name:   "uuid",
			encode: func() ([]byte, error) { return variant.Encode(uuidValue) },
			of:     func() (variant.Value, error) { return variant.Of(uuidValue) },
		},
		{
			name:   "decimal4",
			encode: func() ([]byte, error) { return variant.Encode(decimal4Value) },
			of:     func() (variant.Value, error) { return variant.Of(decimal4Value) },
		},
		{
			name:   "decimal8",
			encode: func() ([]byte, error) { return variant.Encode(decimal8Value) },
			of:     func() (variant.Value, error) { return variant.Of(decimal8Value) },
		},
		{
			name:   "decimal16",
			encode: func() ([]byte, error) { return variant.Encode(decimal16Value) },
			of:     func() (variant.Value, error) { return variant.Of(decimal16Value) },
		},
	}
}

func TestEncodeMatchesOf(t *testing.T) {
	for _, tc := range encodeTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.encode()
			require.NoError(t, err)

			value, err := tc.of()
			require.NoError(t, err)

			assert.Equal(t, value.Bytes(), encoded)
		})
	}
}

var encodeBenchmarkSink []byte

func benchmarkEncode(b *testing.B, encode func() ([]byte, error)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		encoded, err := encode()
		if err != nil {
			b.Fatal(err)
		}
		encodeBenchmarkSink = encoded
	}
}

func benchmarkOf(b *testing.B, of func() (variant.Value, error)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		value, err := of()
		if err != nil {
			b.Fatal(err)
		}
		encodeBenchmarkSink = value.Bytes()
	}
}

func BenchmarkEncode(b *testing.B) {
	for _, tc := range encodeTestCases() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkEncode(b, tc.encode)
		})
	}
}

func BenchmarkOf(b *testing.B) {
	for _, tc := range encodeTestCases() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOf(b, tc.of)
		})
	}
}
