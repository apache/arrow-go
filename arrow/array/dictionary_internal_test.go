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

package array

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestCheckIndexBoundsAllowsSignedIndexAtTypeLimit(t *testing.T) {
	tests := []struct {
		name       string
		indexType  arrow.DataType
		indexBytes []byte
		upperLimit uint64
	}{
		{"int8", arrow.PrimitiveTypes.Int8,
			arrow.Int8Traits.CastToBytes([]int8{math.MaxInt8}), uint64(math.MaxInt8) + 1},
		{"int16", arrow.PrimitiveTypes.Int16,
			arrow.Int16Traits.CastToBytes([]int16{math.MaxInt16}), uint64(math.MaxInt16) + 1},
		{"int32", arrow.PrimitiveTypes.Int32,
			arrow.Int32Traits.CastToBytes([]int32{math.MaxInt32}), uint64(math.MaxInt32) + 1},
		{"int64", arrow.PrimitiveTypes.Int64,
			arrow.Int64Traits.CastToBytes([]int64{math.MaxInt64}), uint64(math.MaxInt64) + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := memory.NewBufferBytes(tt.indexBytes)
			indices := NewData(tt.indexType, 1, []*memory.Buffer{nil, values}, nil, 0, 0)
			values.Release()
			defer indices.Release()

			require.NoError(t, checkIndexBounds(indices, tt.upperLimit))
		})
	}
}

func TestCheckIndexBoundsIgnoresNullValues(t *testing.T) {
	tests := []struct {
		name       string
		indexType  arrow.DataType
		indexBytes []byte
	}{
		{"int8", arrow.PrimitiveTypes.Int8, arrow.Int8Traits.CastToBytes([]int8{99, 99, 0, -1, 2, 99})},
		{"uint8", arrow.PrimitiveTypes.Uint8, arrow.Uint8Traits.CastToBytes([]uint8{99, 99, 0, math.MaxUint8, 2, 99})},
		{"int16", arrow.PrimitiveTypes.Int16, arrow.Int16Traits.CastToBytes([]int16{99, 99, 0, -1, 2, 99})},
		{"uint16", arrow.PrimitiveTypes.Uint16, arrow.Uint16Traits.CastToBytes([]uint16{99, 99, 0, math.MaxUint16, 2, 99})},
		{"int32", arrow.PrimitiveTypes.Int32, arrow.Int32Traits.CastToBytes([]int32{99, 99, 0, -1, 2, 99})},
		{"uint32", arrow.PrimitiveTypes.Uint32, arrow.Uint32Traits.CastToBytes([]uint32{99, 99, 0, math.MaxUint32, 2, 99})},
		{"int64", arrow.PrimitiveTypes.Int64, arrow.Int64Traits.CastToBytes([]int64{99, 99, 0, -1, 2, 99})},
		{"uint64", arrow.PrimitiveTypes.Uint64, arrow.Uint64Traits.CastToBytes([]uint64{99, 99, 0, math.MaxUint64, 2, 99})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validity := make([]byte, bitutil.BytesForBits(6))
			bitutil.SetBit(validity, 2)
			bitutil.SetBit(validity, 4)
			validityBuffer := memory.NewBufferBytes(validity)
			valuesBuffer := memory.NewBufferBytes(tt.indexBytes)
			indices := NewData(
				tt.indexType,
				3,
				[]*memory.Buffer{validityBuffer, valuesBuffer},
				nil,
				1,
				2,
			)
			validityBuffer.Release()
			valuesBuffer.Release()
			defer indices.Release()

			require.NoError(t, checkIndexBounds(indices, 3))
		})
	}
}

func TestCheckIndexBoundsAllowsAllNullIndices(t *testing.T) {
	validityBuffer := memory.NewBufferBytes(make([]byte, bitutil.BytesForBits(3)))
	valuesBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{-1, -1, -1}))
	indices := NewData(
		arrow.PrimitiveTypes.Int32,
		3,
		[]*memory.Buffer{validityBuffer, valuesBuffer},
		nil,
		3,
		0,
	)
	validityBuffer.Release()
	valuesBuffer.Release()
	defer indices.Release()

	require.NoError(t, checkIndexBounds(indices, 0))
}

func TestCheckIndexBoundsIgnoresNullValuesWithFragmentedValidity(t *testing.T) {
	const length = 256
	values := make([]int32, length)
	validity := make([]byte, bitutil.BytesForBits(length))
	for i := range values {
		if i%2 == 0 {
			values[i] = 1
			bitutil.SetBit(validity, i)
		} else {
			values[i] = -1
		}
	}

	validityBuffer := memory.NewBufferBytes(validity)
	valuesBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(values))
	indices := NewData(
		arrow.PrimitiveTypes.Int32,
		length,
		[]*memory.Buffer{validityBuffer, valuesBuffer},
		nil,
		length/2,
		0,
	)
	validityBuffer.Release()
	valuesBuffer.Release()
	defer indices.Release()

	require.NoError(t, checkIndexBounds(indices, 2))

	values[0] = -1
	require.Error(t, checkIndexBounds(indices, 2))
}

func BenchmarkCheckIndexBounds(b *testing.B) {
	const length = 1 << 20

	values := make([]int32, length)
	for i := range values {
		values[i] = int32(i % 1024)
	}
	valuesBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(values))
	defer valuesBuffer.Release()

	benchmarks := []struct {
		name    string
		isValid func(int) bool
	}{
		{name: "all_valid", isValid: func(int) bool { return true }},
		{name: "clustered_10_percent_valid", isValid: func(i int) bool { return i < length/10 }},
		{name: "clustered_50_percent_valid", isValid: func(i int) bool { return i < length/2 }},
		{name: "strided_10_percent_valid", isValid: func(i int) bool { return i%10 == 0 }},
		{name: "strided_90_percent_valid", isValid: func(i int) bool { return i%10 != 0 }},
		{name: "alternating", isValid: func(i int) bool { return i%2 == 0 }},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			var validityBuffer *memory.Buffer
			nulls := 0
			if benchmark.name != "all_valid" {
				validity := make([]byte, bitutil.BytesForBits(length))
				for i := 0; i < length; i++ {
					if benchmark.isValid(i) {
						bitutil.SetBit(validity, i)
					} else {
						nulls++
					}
				}
				validityBuffer = memory.NewBufferBytes(validity)
				defer validityBuffer.Release()
			}

			indices := NewData(
				arrow.PrimitiveTypes.Int32,
				length,
				[]*memory.Buffer{validityBuffer, valuesBuffer},
				nil,
				nulls,
				0,
			)
			defer indices.Release()

			b.ReportAllocs()
			b.SetBytes(length * int64(arrow.Int32Traits.BytesRequired(1)))
			b.ResetTimer()
			for range b.N {
				if err := checkIndexBounds(indices, 1024); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
