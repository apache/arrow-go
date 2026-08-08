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

//go:build go1.18

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func benchmarkInt64Array(b *testing.B, length int, withNulls bool) arrow.Array {
	b.Helper()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Reserve(length)
	for i := 0; i < length; i++ {
		if withNulls && i%100 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(1)
		}
	}
	result := builder.NewArray()
	builder.Release()
	return result
}

func benchmarkFloat64Array(b *testing.B, length int) arrow.Array {
	b.Helper()
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	builder.Reserve(length)
	for i := 0; i < length; i++ {
		builder.Append(1)
	}
	result := builder.NewArray()
	builder.Release()
	return result
}

func benchmarkInt64Chunked(b *testing.B, chunks, length int) *arrow.Chunked {
	b.Helper()
	values := make([]arrow.Array, chunks)
	for i := range values {
		values[i] = benchmarkInt64Array(b, length, false)
	}
	result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, values)
	for _, value := range values {
		value.Release()
	}
	return result
}

func BenchmarkCumulativeSum(b *testing.B) {
	const length = 10_000_000

	intInput := benchmarkInt64Array(b, length, false)
	defer intInput.Release()
	nullInput := benchmarkInt64Array(b, length, true)
	defer nullInput.Release()
	floatInput := benchmarkFloat64Array(b, length)
	defer floatInput.Release()
	chunkedInput := benchmarkInt64Chunked(b, 100, length/100)
	defer chunkedInput.Release()

	ctx := context.Background()
	tests := []struct {
		name    string
		input   compute.Datum
		opts    compute.CumulativeOptions
		checked bool
	}{
		{name: "int64", input: &compute.ArrayDatum{Value: intInput.Data()}},
		{name: "int64_checked", input: &compute.ArrayDatum{Value: intInput.Data()}, checked: true},
		{name: "int64_1pct_nulls_skip", input: &compute.ArrayDatum{Value: nullInput.Data()}, opts: compute.CumulativeOptions{SkipNulls: true}},
		{name: "float64", input: &compute.ArrayDatum{Value: floatInput.Data()}},
		{name: "int64_chunked", input: &compute.ChunkedDatum{Value: chunkedInput}},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var (
					result compute.Datum
					err    error
				)
				if tc.checked {
					result, err = compute.CumulativeSumChecked(ctx, tc.opts, tc.input)
				} else {
					result, err = compute.CumulativeSum(ctx, tc.opts, tc.input)
				}
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}
