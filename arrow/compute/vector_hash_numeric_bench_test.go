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

func BenchmarkDictionaryEncodeNumeric(b *testing.B) {
	const (
		nvalues = 65535
		nunique = 100
	)

	mem := memory.DefaultAllocator
	ctx := compute.WithAllocator(context.Background(), mem)

	b.Run("int32", func(b *testing.B) {
		builder := array.NewInt32Builder(mem)
		values := make([]int32, nvalues)
		for i := range values {
			values[i] = int32(i % nunique)
		}
		builder.AppendValues(values, nil)
		input := builder.NewInt32Array()
		builder.Release()
		defer input.Release()
		benchmarkDictionaryEncodeNumeric(b, ctx, input, arrow.Int32SizeBytes)
	})

	b.Run("int64", func(b *testing.B) {
		builder := array.NewInt64Builder(mem)
		values := make([]int64, nvalues)
		for i := range values {
			values[i] = int64(i % nunique)
		}
		builder.AppendValues(values, nil)
		input := builder.NewInt64Array()
		builder.Release()
		defer input.Release()
		benchmarkDictionaryEncodeNumeric(b, ctx, input, arrow.Int64SizeBytes)
	})

	b.Run("float32", func(b *testing.B) {
		builder := array.NewFloat32Builder(mem)
		values := make([]float32, nvalues)
		for i := range values {
			values[i] = float32(i % nunique)
		}
		builder.AppendValues(values, nil)
		input := builder.NewFloat32Array()
		builder.Release()
		defer input.Release()
		benchmarkDictionaryEncodeNumeric(b, ctx, input, arrow.Float32SizeBytes)
	})

	b.Run("float64", func(b *testing.B) {
		builder := array.NewFloat64Builder(mem)
		values := make([]float64, nvalues)
		for i := range values {
			values[i] = float64(i % nunique)
		}
		builder.AppendValues(values, nil)
		input := builder.NewFloat64Array()
		builder.Release()
		defer input.Release()
		benchmarkDictionaryEncodeNumeric(b, ctx, input, arrow.Float64SizeBytes)
	})
}

func benchmarkDictionaryEncodeNumeric(b *testing.B, ctx context.Context, input arrow.Array, valueSize int) {
	b.ReportAllocs()
	b.SetBytes(int64(input.Len() * valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := compute.DictionaryEncodeArray(ctx, compute.DictionaryEncodeOptions{}, input)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
