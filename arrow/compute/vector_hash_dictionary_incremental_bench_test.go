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

//go:build go1.24

package compute_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkUniqueChunkedChangingDictionaries measures the many-chunk case.
// Each chunk has new dictionary entries; total dictionary size grows with chunks.
func BenchmarkUniqueChunkedChangingDictionaries(b *testing.B) {
	const (
		valuesPerChunk = 128
		dictSize       = 16
	)
	mem := memory.DefaultAllocator
	ctx := compute.WithAllocator(context.Background(), mem)
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	indices := make([]string, valuesPerChunk)
	for i := range indices {
		indices[i] = fmt.Sprint(i % dictSize)
	}
	indexJSON := "[" + strings.Join(indices, ",") + "]"

	for _, numChunks := range []int{8, 32, 128, 256} {
		b.Run(fmt.Sprintf("chunks=%d", numChunks), func(b *testing.B) {
			chunks := make([]arrow.Array, numChunks)
			for i := range chunks {
				values := make([]string, dictSize)
				for j := range values {
					values[j] = fmt.Sprintf("chunk-%04d-value-%02d", i, j)
				}
				dictJSON, err := json.Marshal(values)
				if err != nil {
					b.Fatal(err)
				}
				arr, err := array.DictArrayFromJSON(mem, dictType, indexJSON, string(dictJSON))
				if err != nil {
					b.Fatal(err)
				}
				chunks[i] = arr
			}
			input := arrow.NewChunked(dictType, chunks)
			for _, chunk := range chunks {
				chunk.Release()
			}
			defer input.Release()

			datum := &compute.ChunkedDatum{Value: input}
			b.ReportAllocs()
			b.SetBytes(int64(numChunks * valuesPerChunk))
			b.ResetTimer()
			for b.Loop() {
				result, err := compute.Unique(ctx, datum)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}
