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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var dictionaryFilterBenchmarkOutputLength int

func BenchmarkFilterDictionaryIndices(b *testing.B) {
	patterns := []struct {
		name     string
		selected func(int) bool
		null     func(int) bool
	}{
		{name: "random10", selected: func(i int) bool { return dictionaryFilterSelect(i, 10) }},
		{name: "random50", selected: func(i int) bool { return dictionaryFilterSelect(i, 50) }},
		{name: "random90", selected: func(i int) bool { return dictionaryFilterSelect(i, 90) }},
		{name: "alternating", selected: func(i int) bool { return i%2 == 0 }},
		{name: "clustered50", selected: func(i int) bool { return (i/4096)%2 == 0 }},
		{
			name:     "nullable-random50",
			selected: func(i int) bool { return dictionaryFilterSelect(i, 50) },
			null:     func(i int) bool { return i%11 == 0 },
		},
	}

	for _, size := range []int{1 << 16, 1 << 20} {
		size := size
		for _, pattern := range patterns {
			pattern := pattern
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern.name), func(b *testing.B) {
				values, filter := makeDictionaryFilterBenchmarkInput(b, size, pattern.selected, pattern.null)
				defer values.Release()
				defer filter.Release()

				b.ReportAllocs()
				b.SetBytes(int64(size * 4))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := compute.FilterArray(context.Background(), values, filter, *compute.DefaultFilterOptions())
					if err != nil {
						b.Fatal(err)
					}
					dictionaryFilterBenchmarkOutputLength = result.Len()
					result.Release()
				}
			})
		}
	}
}

func makeDictionaryFilterBenchmarkInput(
	b *testing.B, size int, selected func(int) bool, isNull func(int) bool,
) (arrow.Array, arrow.Array) {
	b.Helper()
	mem := memory.DefaultAllocator

	indicesBuilder := array.NewInt32Builder(mem)
	indicesBuilder.Reserve(size)
	for i := 0; i < size; i++ {
		indicesBuilder.Append(int32(i % 256))
	}
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()

	dictionaryBuilder := array.NewInt64Builder(mem)
	dictionaryBuilder.Reserve(256)
	for i := 0; i < 256; i++ {
		dictionaryBuilder.Append(int64(i))
	}
	dictionary := dictionaryBuilder.NewArray()
	dictionaryBuilder.Release()

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int64}
	values := array.NewDictionaryArray(dictType, indices, dictionary)
	indices.Release()
	dictionary.Release()

	filterBuilder := array.NewBooleanBuilder(mem)
	filterBuilder.Reserve(size)
	for i := 0; i < size; i++ {
		if isNull != nil && isNull(i) {
			filterBuilder.AppendNull()
			continue
		}
		filterBuilder.Append(selected(i))
	}
	filter := filterBuilder.NewArray()
	filterBuilder.Release()

	return values, filter
}

func dictionaryFilterSelect(i, percent int) bool {
	x := uint64(i) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return x%100 < uint64(percent)
}
