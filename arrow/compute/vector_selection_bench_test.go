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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var benchmarkFilterOutputLength int

func BenchmarkFilterInt32MixedMasks(b *testing.B) {
	patterns := []struct {
		name     string
		selected func(int) bool
	}{
		{name: "alternating", selected: func(i int) bool { return i%2 == 0 }},
		{name: "random25", selected: func(i int) bool { return filterBenchmarkRandom(i, 25) }},
		{name: "random50", selected: func(i int) bool { return filterBenchmarkRandom(i, 50) }},
		{name: "random75", selected: func(i int) bool { return filterBenchmarkRandom(i, 75) }},
		{name: "clustered50", selected: func(i int) bool { return (i/32)%2 == 0 }},
		{name: "all-selected", selected: func(int) bool { return true }},
		{name: "none-selected", selected: func(int) bool { return false }},
	}

	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		size := size
		for _, pattern := range patterns {
			pattern := pattern
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern.name), func(b *testing.B) {
				values, filter := makeFilterInt32BenchmarkInput(b, size, pattern.selected)
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
					benchmarkFilterOutputLength = result.Len()
					result.Release()
				}
			})
		}
	}
}

func makeFilterInt32BenchmarkInput(b *testing.B, size int, selected func(int) bool) (arrow.Array, arrow.Array) {
	b.Helper()
	mem := memory.DefaultAllocator

	valuesBuilder := array.NewInt32Builder(mem)
	valuesBuilder.Reserve(size)
	for i := 0; i < size; i++ {
		valuesBuilder.Append(int32(i))
	}
	values := valuesBuilder.NewInt32Array()
	valuesBuilder.Release()

	filterBuilder := array.NewBooleanBuilder(mem)
	filterBuilder.Reserve(size)
	for i := 0; i < size; i++ {
		filterBuilder.Append(selected(i))
	}
	filter := filterBuilder.NewBooleanArray()
	filterBuilder.Release()
	return values, filter
}

func filterBenchmarkRandom(i, selectedPercent int) bool {
	x := uint32(i)*747796405 + 2891336453
	x = ((x >> ((x >> 28) + 4)) ^ x) * 277803737
	x = (x >> 22) ^ x
	return int(x%100) < selectedPercent
}
