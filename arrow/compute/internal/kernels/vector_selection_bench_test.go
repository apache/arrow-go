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

package kernels

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type filterBenchmarkPattern struct {
	name     string
	selected func(int) bool
	nullable bool
}

var filterBenchmarkPatterns = []filterBenchmarkPattern{
	{name: "all", selected: func(int) bool { return true }},
	{name: "long-runs", selected: func(i int) bool { return i%1024 < 900 }},
	{name: "alternating", selected: func(i int) bool { return i%2 == 0 }},
	{name: "short-runs", selected: func(i int) bool { return i%100 < 10 }},
	{name: "nullable-long-runs", selected: func(i int) bool { return i%1024 < 900 }, nullable: true},
}

func makeFilterBenchmarkSpan(tb testing.TB, n int, pattern filterBenchmarkPattern) *exec.ArraySpan {
	tb.Helper()
	bldr := array.NewBooleanBuilder(memory.DefaultAllocator)
	for i := 0; i < n; i++ {
		if pattern.nullable && i%257 == 0 {
			bldr.AppendNull()
		} else {
			bldr.Append(pattern.selected(i))
		}
	}
	filter := bldr.NewArray()
	bldr.Release()
	tb.Cleanup(filter.Release)

	span := &exec.ArraySpan{}
	span.SetMembers(filter.Data())
	return span
}

func BenchmarkGetTakeIndices(b *testing.B) {
	for _, n := range []int{64 * 1024, 1024 * 1024} {
		for _, pattern := range filterBenchmarkPatterns {
			b.Run(fmt.Sprintf("%s/%d", pattern.name, n), func(b *testing.B) {
				filter := makeFilterBenchmarkSpan(b, n, pattern)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := GetTakeIndices(memory.DefaultAllocator, filter, DropNulls)
					if err != nil {
						b.Fatal(err)
					}
					result.Release()
				}
			})
		}
	}
}
