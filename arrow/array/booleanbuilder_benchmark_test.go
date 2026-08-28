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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkBooleanBuilderAppendValues(b *testing.B) {
	const length = 65536
	patterns := []struct {
		name   string
		values []bool
	}{
		{"all-false", makeBooleanBenchmarkValues(length, func(int) bool { return false })},
		{"all-true", makeBooleanBenchmarkValues(length, func(int) bool { return true })},
		{"alternating", makeBooleanBenchmarkValues(length, func(i int) bool { return i%2 == 0 })},
		{"one-in-three", makeBooleanBenchmarkValues(length, func(i int) bool { return i%3 == 0 })},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			benchmarkAppendValues(b, func() (func(), func()) {
				bldr := NewBooleanBuilder(memory.DefaultAllocator)
				bldr.Reserve(length)
				return func() {
					bldr.AppendValues(pattern.values, nil)
				}, bldr.Release
			})
		})
	}
}

func BenchmarkBooleanBuilderAppendValuesSmall(b *testing.B) {
	for _, length := range []int{1, 2, 3, 7, 8} {
		b.Run(fmt.Sprintf("len=%d", length), func(b *testing.B) {
			values := makeBooleanBenchmarkValues(length, func(i int) bool { return i%2 == 0 })
			bldr := NewBooleanBuilder(memory.DefaultAllocator)
			bldr.Reserve(len(values) * b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bldr.AppendValues(values, nil)
			}
			b.StopTimer()
			bldr.Release()
		})
	}
}

func makeBooleanBenchmarkValues(length int, value func(int) bool) []bool {
	values := make([]bool, length)
	for i := range values {
		values[i] = value(i)
	}
	return values
}
