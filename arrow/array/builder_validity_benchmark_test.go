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

func BenchmarkAppendBoolsToBitmap(b *testing.B) {
	const length = 65536
	patterns := []struct {
		name  string
		valid []bool
	}{
		{"all-valid", makeValidityBenchmarkValues(length, func(int) bool { return true })},
		{"1pct-null", makeValidityBenchmarkValues(length, func(i int) bool { return i%100 != 0 })},
		{"10pct-null", makeValidityBenchmarkValues(length, func(i int) bool { return i%10 != 0 })},
		{"50pct-null", makeValidityBenchmarkValues(length, func(i int) bool { return i%2 == 0 })},
		{"runs", makeValidityBenchmarkValues(length, func(i int) bool { return i%128 < 64 })},
	}

	for _, pattern := range patterns {
		for offset := 0; offset < 8; offset++ {
			b.Run(fmt.Sprintf("%s/offset=%d", pattern.name, offset), func(b *testing.B) {
				var bldr builder
				bldr.mem = memory.DefaultAllocator
				bldr.init(length + offset)
				defer bldr.nullBitmap.Release()
				bldr.length = offset

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bldr.length = offset
					bldr.nulls = 0
					bldr.unsafeAppendBoolsToBitmap(pattern.valid, len(pattern.valid))
				}
			})
		}
	}
}

func BenchmarkAppendValuesWithValidity(b *testing.B) {
	const length = 65536
	patterns := []struct {
		name  string
		valid []bool
	}{
		{"all-valid", makeValidityBenchmarkValues(length, func(int) bool { return true })},
		{"10pct-null", makeValidityBenchmarkValues(length, func(i int) bool { return i%10 != 0 })},
		{"50pct-null", makeValidityBenchmarkValues(length, func(i int) bool { return i%2 == 0 })},
	}
	int8Values := make([]int8, length)
	int64Values := make([]int64, length)
	boolValues := makeValidityBenchmarkValues(length, func(i int) bool { return i%2 == 0 })
	stringValues := make([]string, length)
	for i := range stringValues {
		stringValues[i] = "x"
	}

	for _, pattern := range patterns {
		b.Run("int8/"+pattern.name, func(b *testing.B) {
			benchmarkAppendValues(b, func() (func(), func()) {
				bldr := NewInt8Builder(memory.DefaultAllocator)
				bldr.Reserve(length)
				return func() {
					bldr.AppendValues(int8Values, pattern.valid)
				}, bldr.Release
			})
		})
		b.Run("int64/"+pattern.name, func(b *testing.B) {
			benchmarkAppendValues(b, func() (func(), func()) {
				bldr := NewInt64Builder(memory.DefaultAllocator)
				bldr.Reserve(length)
				return func() {
					bldr.AppendValues(int64Values, pattern.valid)
				}, bldr.Release
			})
		})
		b.Run("boolean/"+pattern.name, func(b *testing.B) {
			benchmarkAppendValues(b, func() (func(), func()) {
				bldr := NewBooleanBuilder(memory.DefaultAllocator)
				bldr.Reserve(length)
				return func() {
					bldr.AppendValues(boolValues, pattern.valid)
				}, bldr.Release
			})
		})
		b.Run("string/"+pattern.name, func(b *testing.B) {
			benchmarkAppendValues(b, func() (func(), func()) {
				bldr := NewStringBuilder(memory.DefaultAllocator)
				bldr.Reserve(length)
				bldr.ReserveData(length)
				return func() {
					bldr.AppendValues(stringValues, pattern.valid)
				}, bldr.Release
			})
		})
	}
}

func benchmarkAppendValues(b *testing.B, setup func() (appendValues, release func())) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		appendValues, release := setup()
		b.StartTimer()
		appendValues()
		b.StopTimer()
		release()
	}
}

func makeValidityBenchmarkValues(length int, valid func(int) bool) []bool {
	values := make([]bool, length)
	for i := range values {
		values[i] = valid(i)
	}
	return values
}
