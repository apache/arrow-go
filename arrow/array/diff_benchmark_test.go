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

package array_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var diffBenchmarkEdits array.Edits

func benchmarkInt64Arrays(n int, changed bool) (base, target *array.Int64) {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i)
	}
	targetValues := append([]int64(nil), values...)
	if changed {
		targetValues[n-1]++
	}

	baseBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	baseBuilder.AppendValues(values, nil)
	base = baseBuilder.NewInt64Array()
	baseBuilder.Release()

	targetBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	targetBuilder.AppendValues(targetValues, nil)
	target = targetBuilder.NewInt64Array()
	targetBuilder.Release()
	return
}

func benchmarkStringArrays(n int, changed bool) (base, target *array.String) {
	values := make([]string, n)
	for i := range values {
		values[i] = "value"
	}
	targetValues := append([]string(nil), values...)
	if changed {
		targetValues[n-1] = "other"
	}

	baseBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	baseBuilder.AppendValues(values, nil)
	base = baseBuilder.NewStringArray()
	baseBuilder.Release()

	targetBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	targetBuilder.AppendValues(targetValues, nil)
	target = targetBuilder.NewStringArray()
	targetBuilder.Release()
	return
}

func benchmarkBinaryArrays(n int, changed bool) (base, target *array.Binary) {
	values := make([][]byte, n)
	for i := range values {
		values[i] = []byte("value")
	}
	targetValues := append([][]byte(nil), values...)
	if changed {
		targetValues[n-1] = []byte("other")
	}

	baseBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	baseBuilder.AppendValues(values, nil)
	base = baseBuilder.NewBinaryArray()
	baseBuilder.Release()

	targetBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	targetBuilder.AppendValues(targetValues, nil)
	target = targetBuilder.NewBinaryArray()
	targetBuilder.Release()
	return
}

func BenchmarkDiffInt64Equal(b *testing.B) {
	base, target := benchmarkInt64Arrays(65536, false)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}

func BenchmarkDiffInt64ChangedLast(b *testing.B) {
	base, target := benchmarkInt64Arrays(65536, true)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}

func BenchmarkDiffStringEqual(b *testing.B) {
	base, target := benchmarkStringArrays(65536, false)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}

func BenchmarkDiffStringChangedLast(b *testing.B) {
	base, target := benchmarkStringArrays(65536, true)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}

func BenchmarkDiffBinaryEqual(b *testing.B) {
	base, target := benchmarkBinaryArrays(65536, false)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}

func BenchmarkDiffBinaryChangedLast(b *testing.B) {
	base, target := benchmarkBinaryArrays(65536, true)
	defer base.Release()
	defer target.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		diffBenchmarkEdits, _ = array.Diff(base, target)
	}
}
