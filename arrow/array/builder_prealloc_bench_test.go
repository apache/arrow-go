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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkBuilder_AppendOne tests baseline single append performance
func BenchmarkBuilder_AppendOne_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Append(int64(i))
	}
}

// BenchmarkBuilder_AppendBulk tests bulk append method
func BenchmarkBuilder_AppendBulk_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	// Prepare data
	const batchSize = 1000
	data := make([]int64, batchSize)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

// BenchmarkBuilder_PreReserved tests with manual Reserve()
func BenchmarkBuilder_PreReserved_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := array.NewInt64Builder(mem)
		builder.Reserve(1000)
		b.StartTimer()

		for j := 0; j < 1000; j++ {
			builder.Append(int64(j))
		}

		b.StopTimer()
		builder.Release()
		b.StartTimer()
	}
}

// BenchmarkBuilder_NoReserve tests without Reserve()
func BenchmarkBuilder_NoReserve_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := array.NewInt64Builder(mem)
		b.StartTimer()

		for j := 0; j < 1000; j++ {
			builder.Append(int64(j))
		}

		b.StopTimer()
		builder.Release()
		b.StartTimer()
	}
}

// BenchmarkStringBuilder_VarLength tests variable-length string building
func BenchmarkStringBuilder_VarLength_Small(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	// Small strings (10 chars each)
	const batchSize = 100
	data := make([]string, batchSize)
	for i := range data {
		data[i] = "test_str_x"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

func BenchmarkStringBuilder_VarLength_Medium(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	// Medium strings (100 chars each)
	const batchSize = 100
	data := make([]string, batchSize)
	baseStr := make([]byte, 100)
	for i := range baseStr {
		baseStr[i] = 'a'
	}
	for i := range data {
		data[i] = string(baseStr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

func BenchmarkStringBuilder_VarLength_Large(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	// Large strings (1KB each)
	const batchSize = 100
	data := make([]string, batchSize)
	baseStr := make([]byte, 1024)
	for i := range baseStr {
		baseStr[i] = 'a'
	}
	for i := range data {
		data[i] = string(baseStr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

// BenchmarkStringBuilder_WithReserveData tests ReserveData optimization
func BenchmarkStringBuilder_WithReserveData(b *testing.B) {
	mem := memory.NewGoAllocator()

	const batchSize = 100
	data := make([]string, batchSize)
	baseStr := make([]byte, 100)
	for i := range baseStr {
		baseStr[i] = 'a'
	}
	for i := range data {
		data[i] = string(baseStr)
	}

	totalDataSize := len(data) * len(data[0])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := array.NewStringBuilder(mem)
		builder.Reserve(len(data))
		builder.ReserveData(totalDataSize)
		b.StartTimer()

		builder.AppendValues(data, nil)

		b.StopTimer()
		builder.Release()
		b.StartTimer()
	}
}

func BenchmarkStringBuilder_NoReserveData(b *testing.B) {
	mem := memory.NewGoAllocator()

	const batchSize = 100
	data := make([]string, batchSize)
	baseStr := make([]byte, 100)
	for i := range baseStr {
		baseStr[i] = 'a'
	}
	for i := range data {
		data[i] = string(baseStr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := array.NewStringBuilder(mem)
		b.StartTimer()

		builder.AppendValues(data, nil)

		b.StopTimer()
		builder.Release()
		b.StartTimer()
	}
}

// BenchmarkBinaryBuilder_LargeData tests large binary data
func BenchmarkBinaryBuilder_LargeData(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(mem, nil)
	defer builder.Release()

	// 1MB per element
	const dataSize = 1024 * 1024
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.SetBytes(dataSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Append(data)
	}
}

func BenchmarkBinaryBuilder_LargeData_WithReserve(b *testing.B) {
	mem := memory.NewGoAllocator()

	// 1MB per element
	const dataSize = 1024 * 1024
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.SetBytes(dataSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		builder := array.NewBinaryBuilder(mem, nil)
		builder.Reserve(1)
		builder.ReserveData(dataSize)
		b.StartTimer()

		builder.Append(data)

		b.StopTimer()
		builder.Release()
		b.StartTimer()
	}
}

// Benchmark different sized batches for Int64
func BenchmarkBuilder_Batch10_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	data := make([]int64, 10)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

func BenchmarkBuilder_Batch100_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	data := make([]int64, 100)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

func BenchmarkBuilder_Batch1000_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	data := make([]int64, 1000)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}

func BenchmarkBuilder_Batch10000_Int64(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	data := make([]int64, 10000)
	for i := range data {
		data[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.AppendValues(data, nil)
	}
}
