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

package kernels

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

const (
	maxInt32Value = int32(1<<31 - 1)
	minInt32Value = int32(-1 << 31)
	maxInt64Value = int64(1<<63 - 1)
	minInt64Value = int64(-1 << 63)
)

func TestMultiplyConstant(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65}
	factors := []int64{
		0,
		1,
		-1,
		3,
		-3,
		1_000_000,
		1 << 31,
		-(1 << 31),
		1 << 40,
		-(1 << 40),
		maxInt64Value,
		minInt64Value,
	}

	for _, length := range lengths {
		input32 := makeInt32Values(length)
		input64 := makeInt64Values(length)
		for _, factor := range factors {
			name := fmt.Sprintf("length_%d/factor_%s", length, strconv.FormatInt(factor, 10))
			t.Run(name, func(t *testing.T) {
				assertMultiplyInt32Int32(t, input32, factor)
				assertMultiplyInt32Int64(t, input32, factor)
				assertMultiplyInt64Int32(t, input64, factor)
				assertMultiplyInt64Int64(t, input64, factor)
			})
		}
	}
}

func TestMultiplyConstantRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(23))
	for iteration := 0; iteration < 64; iteration++ {
		for _, length := range []int{1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 65, 257} {
			input32, input64 := make([]int32, length), make([]int64, length)
			for i := range input32 {
				input32[i] = int32(rng.Uint32())
				input64[i] = int64(rng.Uint64())
			}
			factor := int64(rng.Uint64())
			assertMultiplyInt32Int32(t, input32, factor)
			assertMultiplyInt32Int64(t, input32, factor)
			assertMultiplyInt64Int32(t, input64, factor)
			assertMultiplyInt64Int64(t, input64, factor)
		}
	}
}

func assertMultiplyInt32Int32(t *testing.T, input []int32, factor int64) {
	t.Helper()
	want := make([]int32, len(input))
	multiplyConstantGo(input, want, factor)
	got := make([]int32, len(input))
	multiplyConstant(input, got, factor)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("int32 -> int32 mismatch for factor %d: got %v, want %v", factor, got, want)
	}
}

func assertMultiplyInt32Int64(t *testing.T, input []int32, factor int64) {
	t.Helper()
	want := make([]int64, len(input))
	multiplyConstantGo(input, want, factor)
	got := make([]int64, len(input))
	multiplyConstant(input, got, factor)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("int32 -> int64 mismatch for factor %d: got %v, want %v", factor, got, want)
	}
}

func assertMultiplyInt64Int32(t *testing.T, input []int64, factor int64) {
	t.Helper()
	want := make([]int32, len(input))
	multiplyConstantGo(input, want, factor)
	got := make([]int32, len(input))
	multiplyConstant(input, got, factor)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("int64 -> int32 mismatch for factor %d: got %v, want %v", factor, got, want)
	}
}

func assertMultiplyInt64Int64(t *testing.T, input []int64, factor int64) {
	t.Helper()
	want := make([]int64, len(input))
	multiplyConstantGo(input, want, factor)
	got := make([]int64, len(input))
	multiplyConstant(input, got, factor)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("int64 -> int64 mismatch for factor %d: got %v, want %v", factor, got, want)
	}
}

func makeInt32Values(length int) []int32 {
	values := []int32{
		0,
		1,
		-1,
		2,
		-2,
		123456789,
		-123456789,
		maxInt32Value,
		minInt32Value,
	}
	result := make([]int32, length)
	for i := range result {
		result[i] = values[i%len(values)]
	}
	return result
}

func makeInt64Values(length int) []int64 {
	values := []int64{
		0,
		1,
		-1,
		2,
		-2,
		1 << 40,
		-(1 << 40),
		maxInt64Value,
		minInt64Value,
	}
	result := make([]int64, length)
	for i := range result {
		result[i] = values[i%len(values)]
	}
	return result
}

func BenchmarkMultiplyConstant(b *testing.B) {
	for _, size := range []int{1 << 10, 1 << 14, 1 << 20} {
		b.Run(fmt.Sprintf("int32_int32/%d/generic", size), func(b *testing.B) {
			input := makeInt32Values(size)
			output := make([]int32, size)
			benchmarkMultiplyConstant(b, int64(size*4), output, func() {
				multiplyConstantGo(input, output, 1_000_000)
			})
		})
		b.Run(fmt.Sprintf("int32_int32/%d/dispatch", size), func(b *testing.B) {
			input := makeInt32Values(size)
			output := make([]int32, size)
			benchmarkMultiplyConstant(b, int64(size*4), output, func() {
				multiplyConstant(input, output, 1_000_000)
			})
		})

		b.Run(fmt.Sprintf("int32_int64/%d/generic", size), func(b *testing.B) {
			input := makeInt32Values(size)
			output := make([]int64, size)
			benchmarkMultiplyConstant(b, int64(size*4), output, func() {
				multiplyConstantGo(input, output, 1_000_000)
			})
		})
		b.Run(fmt.Sprintf("int32_int64/%d/dispatch", size), func(b *testing.B) {
			input := makeInt32Values(size)
			output := make([]int64, size)
			benchmarkMultiplyConstant(b, int64(size*4), output, func() {
				multiplyConstant(input, output, 1_000_000)
			})
		})

		b.Run(fmt.Sprintf("int64_int32/%d/generic", size), func(b *testing.B) {
			input := makeInt64Values(size)
			output := make([]int32, size)
			benchmarkMultiplyConstant(b, int64(size*8), output, func() {
				multiplyConstantGo(input, output, 1_000_000)
			})
		})
		b.Run(fmt.Sprintf("int64_int32/%d/dispatch", size), func(b *testing.B) {
			input := makeInt64Values(size)
			output := make([]int32, size)
			benchmarkMultiplyConstant(b, int64(size*8), output, func() {
				multiplyConstant(input, output, 1_000_000)
			})
		})

		b.Run(fmt.Sprintf("int64_int64/%d/generic", size), func(b *testing.B) {
			input := makeInt64Values(size)
			output := make([]int64, size)
			benchmarkMultiplyConstant(b, int64(size*8), output, func() {
				multiplyConstantGo(input, output, 1_000_000)
			})
		})
		b.Run(fmt.Sprintf("int64_int64/%d/dispatch", size), func(b *testing.B) {
			input := makeInt64Values(size)
			output := make([]int64, size)
			benchmarkMultiplyConstant(b, int64(size*8), output, func() {
				multiplyConstant(input, output, 1_000_000)
			})
		})
	}
}

func benchmarkMultiplyConstant(b *testing.B, bytes int64, output any, fn func()) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(bytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn()
	}
	b.StopTimer()
	runtime.KeepAlive(output)
}
