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

package utils

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
)

func TestCopyDictionary(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		testCopyDictionary(t, []int32{-10, 0, 10, 100, math.MinInt32, math.MaxInt32})
	})
	t.Run("float32", func(t *testing.T) {
		testCopyDictionary(t, []float32{-10.5, 0, 10.5, 100.25})
	})
	t.Run("int64", func(t *testing.T) {
		testCopyDictionary(t, []int64{-10, 0, 10, 100, math.MinInt64, math.MaxInt64})
	})
	t.Run("float64", func(t *testing.T) {
		testCopyDictionary(t, []float64{-10.5, 0, 10.5, 100.25})
	})
}

func testCopyDictionary[T parquet.ColumnTypes](t *testing.T, dictionary []T) {
	t.Helper()
	for _, length := range []int{0, 1, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33} {
		indices := make([]IndexType, length)
		for i := range indices {
			indices[i] = IndexType((i*5 + 1) % len(dictionary))
		}

		got := make([]T, length)
		if !CopyDictionary(got, dictionary, indices) {
			copyDictionaryScalar(got, dictionary, indices)
		}

		want := make([]T, length)
		copyDictionaryScalar(want, dictionary, indices)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("length %d: got %v, want %v", length, got, want)
		}
	}
}

func TestCopyDictionaryPreservesFloatBits(t *testing.T) {
	dict32 := []float32{
		math.Float32frombits(0x00000001),
		math.Float32frombits(0x80000000),
		math.Float32frombits(0x7fc00001),
		math.Float32frombits(0xffc00001),
	}
	indices := make([]IndexType, 33)
	for i := range indices {
		indices[i] = IndexType((i * 3) % len(dict32))
	}
	got32 := make([]float32, len(indices))
	if !CopyDictionary(got32, dict32, indices) {
		copyDictionaryScalar(got32, dict32, indices)
	}
	for i, idx := range indices {
		if got, want := math.Float32bits(got32[i]), math.Float32bits(dict32[idx]); got != want {
			t.Fatalf("float32 index %d: got %#x, want %#x", i, got, want)
		}
	}

	dict64 := []float64{
		math.Float64frombits(0x0000000000000001),
		math.Float64frombits(0x8000000000000000),
		math.Float64frombits(0x7ff8000000000001),
		math.Float64frombits(0xfff8000000000001),
	}
	got64 := make([]float64, len(indices))
	if !CopyDictionary(got64, dict64, indices) {
		copyDictionaryScalar(got64, dict64, indices)
	}
	for i, idx := range indices {
		if got, want := math.Float64bits(got64[i]), math.Float64bits(dict64[idx]); got != want {
			t.Fatalf("float64 index %d: got %#x, want %#x", i, got, want)
		}
	}
}

func TestCopyDictionaryRejectsUnsupportedOrShortOutput(t *testing.T) {
	if CopyDictionary(make([]bool, 64), []bool{false, true}, make([]IndexType, 64)) {
		t.Fatal("boolean dictionary unexpectedly used fixed-width gather")
	}

	indices := make([]IndexType, 64)
	out := make([]int32, len(indices)-1)
	for i := range out {
		out[i] = -1
	}
	if CopyDictionary(out, []int32{1, 2}, indices) {
		t.Fatal("short output unexpectedly used fixed-width gather")
	}
	for i, got := range out {
		if got != -1 {
			t.Fatalf("short output was modified at %d: got %d", i, got)
		}
	}
}

func copyDictionaryScalar[T parquet.ColumnTypes](out, dictionary []T, indices []IndexType) {
	for i, idx := range indices {
		out[i] = dictionary[idx]
	}
}

func BenchmarkCopyDictionary(b *testing.B) {
	for _, typ := range []struct {
		name string
		run  func(*testing.B, int, int, string, bool)
	}{
		{
			name: "int32",
			run: func(b *testing.B, dictionarySize, length int, distribution string, dispatch bool) {
				benchmarkCopyDictionary(b, makeInt32Dictionary(dictionarySize), makeDictionaryIndices(length, dictionarySize, distribution), 4, dispatch)
			},
		},
		{
			name: "float32",
			run: func(b *testing.B, dictionarySize, length int, distribution string, dispatch bool) {
				benchmarkCopyDictionary(b, makeFloat32Dictionary(dictionarySize), makeDictionaryIndices(length, dictionarySize, distribution), 4, dispatch)
			},
		},
		{
			name: "int64",
			run: func(b *testing.B, dictionarySize, length int, distribution string, dispatch bool) {
				benchmarkCopyDictionary(b, makeInt64Dictionary(dictionarySize), makeDictionaryIndices(length, dictionarySize, distribution), 8, dispatch)
			},
		},
		{
			name: "float64",
			run: func(b *testing.B, dictionarySize, length int, distribution string, dispatch bool) {
				benchmarkCopyDictionary(b, makeFloat64Dictionary(dictionarySize), makeDictionaryIndices(length, dictionarySize, distribution), 8, dispatch)
			},
		},
	} {
		for _, dictionarySize := range []int{16, 256, 4096, 65536} {
			for _, length := range []int{1024, 65536} {
				for _, distribution := range []string{"sequential", "clustered", "uniform"} {
					name := fmt.Sprintf("%s/dict=%d/values=%d/%s", typ.name, dictionarySize, length, distribution)
					b.Run(name+"/scalar", func(b *testing.B) {
						typ.run(b, dictionarySize, length, distribution, false)
					})
					b.Run(name+"/dispatch", func(b *testing.B) {
						typ.run(b, dictionarySize, length, distribution, true)
					})
				}
			}
		}
	}
}

func benchmarkCopyDictionary[T parquet.ColumnTypes](b *testing.B, dictionary []T, indices []IndexType, bytes int64, dispatch bool) {
	out := make([]T, len(indices))
	b.ReportAllocs()
	b.SetBytes(int64(len(indices)) * bytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if dispatch && CopyDictionary(out, dictionary, indices) {
			continue
		}
		copyDictionaryScalar(out, dictionary, indices)
	}
	b.StopTimer()
	runtime.KeepAlive(out)
}

func makeDictionaryIndices(length, dictionarySize int, distribution string) []IndexType {
	indices := make([]IndexType, length)
	state := uint32(1)
	for i := range indices {
		switch distribution {
		case "sequential":
			indices[i] = IndexType(i % dictionarySize)
		case "clustered":
			indices[i] = IndexType((i/8 + i%4) % dictionarySize)
		case "uniform":
			state = state*1664525 + 1013904223
			indices[i] = IndexType(state % uint32(dictionarySize))
		}
	}
	return indices
}

func makeInt32Dictionary(length int) []int32 {
	dict := make([]int32, length)
	for i := range dict {
		dict[i] = int32(i*17 - length)
	}
	return dict
}

func makeFloat32Dictionary(length int) []float32 {
	dict := make([]float32, length)
	for i := range dict {
		dict[i] = float32(i)*1.25 - float32(length)
	}
	return dict
}

func makeInt64Dictionary(length int) []int64 {
	dict := make([]int64, length)
	for i := range dict {
		dict[i] = int64(i*17 - length)
	}
	return dict
}

func makeFloat64Dictionary(length int) []float64 {
	dict := make([]float64, length)
	for i := range dict {
		dict[i] = float64(i)*1.25 - float64(length)
	}
	return dict
}
