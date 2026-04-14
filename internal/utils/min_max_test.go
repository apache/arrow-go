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
	"math/rand/v2"
	"testing"
)

func TestMinMaxInt32(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 63, 64, 100, 1024} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
			values := make([]int32, size)
			for i := range values {
				values[i] = r.Int32() - math.MaxInt32/2
			}
			if size > 0 {
				values[r.IntN(size)] = math.MinInt32
				values[r.IntN(size)] = math.MaxInt32
			}

			goMin, goMax := int32MinMax(values)
			min, max := GetMinMaxInt32(values)
			if min != goMin || max != goMax {
				t.Errorf("n=%d: got min=%d max=%d, want min=%d max=%d", size, min, max, goMin, goMax)
			}
		})
	}
}

func TestMinMaxUint32(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 63, 64, 100, 1024} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			values := make([]uint32, size)
			r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
			for i := range values {
				values[i] = r.Uint32()
			}
			if size > 0 {
				values[r.IntN(size)] = 0
				values[r.IntN(size)] = math.MaxUint32
			}

			goMin, goMax := uint32MinMax(values)
			min, max := GetMinMaxUint32(values)
			if min != goMin || max != goMax {
				t.Errorf("n=%d: got min=%d max=%d, want min=%d max=%d", size, min, max, goMin, goMax)
			}
		})
	}
}

func TestMinMaxInt64(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 63, 64, 100, 1024} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
			values := make([]int64, size)
			for i := range values {
				values[i] = r.Int64() - math.MaxInt64/2
			}
			if size > 0 {
				values[r.IntN(size)] = math.MinInt64
				values[r.IntN(size)] = math.MaxInt64
			}

			goMin, goMax := int64MinMax(values)
			min, max := GetMinMaxInt64(values)
			if min != goMin || max != goMax {
				t.Errorf("n=%d: got min=%d max=%d, want min=%d max=%d", size, min, max, goMin, goMax)
			}
		})
	}
}

func TestMinMaxUint64(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 63, 64, 100, 1024} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
			values := make([]uint64, size)
			for i := range values {
				values[i] = r.Uint64()
			}
			if size > 0 {
				values[r.IntN(size)] = 0
				values[r.IntN(size)] = math.MaxUint64
			}

			goMin, goMax := uint64MinMax(values)
			min, max := GetMinMaxUint64(values)
			if min != goMin || max != goMax {
				t.Errorf("n=%d: got min=%d max=%d, want min=%d max=%d", size, min, max, goMin, goMax)
			}
		})
	}
}

var (
	benchMinI32 int32
	benchMaxI32 int32
	benchMinU32 uint32
	benchMaxU32 uint32
	benchMinI64 int64
	benchMaxI64 int64
	benchMinU64 uint64
	benchMaxU64 uint64
)

func BenchmarkMinMaxInt32(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 8192, 65536} {
		values := make([]int32, size)
		r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
		for i := range values {
			values[i] = r.Int32() - math.MaxInt32/2
		}
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size) * 4)
			for i := 0; i < b.N; i++ {
				benchMinI32, benchMaxI32 = GetMinMaxInt32(values)
			}
		})
	}
}

func BenchmarkMinMaxUint32(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 8192, 65536} {
		values := make([]uint32, size)
		r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
		for i := range values {
			values[i] = r.Uint32()
		}
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size) * 4)
			for i := 0; i < b.N; i++ {
				benchMinU32, benchMaxU32 = GetMinMaxUint32(values)
			}
		})
	}
}

func BenchmarkMinMaxInt64(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 8192, 65536} {
		values := make([]int64, size)
		r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
		for i := range values {
			values[i] = r.Int64() - math.MaxInt64/2
		}
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size) * 8)
			for i := 0; i < b.N; i++ {
				benchMinI64, benchMaxI64 = GetMinMaxInt64(values)
			}
		})
	}
}

func BenchmarkMinMaxUint64(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 8192, 65536} {
		values := make([]uint64, size)
		r := rand.New(&rand.PCG{}) // zero-seed for reproducibility
		for i := range values {
			values[i] = r.Uint64()
		}
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size) * 8)
			for i := 0; i < b.N; i++ {
				benchMinU64, benchMaxU64 = GetMinMaxUint64(values)
			}
		})
	}
}
