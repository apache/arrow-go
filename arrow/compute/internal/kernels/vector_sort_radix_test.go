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

//go:build go1.22

package kernels

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func checkRadixIntegerType[T radixInteger](t *testing.T, numBytes int, signBit uint64) {
	t.Helper()
	ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), memory.NewGoAllocator())}
	for _, n := range []int{0, 1, 2, 255, 256, 257, 2048, 8192} {
		for _, pattern := range []string{"random", "duplicates", "ascending", "descending", "constant", "extremes"} {
			values := make([]T, n)
			rng := uint64(0x9e3779b97f4a7c15)
			maxUnsigned := ^uint64(0)
			for i := range values {
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				switch pattern {
				case "random":
					values[i] = T(rng)
				case "duplicates":
					values[i] = T(int64(i%19) - 9)
				case "ascending":
					values[i] = T(i)
				case "descending":
					values[i] = T(n - i)
				case "constant":
					values[i] = T(7)
				case "extremes":
					switch i % 4 {
					case 0:
						values[i] = T(uint64(1) << uint(numBytes*8-1))
					case 1:
						values[i] = T(maxUnsigned)
					case 2:
						values[i] = T(0)
					case 3:
						values[i] = T(uint64(1) << uint(numBytes*8-2))
					}
				}
			}
			for _, order := range []SortOrder{Ascending, Descending} {
				got := make([]uint64, n)
				want := make([]uint64, n)
				for i := range got {
					got[i], want[i] = uint64(i), uint64(i)
				}
				slices.SortStableFunc(want, func(a, b uint64) int {
					return compareOrdered(order, values[a], values[b])
				})
				radixSortIntegerIndices(ctx, got, values, numBytes, signBit, order)
				if !slices.Equal(got, want) {
					t.Fatalf("%d-byte n=%d %s order=%v: got %v, want %v", numBytes, n, pattern, order, got, want)
				}
			}
		}
	}
}

func TestRadixSortIntegerIndices(t *testing.T) {
	t.Run("int8", func(t *testing.T) { checkRadixIntegerType[int8](t, 1, 1<<7) })
	t.Run("int16", func(t *testing.T) { checkRadixIntegerType[int16](t, 2, 1<<15) })
	t.Run("int32", func(t *testing.T) { checkRadixIntegerType[int32](t, 4, 1<<31) })
	t.Run("int64", func(t *testing.T) { checkRadixIntegerType[int64](t, 8, 1<<63) })
	t.Run("uint8", func(t *testing.T) { checkRadixIntegerType[uint8](t, 1, 0) })
	t.Run("uint16", func(t *testing.T) { checkRadixIntegerType[uint16](t, 2, 0) })
	t.Run("uint32", func(t *testing.T) { checkRadixIntegerType[uint32](t, 4, 0) })
	t.Run("uint64", func(t *testing.T) { checkRadixIntegerType[uint64](t, 8, 0) })
	t.Run("timestamp", func(t *testing.T) { checkRadixIntegerType[arrow.Timestamp](t, 8, 1<<63) })
	t.Run("date32", func(t *testing.T) { checkRadixIntegerType[arrow.Date32](t, 4, 1<<31) })
}

func TestSortIndicesSingleChunkRadixSliced(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
	const start, n = 73, 4096
	values := make([]int64, start+n+41)
	for i := range values {
		x := int64(i)
		v := (x * 6364136223846793005) ^ ((x + 1) * 1442695040888963407)
		values[i] = (v % 113) - 56
	}

	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(values, nil)
	full := b.NewArray()
	defer full.Release()
	sliced := array.NewSlice(full, start, start+n)
	defer sliced.Release()
	col := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{sliced})
	defer col.Release()

	for _, order := range []SortOrder{Ascending, Descending} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			result, err := SortIndices(ctx, []*arrow.Chunked{col}, []SortKey{{Order: order, NullPlacement: NullsAtStart}})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			got := arrow.GetData[uint64](result.Buffers[1].Buf)[:n]
			want := make([]uint64, n)
			for i := range want {
				want[i] = uint64(i)
			}
			slices.SortStableFunc(want, func(a, b uint64) int {
				v := cmp.Compare(values[start+int(a)], values[start+int(b)])
				if order == Descending {
					return -v
				}
				return v
			})
			if !slices.Equal(got, want) {
				t.Fatalf("radix sort of sliced array differs from stable sort")
			}
		})
	}
}
