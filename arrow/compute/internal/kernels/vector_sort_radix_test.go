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
	"math"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func checkRadixIntegerType[T radixInteger](t *testing.T, numBytes int, signBit uint64) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
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
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
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

// specTotalOrder is an independent reference for IEEE 754 totalOrder on non-payload NaNs:
// -NaN < -Inf < negatives < -0 < +0 < positives < +Inf < +NaN.
func specTotalOrder(a, b float64) int {
	rank := func(v float64) int {
		switch {
		case math.IsNaN(v) && math.Signbit(v):
			return 0
		case math.IsNaN(v):
			return 2
		}
		return 1
	}
	if c := cmp.Compare(rank(a), rank(b)); c != 0 || rank(a) != 1 {
		return c
	}
	if c := cmp.Compare(a, b); c != 0 {
		return c
	}
	// Equal numerically: only -0 vs +0 can still differ.
	return cmp.Compare(boolInt(!math.Signbit(a)), boolInt(!math.Signbit(b)))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// TestSortIndicesSingleChunkRadixFloatTotalOrder covers the radix fast path (single sliced
// chunk, no nulls, >= radixSortMinRows) for SortKey.FloatTotalOrder against the reference order.
func TestSortIndicesSingleChunkRadixFloatTotalOrder(t *testing.T) {
	const start, n = 37, 4096
	special := []float64{math.NaN(), math.Float64frombits(0xFFF8000000000000), math.Inf(1), math.Inf(-1),
		0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64}
	values := make([]float64, start+n+19)
	for i := range values {
		x := uint64(i)*6364136223846793005 + 1442695040888963407
		if x%5 == 0 {
			values[i] = special[int(x>>33)%len(special)]
		} else {
			values[i] = float64(int64(x>>40)%2001-1000) / 8
		}
	}

	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float32, arrow.FixedWidthTypes.Float16} {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
		bldr := array.NewBuilder(mem, dt)
		// Values as stored in the column type (narrowing keeps NaN signs, may round magnitudes).
		stored := make([]float64, len(values))
		for i, v := range values {
			switch b := bldr.(type) {
			case *array.Float64Builder:
				b.Append(v)
				stored[i] = v
			case *array.Float32Builder:
				b.Append(float32(v))
				stored[i] = float64(float32(v))
			case *array.Float16Builder:
				h := float16.New(float32(v))
				b.Append(h)
				stored[i] = float64(h.Float32())
			}
		}
		full := bldr.NewArray()
		bldr.Release()
		sliced := array.NewSlice(full, start, start+n)
		col := arrow.NewChunked(dt, []arrow.Array{sliced})

		for _, order := range []SortOrder{Ascending, Descending} {
			t.Run(fmt.Sprintf("%s/%v", dt, order), func(t *testing.T) {
				result, err := SortIndices(ctx, []*arrow.Chunked{col}, []SortKey{{Order: order, FloatTotalOrder: true}})
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
					c := specTotalOrder(stored[start+int(a)], stored[start+int(b)])
					if order == Descending {
						return -c
					}
					return c
				})
				if !slices.Equal(got, want) {
					t.Fatalf("radix total-order sort differs from reference stable sort")
				}
			})
		}
		col.Release()
		sliced.Release()
		full.Release()
		mem.AssertSize(t, 0)
	}
}

// totalOrderBitPatterns lists raw bit patterns in ascending IEEE 754 totalOrder for a float
// format with the given exponent and mantissa widths. It covers NaN payloads and
// signaling vs quiet NaNs: for positive NaNs, signaling < quiet and larger payloads sort
// later; negative NaNs mirror that.
func totalOrderBitPatterns(expBits, mantBits uint) []uint64 {
	sign := uint64(1) << (expBits + mantBits)
	exp := ((uint64(1) << expBits) - 1) << mantBits
	quiet := uint64(1) << (mantBits - 1)
	one := (((uint64(1) << (expBits - 1)) - 1) << mantBits) // 1.0
	pos := []uint64{
		0,                    // +0
		1,                    // smallest subnormal
		one,                  // 1.0
		exp - 1,              // largest finite
		exp,                  // +Inf
		exp | 1,              // sNaN, payload 1
		exp | 2,              // sNaN, payload 2
		exp | quiet,          // canonical qNaN
		exp | quiet | 1,      // qNaN, payload 1
		exp | (quiet<<1 - 1), // qNaN, all mantissa bits set
	}
	out := make([]uint64, 0, 2*len(pos))
	for i := len(pos) - 1; i >= 0; i-- {
		out = append(out, pos[i]|sign) // negatives, most negative first
	}
	return append(out, pos...)
}

// TestSortIndicesFloatTotalOrderNaNPayloads checks NaN payload and signaling/quiet ordering
// with raw bit patterns, on both the comparator path (below radixSortMinRows) and the
// radix fast path.
func TestSortIndicesFloatTotalOrderNaNPayloads(t *testing.T) {
	formats := []struct {
		dt                arrow.DataType
		expBits, mantBits uint
	}{
		{arrow.PrimitiveTypes.Float64, 11, 52},
		{arrow.PrimitiveTypes.Float32, 8, 23},
		{arrow.FixedWidthTypes.Float16, 5, 10},
	}
	for _, f := range formats {
		sorted := totalOrderBitPatterns(f.expBits, f.mantBits)
		for _, reps := range []int{1, radixSortMinRows/len(sorted) + 1} {
			for _, order := range []SortOrder{Ascending, Descending} {
				t.Run(fmt.Sprintf("%s/reps=%d/%v", f.dt, reps, order), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
					defer mem.AssertSize(t, 0)
					ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}

					// Deterministically shuffled input, repeated reps times.
					bits := make([]uint64, 0, len(sorted)*reps)
					for r := range reps {
						for i := range sorted {
							bits = append(bits, sorted[(i*7+r)%len(sorted)])
						}
					}
					bldr := array.NewBuilder(mem, f.dt)
					for _, b := range bits {
						switch bb := bldr.(type) {
						case *array.Float64Builder:
							bb.Append(math.Float64frombits(b))
						case *array.Float32Builder:
							bb.Append(math.Float32frombits(uint32(b)))
						case *array.Float16Builder:
							bb.Append(float16.FromBits(uint16(b)))
						}
					}
					arr := bldr.NewArray()
					bldr.Release()
					defer arr.Release()
					col := arrow.NewChunked(f.dt, []arrow.Array{arr})
					defer col.Release()

					result, err := SortIndices(ctx, []*arrow.Chunked{col}, []SortKey{{Order: order, FloatTotalOrder: true}})
					if err != nil {
						t.Fatal(err)
					}
					defer result.Release()
					got := arrow.GetData[uint64](result.Buffers[1].Buf)[:len(bits)]

					rank := make(map[uint64]int, len(sorted))
					for i, b := range sorted {
						rank[b] = i
					}
					for k := 1; k < len(got); k++ {
						prev, cur := rank[bits[got[k-1]]], rank[bits[got[k]]]
						if (order == Ascending && prev > cur) || (order == Descending && prev < cur) {
							t.Fatalf("position %d: bits %#x sorted before %#x", k, bits[got[k-1]], bits[got[k]])
						}
					}
				})
			}
		}
	}
}
