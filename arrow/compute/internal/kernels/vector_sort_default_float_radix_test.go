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

// compareDefaultFloatReference independently encodes the ordering used by
// Arrow's default single-key stable sort, not its radix key implementation.
func compareDefaultFloatReference(a, b float64, aValid, bValid bool, key SortKey) int {
	if !aValid || !bValid {
		if aValid == bValid {
			return 0
		}
		if !aValid {
			if key.NullPlacement == NullsAtStart {
				return -1
			}
			return 1
		}
		if key.NullPlacement == NullsAtStart {
			return 1
		}
		return -1
	}

	aNaN, bNaN := math.IsNaN(a), math.IsNaN(b)
	if aNaN || bNaN {
		if aNaN == bNaN {
			return 0
		}
		if aNaN {
			if key.NullPlacement == NullsAtStart {
				return -1
			}
			return 1
		}
		if key.NullPlacement == NullsAtStart {
			return 1
		}
		return -1
	}

	result := cmp.Compare(a, b) // -0 == +0, even for descending order
	if key.Order == Descending {
		return -result
	}
	return result
}

func TestSortIndicesSingleChunkRadixDefaultFloat(t *testing.T) {
	const start, n = 37, 4096
	for _, dt := range []arrow.DataType{
		arrow.FixedWidthTypes.Float16,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	} {
		for _, nullEvery := range []int{0, 29, 1} {
			t.Run(fmt.Sprintf("%s/nullEvery=%d", dt, nullEvery), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
				ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
				bldr := array.NewBuilder(mem, dt)
				stored := make([]float64, start+n+19)
				valid := make([]bool, len(stored))
				for i := range stored {
					if nullEvery != 0 && i%nullEvery == 0 {
						bldr.AppendNull()
						continue
					}
					valid[i] = true
					x := uint64(i)*6364136223846793005 + 1442695040888963407
					v := float64(int64(x>>40)%2001-1000) / 8
					switch i % 11 {
					case 0:
						v = math.NaN()
					case 1:
						v = math.Float64frombits(0xfff8000000000001) // negative NaN
					case 2:
						v = math.Inf(-1)
					case 3:
						v = math.Inf(1)
					case 4:
						v = math.Copysign(0, -1)
					case 5:
						v = 0
					}
					switch b := bldr.(type) {
					case *array.Float16Builder:
						h := float16.New(float32(v))
						b.Append(h)
						stored[i] = float64(h.Float32())
					case *array.Float32Builder:
						b.Append(float32(v))
						stored[i] = float64(float32(v))
					case *array.Float64Builder:
						b.Append(v)
						stored[i] = v
					}
				}
				full := bldr.NewArray()
				bldr.Release()
				sliced := array.NewSlice(full, start, start+n)
				col := arrow.NewChunked(dt, []arrow.Array{sliced})

				for _, order := range []SortOrder{Ascending, Descending} {
					for _, placement := range []NullPlacement{NullsAtStart, NullsAtEnd} {
						key := SortKey{Order: order, NullPlacement: placement}
						result, err := SortIndices(ctx, []*arrow.Chunked{col}, []SortKey{key})
						if err != nil {
							t.Fatalf("order=%d placement=%d: %v", order, placement, err)
						}
						got := arrow.GetData[uint64](result.Buffers[1].Buf)[:n]
						want := make([]uint64, n)
						for i := range want {
							want[i] = uint64(i)
						}
						slices.SortStableFunc(want, func(a, b uint64) int {
							ai, bi := start+int(a), start+int(b)
							return compareDefaultFloatReference(stored[ai], stored[bi], valid[ai], valid[bi], key)
						})
						if !slices.Equal(got, want) {
							for i := range want {
								if got[i] != want[i] {
									t.Errorf("order=%d placement=%d index=%d: got row %d, want row %d",
										order, placement, i, got[i], want[i])
									break
								}
							}
						}
						result.Release()
					}
				}

				col.Release()
				sliced.Release()
				full.Release()
				mem.AssertSize(t, 0)
			})
		}
	}
}

// TestSortIndicesDefaultFloat16AllBitPatterns exercises every Float16 bit pattern,
// including both NaN signs, signaling NaNs, infinities, subnormals and signed
// zeros. The reference uses numeric comparison rather than the radix key map.
func TestSortIndicesDefaultFloat16AllBitPatterns(t *testing.T) {
	const n = 1 << 16
	for _, nullEvery := range []int{0, 29} {
		t.Run(fmt.Sprintf("nullEvery=%d", nullEvery), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
			builder := array.NewFloat16Builder(mem)
			values := make([]float64, n)
			valid := make([]bool, n)
			for i := range values {
				if nullEvery != 0 && i%nullEvery == 0 {
					builder.AppendNull()
					continue
				}
				h := float16.FromBits(uint16(i))
				builder.Append(h)
				values[i] = float64(h.Float32())
				valid[i] = true
			}
			arr := builder.NewArray()
			builder.Release()
			col := arrow.NewChunked(arrow.FixedWidthTypes.Float16, []arrow.Array{arr})

			for _, order := range []SortOrder{Ascending, Descending} {
				for _, placement := range []NullPlacement{NullsAtStart, NullsAtEnd} {
					key := SortKey{Order: order, NullPlacement: placement}
					result, err := SortIndices(ctx, []*arrow.Chunked{col}, []SortKey{key})
					if err != nil {
						t.Fatalf("order=%d placement=%d: %v", order, placement, err)
					}
					got := arrow.GetData[uint64](result.Buffers[1].Buf)[:n]
					want := make([]uint64, n)
					for i := range want {
						want[i] = uint64(i)
					}
					slices.SortStableFunc(want, func(a, b uint64) int {
						return compareDefaultFloatReference(
							values[a], values[b], valid[a], valid[b], key)
					})
					if !slices.Equal(got, want) {
						for i := range want {
							if got[i] != want[i] {
								t.Errorf("order=%d placement=%d at %d: got %d, want %d",
									order, placement, i, got[i], want[i])
								break
							}
						}
					}
					result.Release()
				}
			}
			col.Release()
			arr.Release()
			mem.AssertSize(t, 0)
		})
	}
}
