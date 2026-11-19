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
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

const radixSortMinRows = 2048

type radixInteger interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// sortIndicesSingleIntRadix handles single-chunk integer and temporal values,
// including nullable arrays. Other types and multi-chunk arrays continue to
// use the stable comparator sort.
func sortIndicesSingleIntRadix(ctx *exec.KernelCtx, arr arrow.Array, indices []uint64, order SortOrder, nullPlacement NullPlacement) bool {
	switch values := arr.(type) {
	case *array.Int8:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 1, 1<<7, order, nullPlacement)
	case *array.Int16:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 2, 1<<15, order, nullPlacement)
	case *array.Int32:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 4, 1<<31, order, nullPlacement)
	case *array.Int64:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 1<<63, order, nullPlacement)
	case *array.Uint8:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 1, 0, order, nullPlacement)
	case *array.Uint16:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 2, 0, order, nullPlacement)
	case *array.Uint32:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 4, 0, order, nullPlacement)
	case *array.Uint64:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 0, order, nullPlacement)
	case *array.Date32:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 4, 1<<31, order, nullPlacement)
	case *array.Date64:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 1<<63, order, nullPlacement)
	case *array.Time32:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 4, 1<<31, order, nullPlacement)
	case *array.Time64:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 1<<63, order, nullPlacement)
	case *array.Timestamp:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 1<<63, order, nullPlacement)
	case *array.Duration:
		radixSortNullableIntegerIndices(ctx, arr, indices, values.Values(), 8, 1<<63, order, nullPlacement)
	default:
		return false
	}
	return true
}

// radixSortNullableIntegerIndices keeps nulls in stable input order while
// allowing the non-null rows to use the radix fast path. The partition buffer
// doubles as radix scratch, so only one index-sized temporary is allocated.
func radixSortNullableIntegerIndices[T radixInteger](ctx *exec.KernelCtx, arr arrow.Array, indices []uint64, values []T, numBytes int, signBit uint64, order SortOrder, nullPlacement NullPlacement) {
	if arr.NullN() == len(indices) {
		// All rows compare equal; the input permutation is already stable.
		return
	}

	var scratch []uint64
	if arr.NullN() != 0 {
		buf := ctx.Allocate(len(indices) * arrow.Uint64SizeBytes)
		defer buf.Release()
		scratch = arrow.GetData[uint64](buf.Buf())[:len(indices)]

		partition := partitionNullsOnly(indices, scratch, 0, len(indices), nullPlacement,
			func(idx uint64) bool { return arr.IsNull(int(idx)) })
		indices = indices[partition.nonNullsLo:partition.nonNullsHi]
	}

	radixSortIntegerIndicesWithScratch(ctx, indices, values, numBytes, signBit, order, scratch)
}

// sortIndicesSingleFloatTotalOrderRadix is the SortKey.FloatTotalOrder counterpart of
// sortIndicesSingleIntRadix for a single chunk of non-null floats. The IEEE 754
// totalOrder maps each float to a signed integer key, so the floats are radix
// sorted through a materialized key buffer.
func sortIndicesSingleFloatTotalOrderRadix(ctx *exec.KernelCtx, arr arrow.Array, indices []uint64, order SortOrder) bool {
	switch values := arr.(type) {
	case *array.Float16:
		radixSortFloatTotalOrder(ctx, indices, values.Values(), 2, 1<<15, order,
			func(v float16.Num) int16 { return totalOrderKey16(v.Uint16()) })
	case *array.Float32:
		radixSortFloatTotalOrder(ctx, indices, values.Float32Values(), 4, 1<<31, order,
			func(v float32) int32 { return totalOrderKey32(math.Float32bits(v)) })
	case *array.Float64:
		radixSortFloatTotalOrder(ctx, indices, values.Float64Values(), 8, 1<<63, order,
			func(v float64) int64 { return totalOrderKey64(math.Float64bits(v)) })
	default:
		return false
	}
	return true
}

func radixSortFloatTotalOrder[F any, K int16 | int32 | int64](ctx *exec.KernelCtx, indices []uint64, values []F, numBytes int, signBit uint64, order SortOrder, toKey func(F) K) {
	if len(values) < 2 {
		return
	}
	var zero K
	keyBuf := ctx.Allocate(len(values) * int(unsafe.Sizeof(zero)))
	defer keyBuf.Release()
	keys := arrow.GetData[K](keyBuf.Buf())[:len(values)]
	for i, v := range values {
		keys[i] = toKey(v)
	}
	radixSortIntegerIndices(ctx, indices, keys, numBytes, signBit, order)
}

// radixSortIntegerIndices performs a stable LSD radix sort over the logical row
// indices. Flipping the sign bit orders signed integers correctly; inverting the
// key reverses the sort order without reversing equal-value rows.
//
// All histograms are gathered before scattering so bytes that are constant
// across the input can be skipped, including the upper bytes of small values.
func radixSortIntegerIndices[T radixInteger](ctx *exec.KernelCtx, indices []uint64, values []T, numBytes int, signBit uint64, order SortOrder) {
	radixSortIntegerIndicesWithScratch(ctx, indices, values, numBytes, signBit, order, nil)
}

// radixSortIntegerIndicesWithScratch accepts a caller-owned scratch slice,
// avoiding another allocation after the stable null partition.
func radixSortIntegerIndicesWithScratch[T radixInteger](ctx *exec.KernelCtx, indices []uint64, values []T, numBytes int, signBit uint64, order SortOrder, scratch []uint64) {
	if len(indices) < 2 {
		return
	}

	// Input indices start in row order. Leave already ordered and all-equal
	// values untouched instead of paying for histogram and scatter passes.
	previous := values[indices[0]]
	alreadyOrdered := true
	for _, idx := range indices[1:] {
		value := values[idx]
		if (order == Ascending && previous > value) ||
			(order == Descending && previous < value) {
			alreadyOrdered = false
			break
		}
		previous = value
	}
	if alreadyOrdered {
		return
	}

	flip := signBit
	if order == Descending {
		flip = ^flip
	}

	var hist [8][256]int
	for _, idx := range indices {
		key := uint64(values[idx]) ^ flip
		for pass := 0; pass < numBytes; pass++ {
			hist[pass][byte(key>>(8*pass))]++
		}
	}

	var active [8]bool
	activePasses := 0
	for pass := 0; pass < numBytes; pass++ {
		active[pass] = true
		for _, count := range hist[pass] {
			if count == len(indices) {
				active[pass] = false
				break
			}
		}
		if active[pass] {
			activePasses++
		}
	}
	if activePasses == 0 {
		return
	}

	if len(scratch) < len(indices) {
		scratchBuf := ctx.Allocate(len(indices) * arrow.Uint64SizeBytes)
		defer scratchBuf.Release()
		scratch = arrow.GetData[uint64](scratchBuf.Buf())[:len(indices)]
	} else {
		scratch = scratch[:len(indices)]
	}
	src, dst := indices, scratch
	for pass := 0; pass < numBytes; pass++ {
		if !active[pass] {
			continue
		}
		var positions [256]int
		sum := 0
		for i, count := range hist[pass] {
			positions[i] = sum
			sum += count
		}
		shift := 8 * pass
		for _, idx := range src {
			bucket := byte((uint64(values[idx]) ^ flip) >> shift)
			dst[positions[bucket]] = idx
			positions[bucket]++
		}
		src, dst = dst, src
	}
	if activePasses%2 != 0 {
		copy(indices, src)
	}
}
