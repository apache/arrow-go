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

//go:build go1.18

package kernels

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Benchmarks target kernels.SortIndices (chunked comparators + stable sort) without compute
// registry or CallFunction overhead. Use e.g.:
//
//	go test -bench=BenchmarkSortIndices -benchmem -cpuprofile=cpu.prof ./arrow/compute/internal/kernels/
//	go tool pprof -http=:8080 cpu.prof

func newBenchKernelCtx(tb testing.TB) (*exec.KernelCtx, memory.Allocator) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
	return ctx, mem
}

// makeChunkedInt64Split returns n int64 rows in numChunks contiguous arrays. Values are a
// deterministic function of global row index so the sort does non-trivial work.
func makeChunkedInt64Split(tb testing.TB, mem memory.Allocator, n, numChunks int) *arrow.Chunked {
	tb.Helper()
	if numChunks < 1 {
		numChunks = 1
	}
	if n < numChunks {
		numChunks = n
	}
	base := n / numChunks
	rem := n % numChunks
	chunks := make([]arrow.Array, 0, numChunks)
	global := 0
	for c := 0; c < numChunks; c++ {
		sz := base
		if c < rem {
			sz++
		}
		bld := array.NewInt64Builder(mem)
		for i := 0; i < sz; i++ {
			x := int64(global + i)
			bld.Append((x * 6364136223846793005) ^ (x >> 12))
		}
		arr := bld.NewArray()
		chunks = append(chunks, arr)
		global += sz
	}
	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	tb.Cleanup(func() { ch.Release() })
	return ch
}

func BenchmarkSortIndices_Int64(b *testing.B) {
	const rows = 65536
	for _, numChunks := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("rows=%d/chunks=%d", rows, numChunks), func(b *testing.B) {
			ctx, mem := newBenchKernelCtx(b)
			col := makeChunkedInt64Split(b, mem, rows, numChunks)
			keys := []SortKey{{ColumnIndex: 0, Order: Ascending, NullPlacement: NullsAtEnd}}
			columns := []*arrow.Chunked{col}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := SortIndices(ctx, columns, keys)
				if err != nil {
					b.Fatal(err)
				}
				res.Release()
			}
		})
	}
}

func BenchmarkSortIndices_Int64_TwoKeys(b *testing.B) {
	const rows = 65536
	const numChunks = 64
	ctx, mem := newBenchKernelCtx(b)
	colA := makeChunkedInt64Split(b, mem, rows, numChunks)
	colB := makeChunkedInt64Split(b, mem, rows, numChunks)
	keys := []SortKey{
		{ColumnIndex: 0, Order: Ascending, NullPlacement: NullsAtEnd},
		{ColumnIndex: 1, Order: Descending, NullPlacement: NullsAtStart},
	}
	columns := []*arrow.Chunked{colA, colB}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := SortIndices(ctx, columns, keys)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func makeChunkedStringSplit(tb testing.TB, mem memory.Allocator, n, numChunks int) *arrow.Chunked {
	tb.Helper()
	if numChunks < 1 {
		numChunks = 1
	}
	if n < numChunks {
		numChunks = n
	}
	base := n / numChunks
	rem := n % numChunks
	chunks := make([]arrow.Array, 0, numChunks)
	global := 0
	for c := 0; c < numChunks; c++ {
		sz := base
		if c < rem {
			sz++
		}
		bld := array.NewStringBuilder(mem)
		for i := 0; i < sz; i++ {
			x := global + i
			v := (x * 6364136223846793005) ^ (x >> 12)
			bld.Append(fmt.Sprintf("%016x", v))
		}
		arr := bld.NewArray()
		chunks = append(chunks, arr)
		global += sz
	}
	ch := arrow.NewChunked(arrow.BinaryTypes.String, chunks)
	tb.Cleanup(func() { ch.Release() })
	return ch
}

func BenchmarkSortIndices_String(b *testing.B) {
	const rows = 65536
	for _, numChunks := range []int{1, 32} {
		b.Run(fmt.Sprintf("rows=%d/chunks=%d", rows, numChunks), func(b *testing.B) {
			ctx, mem := newBenchKernelCtx(b)
			col := makeChunkedStringSplit(b, mem, rows, numChunks)
			keys := []SortKey{{ColumnIndex: 0, Order: Ascending, NullPlacement: NullsAtEnd}}
			columns := []*arrow.Chunked{col}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := SortIndices(ctx, columns, keys)
				if err != nil {
					b.Fatal(err)
				}
				res.Release()
			}
		})
	}
}
