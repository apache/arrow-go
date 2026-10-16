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

package gentypes_test

import (
	"testing"

	"github.com/apache/arrow-go/arrgen/internal/gentypes"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestStreamingAppendIsAllocationFree is the strongest allocation claim arrgen
// makes, so it is asserted rather than left to a benchmark to hint at: with
// room reserved up front, appending a row of fixed-width columns allocates
// nothing at all. The reflection path has no equivalent - it cannot encode
// anything until the caller has materialized the whole slice.
func TestStreamingAppendIsAllocationFree(t *testing.T) {
	skipIfInstrumented(t)
	const runs = 2000

	a := gentypes.NewFixedAppender(memory.DefaultAllocator)
	defer a.Release()
	a.Reserve(runs + 8) // AllocsPerRun calls the function runs+1 times

	rows := makeFixed(4)
	i := 0
	// The row is read through a single reused variable, the way a caller
	// draining a stream would do it.
	var row gentypes.Fixed
	got := testing.AllocsPerRun(runs, func() {
		row = rows[i%len(rows)]
		i++
		a.Append(&row)
	})
	if got != 0 {
		t.Errorf("Append allocated %.2f times per row, want 0", got)
	}
	a.NewRecordBatch().Release()
}

// TestSchemaIsAllocationFree pins the other cost generation removes outright:
// arreflect walks the struct and builds a fresh schema on every call, while the
// generated schema is a package-level variable.
func TestSchemaIsAllocationFree(t *testing.T) {
	skipIfInstrumented(t)
	if got := testing.AllocsPerRun(1000, func() { _ = gentypes.RowSchema() }); got != 0 {
		t.Errorf("RowSchema allocated %.2f times per call, want 0", got)
	}

	reflected := testing.AllocsPerRun(100, func() {
		if _, err := arreflect.InferSchema[gentypes.Row](); err != nil {
			t.Fatal(err)
		}
	})
	if reflected == 0 {
		t.Skip("arreflect.InferSchema no longer allocates; the comparison is moot")
	}
	t.Logf("schema per call: arrgen 0 allocs, arreflect %.0f allocs", reflected)
}

// TestBatchEncodingAllocatesLessThanReflection compares whole-batch encoding.
// The margin here is smaller than for streaming because both paths pay for the
// same Arrow buffers; what the generated path saves is the per-value reflection
// machinery on top of them. The threshold is deliberately loose - the point is
// to catch a regression that erases the advantage, not to pin an exact number
// that Arrow's own buffer growth policy is free to change.
func TestBatchEncodingAllocatesLessThanReflection(t *testing.T) {
	skipIfInstrumented(t)
	const rows = 1024
	fixtures := makeFixed(rows)
	mem := memory.DefaultAllocator

	reflected := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(fixtures, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})
	generated := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			rec, err := gentypes.FixedRecordBatch(mem, fixtures)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})

	t.Logf("%d-row batch: arrgen %d allocs / %d ns, arreflect %d allocs / %d ns",
		rows, generated.AllocsPerOp(), generated.NsPerOp(), reflected.AllocsPerOp(), reflected.NsPerOp())

	if generated.AllocsPerOp() >= reflected.AllocsPerOp() {
		t.Errorf("generated encoder allocated %d times per batch, want fewer than arreflect's %d",
			generated.AllocsPerOp(), reflected.AllocsPerOp())
	}
	if got, limit := generated.AllocsPerOp(), reflected.AllocsPerOp()*3/4; got > limit {
		t.Errorf("generated encoder allocated %d times per batch, want at most %d (three quarters of arreflect's %d)",
			got, limit, reflected.AllocsPerOp())
	}
}

// TestBatchEncodingIsFasterThanReflection guards the headline claim. Wall-clock
// assertions are noisy on shared CI, so the bar is set at a fraction of the
// margin actually measured (around 3.5x on the fixed-width fixture) and the
// test steps aside for -short runs.
func TestBatchEncodingIsFasterThanReflection(t *testing.T) {
	skipIfInstrumented(t)
	if testing.Short() {
		t.Skip("timing comparison skipped in short mode")
	}
	const rows = 1024
	fixtures := makeFixed(rows)
	mem := memory.DefaultAllocator

	reflected := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(fixtures, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})
	generated := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			rec, err := gentypes.FixedRecordBatch(mem, fixtures)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})
	if generated.NsPerOp() == 0 || reflected.NsPerOp() == 0 {
		t.Skip("benchmark produced no timing data")
	}

	speedup := float64(reflected.NsPerOp()) / float64(generated.NsPerOp())
	t.Logf("%d-row batch: arrgen %d ns, arreflect %d ns (%.2fx)", rows, generated.NsPerOp(), reflected.NsPerOp(), speedup)
	if speedup < 1.5 {
		t.Errorf("generated encoder is only %.2fx faster than arreflect, want at least 1.5x", speedup)
	}
}

func skipIfInstrumented(t *testing.T) {
	t.Helper()
	if instrumented {
		t.Skip("allocation counts and timings are not meaningful under -race or -asan")
	}
}
