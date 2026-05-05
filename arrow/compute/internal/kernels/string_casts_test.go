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
	"errors"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestReserveFormattedDataOverflowGuard verifies that reserveFormattedData
// returns arrow.ErrInvalid (rather than panicking inside the builder's
// ReserveData) when (Len - Nulls) * perValueBytes would overflow int32.
// Tested at the kernel level so we don't need to drive >2 GB of input
// through the full executor to trigger the guard. See GH-184.
func TestReserveFormattedDataOverflowGuard(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()

	span := &exec.ArraySpan{
		Len:   int64(math.MaxInt32/20) + 1,
		Nulls: 0,
	}

	err := reserveFormattedData(bldr, span, 20)
	if err == nil {
		t.Fatal("expected overflow guard to return an error")
	}
	if !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("expected arrow.ErrInvalid, got %v", err)
	}
}

// TestFormattedDataLimitPerBuilder verifies the per-destination single-buffer
// limit used by reserveFormattedData: utf8 and string_view are capped at
// MaxInt32 (int32 offsets / single overflow buffer), while large_utf8 is
// capped at MaxInt64 (int64 offsets). Regression test for GH-184: the
// previous hardcoded MaxInt32 guard incorrectly rejected valid large_utf8
// casts above 2 GiB.
func TestFormattedDataLimitPerBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	utf8Bldr := array.NewStringBuilder(mem)
	defer utf8Bldr.Release()
	viewBldr := array.NewStringViewBuilder(mem)
	defer viewBldr.Release()
	largeBldr := array.NewLargeStringBuilder(mem)
	defer largeBldr.Release()

	cases := []struct {
		name string
		bldr array.StringLikeBuilder
		want int64
	}{
		{"utf8", utf8Bldr, math.MaxInt32},
		{"string_view", viewBldr, math.MaxInt32},
		{"large_utf8", largeBldr, math.MaxInt64},
	}
	for _, tc := range cases {
		if got := formattedDataLimit(tc.bldr); got != tc.want {
			t.Errorf("formattedDataLimit(%s): want %d, got %d", tc.name, tc.want, got)
		}
	}
}

// TestReserveFormattedDataAllowsLargeUtf8AboveInt32 is the regression test
// for the GH-184 second-round review finding: reserveFormattedData must not
// reject totals above MaxInt32 when the destination is large_utf8
// (LargeStringBuilder), because int64 offsets can represent them. We assert
// the destination-specific limit via formattedDataLimit rather than driving
// a multi-gigabyte allocation through the full kernel path, and separately
// confirm the guard still fires for string_view at the int32 boundary.
func TestReserveFormattedDataAllowsLargeUtf8AboveInt32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	largeBldr := array.NewLargeStringBuilder(mem)
	defer largeBldr.Release()

	if got := formattedDataLimit(largeBldr); got <= math.MaxInt32 {
		t.Fatalf("large_utf8 destination limit must exceed MaxInt32, got %d", got)
	}

	viewBldr := array.NewStringViewBuilder(mem)
	defer viewBldr.Release()
	span := &exec.ArraySpan{
		Len:   int64(math.MaxInt32/20) + 1,
		Nulls: 0,
	}
	if err := reserveFormattedData(viewBldr, span, 20); !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("string_view guard should reject total > MaxInt32, got %v", err)
	}
}
