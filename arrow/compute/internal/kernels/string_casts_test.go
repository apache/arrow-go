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
// capped at min(MaxInt64, platform int max). On 64-bit builds the large_utf8
// limit is MaxInt64; on 32-bit builds it is clamped to MaxInt32 so the
// int(total) conversion in reserveFormattedData never overflows. Regression
// test for GH-184: the original MaxInt32 guard incorrectly rejected valid
// large_utf8 casts above 2 GiB on 64-bit platforms.
func TestFormattedDataLimitPerBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	utf8Bldr := array.NewStringBuilder(mem)
	defer utf8Bldr.Release()
	viewBldr := array.NewStringViewBuilder(mem)
	defer viewBldr.Release()
	largeBldr := array.NewLargeStringBuilder(mem)
	defer largeBldr.Release()

	largeWant := int64(math.MaxInt64)
	if largeWant > int64(math.MaxInt) {
		largeWant = int64(math.MaxInt)
	}

	cases := []struct {
		name string
		bldr array.StringLikeBuilder
		want int64
	}{
		{"utf8", utf8Bldr, math.MaxInt32},
		{"string_view", viewBldr, math.MaxInt32},
		{"large_utf8", largeBldr, largeWant},
	}
	for _, tc := range cases {
		if got := formattedDataLimit(tc.bldr); got != tc.want {
			t.Errorf("formattedDataLimit(%s): want %d, got %d", tc.name, tc.want, got)
		}
	}
}

// TestFormattedDataLimitFitsInPlatformInt is the regression test for the
// GH-184 third-round review finding: reserveFormattedData passes int(total)
// to ReserveData, so the returned limit must fit in the platform int to
// prevent overflow on 32-bit builds where int is 32-bit. The assertion
// holds on both 32- and 64-bit builds by construction.
func TestFormattedDataLimitFitsInPlatformInt(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldrs := []struct {
		name string
		bldr array.StringLikeBuilder
	}{
		{"utf8", array.NewStringBuilder(mem)},
		{"string_view", array.NewStringViewBuilder(mem)},
		{"large_utf8", array.NewLargeStringBuilder(mem)},
	}
	for _, tc := range bldrs {
		defer tc.bldr.Release()
		if limit := formattedDataLimit(tc.bldr); limit > int64(math.MaxInt) {
			t.Errorf("formattedDataLimit(%s) = %d exceeds platform math.MaxInt %d",
				tc.name, limit, int64(math.MaxInt))
		}
	}
}

// TestReserveFormattedDataAllowsLargeUtf8AboveInt32 is a platform-aware
// regression test for the GH-184 second-round review finding: on 64-bit
// builds, reserveFormattedData must not reject large_utf8 totals above
// MaxInt32 (int64 offsets can represent them); on 32-bit builds, the
// destination-specific limit is instead clamped to math.MaxInt32 so
// int(total) cannot overflow in reserveFormattedData. We assert the
// destination-specific limit via formattedDataLimit rather than driving
// a multi-gigabyte allocation through the full kernel path, and
// separately confirm the guard still fires for string_view at the
// int32 boundary.
func TestReserveFormattedDataAllowsLargeUtf8AboveInt32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	largeBldr := array.NewLargeStringBuilder(mem)
	defer largeBldr.Release()

	got := formattedDataLimit(largeBldr)
	if int64(math.MaxInt) > math.MaxInt32 {
		if got <= math.MaxInt32 {
			t.Fatalf("64-bit: large_utf8 destination limit must exceed MaxInt32, got %d", got)
		}
	} else {
		if got != math.MaxInt32 {
			t.Fatalf("32-bit: large_utf8 destination limit must clamp to MaxInt32, got %d", got)
		}
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

// TestReserveFormattedDataInlineViewSkip is the regression test for the
// GH-184 seventh-round review finding: for view builders, values whose
// per-value upper bound passes arrow.IsViewInline (strictly < 12 bytes)
// are stored inline in view headers and never consume overflow data.
// reserveFormattedData must skip both the reservation and the
// single-buffer limit check in that case, so large casts with
// inline-bounded values (bool, int8..int32, date32, etc.) are not
// rejected against the overflow-buffer limit they never use.
func TestReserveFormattedDataInlineViewSkip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	viewBldr := array.NewStringViewBuilder(mem)
	defer viewBldr.Release()

	span := &exec.ArraySpan{Len: math.MaxInt32, Nulls: 0}

	if err := reserveFormattedData(viewBldr, span, 5); err != nil {
		t.Fatalf("string_view with inline-bounded perValueBytes=5 must not error: %v", err)
	}
	if err := reserveFormattedData(viewBldr, span, 11); err != nil {
		t.Fatalf("string_view with boundary perValueBytes=11 must not error: %v", err)
	}
	// arrow.IsViewInline uses strict '<' against the 12-byte inline slot,
	// so 12 is already non-inline and the overflow-buffer limit applies.
	if err := reserveFormattedData(viewBldr, span, 12); !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("string_view with non-inline perValueBytes=12 must hit the limit, got %v", err)
	}

	utf8Bldr := array.NewStringBuilder(mem)
	defer utf8Bldr.Release()
	// utf8 builders are offset-based; inline skipping does not apply and
	// a 5-byte-per-row upper bound on MaxInt32 rows overflows int32.
	if err := reserveFormattedData(utf8Bldr, span, 5); !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("utf8 inline-skip must not apply; expected ErrInvalid, got %v", err)
	}
}
