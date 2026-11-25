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
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var dataReservationTypes = []struct {
	name string
	typ  arrow.DataType
}{
	{"utf8", arrow.BinaryTypes.String},
	{"large_utf8", arrow.BinaryTypes.LargeString},
	{"string_view", arrow.BinaryTypes.StringView},
}

// offsetReservationTypes lists the offset builders, which grow their data
// buffer in place; a view builder's data buffer cannot grow.
var offsetReservationTypes = []struct {
	name string
	typ  arrow.DataType
}{
	{"utf8", arrow.BinaryTypes.String},
	{"large_utf8", arrow.BinaryTypes.LargeString},
}

// TestDataReservationOutgrowsHint verifies that a dataReservation on an
// offset builder appends correctly when the output is larger than its
// hint, the path the utf8 case kernels would take if a future Go version's
// Unicode tables grew a code point by more than the hint assumes. Without
// the growth, the append would silently truncate the value.
func TestDataReservationOutgrowsHint(t *testing.T) {
	// A nil entry is appended as null; nulls consume no reservation.
	values := [][]byte{
		[]byte("ab"),
		nil,
		bytes.Repeat([]byte("x"), 12),
		bytes.Repeat([]byte("y"), 70000),
		nil,
		{},
		bytes.Repeat([]byte("z"), 100000),
	}
	var total int64
	for _, v := range values {
		total += int64(len(v))
	}
	// The output size right before the last value.
	boundary := int64(len(values[0]) + len(values[2]) + len(values[3]))

	for _, tc := range offsetReservationTypes {
		// Hints below total exercise growth at different points: before
		// the first value, mid-array, exactly at a value boundary; the
		// last one fits the whole output.
		for _, hint := range []int64{0, 3, 1 << 10, boundary, total} {
			t.Run(fmt.Sprintf("%s/hint=%d", tc.name, hint), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
				defer mem.AssertSize(t, 0)

				bldr := array.NewBuilder(mem, tc.typ).(array.StringLikeBuilder)
				defer bldr.Release()
				bldr.Reserve(len(values))
				resv, err := newDataReservation(bldr, hint)
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range values {
					if v == nil {
						bldr.AppendNull()
						continue
					}
					if err := resv.append(v); err != nil {
						t.Fatal(err)
					}
				}

				arr := bldr.NewArray().(array.StringLike)
				defer arr.Release()
				if arr.Len() != len(values) {
					t.Fatalf("length: got %d, want %d", arr.Len(), len(values))
				}
				for i, v := range values {
					if v == nil {
						if arr.IsValid(i) {
							t.Errorf("value %d: got %q, want null", i, arr.Value(i))
						}
						continue
					}
					if got := arr.Value(i); got != string(v) {
						t.Errorf("value %d: got %d bytes, want %d bytes", i, len(got), len(v))
					}
				}
			})
		}
	}
}

// TestDataReservationViewOutgrowsHint verifies that a string_view output
// which outgrows its hint fails with arrow.ErrInvalid instead of spilling
// into a second data buffer, which a kernel could not return.
func TestDataReservationViewOutgrowsHint(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()
	bldr.Reserve(3)
	// Room for the first value only.
	v := bytes.Repeat([]byte("y"), 70000)
	resv, err := newDataReservation(bldr, int64(len(v)))
	if err != nil {
		t.Fatal(err)
	}
	if err := resv.append(v); err != nil {
		t.Fatal(err)
	}
	if err := resv.append(bytes.Repeat([]byte("z"), 100000)); !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("second value: got %v, want arrow.ErrInvalid", err)
	}
	// An inline value still fits.
	if err := resv.append([]byte("ab")); err != nil {
		t.Fatal(err)
	}

	arr := bldr.NewArray().(array.StringLike)
	defer arr.Release()
	if arr.Len() != 2 || arr.Value(0) != string(v) || arr.Value(1) != "ab" {
		t.Fatalf("got %d values %q, %q; want the first value and %q", arr.Len(), arr.Value(0), arr.Value(1), "ab")
	}
	if n := len(arr.Data().Buffers()) - 2; n != 1 {
		t.Errorf("got %d data buffers, want 1", n)
	}
}

// countingBuilder counts the data reservations asked of a builder.
type countingBuilder struct {
	array.StringLikeBuilder
	reserves int
}

func (b *countingBuilder) ReserveData(n int) {
	b.reserves++
	b.StringLikeBuilder.ReserveData(n)
}

// TestDataReservationGrowsGeometrically verifies that an output past its
// hint doubles the reservation per growth rather than reserving room for
// one value at a time, which would reallocate the data buffer per value.
func TestDataReservationGrowsGeometrically(t *testing.T) {
	const (
		values = 1000
		size   = 16
	)
	v := bytes.Repeat([]byte("a"), size)

	for _, tc := range offsetReservationTypes {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			bldr := &countingBuilder{StringLikeBuilder: array.NewBuilder(mem, tc.typ).(array.StringLikeBuilder)}
			defer bldr.Release()
			bldr.Reserve(values)
			// A hint for a single value; the output is values times that.
			resv, err := newDataReservation(bldr, size)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < values; i++ {
				if err := resv.append(v); err != nil {
					t.Fatal(err)
				}
			}

			// Doubling from size to values*size takes log2(values) growths.
			if grows := bldr.reserves - 1; grows > 12 {
				t.Errorf("got %d data reservations for %d values, want at most 12", bldr.reserves, values)
			}
			if resv.written != values*size || resv.reserved < resv.written {
				t.Errorf("written %d, reserved %d, want reserved >= written == %d", resv.written, resv.reserved, values*size)
			}

			arr := bldr.NewArray().(array.StringLike)
			defer arr.Release()
			for i := 0; i < values; i++ {
				if got := arr.Value(i); got != string(v) {
					t.Fatalf("value %d: got %d bytes, want %d bytes", i, len(got), size)
				}
			}
		})
	}
}

// TestDataReservationGrowthCappedAtLimit verifies that growth stops at the
// builder's limit: the reservation never exceeds it, and the next value
// that does not fit returns arrow.ErrInvalid.
func TestDataReservationGrowthCappedAtLimit(t *testing.T) {
	v := []byte("abcdefghijklmnop")

	for _, tc := range offsetReservationTypes {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			bldr := array.NewBuilder(mem, tc.typ).(array.StringLikeBuilder)
			defer bldr.Release()
			bldr.Reserve(4)
			// Room for one value under the hint and three under the limit.
			resv, err := newDataReservation(bldr, int64(len(v)))
			if err != nil {
				t.Fatal(err)
			}
			resv.limit = 3 * int64(len(v))
			for i := 0; i < 3; i++ {
				if err := resv.append(v); err != nil {
					t.Fatalf("value %d: %v", i, err)
				}
			}
			if resv.reserved != resv.limit {
				t.Errorf("reserved %d bytes, want the %d byte limit", resv.reserved, resv.limit)
			}
			if err := resv.append(v); !errors.Is(err, arrow.ErrInvalid) {
				t.Fatalf("fourth value: got %v, want arrow.ErrInvalid", err)
			}
		})
	}
}

// TestDataReservationLimit verifies that append returns arrow.ErrInvalid
// instead of letting the output exceed what the builder can address, and
// leaves the values appended before the failure intact.
func TestDataReservationLimit(t *testing.T) {
	// Long enough to be out-of-line for string_view.
	v := []byte("abcdefghijklmnop")

	for _, tc := range dataReservationTypes {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			bldr := array.NewBuilder(mem, tc.typ).(array.StringLikeBuilder)
			defer bldr.Release()
			bldr.Reserve(5)
			// Room for three values only.
			resv, err := newDataReservation(bldr, 3*int64(len(v)))
			if err != nil {
				t.Fatal(err)
			}
			resv.limit = 3 * int64(len(v))

			var appended int
			for err == nil && appended < 5 {
				err = resv.append(v)
				if err == nil {
					appended++
				}
			}
			if !errors.Is(err, arrow.ErrInvalid) {
				t.Fatalf("fourth value: got %v, want arrow.ErrInvalid", err)
			}
			if appended != 3 || resv.written != 3*int64(len(v)) {
				t.Errorf("appended %d values, %d bytes; want 3 values, %d bytes", appended, resv.written, 3*len(v))
			}

			arr := bldr.NewArray().(array.StringLike)
			defer arr.Release()
			if arr.Len() != 3 {
				t.Fatalf("length: got %d, want 3", arr.Len())
			}
			for i := 0; i < 3; i++ {
				if got := arr.Value(i); got != string(v) {
					t.Errorf("value %d: got %q, want %q", i, got, v)
				}
			}
		})
	}
}

// TestDataReservationInlineViews verifies that inline string_view values
// consume no reservation: they live in their headers, so neither the
// reservation nor the limit counts them.
func TestDataReservationInlineViews(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()
	bldr.Reserve(100)
	// Nothing reserved and no room under the limit.
	resv, err := newDataReservation(bldr, 0)
	if err != nil {
		t.Fatal(err)
	}
	resv.limit = 0
	for i := 0; i < 100; i++ {
		if err := resv.append([]byte("ab")); err != nil {
			t.Fatalf("inline value %d: %v", i, err)
		}
	}
	if resv.written != 0 {
		t.Errorf("inline values counted %d bytes against the reservation, want 0", resv.written)
	}

	arr := bldr.NewArray().(array.StringLike)
	defer arr.Release()
	if arr.Len() != 100 {
		t.Fatalf("length: got %d, want 100", arr.Len())
	}
	for i := 0; i < 100; i++ {
		if got := arr.Value(i); got != "ab" {
			t.Fatalf("value %d: got %q, want %q", i, got, "ab")
		}
	}
}

// requestingBuilder records the data reservation asked of it without
// making it, so a limit-sized request needs no memory.
type requestingBuilder struct {
	array.StringLikeBuilder
	asked int
}

func (b *requestingBuilder) ReserveData(n int) { b.asked += n }

// TestDataReservationHintCappedAtLimit verifies how a hint over the
// builder's limit is handled: an offset builder reserves the limit, since
// the hint is not a bound and append enforces the limit on the actual
// output, while a view builder fails up front since its output cannot
// grow past the reserved buffer.
func TestDataReservationHintCappedAtLimit(t *testing.T) {
	t.Run("utf8", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		bldr := &requestingBuilder{StringLikeBuilder: array.NewStringBuilder(mem)}
		defer bldr.Release()
		limit := formattedDataLimit(bldr)
		resv, err := newDataReservation(bldr, limit+1)
		if err != nil {
			t.Fatalf("hint over the limit: got %v, want no error", err)
		}
		if resv.reserved != limit || bldr.asked != int(limit) {
			t.Errorf("reserved %d bytes (asked %d), want the %d byte limit", resv.reserved, bldr.asked, limit)
		}
	})

	t.Run("string_view", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		bldr := array.NewStringViewBuilder(mem)
		defer bldr.Release()
		_, err := newDataReservation(bldr, formattedDataLimit(bldr)+1)
		if !errors.Is(err, arrow.ErrInvalid) {
			t.Fatalf("hint over the limit: got %v, want arrow.ErrInvalid", err)
		}
	})
}
