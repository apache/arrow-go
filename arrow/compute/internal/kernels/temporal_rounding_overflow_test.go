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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package kernels

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestTemporalCheckedArithmeticBoundaries(t *testing.T) {
	tests := []struct {
		name string
		fn   func() (int64, error)
	}{
		{name: "positive multiply overflow", fn: func() (int64, error) {
			return checkedMulInt64(math.MaxInt64, 2)
		}},
		{name: "negative multiply overflow", fn: func() (int64, error) {
			return checkedMulInt64(math.MinInt64, 2)
		}},
		{name: "minimum times negative one", fn: func() (int64, error) {
			return checkedMulInt64(math.MinInt64, -1)
		}},
		{name: "subtraction below minimum", fn: func() (int64, error) {
			return checkedSubInt64(math.MinInt64, 1)
		}},
		{name: "subtraction above maximum", fn: func() (int64, error) {
			return checkedSubInt64(math.MaxInt64, -1)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if !errors.Is(err, arrow.ErrInvalid) {
				t.Fatalf("got error %v, want arrow.ErrInvalid", err)
			}
		})
	}
}

func TestTimeToNanosBoundaries(t *testing.T) {
	minTime := time.Unix(0, math.MinInt64)
	maxTime := time.Unix(0, math.MaxInt64)
	tests := []struct {
		name  string
		value time.Time
		want  int64
		err   bool
	}{
		{name: "exact minimum", value: minTime, want: math.MinInt64},
		{name: "exact maximum", value: maxTime, want: math.MaxInt64},
		{name: "one below minimum", value: minTime.Add(-time.Nanosecond), err: true},
		{name: "one above maximum", value: maxTime.Add(time.Nanosecond), err: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := timeToNanos(tc.value)
			if tc.err {
				if !errors.Is(err, arrow.ErrInvalid) {
					t.Fatalf("got error %v, want arrow.ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("timeToNanos returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("timeToNanos(%s) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}

func TestRoundToMultipleInt64OverflowBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		multiple int64
		mode     RoundMode
		strict   bool
		want     int64
		err      bool
	}{
		{name: "strict ceiling past maximum", value: math.MaxInt64, multiple: 2, mode: RoundUp, strict: true, err: true},
		{name: "strict ceiling exact past maximum", value: math.MaxInt64 - 1, multiple: 2, mode: RoundUp, strict: true, err: true},
		{name: "floor at minimum", value: math.MinInt64, multiple: 2, mode: RoundDown, want: math.MinInt64},
		{name: "odd multiple three", value: 2, multiple: 3, mode: HalfToEven, want: 3},
		{name: "odd multiple five", value: 2, multiple: 5, mode: HalfDown, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := roundToMultipleInt64(tc.value, tc.multiple, tc.mode, tc.strict)
			if tc.err {
				if !errors.Is(err, arrow.ErrInvalid) {
					t.Fatalf("got error %v, want arrow.ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("roundToMultipleInt64 returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("roundToMultipleInt64(%d, %d) = %d, want %d", tc.value, tc.multiple, got, tc.want)
			}
		})
	}
}

func TestRoundToMultipleInt64HalfModes(t *testing.T) {
	tests := []struct {
		name  string
		mode  RoundMode
		value int64
		want  int64
	}{
		{name: "half down positive", mode: HalfDown, value: 2, want: 0},
		{name: "half down negative", mode: HalfDown, value: -2, want: -4},
		{name: "half up positive", mode: HalfUp, value: 2, want: 4},
		{name: "half up negative", mode: HalfUp, value: -2, want: 0},
		{name: "half to even lower", mode: HalfToEven, value: 2, want: 0},
		{name: "half to even upper", mode: HalfToEven, value: 6, want: 8},
		{name: "half to even negative lower", mode: HalfToEven, value: -2, want: 0},
		{name: "half to even negative upper", mode: HalfToEven, value: -6, want: -8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := roundToMultipleInt64(tc.value, 4, tc.mode, false)
			if err != nil {
				t.Fatalf("roundToMultipleInt64 returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("roundToMultipleInt64(%d, 4, %s) = %d, want %d", tc.value, tc.mode, got, tc.want)
			}
		})
	}
}
