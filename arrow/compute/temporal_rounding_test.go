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

package compute_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Standard input set for all temporal rounding tests
var standardTemporalInputs = []time.Time{
	time.Date(2023, 1, 15, 10, 7, 30, 123456789, time.UTC),   // 0: early in hour
	time.Date(2023, 1, 15, 10, 30, 45, 500000000, time.UTC),  // 1: exactly half hour
	time.Date(2023, 1, 15, 10, 52, 15, 750000000, time.UTC),  // 2: late in hour
	time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),            // 3: exactly on hour boundary
	time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),             // 4: midnight
	time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),              // 5: start of month
	time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),           // 6: mid-month
	time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),              // 7: epoch
	time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),          // 8: before epoch
	time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC), // 9: EDGE: year boundary (almost midnight)
	time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),            // 10: EDGE: leap day
	time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC),              // 11: EDGE: 1 second after boundary
	time.Date(2050, 12, 31, 23, 59, 59, 999999999, time.UTC), // 12: EDGE: far future with nanoseconds
}

type temporalTestVector struct {
	name      string
	funcName  string // "floor_temporal", "ceil_temporal", or "round_temporal"
	inputUnit arrow.TimeUnit
	expected  []time.Time
	opts      compute.RoundTemporalOptions
}

func TestTemporalRoundingVectors(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	testVectors := []temporalTestVector{
		{
			name:      "floor to hour",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:      "ceil to hour",
			funcName:  "ceil_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 15, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:      "ceil to hour strictly greater",
			funcName:  "ceil_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 1, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 1, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 15, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 1, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 13, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple:              1,
				Unit:                  compute.RoundTemporalHour,
				CeilIsStrictlyGreater: true,
			},
		},
		{
			name:      "round to hour",
			funcName:  "round_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 15, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:      "floor to 3 hours",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 12, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 21, 0, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 21, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 21, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 3,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:      "floor to 15 minutes",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 45, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 45, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 45, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 15,
				Unit:     compute.RoundTemporalMinute,
			},
		},
		{
			name:      "round to 15 minutes",
			funcName:  "round_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 15, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 45, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 15,
				Unit:     compute.RoundTemporalMinute,
			},
		},
		{
			name:      "floor to minute",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 59, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 59, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalMinute,
			},
		},
		{
			name:      "floor to second",
			funcName:  "floor_temporal",
			inputUnit: arrow.Millisecond,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 30, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 45, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 15, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 59, 59, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalSecond,
			},
		},
		{
			name:      "floor to millisecond",
			funcName:  "floor_temporal",
			inputUnit: arrow.Microsecond,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 30, 123000000, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 45, 500000000, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 15, 750000000, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 59, 59, 999000000, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 59, 59, 999000000, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalMillisecond,
			},
		},
		{
			name:      "floor to microsecond",
			funcName:  "floor_temporal",
			inputUnit: arrow.Nanosecond,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 30, 123456000, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 45, 500000000, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 15, 750000000, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 59, 59, 999999000, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 59, 59, 999999000, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalMicrosecond,
			},
		},
		{
			name:      "floor to day",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalDay,
			},
		},
		{
			name:      "floor to month",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalMonth,
			},
		},
		{
			name:      "ceil to month",
			funcName:  "ceil_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalMonth,
			},
		},
		{
			name:      "floor to year",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalYear,
			},
		},
		{
			name:      "floor to 30 seconds",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 30, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 30, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 23, 59, 30, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 23, 59, 30, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 30,
				Unit:     compute.RoundTemporalSecond,
			},
		},
		{
			name:      "round to 10 milliseconds",
			funcName:  "round_temporal",
			inputUnit: arrow.Millisecond,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 7, 30, 120000000, time.UTC),
				time.Date(2023, 1, 15, 10, 30, 45, 500000000, time.UTC),
				time.Date(2023, 1, 15, 10, 52, 15, 750000000, time.UTC),
				time.Date(2023, 1, 15, 11, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 30, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC),
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 10,
				Unit:     compute.RoundTemporalMillisecond,
			},
		},
		{
			name:      "floor to 2 hours with calendar origin",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 3, 15, 14, 0, 0, 0, time.UTC),
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 22, 0, 0, 0, time.UTC),
				time.Date(2023, 12, 31, 22, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2050, 12, 31, 22, 0, 0, 0, time.UTC),
			},
			opts: compute.RoundTemporalOptions{
				Multiple:            2,
				Unit:                compute.RoundTemporalHour,
				CalendarBasedOrigin: true,
			},
		},
	}

	for _, tv := range testVectors {
		t.Run(tv.name, func(t *testing.T) {
			// Build input array using standard inputs
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: tv.inputUnit})
			defer bldr.Release()

			for _, ts := range standardTemporalInputs {
				var tsVal arrow.Timestamp
				switch tv.inputUnit {
				case arrow.Second:
					tsVal = arrow.Timestamp(ts.Unix())
				case arrow.Millisecond:
					tsVal = arrow.Timestamp(ts.UnixMilli())
				case arrow.Microsecond:
					tsVal = arrow.Timestamp(ts.UnixMicro())
				case arrow.Nanosecond:
					tsVal = arrow.Timestamp(ts.UnixNano())
				}
				bldr.Append(tsVal)
			}

			input := bldr.NewArray()
			defer input.Release()

			// Execute function
			result, err := compute.CallFunction(ctx, tv.funcName, &tv.opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			resultArr := result.(*compute.ArrayDatum).MakeArray()
			defer resultArr.Release()

			tsArr := resultArr.(*array.Timestamp)

			// Verify results
			require.Equal(t, len(tv.expected), tsArr.Len(), "result length mismatch")

			for i, exp := range tv.expected {
				var expectedVal arrow.Timestamp
				switch tv.inputUnit {
				case arrow.Second:
					expectedVal = arrow.Timestamp(exp.Unix())
				case arrow.Millisecond:
					expectedVal = arrow.Timestamp(exp.UnixMilli())
				case arrow.Microsecond:
					expectedVal = arrow.Timestamp(exp.UnixMicro())
				case arrow.Nanosecond:
					expectedVal = arrow.Timestamp(exp.UnixNano())
				}

				assert.Equal(t, expectedVal, tsArr.Value(i), "mismatch at index %d: input=%v expected=%v got=%v",
					i, standardTemporalInputs[i], exp, tsArr.Value(i).ToTime(tv.inputUnit))
			}
		})
	}
}

func TestTemporalWithNulls(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	testCases := []struct {
		name     string
		funcName string
		opts     compute.RoundTemporalOptions
	}{
		{
			name:     "floor with nulls",
			funcName: "floor_temporal",
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:     "ceil with nulls",
			funcName: "ceil_temporal",
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
		{
			name:     "round with nulls",
			funcName: "round_temporal",
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalHour,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Second})
			defer bldr.Release()

			ts := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)
			bldr.Append(arrow.Timestamp(ts.Unix()))
			bldr.AppendNull()
			bldr.Append(arrow.Timestamp(ts.Unix()))
			bldr.AppendNull()
			bldr.Append(arrow.Timestamp(ts.Unix()))

			input := bldr.NewArray()
			defer input.Release()

			result, err := compute.CallFunction(ctx, tc.funcName, &tc.opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			resultArr := result.(*compute.ArrayDatum).MakeArray()
			defer resultArr.Release()

			tsArr := resultArr.(*array.Timestamp)

			// Verify null pattern is preserved
			assert.True(t, tsArr.IsValid(0))
			assert.False(t, tsArr.IsValid(1))
			assert.True(t, tsArr.IsValid(2))
			assert.False(t, tsArr.IsValid(3))
			assert.True(t, tsArr.IsValid(4))
		})
	}
}

func TestTemporalRoundingErrors(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Second})
	defer bldr.Release()
	bldr.Append(arrow.Timestamp(time.Now().Unix()))
	input := bldr.NewArray()
	defer input.Release()
	datum := compute.NewDatum(input)

	testCases := []struct {
		name     string
		funcName string
		opts     compute.FunctionOptions
		errMsg   string
	}{
		{
			name:     "zero multiple",
			funcName: "floor_temporal",
			opts: &compute.RoundTemporalOptions{
				Multiple: 0,
				Unit:     compute.RoundTemporalHour,
			},
			errMsg: "rounding multiple must be positive",
		},
		{
			name:     "negative multiple",
			funcName: "ceil_temporal",
			opts: &compute.RoundTemporalOptions{
				Multiple: -5,
				Unit:     compute.RoundTemporalDay,
			},
			errMsg: "rounding multiple must be positive",
		},
		{
			name:     "unsupported calendar unit",
			funcName: "round_temporal",
			opts: &compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalQuarter,
			},
			errMsg: "unsupported calendar unit",
		},
		{
			name:     "invalid options type",
			funcName: "floor_temporal",
			opts:     &compute.RoundOptions{NDigits: 2},
			errMsg:   "invalid function options",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compute.CallFunction(ctx, tc.funcName, tc.opts, datum)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
