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
	"strings"
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
	mem := memory.DefaultAllocator

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
			name:      "floor to quarter",
			funcName:  "floor_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023 (March is Q1)
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 1970
				time.Date(1969, 10, 1, 0, 0, 0, 0, time.UTC), // Q4 1969 (Oct-Dec)
				time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), // Q4 2023
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2024 (Feb is Q1)
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // Q1 2023
				time.Date(2050, 10, 1, 0, 0, 0, 0, time.UTC), // Q4 2050
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalQuarter,
			},
		},
		{
			name:      "ceil to quarter",
			funcName:  "ceil_temporal",
			inputUnit: arrow.Second,
			expected: []time.Time{
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023 (not at quarter boundary)
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023 (March 1 is in Q1)
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), // already at Q1 start
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), // -> Q1 1970
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), // -> Q1 2024
				time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2024
				time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), // -> Q2 2023
				time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC), // -> Q1 2051
			},
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalQuarter,
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
	mem := memory.DefaultAllocator

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
	mem := memory.DefaultAllocator

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

func TestTemporalTimezoneAware(t *testing.T) {
	// This test verifies that temporal rounding operates on local time in the specified timezone,
	// matching PyArrow behavior.
	// It tests calendar-based rounding (year, quarter, month, week, day) across multiple timezones.
	ctx := context.Background()
	mem := memory.DefaultAllocator

	// Test timestamp: 2024-07-15 14:30:00 UTC
	// In America/New_York (UTC-4 in July), this is 2024-07-15 10:30:00 local
	// In Asia/Tokyo (UTC+9), this is 2024-07-15 23:30:00 local
	testTime := time.Date(2024, 7, 15, 14, 30, 0, 0, time.UTC)

	testCases := []struct {
		name        string
		tz          string
		unit        compute.RoundTemporalUnit
		expectedUTC time.Time
	}{
		// Day rounding
		{name: "day_utc", tz: "UTC", unit: compute.RoundTemporalDay,
			expectedUTC: time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)},
		{name: "day_ny", tz: "America/New_York", unit: compute.RoundTemporalDay,
			expectedUTC: time.Date(2024, 7, 15, 4, 0, 0, 0, time.UTC)}, // 2024-07-15 00:00 EDT = 04:00 UTC
		{name: "day_tokyo", tz: "Asia/Tokyo", unit: compute.RoundTemporalDay,
			expectedUTC: time.Date(2024, 7, 14, 15, 0, 0, 0, time.UTC)}, // 2024-07-15 00:00 JST = Jul 14 15:00 UTC

		// Week rounding (Monday start) - input is 2024-07-15 14:30 UTC = Mon 10:30 EDT, Mon 23:30 JST
		{name: "week_utc", tz: "UTC", unit: compute.RoundTemporalWeek,
			expectedUTC: time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)}, // Floor to Monday 00:00 UTC
		{name: "week_ny", tz: "America/New_York", unit: compute.RoundTemporalWeek,
			expectedUTC: time.Date(2024, 7, 8, 4, 0, 0, 0, time.UTC)}, // Floor to Monday 00:00 EDT (previous Monday in NY time)
		{name: "week_tokyo", tz: "Asia/Tokyo", unit: compute.RoundTemporalWeek,
			expectedUTC: time.Date(2024, 7, 14, 15, 0, 0, 0, time.UTC)}, // Floor to Monday 00:00 JST

		// Month rounding
		{name: "month_utc", tz: "UTC", unit: compute.RoundTemporalMonth,
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "month_ny", tz: "America/New_York", unit: compute.RoundTemporalMonth,
			expectedUTC: time.Date(2024, 7, 1, 4, 0, 0, 0, time.UTC)}, // 2024-07-01 00:00 EDT
		{name: "month_tokyo", tz: "Asia/Tokyo", unit: compute.RoundTemporalMonth,
			expectedUTC: time.Date(2024, 6, 30, 15, 0, 0, 0, time.UTC)}, // 2024-07-01 00:00 JST

		// Quarter rounding (Q3 starts July 1)
		{name: "quarter_utc", tz: "UTC", unit: compute.RoundTemporalQuarter,
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "quarter_ny", tz: "America/New_York", unit: compute.RoundTemporalQuarter,
			expectedUTC: time.Date(2024, 7, 1, 4, 0, 0, 0, time.UTC)}, // 2024-07-01 00:00 EDT
		{name: "quarter_tokyo", tz: "Asia/Tokyo", unit: compute.RoundTemporalQuarter,
			expectedUTC: time.Date(2024, 6, 30, 15, 0, 0, 0, time.UTC)}, // 2024-07-01 00:00 JST

		// Year rounding
		{name: "year_utc", tz: "UTC", unit: compute.RoundTemporalYear,
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{name: "year_ny", tz: "America/New_York", unit: compute.RoundTemporalYear,
			expectedUTC: time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC)}, // 2024-01-01 00:00 EST = 05:00 UTC
		{name: "year_tokyo", tz: "Asia/Tokyo", unit: compute.RoundTemporalYear,
			expectedUTC: time.Date(2023, 12, 31, 15, 0, 0, 0, time.UTC)}, // 2024-01-01 00:00 JST
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tc.tz})
			defer bldr.Release()
			bldr.Append(arrow.Timestamp(testTime.UnixMicro()))
			input := bldr.NewArray()
			defer input.Release()

			result, err := compute.FloorTemporal(ctx, compute.RoundTemporalOptions{
				Multiple:         1,
				Unit:             tc.unit,
				WeekStartsMonday: true,
			}, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			tsArr := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer tsArr.Release()

			// Verify the result matches the expected UTC time
			expected := arrow.Timestamp(tc.expectedUTC.UnixMicro())
			actual := tsArr.Value(0)
			if expected != actual {
				t.Errorf("Expected %v (%s), got %v (%s)",
					expected, time.UnixMicro(int64(expected)).UTC(),
					actual, time.UnixMicro(int64(actual)).UTC())
			}

			// Timezone metadata should be preserved
			assert.Equal(t, tc.tz, tsArr.DataType().(*arrow.TimestampType).TimeZone)
		})
	}
}

func TestTemporalTimezoneNaiveCalendarRounding(t *testing.T) {
	// This test verifies that timezone-naive timestamps can still be rounded with calendar units.
	// They should be treated as if they are in UTC.
	ctx := context.Background()
	mem := memory.DefaultAllocator

	testTime := time.Date(2024, 7, 15, 14, 30, 0, 0, time.UTC)

	testCases := []struct {
		name        string
		unit        compute.RoundTemporalUnit
		expectedUTC time.Time
	}{
		{name: "day", unit: compute.RoundTemporalDay,
			expectedUTC: time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)},
		{name: "week", unit: compute.RoundTemporalWeek,
			expectedUTC: time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)}, // Monday
		{name: "month", unit: compute.RoundTemporalMonth,
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "quarter", unit: compute.RoundTemporalQuarter,
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}, // Q3
		{name: "year", unit: compute.RoundTemporalYear,
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create timezone-naive timestamp (empty TimeZone string)
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""})
			defer bldr.Release()
			bldr.Append(arrow.Timestamp(testTime.UnixMicro()))
			input := bldr.NewArray()
			defer input.Release()

			result, err := compute.FloorTemporal(ctx, compute.RoundTemporalOptions{
				Multiple:         1,
				Unit:             tc.unit,
				WeekStartsMonday: true,
			}, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			tsArr := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer tsArr.Release()

			// Verify the result (should be treated as UTC)
			expected := arrow.Timestamp(tc.expectedUTC.UnixMicro())
			actual := tsArr.Value(0)
			if expected != actual {
				t.Errorf("Expected %v (%s), got %v (%s)",
					expected, time.UnixMicro(int64(expected)).UTC(),
					actual, time.UnixMicro(int64(actual)).UTC())
			}

			// Timezone should remain empty
			assert.Equal(t, "", tsArr.DataType().(*arrow.TimestampType).TimeZone)
		})
	}
}

func TestTemporalMultiPeriodRounding(t *testing.T) {
	// Tests for rounding with Multiple > 1 (N-period rounding)
	ctx := context.Background()
	mem := memory.DefaultAllocator

	testCases := []struct {
		name        string
		input       time.Time
		unit        compute.RoundTemporalUnit
		multiple    int64
		funcName    string
		expectedUTC time.Time
	}{
		// 2-week periods from epoch (epoch Monday = Dec 29, 1969)
		{name: "floor_2weeks", input: time.Date(2024, 1, 17, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 2, funcName: "floor_temporal",
			expectedUTC: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}, // Start of 2-week period (Monday Jan 15)
		{name: "ceil_2weeks", input: time.Date(2024, 1, 17, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 2, funcName: "ceil_temporal",
			expectedUTC: time.Date(2024, 1, 29, 0, 0, 0, 0, time.UTC)}, // Next 2-week period
		{name: "round_2weeks_before_mid", input: time.Date(2024, 1, 19, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}, // Before midpoint (Jan 22)
		{name: "round_2weeks_after_mid", input: time.Date(2024, 1, 25, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 1, 29, 0, 0, 0, 0, time.UTC)}, // After midpoint

		// 3-month periods
		{name: "floor_3months", input: time.Date(2024, 5, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 3, funcName: "floor_temporal",
			expectedUTC: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)}, // Apr-Jun period
		{name: "ceil_3months", input: time.Date(2024, 5, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 3, funcName: "ceil_temporal",
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}, // Next period
		{name: "round_3months_before_mid", input: time.Date(2024, 4, 20, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 3, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)}, // Before midpoint
		{name: "round_3months_after_mid", input: time.Date(2024, 5, 20, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 3, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint

		// 2-quarter periods (6 months)
		{name: "floor_2quarters", input: time.Date(2024, 8, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 2, funcName: "floor_temporal",
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}, // Q3-Q4 period starts July 1
		{name: "ceil_2quarters", input: time.Date(2024, 8, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 2, funcName: "ceil_temporal",
			expectedUTC: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}, // Next period
		{name: "round_2quarters_before_mid", input: time.Date(2024, 9, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}, // Before midpoint (Oct 1)
		{name: "round_2quarters_after_mid", input: time.Date(2024, 11, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint

		// 2-year periods
		{name: "floor_2years", input: time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 2, funcName: "floor_temporal",
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, // 2024-2025 period
		{name: "ceil_2years", input: time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 2, funcName: "ceil_temporal",
			expectedUTC: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, // Next period
		{name: "round_2years_before_mid", input: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, // Before midpoint (July 1 2025)
		{name: "round_2years_after_mid", input: time.Date(2025, 8, 15, 12, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 2, funcName: "round_temporal",
			expectedUTC: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""})
			defer bldr.Release()
			bldr.Append(arrow.Timestamp(tc.input.UnixMicro()))
			input := bldr.NewArray()
			defer input.Release()

			opts := compute.RoundTemporalOptions{
				Multiple:         tc.multiple,
				Unit:             tc.unit,
				WeekStartsMonday: true,
			}

			result, err := compute.CallFunction(ctx, tc.funcName, &opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			tsArr := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer tsArr.Release()

			expected := arrow.Timestamp(tc.expectedUTC.UnixMicro())
			actual := tsArr.Value(0)
			if expected != actual {
				t.Errorf("Expected %v (%s), got %v (%s)",
					expected, time.UnixMicro(int64(expected)).UTC(),
					actual, time.UnixMicro(int64(actual)).UTC())
			}
		})
	}
}

func TestTemporalHalfRoundingModes(t *testing.T) {
	// Tests for half-rounding modes (HalfUp, HalfDown, HalfToEven) with calendar units
	ctx := context.Background()
	mem := memory.DefaultAllocator

	testCases := []struct {
		name        string
		input       time.Time
		unit        compute.RoundTemporalUnit
		multiple    int64
		expectedUTC time.Time
	}{
		// Week half-rounding
		{name: "week_before_mid", input: time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 1,
			expectedUTC: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)}, // Before Thursday midpoint
		{name: "week_after_mid", input: time.Date(2024, 1, 13, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalWeek, multiple: 1,
			expectedUTC: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}, // After Thursday midpoint

		// Month half-rounding
		{name: "month_before_mid", input: time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 1,
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, // Before Jan 16th midpoint
		{name: "month_after_mid", input: time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalMonth, multiple: 1,
			expectedUTC: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint

		// Quarter half-rounding
		{name: "quarter_before_mid", input: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 1,
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, // Before Feb 15th midpoint
		{name: "quarter_after_mid", input: time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalQuarter, multiple: 1,
			expectedUTC: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint

		// Year half-rounding
		{name: "year_before_mid", input: time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 1,
			expectedUTC: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, // Before July 2nd midpoint
		{name: "year_after_mid", input: time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
			unit: compute.RoundTemporalYear, multiple: 1,
			expectedUTC: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}, // After midpoint
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""})
			defer bldr.Release()
			bldr.Append(arrow.Timestamp(tc.input.UnixMicro()))
			input := bldr.NewArray()
			defer input.Release()

			opts := compute.RoundTemporalOptions{
				Multiple:         tc.multiple,
				Unit:             tc.unit,
				WeekStartsMonday: true,
			}

			result, err := compute.RoundTemporal(ctx, opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			tsArr := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer tsArr.Release()

			expected := arrow.Timestamp(tc.expectedUTC.UnixMicro())
			actual := tsArr.Value(0)
			if expected != actual {
				t.Errorf("Expected %v (%s), got %v (%s)",
					expected, time.UnixMicro(int64(expected)).UTC(),
					actual, time.UnixMicro(int64(actual)).UTC())
			}
		})
	}
}

func TestTemporalRoundingDateSupport(t *testing.T) {
	// Test that date types are supported (PyArrow compatibility)
	ctx := context.Background()
	mem := memory.DefaultAllocator

	// Common date32 inputs for vectorized testing
	date32Input := []arrow.Date32{1, 4, 7, 11} // 1970-01-02 (Fri), 1970-01-05 (Mon), 1970-01-08 (Thu), 1970-01-12 (Mon)

	// Common date64 inputs (with sub-day precision)
	date64Input := []arrow.Date64{
		86400000,             // Exactly 1 day (midnight)
		86400000 + 43200000,  // 1.5 days
		172800000 + 1,        // 2 days + 1ms
		259200000 + 64800000, // 3.75 days
	}

	testVectors := []struct {
		name     string
		dateType arrow.DataType
		input    interface{} // []arrow.Date32 or []arrow.Date64
		opts     compute.RoundTemporalOptions
		fn       func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		expected interface{} // []arrow.Date32 or []arrow.Date64
	}{
		{
			name:     "date32_floor_to_day",
			dateType: arrow.FixedWidthTypes.Date32,
			input:    []arrow.Date32{1, 2, 3, 4, 5},
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay},
			fn:       compute.FloorTemporal,
			expected: []arrow.Date32{1, 2, 3, 4, 5}, // Identity operation
		},
		{
			name:     "date32_floor_to_week",
			dateType: arrow.FixedWidthTypes.Date32,
			input:    date32Input,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalWeek, WeekStartsMonday: true},
			fn:       compute.FloorTemporal,
			expected: []arrow.Date32{-3, 4, 4, 11}, // Floor to Monday
		},
		{
			name:     "date32_ceil_to_week",
			dateType: arrow.FixedWidthTypes.Date32,
			input:    date32Input,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalWeek, WeekStartsMonday: true},
			fn:       compute.CeilTemporal,
			expected: []arrow.Date32{4, 4, 11, 11}, // Ceil to Monday
		},
		{
			name:     "date64_floor_to_day",
			dateType: arrow.FixedWidthTypes.Date64,
			input:    date64Input,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay},
			fn:       compute.FloorTemporal,
			expected: []arrow.Date64{86400000, 86400000, 172800000, 259200000}, // Floor to midnight
		},
		{
			name:     "date64_ceil_to_day",
			dateType: arrow.FixedWidthTypes.Date64,
			input:    date64Input,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay},
			fn:       compute.CeilTemporal,
			expected: []arrow.Date64{86400000, 172800000, 259200000, 345600000}, // Ceil to midnight
		},
		{
			name:     "date64_round_to_day",
			dateType: arrow.FixedWidthTypes.Date64,
			input:    date64Input,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay},
			fn:       compute.RoundTemporal,
			expected: []arrow.Date64{86400000, 172800000, 172800000, 345600000}, // Round to nearest midnight
		},
		{
			name:     "date64_floor_to_month",
			dateType: arrow.FixedWidthTypes.Date64,
			input:    []arrow.Date64{2678400000, 5097600000}, // 1970-02-01, 1970-03-01
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalMonth},
			fn:       compute.FloorTemporal,
			expected: []arrow.Date64{2678400000, 5097600000}, // Already at month boundaries
		},
	}

	for _, tv := range testVectors {
		t.Run(tv.name, func(t *testing.T) {
			var arr arrow.Array
			switch tv.dateType.ID() {
			case arrow.DATE32:
				bldr := array.NewDate32Builder(mem)
				defer bldr.Release()
				bldr.AppendValues(tv.input.([]arrow.Date32), nil)
				arr = bldr.NewArray()
			case arrow.DATE64:
				bldr := array.NewDate64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues(tv.input.([]arrow.Date64), nil)
				arr = bldr.NewArray()
			}
			defer arr.Release()

			result, err := tv.fn(ctx, tv.opts, compute.NewDatum(arr))
			require.NoError(t, err)
			defer result.Release()

			resultArr := result.(*compute.ArrayDatum).MakeArray()
			defer resultArr.Release()

			switch tv.dateType.ID() {
			case arrow.DATE32:
				date32Result := resultArr.(*array.Date32)
				expected := tv.expected.([]arrow.Date32)
				require.Equal(t, len(expected), date32Result.Len())
				for i := 0; i < date32Result.Len(); i++ {
					assert.Equal(t, expected[i], date32Result.Value(i), "index %d", i)
				}
			case arrow.DATE64:
				date64Result := resultArr.(*array.Date64)
				expected := tv.expected.([]arrow.Date64)
				require.Equal(t, len(expected), date64Result.Len())
				for i := 0; i < date64Result.Len(); i++ {
					assert.Equal(t, expected[i], date64Result.Value(i), "index %d", i)
				}
			}
		})
	}

	t.Run("date32_with_nulls", func(t *testing.T) {
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()
		bldr.AppendValues([]arrow.Date32{1, 0, 3}, []bool{true, false, true})
		arr := bldr.NewArray()
		defer arr.Release()

		opts := compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay}
		result, err := compute.FloorTemporal(ctx, opts, compute.NewDatum(arr))
		require.NoError(t, err)
		defer result.Release()

		resultArr := result.(*compute.ArrayDatum).MakeArray()
		defer resultArr.Release()
		date32Result := resultArr.(*array.Date32)

		assert.Equal(t, 3, date32Result.Len())
		assert.True(t, date32Result.IsValid(0))
		assert.False(t, date32Result.IsValid(1))
		assert.True(t, date32Result.IsValid(2))
		assert.Equal(t, arrow.Date32(1), date32Result.Value(0))
		assert.Equal(t, arrow.Date32(3), date32Result.Value(2))
	})
}

func TestTemporalRoundingTimeSupport(t *testing.T) {
	// Test that time types are supported (PyArrow compatibility)
	ctx := context.Background()
	mem := memory.DefaultAllocator

	testVectors := []struct {
		name     string
		timeType arrow.DataType
		input    interface{} // []arrow.Time32 or []arrow.Time64
		opts     compute.RoundTemporalOptions
		fn       func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		expected interface{} // []arrow.Time32 or []arrow.Time64
	}{
		{
			name:     "time32_s_floor_to_minute",
			timeType: &arrow.Time32Type{Unit: arrow.Second},
			input:    []arrow.Time32{3661, 7322, 10983}, // 1:01:01, 2:02:02, 3:03:03
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalMinute},
			fn:       compute.FloorTemporal,
			expected: []arrow.Time32{3660, 7320, 10980}, // 1:01:00, 2:02:00, 3:03:00
		},
		{
			name:     "time32_ms_floor_to_second",
			timeType: &arrow.Time32Type{Unit: arrow.Millisecond},
			input:    []arrow.Time32{3661500, 7322750}, // 1:01:01.500, 2:02:02.750
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalSecond},
			fn:       compute.FloorTemporal,
			expected: []arrow.Time32{3661000, 7322000}, // 1:01:01.000, 2:02:02.000
		},
		{
			name:     "time64_us_ceil_to_second",
			timeType: &arrow.Time64Type{Unit: arrow.Microsecond},
			input:    []arrow.Time64{3661000001, 7322000002}, // 1:01:01.000001, 2:02:02.000002
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalSecond},
			fn:       compute.CeilTemporal,
			expected: []arrow.Time64{3662000000, 7323000000}, // 1:01:02.000000, 2:02:03.000000
		},
		{
			name:     "time64_ns_round_to_millisecond",
			timeType: &arrow.Time64Type{Unit: arrow.Nanosecond},
			input:    []arrow.Time64{3661000400000, 3661000600000}, // 1:01:01.000400000, 1:01:01.000600000
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalMillisecond},
			fn:       compute.RoundTemporal,
			expected: []arrow.Time64{3661000000000, 3661001000000}, // 1:01:01.000000000, 1:01:01.001000000
		},
		{
			name:     "time32_wrap_at_midnight_ceil",
			timeType: &arrow.Time32Type{Unit: arrow.Second},
			input:    []arrow.Time32{86399}, // 23:59:59
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalMinute},
			fn:       compute.CeilTemporal,
			expected: []arrow.Time32{0}, // Wraps to 00:00:00
		},
		{
			name:     "time64_wrap_at_midnight_ceil",
			timeType: &arrow.Time64Type{Unit: arrow.Microsecond},
			input:    []arrow.Time64{86399999999}, // 23:59:59.999999
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalSecond},
			fn:       compute.CeilTemporal,
			expected: []arrow.Time64{0}, // Wraps to 00:00:00.000000
		},
	}

	for _, tv := range testVectors {
		t.Run(tv.name, func(t *testing.T) {
			var arr arrow.Array
			switch tt := tv.timeType.(type) {
			case *arrow.Time32Type:
				bldr := array.NewTime32Builder(mem, tt)
				defer bldr.Release()
				bldr.AppendValues(tv.input.([]arrow.Time32), nil)
				arr = bldr.NewArray()
			case *arrow.Time64Type:
				bldr := array.NewTime64Builder(mem, tt)
				defer bldr.Release()
				bldr.AppendValues(tv.input.([]arrow.Time64), nil)
				arr = bldr.NewArray()
			}
			defer arr.Release()

			result, err := tv.fn(ctx, tv.opts, compute.NewDatum(arr))
			require.NoError(t, err)
			defer result.Release()

			resultArr := result.(*compute.ArrayDatum).MakeArray()
			defer resultArr.Release()

			switch tv.timeType.ID() {
			case arrow.TIME32:
				time32Result := resultArr.(*array.Time32)
				expected := tv.expected.([]arrow.Time32)
				require.Equal(t, len(expected), time32Result.Len())
				for i := 0; i < time32Result.Len(); i++ {
					assert.Equal(t, expected[i], time32Result.Value(i), "index %d", i)
				}
			case arrow.TIME64:
				time64Result := resultArr.(*array.Time64)
				expected := tv.expected.([]arrow.Time64)
				require.Equal(t, len(expected), time64Result.Len())
				for i := 0; i < time64Result.Len(); i++ {
					assert.Equal(t, expected[i], time64Result.Value(i), "index %d", i)
				}
			}
		})
	}
}

func TestTemporalRoundingInvalidType(t *testing.T) {
	// Test that non-temporal types return appropriate errors
	ctx := context.Background()
	mem := memory.DefaultAllocator

	testCases := []struct {
		name     string
		makeArr  func() arrow.Array
		typeName string
	}{
		{
			name: "int64",
			makeArr: func() arrow.Array {
				bldr := array.NewInt64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
				return bldr.NewArray()
			},
			typeName: "int64",
		},
		{
			name: "float64",
			makeArr: func() arrow.Array {
				bldr := array.NewFloat64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float64{1.0, 2.0, 3.0}, nil)
				return bldr.NewArray()
			},
			typeName: "float64",
		},
		{
			name: "string",
			makeArr: func() arrow.Array {
				bldr := array.NewStringBuilder(mem)
				defer bldr.Release()
				bldr.AppendValues([]string{"a", "b", "c"}, nil)
				return bldr.NewArray()
			},
			typeName: "utf8",
		},
	}

	opts := compute.RoundTemporalOptions{
		Multiple: 1,
		Unit:     compute.RoundTemporalHour,
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.makeArr()
			defer arr.Release()

			_, err := compute.FloorTemporal(ctx, opts, compute.NewDatum(arr))
			if err == nil {
				t.Fatalf("expected error for %s type, got nil", tc.typeName)
			}

			// Should mention the type in the error message
			errMsg := err.Error()
			if !strings.Contains(errMsg, "timestamp") && !strings.Contains(errMsg, "temporal") {
				t.Errorf("error message: %s", errMsg)
			}
		})
	}
}

// Benchmarks for temporal rounding functions

// Helper function to create timestamp arrays for benchmarks
func makeTimestampArray(count int, unit arrow.TimeUnit, interval time.Duration) arrow.Array {
	mem := memory.DefaultAllocator
	bldr := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: unit, TimeZone: "UTC"})
	defer bldr.Release()
	bldr.Reserve(count)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		ts := baseTime.Add(time.Duration(i) * interval)
		switch unit {
		case arrow.Second:
			bldr.Append(arrow.Timestamp(ts.Unix()))
		case arrow.Millisecond:
			bldr.Append(arrow.Timestamp(ts.UnixMilli()))
		case arrow.Microsecond:
			bldr.Append(arrow.Timestamp(ts.UnixMicro()))
		case arrow.Nanosecond:
			bldr.Append(arrow.Timestamp(ts.UnixNano()))
		}
	}
	return bldr.NewArray()
}

func BenchmarkTemporalRounding(b *testing.B) {
	benchmarks := []struct {
		name     string
		fn       func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		unit     arrow.TimeUnit
		interval time.Duration
		opts     compute.RoundTemporalOptions
	}{
		{
			name:     "FloorToHour_Microsecond",
			fn:       compute.FloorTemporal,
			unit:     arrow.Microsecond,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalHour},
		},
		{
			name:     "FloorToDay_Millisecond",
			fn:       compute.FloorTemporal,
			unit:     arrow.Millisecond,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalDay},
		},
		{
			name:     "CeilToHour_Nanosecond",
			fn:       compute.CeilTemporal,
			unit:     arrow.Nanosecond,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalHour},
		},
		{
			name:     "RoundToHour_Microsecond",
			fn:       compute.RoundTemporal,
			unit:     arrow.Microsecond,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalHour},
		},
		{
			name:     "FloorWithCalendarOrigin",
			fn:       compute.FloorTemporal,
			unit:     arrow.Microsecond,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 2, Unit: compute.RoundTemporalHour, CalendarBasedOrigin: true},
		},
		{
			name:     "FloorSeconds",
			fn:       compute.FloorTemporal,
			unit:     arrow.Second,
			interval: time.Minute,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalHour},
		},
		{
			name:     "FloorToMonth_CalendarBased",
			fn:       compute.FloorTemporal,
			unit:     arrow.Microsecond,
			interval: 24 * time.Hour,
			opts:     compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalMonth},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			arr := makeTimestampArray(100000, bm.unit, bm.interval)
			defer arr.Release()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				datum, err := bm.fn(context.Background(), bm.opts, compute.NewDatum(arr))
				if err != nil {
					b.Fatal(err)
				}
				datum.Release()
			}
		})
	}
}
