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
	"github.com/stretchr/testify/require"
)

type temporalRoundingFunc func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)

func requireTemporalRoundingTime(t *testing.T, fn temporalRoundingFunc, input, expected time.Time,
	inputUnit arrow.TimeUnit, timezone string, opts compute.RoundTemporalOptions) {
	t.Helper()

	value, err := arrow.TimestampFromTime(input, inputUnit)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{
		Unit:     inputUnit,
		TimeZone: timezone,
	})
	builder.Append(value)
	inputArray := builder.NewArray()
	builder.Release()
	defer inputArray.Release()

	result, err := fn(context.Background(), opts, compute.NewDatum(inputArray))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()

	expectedValue, err := arrow.TimestampFromTime(expected, inputUnit)
	require.NoError(t, err)
	require.Equal(t, expectedValue, output.Value(0))
}

func TestTemporalRoundingUsesUnixEpochForCalendarMultiples(t *testing.T) {
	input := time.Date(2024, time.January, 15, 10, 37, 0, 0, time.UTC)
	tests := []struct {
		name     string
		fn       temporalRoundingFunc
		unit     compute.RoundTemporalUnit
		multiple int64
		expected time.Time
	}{
		{
			name:     "floor month",
			fn:       compute.FloorTemporal,
			unit:     compute.RoundTemporalMonth,
			multiple: 7,
			expected: time.Date(2023, time.September, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil month",
			fn:       compute.CeilTemporal,
			unit:     compute.RoundTemporalMonth,
			multiple: 7,
			expected: time.Date(2024, time.April, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "floor quarter",
			fn:       compute.FloorTemporal,
			unit:     compute.RoundTemporalQuarter,
			multiple: 3,
			expected: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil quarter",
			fn:       compute.CeilTemporal,
			unit:     compute.RoundTemporalQuarter,
			multiple: 3,
			expected: time.Date(2024, time.October, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireTemporalRoundingTime(t, test.fn, input, test.expected, arrow.Microsecond, "", compute.RoundTemporalOptions{
				Multiple: test.multiple,
				Unit:     test.unit,
			})
		})
	}
}

func TestTemporalRoundingCalendarBasedOrigins(t *testing.T) {
	tests := []struct {
		name     string
		fn       temporalRoundingFunc
		input    time.Time
		unit     compute.RoundTemporalUnit
		multiple int64
		expected time.Time
	}{
		{
			name:     "floor day from month",
			fn:       compute.FloorTemporal,
			input:    time.Date(2024, time.March, 10, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalDay,
			multiple: 2,
			expected: time.Date(2024, time.March, 9, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil day from month",
			fn:       compute.CeilTemporal,
			input:    time.Date(2024, time.March, 10, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalDay,
			multiple: 2,
			expected: time.Date(2024, time.March, 11, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "floor minute from hour",
			fn:       compute.FloorTemporal,
			input:    time.Date(2024, time.January, 15, 10, 37, 30, 123000, time.UTC),
			unit:     compute.RoundTemporalMinute,
			multiple: 7,
			expected: time.Date(2024, time.January, 15, 10, 35, 0, 0, time.UTC),
		},
		{
			name:     "ceil minute from hour",
			fn:       compute.CeilTemporal,
			input:    time.Date(2024, time.January, 15, 10, 37, 30, 123000, time.UTC),
			unit:     compute.RoundTemporalMinute,
			multiple: 7,
			expected: time.Date(2024, time.January, 15, 10, 42, 0, 0, time.UTC),
		},
		{
			name:     "floor month from year",
			fn:       compute.FloorTemporal,
			input:    time.Date(2024, time.May, 15, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalMonth,
			multiple: 5,
			expected: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil month from year",
			fn:       compute.CeilTemporal,
			input:    time.Date(2024, time.May, 15, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalMonth,
			multiple: 5,
			expected: time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "floor quarter from year",
			fn:       compute.FloorTemporal,
			input:    time.Date(2024, time.May, 15, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalQuarter,
			multiple: 2,
			expected: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil quarter from year",
			fn:       compute.CeilTemporal,
			input:    time.Date(2024, time.May, 15, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalQuarter,
			multiple: 2,
			expected: time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "floor week from year",
			fn:       compute.FloorTemporal,
			input:    time.Date(2024, time.January, 8, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalWeek,
			multiple: 5,
			expected: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "ceil week from year",
			fn:       compute.CeilTemporal,
			input:    time.Date(2024, time.January, 8, 12, 0, 0, 0, time.UTC),
			unit:     compute.RoundTemporalWeek,
			multiple: 5,
			expected: time.Date(2024, time.February, 5, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireTemporalRoundingTime(t, test.fn, test.input, test.expected, arrow.Microsecond, "", compute.RoundTemporalOptions{
				Multiple:            test.multiple,
				Unit:                test.unit,
				WeekStartsMonday:    test.unit == compute.RoundTemporalWeek,
				CalendarBasedOrigin: true,
			})
		})
	}
}

func TestTemporalRoundingCalendarOriginUsesWallClockAcrossDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	input := time.Date(2024, time.March, 10, 12, 0, 0, 0, time.UTC)
	opts := compute.RoundTemporalOptions{
		Multiple:            5,
		Unit:                compute.RoundTemporalHour,
		CalendarBasedOrigin: true,
	}

	requireTemporalRoundingTime(t, compute.FloorTemporal, input,
		time.Date(2024, time.March, 10, 5, 0, 0, 0, location), arrow.Microsecond,
		location.String(), opts)
	requireTemporalRoundingTime(t, compute.CeilTemporal, input,
		time.Date(2024, time.March, 10, 10, 0, 0, 0, location), arrow.Microsecond,
		location.String(), opts)
}

func TestTemporalRoundingCalendarOriginRejectsDSTGap(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	input := time.Date(2024, time.March, 10, 1, 30, 0, 0, location)
	value, err := arrow.TimestampFromTime(input, arrow.Microsecond)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{
		Unit:     arrow.Microsecond,
		TimeZone: location.String(),
	})
	builder.Append(value)
	inputArray := builder.NewArray()
	builder.Release()
	defer inputArray.Release()

	_, err = compute.CeilTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple:            1,
		Unit:                compute.RoundTemporalHour,
		CalendarBasedOrigin: true,
	}, compute.NewDatum(inputArray))
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestTemporalRoundingTimezoneDayMultipleUsesEpochOrigin(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	input := time.Date(2024, time.January, 15, 10, 37, 0, 0, time.UTC)
	opts := compute.RoundTemporalOptions{
		Multiple: 2,
		Unit:     compute.RoundTemporalDay,
	}

	requireTemporalRoundingTime(t, compute.FloorTemporal, input,
		time.Date(2024, time.January, 14, 0, 0, 0, 0, location), arrow.Microsecond,
		location.String(), opts)
	requireTemporalRoundingTime(t, compute.CeilTemporal, input,
		time.Date(2024, time.January, 16, 0, 0, 0, 0, location), arrow.Microsecond,
		location.String(), opts)
}
