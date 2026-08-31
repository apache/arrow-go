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

	inputDatum := compute.NewDatum(inputArray)
	defer inputDatum.Release()
	result, err := fn(context.Background(), opts, inputDatum)
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

func TestTemporalRoundingCalendarOriginMultiWeek(t *testing.T) {
	input := time.Date(1078, time.June, 14, 4, 11, 8, 0, time.UTC)
	tests := []struct {
		name             string
		multiple         int64
		weekStartsMonday bool
		floor            time.Time
		ceil             time.Time
	}{
		{
			name:             "monday start, two weeks",
			multiple:         2,
			weekStartsMonday: true,
			floor:            time.Date(1078, time.June, 3, 0, 0, 0, 0, time.UTC),
			ceil:             time.Date(1078, time.June, 17, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "sunday start, two weeks",
			multiple: 2,
			floor:    time.Date(1078, time.June, 2, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(1078, time.June, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name:             "monday start, three weeks",
			multiple:         3,
			weekStartsMonday: true,
			floor:            time.Date(1078, time.May, 27, 0, 0, 0, 0, time.UTC),
			ceil:             time.Date(1078, time.June, 17, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := compute.RoundTemporalOptions{
				Multiple:            test.multiple,
				Unit:                compute.RoundTemporalWeek,
				WeekStartsMonday:    test.weekStartsMonday,
				CalendarBasedOrigin: true,
			}
			requireTemporalRoundingTime(t, compute.FloorTemporal, input, test.floor, arrow.Second, "", opts)
			requireTemporalRoundingTime(t, compute.CeilTemporal, input, test.ceil, arrow.Second, "", opts)
		})
	}
}

func TestTemporalRoundingCalendarOriginMultiWeekYearBoundaries(t *testing.T) {
	tests := []struct {
		name             string
		input            time.Time
		multiple         int64
		weekStartsMonday bool
		floor            time.Time
		ceil             time.Time
	}{
		{
			name:     "sunday start at end of year",
			input:    time.Date(2025, time.December, 31, 4, 58, 46, 0, time.UTC),
			multiple: 2,
			floor:    time.Date(2025, time.December, 28, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(2026, time.January, 11, 0, 0, 0, 0, time.UTC),
		},
		{
			name:             "monday start at end of year",
			input:            time.Date(2025, time.December, 31, 4, 58, 46, 0, time.UTC),
			multiple:         2,
			weekStartsMonday: true,
			floor:            time.Date(2025, time.December, 29, 0, 0, 0, 0, time.UTC),
			ceil:             time.Date(2026, time.January, 12, 0, 0, 0, 0, time.UTC),
		},
		{
			name:             "monday start before first week",
			input:            time.Date(2023, time.January, 1, 4, 58, 46, 0, time.UTC),
			multiple:         2,
			weekStartsMonday: true,
			floor:            time.Date(2022, time.December, 19, 0, 0, 0, 0, time.UTC),
			ceil:             time.Date(2023, time.January, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := compute.RoundTemporalOptions{
				Multiple:            test.multiple,
				Unit:                compute.RoundTemporalWeek,
				WeekStartsMonday:    test.weekStartsMonday,
				CalendarBasedOrigin: true,
			}
			requireTemporalRoundingTime(t, compute.FloorTemporal, test.input, test.floor, arrow.Second, "", opts)
			requireTemporalRoundingTime(t, compute.FloorTemporal, test.floor, test.floor, arrow.Second, "", opts)
			requireTemporalRoundingTime(t, compute.CeilTemporal, test.input, test.ceil, arrow.Second, "", opts)
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

func TestTemporalRoundingCalendarOriginAfterMidnightGap(t *testing.T) {
	// Sao Paulo skipped midnight to 01:00. Monrovia skipped midnight to
	// 00:44:30, so even an hour or minute boundary may not exist locally.
	for _, tc := range []struct {
		zone, input  string
		unit         compute.RoundTemporalUnit
		multiple     int64
		interval     time.Duration
		minInputUnit arrow.TimeUnit
	}{
		{"America/Sao_Paulo", "2018-11-04T02:00:00-02:00", compute.RoundTemporalHour, 2, 2 * time.Hour, arrow.Second},
		{"America/Sao_Paulo", "2018-11-04T01:30:00-02:00", compute.RoundTemporalMinute, 30, 30 * time.Minute, arrow.Second},
		{"Africa/Monrovia", "1972-01-07T01:00:00Z", compute.RoundTemporalHour, 1, time.Hour, arrow.Second},
		{"Africa/Monrovia", "1972-01-07T00:45:00Z", compute.RoundTemporalMinute, 15, 15 * time.Minute, arrow.Second},
		{"Africa/Monrovia", "1972-01-07T00:44:30Z", compute.RoundTemporalSecond, 10, 10 * time.Second, arrow.Second},
		{"Africa/Monrovia", "1972-01-07T00:44:30.125Z", compute.RoundTemporalMillisecond, 5, 5 * time.Millisecond, arrow.Millisecond},
		{"Africa/Monrovia", "1972-01-07T00:44:30.123455Z", compute.RoundTemporalMicrosecond, 5, 5 * time.Microsecond, arrow.Microsecond},
		{"Africa/Monrovia", "1972-01-07T00:44:30.123456785Z", compute.RoundTemporalNanosecond, 5, 5 * time.Nanosecond, arrow.Nanosecond},
	} {
		t.Run(tc.zone+"/"+tc.input, func(t *testing.T) {
			input, err := time.Parse(time.RFC3339Nano, tc.input)
			require.NoError(t, err)
			for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
				if unit < tc.minInputUnit {
					continue
				}
				t.Run(unit.String(), func(t *testing.T) {
					opts := compute.RoundTemporalOptions{
						Multiple:            tc.multiple,
						Unit:                tc.unit,
						CalendarBasedOrigin: true,
					}
					for _, fn := range []temporalRoundingFunc{compute.FloorTemporal, compute.CeilTemporal, compute.RoundTemporal} {
						requireTemporalRoundingTime(t, fn, input, input, unit, tc.zone, opts)
					}

					unaligned := input.Add(tc.interval * 4 / 5)
					requireTemporalRoundingTime(t, compute.FloorTemporal, unaligned, input, unit, tc.zone, opts)
					for _, fn := range []temporalRoundingFunc{compute.CeilTemporal, compute.RoundTemporal} {
						requireTemporalRoundingTime(t, fn, unaligned, input.Add(tc.interval), unit, tc.zone, opts)
					}
					opts.CeilIsStrictlyGreater = true
					requireTemporalRoundingTime(t, compute.CeilTemporal, input, input.Add(tc.interval), unit, tc.zone, opts)
				})
			}
		})
	}
}

func TestTemporalRoundingCalendarOriginCrossesMidnightGap(t *testing.T) {
	input, err := time.Parse(time.RFC3339, "2018-11-03T23:30:00-03:00")
	require.NoError(t, err)
	floor, err := time.Parse(time.RFC3339, "2018-11-03T20:00:00-03:00")
	require.NoError(t, err)
	ceil, err := time.Parse(time.RFC3339, "2018-11-04T01:00:00-02:00")
	require.NoError(t, err)
	opts := compute.RoundTemporalOptions{
		Multiple:            5,
		Unit:                compute.RoundTemporalHour,
		CalendarBasedOrigin: true,
	}
	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		t.Run(unit.String(), func(t *testing.T) {
			requireTemporalRoundingTime(t, compute.FloorTemporal, input, floor, unit, "America/Sao_Paulo", opts)
			for _, fn := range []temporalRoundingFunc{compute.CeilTemporal, compute.RoundTemporal} {
				requireTemporalRoundingTime(t, fn, input, ceil, unit, "America/Sao_Paulo", opts)
			}
		})
	}
}

func TestTemporalRoundingCalendarOriginRejectsDSTGap(t *testing.T) {
	for _, tc := range []struct {
		zone, input string
		fn          temporalRoundingFunc
		unit        compute.RoundTemporalUnit
		multiple    int64
	}{
		{"America/New_York", "2024-03-10T01:30:00-05:00", compute.CeilTemporal, compute.RoundTemporalHour, 1},
		{"America/Sao_Paulo", "2018-11-04T01:30:00-02:00", compute.FloorTemporal, compute.RoundTemporalHour, 2},
		{"America/Sao_Paulo", "2018-11-03T23:30:00-03:00", compute.CeilTemporal, compute.RoundTemporalHour, 1},
		{"Africa/Monrovia", "1972-01-07T00:45:00Z", compute.FloorTemporal, compute.RoundTemporalMinute, 30},
		{"Africa/Monrovia", "1972-01-07T00:44:35Z", compute.FloorTemporal, compute.RoundTemporalSecond, 60},
		{"Pacific/Apia", "2011-12-29T23:30:00-10:00", compute.CeilTemporal, compute.RoundTemporalHour, 5},
	} {
		t.Run(tc.zone+"/"+tc.input, func(t *testing.T) {
			input, err := time.Parse(time.RFC3339, tc.input)
			require.NoError(t, err)
			for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
				t.Run(unit.String(), func(t *testing.T) {
					value, err := arrow.TimestampFromTime(input, unit)
					require.NoError(t, err)

					builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{
						Unit:     unit,
						TimeZone: tc.zone,
					})
					builder.Append(value)
					inputArray := builder.NewArray()
					builder.Release()
					defer inputArray.Release()
					inputDatum := compute.NewDatum(inputArray)
					defer inputDatum.Release()

					result, err := tc.fn(context.Background(), compute.RoundTemporalOptions{
						Multiple:            tc.multiple,
						Unit:                tc.unit,
						CalendarBasedOrigin: true,
					}, inputDatum)
					if err == nil && result != nil {
						defer result.Release()
					}
					require.ErrorIs(t, err, arrow.ErrInvalid)
				})
			}
		})
	}
}

func TestTemporalRoundingRejectsMissingCalendarBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, zone, input string
		unit              compute.RoundTemporalUnit
		calendarOrigin    bool
	}{
		{
			name:           "month start",
			zone:           "America/Argentina/Buenos_Aires",
			input:          "1930-12-02T12:00:00-03:00",
			unit:           compute.RoundTemporalMonth,
			calendarOrigin: true,
		},
		{
			name:  "day start",
			zone:  "America/Sao_Paulo",
			input: "2018-11-04T01:30:00-02:00",
			unit:  compute.RoundTemporalDay,
		},
		{
			name:  "week start",
			zone:  "America/Sao_Paulo",
			input: "2018-11-04T01:30:00-02:00",
			unit:  compute.RoundTemporalWeek,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := time.Parse(time.RFC3339, tc.input)
			require.NoError(t, err)
			value, err := arrow.TimestampFromTime(input, arrow.Second)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{
				Unit:     arrow.Second,
				TimeZone: tc.zone,
			})
			builder.Append(value)
			inputArray := builder.NewArray()
			builder.Release()
			defer inputArray.Release()

			inputDatum := compute.NewDatum(inputArray)
			defer inputDatum.Release()
			result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
				Multiple:            1,
				Unit:                tc.unit,
				CalendarBasedOrigin: tc.calendarOrigin,
			}, inputDatum)
			if result != nil {
				defer result.Release()
			}
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestTemporalRoundingCalendarOriginPreservesRepeatedHour(t *testing.T) {
	for _, tc := range []struct {
		zone, input string
	}{
		{"America/New_York", "2024-11-03T01:30:00-04:00"},
		{"America/New_York", "2024-11-03T01:30:00-05:00"},
		{"Europe/Berlin", "2024-10-27T02:30:00+02:00"},
		{"Europe/Berlin", "2024-10-27T02:30:00+01:00"},
		{"Australia/Lord_Howe", "2024-04-07T01:45:00+11:00"},
		{"Australia/Lord_Howe", "2024-04-07T01:45:00+10:30"},
	} {
		t.Run(tc.zone+"/"+tc.input, func(t *testing.T) {
			input, err := time.Parse(time.RFC3339, tc.input)
			require.NoError(t, err)
			for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
				t.Run(unit.String(), func(t *testing.T) {
					opts := compute.RoundTemporalOptions{
						Multiple:            1,
						Unit:                compute.RoundTemporalMinute,
						CalendarBasedOrigin: true,
					}
					for _, fn := range []temporalRoundingFunc{compute.FloorTemporal, compute.CeilTemporal, compute.RoundTemporal} {
						requireTemporalRoundingTime(t, fn, input, input, unit, tc.zone, opts)
					}

					unaligned := input.Add(40 * time.Second)
					requireTemporalRoundingTime(t, compute.FloorTemporal, unaligned, input, unit, tc.zone, opts)
					for _, fn := range []temporalRoundingFunc{compute.CeilTemporal, compute.RoundTemporal} {
						requireTemporalRoundingTime(t, fn, unaligned, input.Add(time.Minute), unit, tc.zone, opts)
					}
					opts.CeilIsStrictlyGreater = true
					requireTemporalRoundingTime(t, compute.CeilTemporal, input, input.Add(time.Minute), unit, tc.zone, opts)
				})
			}
		})
	}
}

func TestTemporalRoundingCalendarOriginCrossesRepeatedHour(t *testing.T) {
	for _, tc := range []struct {
		zone, input, expected string
	}{
		{"America/New_York", "2024-11-03T01:59:40-04:00", "2024-11-03T02:00:00-05:00"},
		{"Europe/Berlin", "2024-10-27T02:59:40+02:00", "2024-10-27T03:00:00+01:00"},
		{"Australia/Lord_Howe", "2024-04-07T01:59:40+11:00", "2024-04-07T02:00:00+10:30"},
	} {
		t.Run(tc.zone, func(t *testing.T) {
			input, err := time.Parse(time.RFC3339, tc.input)
			require.NoError(t, err)
			expected, err := time.Parse(time.RFC3339, tc.expected)
			require.NoError(t, err)
			for _, fn := range []temporalRoundingFunc{compute.CeilTemporal, compute.RoundTemporal} {
				requireTemporalRoundingTime(t, fn, input, expected, arrow.Microsecond, tc.zone,
					compute.RoundTemporalOptions{
						Multiple:            1,
						Unit:                compute.RoundTemporalMinute,
						CalendarBasedOrigin: true,
					})
			}
		})
	}
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
