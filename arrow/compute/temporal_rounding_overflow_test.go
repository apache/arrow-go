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
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestTemporalRoundingOverflow(t *testing.T) {
	tests := []struct {
		name  string
		unit  arrow.TimeUnit
		value arrow.Timestamp
		opt   compute.RoundTemporalOptions
	}{
		{
			name:  "input conversion",
			unit:  arrow.Second,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 3, Unit: compute.RoundTemporalNanosecond},
		},
		{
			name:  "millisecond input conversion",
			unit:  arrow.Millisecond,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 3, Unit: compute.RoundTemporalNanosecond},
		},
		{
			name:  "microsecond input conversion",
			unit:  arrow.Microsecond,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 3, Unit: compute.RoundTemporalNanosecond},
		},
		{
			name:  "interval conversion",
			unit:  arrow.Second,
			value: 1,
			opt:   compute.RoundTemporalOptions{Multiple: math.MaxInt64, Unit: compute.RoundTemporalSecond},
		},
		{
			name:  "rounded result",
			unit:  arrow.Nanosecond,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 2, Unit: compute.RoundTemporalNanosecond},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: tc.unit})
			builder.Append(tc.value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			_, err := compute.RoundTemporal(context.Background(), tc.opt, compute.NewDatum(input))
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestTemporalRoundingCalendarMultipleOverflow(t *testing.T) {
	value, err := arrow.TimestampFromTime(time.Date(2024, time.January, 15, 10, 37, 0, 0, time.UTC), arrow.Nanosecond)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
	builder.Append(value)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	tests := []struct {
		name string
		fn   func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		unit compute.RoundTemporalUnit
	}{
		{name: "year ceiling", fn: compute.CeilTemporal, unit: compute.RoundTemporalYear},
		{name: "year half-rounding", fn: compute.RoundTemporal, unit: compute.RoundTemporalYear},
		{name: "quarter ceiling", fn: compute.CeilTemporal, unit: compute.RoundTemporalQuarter},
		{name: "quarter half-rounding", fn: compute.RoundTemporal, unit: compute.RoundTemporalQuarter},
		{name: "month ceiling", fn: compute.CeilTemporal, unit: compute.RoundTemporalMonth},
		{name: "month half-rounding", fn: compute.RoundTemporal, unit: compute.RoundTemporalMonth},
		{name: "week ceiling", fn: compute.CeilTemporal, unit: compute.RoundTemporalWeek},
		{name: "week half-rounding", fn: compute.RoundTemporal, unit: compute.RoundTemporalWeek},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn(context.Background(), compute.RoundTemporalOptions{
				Multiple: math.MaxInt64,
				Unit:     tc.unit,
			}, compute.NewDatum(input))
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestTemporalRoundingDate32Overflow(t *testing.T) {
	tests := []struct {
		name  string
		value arrow.Date32
		fn    func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		opts  compute.RoundTemporalOptions
	}{
		{
			name:  "ceiling past maximum",
			value: math.MaxInt32,
			fn:    compute.CeilTemporal,
			opts: compute.RoundTemporalOptions{
				Multiple:              1,
				Unit:                  compute.RoundTemporalDay,
				CeilIsStrictlyGreater: true,
			},
		},
		{
			name:  "floor below minimum",
			value: math.MinInt32,
			fn:    compute.FloorTemporal,
			opts: compute.RoundTemporalOptions{
				Multiple: 3,
				Unit:     compute.RoundTemporalDay,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewDate32Builder(memory.DefaultAllocator)
			builder.Append(tc.value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			_, err := tc.fn(context.Background(), tc.opts, compute.NewDatum(input))
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestTemporalRoundingTimezonePaths(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		name     string
		value    time.Time
		unit     compute.RoundTemporalUnit
		multiple int64
		calendar bool
		want     time.Time
	}{
		{
			name:     "timezone input",
			value:    time.Date(2024, time.January, 15, 10, 37, 0, 0, loc),
			unit:     compute.RoundTemporalHour,
			multiple: 2,
			want:     time.Date(2024, time.January, 15, 9, 0, 0, 0, loc),
		},
		{
			name:     "spring DST day",
			value:    time.Date(2024, time.March, 10, 12, 0, 0, 0, loc),
			unit:     compute.RoundTemporalDay,
			multiple: 1,
			want:     time.Date(2024, time.March, 10, 0, 0, 0, 0, loc),
		},
		{
			name:     "timezone calendar origin",
			value:    time.Date(2024, time.January, 15, 10, 37, 0, 0, time.UTC),
			unit:     compute.RoundTemporalHour,
			multiple: 2,
			calendar: true,
			want:     time.Date(2024, time.January, 15, 9, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, err := arrow.TimestampFromTime(tc.value, arrow.Nanosecond)
			require.NoError(t, err)
			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{
				Unit:     arrow.Nanosecond,
				TimeZone: loc.String(),
			})
			builder.Append(value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
				Multiple:            tc.multiple,
				Unit:                tc.unit,
				CalendarBasedOrigin: tc.calendar,
			}, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer output.Release()
			want, err := arrow.TimestampFromTime(tc.want, arrow.Nanosecond)
			require.NoError(t, err)
			require.Equal(t, want, output.Value(0))
		})
	}
}

func TestTemporalRoundingCalendarBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		value time.Time
		want  time.Time
		opts  compute.RoundTemporalOptions
	}{
		{
			name:  "year result before unused end boundary",
			value: time.Date(2262, time.January, 5, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC),
			opts:  compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalYear},
		},
		{
			name:  "quarter result before unused end boundary",
			value: time.Date(2262, time.January, 5, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC),
			opts:  compute.RoundTemporalOptions{Multiple: 4, Unit: compute.RoundTemporalQuarter},
		},
		{
			name:  "month result before unused end boundary",
			value: time.Date(2262, time.January, 5, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC),
			opts:  compute.RoundTemporalOptions{Multiple: 12, Unit: compute.RoundTemporalMonth},
		},
		{
			name:  "wide year multiple",
			value: time.Date(2201, time.January, 1, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2200, time.January, 1, 0, 0, 0, 0, time.UTC),
			opts:  compute.RoundTemporalOptions{Multiple: 100, Unit: compute.RoundTemporalYear},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, err := arrow.TimestampFromTime(tc.value, arrow.Nanosecond)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
			builder.Append(value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.RoundTemporal(context.Background(), tc.opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer output.Release()
			want, err := arrow.TimestampFromTime(tc.want, arrow.Nanosecond)
			require.NoError(t, err)
			require.Equal(t, want, output.Value(0))
		})
	}
}

func TestTemporalRoundingCalendarPreservesInputUnit(t *testing.T) {
	value := time.Date(1500, time.June, 15, 12, 34, 56, 0, time.UTC)
	want := time.Date(1500, time.January, 1, 0, 0, 0, 0, time.UTC)
	opts := compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalYear}

	t.Run("date32", func(t *testing.T) {
		builder := array.NewDate32Builder(memory.DefaultAllocator)
		builder.Append(arrow.Date32FromTime(value))
		input := builder.NewArray()
		builder.Release()
		defer input.Release()

		result, err := compute.FloorTemporal(context.Background(), opts, compute.NewDatum(input))
		require.NoError(t, err)
		defer result.Release()

		output := result.(*compute.ArrayDatum).MakeArray().(*array.Date32)
		defer output.Release()
		require.Equal(t, arrow.Date32FromTime(want), output.Value(0))
	})

	t.Run("date64", func(t *testing.T) {
		builder := array.NewDate64Builder(memory.DefaultAllocator)
		builder.Append(arrow.Date64FromTime(value))
		input := builder.NewArray()
		builder.Release()
		defer input.Release()

		result, err := compute.FloorTemporal(context.Background(), opts, compute.NewDatum(input))
		require.NoError(t, err)
		defer result.Release()

		output := result.(*compute.ArrayDatum).MakeArray().(*array.Date64)
		defer output.Release()
		require.Equal(t, arrow.Date64FromTime(want), output.Value(0))
	})

	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond} {
		t.Run(unit.String(), func(t *testing.T) {
			value, err := arrow.TimestampFromTime(value, unit)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit})
			builder.Append(value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.FloorTemporal(context.Background(), opts, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer output.Release()
			want, err := arrow.TimestampFromTime(want, unit)
			require.NoError(t, err)
			require.Equal(t, want, output.Value(0))
		})
	}
}

func TestTemporalRoundingFinerUnitPreservesInputPrecision(t *testing.T) {
	value := time.Date(1500, time.June, 15, 12, 34, 56, 0, time.UTC)

	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond} {
		t.Run(unit.String(), func(t *testing.T) {
			inputValue, err := arrow.TimestampFromTime(value, unit)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit})
			builder.Append(inputValue)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalNanosecond,
			}, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer output.Release()
			require.Equal(t, inputValue, output.Value(0))
		})
	}

	t.Run("date32", func(t *testing.T) {
		builder := array.NewDate32Builder(memory.DefaultAllocator)
		builder.Append(arrow.Date32FromTime(value))
		input := builder.NewArray()
		builder.Release()
		defer input.Release()

		result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
			Multiple: 1,
			Unit:     compute.RoundTemporalNanosecond,
		}, compute.NewDatum(input))
		require.NoError(t, err)
		defer result.Release()

		output := result.(*compute.ArrayDatum).MakeArray().(*array.Date32)
		defer output.Release()
		require.Equal(t, arrow.Date32FromTime(value), output.Value(0))
	})

	t.Run("date64", func(t *testing.T) {
		builder := array.NewDate64Builder(memory.DefaultAllocator)
		builder.Append(arrow.Date64FromTime(value))
		input := builder.NewArray()
		builder.Release()
		defer input.Release()

		result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
			Multiple: 1,
			Unit:     compute.RoundTemporalNanosecond,
		}, compute.NewDatum(input))
		require.NoError(t, err)
		defer result.Release()

		output := result.(*compute.ArrayDatum).MakeArray().(*array.Date64)
		defer output.Release()
		require.Equal(t, arrow.Date64FromTime(value), output.Value(0))
	})
}

func TestTemporalRoundingCalendarOriginWideUnits(t *testing.T) {
	valueTime := time.Date(1500, time.June, 15, 12, 34, 56, 0, time.UTC)
	wantTime := time.Date(1500, time.June, 15, 12, 0, 0, 0, time.UTC)

	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond} {
		t.Run(unit.String(), func(t *testing.T) {
			value, err := arrow.TimestampFromTime(valueTime, unit)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit})
			builder.Append(value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
				Multiple:            2,
				Unit:                compute.RoundTemporalHour,
				CalendarBasedOrigin: true,
			}, compute.NewDatum(input))
			require.NoError(t, err)
			defer result.Release()

			output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
			defer output.Release()
			want, err := arrow.TimestampFromTime(wantTime, unit)
			require.NoError(t, err)
			require.Equal(t, want, output.Value(0))
		})
	}
}

func TestTemporalRoundingCalendarOriginAtMinimum(t *testing.T) {
	minTime := arrow.Timestamp(math.MinInt64).ToTime(arrow.Nanosecond)
	wantTime := time.Date(minTime.Year(), minTime.Month(), minTime.Day(), minTime.Hour()+1, 0, 0, 0, time.UTC)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
	builder.Append(arrow.Timestamp(math.MinInt64))
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.CeilTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple:            1,
		Unit:                compute.RoundTemporalHour,
		CalendarBasedOrigin: true,
	}, compute.NewDatum(input))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()
	want, err := arrow.TimestampFromTime(wantTime, arrow.Nanosecond)
	require.NoError(t, err)
	require.Equal(t, want, output.Value(0))
}

func TestTemporalRoundingCalendarLongRangeWeek(t *testing.T) {
	value, err := arrow.TimestampFromTime(
		time.Date(1500, time.June, 15, 12, 34, 56, 0, time.UTC),
		arrow.Second,
	)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	builder.Append(value)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.FloorTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple: 1,
		Unit:     compute.RoundTemporalWeek,
	}, compute.NewDatum(input))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()
	want, err := arrow.TimestampFromTime(time.Date(1500, time.June, 10, 0, 0, 0, 0, time.UTC), arrow.Second)
	require.NoError(t, err)
	require.Equal(t, want, output.Value(0))
}

func TestTemporalRoundingCalendarSecondBoundary(t *testing.T) {
	maxSecondTime := arrow.Timestamp(math.MaxInt64).ToTime(arrow.Second)
	valueTime := time.Date(maxSecondTime.Year(), time.January, 2, 0, 0, 0, 0, time.UTC)
	value, err := arrow.TimestampFromTime(valueTime, arrow.Second)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	builder.Append(value)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.RoundTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple: 1,
		Unit:     compute.RoundTemporalYear,
	}, compute.NewDatum(input))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()
	want, err := arrow.TimestampFromTime(time.Date(maxSecondTime.Year(), time.January, 1, 0, 0, 0, 0, time.UTC), arrow.Second)
	require.NoError(t, err)
	require.Equal(t, want, output.Value(0))
}

func TestTemporalRoundingSecondOverflow(t *testing.T) {
	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	builder.Append(arrow.Timestamp(math.MaxInt64))
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	_, err := compute.CeilTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple: 1,
		Unit:     compute.RoundTemporalYear,
	}, compute.NewDatum(input))
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestTemporalRoundingSecondCalendarExtremes(t *testing.T) {
	tests := []struct {
		name  string
		value arrow.Timestamp
		fn    func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
		opts  compute.RoundTemporalOptions
	}{
		{
			name:  "floor at minimum",
			value: arrow.Timestamp(math.MinInt64),
			fn:    compute.FloorTemporal,
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalYear,
			},
		},
		{
			name:  "ceil at maximum to day",
			value: arrow.Timestamp(math.MaxInt64),
			fn:    compute.CeilTemporal,
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalDay,
			},
		},
		{
			name:  "ceil immediately below maximum to day",
			value: arrow.Timestamp(math.MaxInt64 - 1),
			fn:    compute.CeilTemporal,
			opts: compute.RoundTemporalOptions{
				Multiple: 1,
				Unit:     compute.RoundTemporalDay,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
			builder.Append(tc.value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			_, err := tc.fn(context.Background(), tc.opts, compute.NewDatum(input))
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestTemporalRoundingNegativeCalendarMultiples(t *testing.T) {
	tests := []struct {
		name     string
		value    time.Time
		floor    time.Time
		ceil     time.Time
		multiple int64
		unit     compute.RoundTemporalUnit
	}{
		{
			name:     "year",
			value:    time.Date(-5, time.June, 15, 12, 0, 0, 0, time.UTC),
			floor:    time.Date(-6, time.January, 1, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(-4, time.January, 1, 0, 0, 0, 0, time.UTC),
			multiple: 2,
			unit:     compute.RoundTemporalYear,
		},
		{
			name:     "quarter",
			value:    time.Date(-5, time.June, 15, 12, 0, 0, 0, time.UTC),
			floor:    time.Date(-5, time.January, 1, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(-5, time.July, 1, 0, 0, 0, 0, time.UTC),
			multiple: 2,
			unit:     compute.RoundTemporalQuarter,
		},
		{
			name:     "month",
			value:    time.Date(-5, time.June, 15, 12, 0, 0, 0, time.UTC),
			floor:    time.Date(-5, time.May, 1, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(-5, time.July, 1, 0, 0, 0, 0, time.UTC),
			multiple: 2,
			unit:     compute.RoundTemporalMonth,
		},
		{
			name:     "pre-epoch weeks",
			value:    time.Date(1969, time.December, 21, 12, 0, 0, 0, time.UTC),
			floor:    time.Date(1969, time.December, 14, 0, 0, 0, 0, time.UTC),
			ceil:     time.Date(1969, time.December, 28, 0, 0, 0, 0, time.UTC),
			multiple: 2,
			unit:     compute.RoundTemporalWeek,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, err := arrow.TimestampFromTime(tc.value, arrow.Second)
			require.NoError(t, err)

			builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
			builder.Append(value)
			input := builder.NewArray()
			builder.Release()
			defer input.Release()

			for _, round := range []struct {
				name string
				fn   func(context.Context, compute.RoundTemporalOptions, compute.Datum) (compute.Datum, error)
				want time.Time
			}{
				{name: "floor", fn: compute.FloorTemporal, want: tc.floor},
				{name: "ceil", fn: compute.CeilTemporal, want: tc.ceil},
			} {
				t.Run(round.name, func(t *testing.T) {
					result, err := round.fn(context.Background(), compute.RoundTemporalOptions{
						Multiple: tc.multiple,
						Unit:     tc.unit,
					}, compute.NewDatum(input))
					require.NoError(t, err)
					defer result.Release()

					output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
					defer output.Release()
					want, err := arrow.TimestampFromTime(round.want, arrow.Second)
					require.NoError(t, err)
					require.Equal(t, want, output.Value(0))
				})
			}
		})
	}
}

func TestTemporalRoundingCalendarLongPeriod(t *testing.T) {
	value, err := arrow.TimestampFromTime(
		time.Date(1948, time.January, 1, 0, 0, 0, 0, time.UTC),
		arrow.Nanosecond,
	)
	require.NoError(t, err)

	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
	builder.Append(value)
	input := builder.NewArray()
	builder.Release()
	defer input.Release()

	result, err := compute.RoundTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple: 300,
		Unit:     compute.RoundTemporalYear,
	}, compute.NewDatum(input))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()
	want, err := arrow.TimestampFromTime(time.Date(1800, time.January, 1, 0, 0, 0, 0, time.UTC), arrow.Nanosecond)
	require.NoError(t, err)
	require.Equal(t, want, output.Value(0))
}
