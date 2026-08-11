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
			opt:   compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalNanosecond},
		},
		{
			name:  "millisecond input conversion",
			unit:  arrow.Millisecond,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalNanosecond},
		},
		{
			name:  "microsecond input conversion",
			unit:  arrow.Microsecond,
			value: arrow.Timestamp(math.MaxInt64),
			opt:   compute.RoundTemporalOptions{Multiple: 1, Unit: compute.RoundTemporalNanosecond},
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
			name:     "UTC calendar origin",
			value:    time.Date(2024, time.January, 15, 10, 37, 0, 0, time.UTC),
			unit:     compute.RoundTemporalHour,
			multiple: 2,
			calendar: true,
			want:     time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC),
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
