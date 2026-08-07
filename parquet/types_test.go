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

package parquet

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestInt96ToTimestamp(t *testing.T) {
	maxDays := math.MaxInt64 / nanosPerDay
	maxNanos := math.MaxInt64 % nanosPerDay
	minDays := math.MinInt64 / nanosPerDay
	minNanos := nanosPerDay + math.MinInt64%nanosPerDay

	tests := []struct {
		name    string
		value   Int96
		want    arrow.Timestamp
		wantErr error
	}{
		{
			name:  "exact minimum",
			value: newInt96Timestamp(minDays-1, minNanos),
			want:  arrow.Timestamp(math.MinInt64),
		},
		{
			name:    "below minimum",
			value:   newInt96Timestamp(minDays-1, minNanos-1),
			wantErr: arrow.ErrInvalid,
		},
		{
			name:  "exact maximum",
			value: newInt96Timestamp(maxDays, maxNanos),
			want:  arrow.Timestamp(math.MaxInt64),
		},
		{
			name:    "above maximum",
			value:   newInt96Timestamp(maxDays, maxNanos+1),
			wantErr: arrow.ErrInvalid,
		},
		{
			name:  "last nanosecond of day",
			value: newInt96Timestamp(0, nanosPerDay-1),
			want:  arrow.Timestamp(nanosPerDay - 1),
		},
		{
			name:    "nanoseconds at end of day",
			value:   newInt96Timestamp(0, nanosPerDay),
			wantErr: arrow.ErrInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.value.ToTimestamp()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func newInt96Timestamp(days, nanosOfDay int64) Int96 {
	nanos := uint64(nanosOfDay)
	return NewInt96([3]uint32{
		uint32(nanos),
		uint32(nanos >> 32),
		uint32(days + julianUnixEpoch),
	})
}
