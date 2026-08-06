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
