// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
)

func TestArrowTimestampToImpalaTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		unit  arrow.TimeUnit
		value arrow.Timestamp
	}{
		{"seconds after epoch", arrow.Second, 946_684_801},
		{"seconds before epoch", arrow.Second, -1},
		{"milliseconds after epoch", arrow.Millisecond, 946_684_800_001},
		{"milliseconds before epoch", arrow.Millisecond, -1},
		{"microseconds after epoch", arrow.Microsecond, 946_684_800_000_001},
		{"microseconds before epoch", arrow.Microsecond, -1},
		{"nanoseconds after epoch", arrow.Nanosecond, 946_684_800_000_000_001},
		{"nanoseconds before epoch", arrow.Nanosecond, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got parquet.Int96
			arrowTimestampToImpalaTimestamp(tt.unit, int64(tt.value), &got)

			assert.Equal(t, tt.value.ToTime(tt.unit), got.ToTime())
		})
	}
}
