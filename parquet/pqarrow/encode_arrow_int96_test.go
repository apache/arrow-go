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
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
)

func TestArrowTimestampToImpalaTimestamp(t *testing.T) {
	tests := []struct {
		unit         arrow.TimeUnit
		unitsPerDay  int64
		nanosPerUnit int64
	}{
		{arrow.Second, 86400, 1_000_000_000},
		{arrow.Millisecond, 86_400_000, 1_000_000},
		{arrow.Microsecond, 86_400_000_000, 1_000},
		{arrow.Nanosecond, 86_400_000_000_000, 1},
	}

	for _, tt := range tests {
		for _, value := range []int64{-tt.unitsPerDay - 1, -1, 0, 1, tt.unitsPerDay + 1} {
			var got parquet.Int96
			arrowTimestampToImpalaTimestamp(tt.unit, value, &got)

			days := value / tt.unitsPerDay
			remainder := value % tt.unitsPerDay
			if remainder < 0 {
				days--
				remainder += tt.unitsPerDay
			}
			assert.Equal(t, uint64(remainder*tt.nanosPerUnit), binary.LittleEndian.Uint64(got[:8]))
			assert.Equal(t, uint32(days+julianEpochOffsetDays), binary.LittleEndian.Uint32(got[8:]))
		}
	}
}
