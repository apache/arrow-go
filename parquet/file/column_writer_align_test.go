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

package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAlignBatchToRowBoundary exercises the row-boundary alignment used by the
// V2 writer batching. A repetition level of 0 marks the first level of a new
// row; every other level continues the current row. The helper must return a
// batch whose end (offset+batch) sits on such a boundary or on the end of the
// slice, shrinking toward the previous boundary and only growing forward when a
// single row is wider than the requested batch.
func TestAlignBatchToRowBoundary(t *testing.T) {
	tests := []struct {
		name      string
		repLevels []int16
		offset    int64
		batch     int64
		want      int64
	}{
		{
			// offset+batch already lands on a row start, so it is returned as-is.
			name:      "already on boundary",
			repLevels: []int16{0, 1, 0, 1, 0, 1},
			offset:    0,
			batch:     2,
			want:      2,
		},
		{
			// rows are {0}, {1,2,3}, {4}; batch 3 ends mid-row at index 3, the
			// nearest earlier boundary is index 1.
			name:      "shrink to previous boundary",
			repLevels: []int16{0, 0, 1, 1, 0},
			offset:    0,
			batch:     3,
			want:      1,
		},
		{
			// the first row {0,1,2,3} is wider than batch 2 and has no earlier
			// boundary to back off to, so the whole row is kept in one batch.
			name:      "grow forward when row wider than batch",
			repLevels: []int16{0, 1, 1, 1, 0},
			offset:    0,
			batch:     2,
			want:      4,
		},
		{
			// growing forward runs to the end of the slice when the wide row is
			// the last one.
			name:      "grow forward to end of slice",
			repLevels: []int16{0, 1, 1, 1},
			offset:    0,
			batch:     2,
			want:      4,
		},
		{
			// offset+batch == len: at the tail there is nothing past the end to
			// split, so the batch is returned untouched.
			name:      "batch reaches end untouched",
			repLevels: []int16{0, 1, 0},
			offset:    0,
			batch:     3,
			want:      3,
		},
		{
			// offset+batch > len (a batch larger than the remaining data) is
			// likewise returned untouched; callers only reach this at the tail.
			name:      "large batch past end untouched",
			repLevels: []int16{0, 1, 0},
			offset:    0,
			batch:     100,
			want:      100,
		},
		{
			// a degenerate zero batch stays zero; callers clamp it up to 1.
			name:      "zero batch stays zero",
			repLevels: []int16{0, 1, 0},
			offset:    0,
			batch:     0,
			want:      0,
		},
		{
			// batch 1 that ends on a boundary is unchanged.
			name:      "one batch on boundary",
			repLevels: []int16{0, 0, 0},
			offset:    0,
			batch:     1,
			want:      1,
		},
		{
			// batch 1 that ends mid-row grows to keep the row {0,1} whole.
			name:      "one batch grows through a row",
			repLevels: []int16{0, 1, 0},
			offset:    0,
			batch:     1,
			want:      2,
		},
		{
			// a non-zero offset that already sits on a boundary: the row at
			// index 2 is {2,3,4}, wider than batch 2, so it grows to 3.
			name:      "non-zero offset grows current row",
			repLevels: []int16{0, 1, 0, 1, 1, 0},
			offset:    2,
			batch:     2,
			want:      3,
		},
		{
			// a large batch spanning several whole rows that happens to land on
			// a boundary is returned as-is.
			name:      "large batch on boundary spanning rows",
			repLevels: []int16{0, 1, 0, 1, 0, 1},
			offset:    0,
			batch:     4,
			want:      4,
		},
		{
			// a large batch that lands mid-row shrinks back across the rows it
			// oversteps to the last boundary at or before the split (index 2).
			name:      "large batch shrinks across rows",
			repLevels: []int16{0, 1, 0, 1, 1, 1, 0},
			offset:    0,
			batch:     5,
			want:      2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := alignBatchToRowBoundary(tt.repLevels, tt.offset, tt.batch)
			assert.Equal(t, tt.want, got)

			// Post-conditions that must hold for every non-tail result: the
			// batch ends on a row boundary or the end of the slice, and it never
			// splits inside the current row (offset+batch is never a rep>0 that
			// has a rep==0 immediately reachable behind it within the batch).
			if tt.offset+got < int64(len(tt.repLevels)) && got > 0 {
				assert.Zero(t, tt.repLevels[tt.offset+got],
					"aligned batch must end on a row boundary")
			}
		})
	}
}
