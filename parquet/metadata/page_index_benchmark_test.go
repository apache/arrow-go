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

package metadata

import (
	"fmt"
	"testing"

	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
)

func makeRowGroupMetaData(numCols int) *RowGroupMetaData {
	columns := make([]*format.ColumnChunk, numCols)
	for i := range numCols {
		ciOffset := int64(1000 * (i + 1))
		ciLength := int32(500)
		oiOffset := int64(2000 * (i + 1))
		oiLength := int32(300)
		columns[i] = &format.ColumnChunk{
			ColumnIndexOffset: &ciOffset,
			ColumnIndexLength: &ciLength,
			OffsetIndexOffset: &oiOffset,
			OffsetIndexLength: &oiLength,
		}
	}

	return NewRowGroupMetaData(&format.RowGroup{Columns: columns}, nil, nil, nil)
}

func TestDeterminePageIndexRangesInRowGroupAllocs(t *testing.T) {
	rgMeta10 := makeRowGroupMetaData(10)
	rgMeta100 := makeRowGroupMetaData(100)
	cols10 := make([]int32, 10)
	for i := range cols10 {
		cols10[i] = int32(i)
	}
	cols100 := make([]int32, 100)
	for i := range cols100 {
		cols100[i] = int32(i)
	}

	allocs10 := testing.AllocsPerRun(100, func() {
		if _, err := determinePageIndexRangesInRowGroup(rgMeta10, cols10); err != nil {
			t.Fatal(err)
		}
	})
	allocs100 := testing.AllocsPerRun(100, func() {
		if _, err := determinePageIndexRangesInRowGroup(rgMeta100, cols100); err != nil {
			t.Fatal(err)
		}
	})

	if allocs10 != allocs100 {
		t.Errorf("allocations should not scale with column count: 10 cols = %v, 100 cols = %v", allocs10, allocs100)
	}
}

func BenchmarkDeterminePageIndexRangesInRowGroup(b *testing.B) {
	for _, numCols := range []int{10, 50, 100} {
		rgMeta := makeRowGroupMetaData(numCols)
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := determinePageIndexRangesInRowGroup(rgMeta, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
