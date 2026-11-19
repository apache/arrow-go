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

//go:build go1.24

package kernels

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// Run alongside the existing BenchmarkSortIndices_Float64 cases to compare
// the default float radix path with the old comparator path on a base commit.
func BenchmarkSortIndicesFloat64DefaultRadix(b *testing.B) {
	const rows = 65536
	for _, tc := range []struct {
		name                string
		chunks, nulls, nans int
	}{
		{"finite", 1, 0, 0},
		{"nans", 1, 0, 97},
		{"nulls", 1, 31, 0},
		{"nulls-and-nans", 1, 31, 97},
		{"chunked-control", 16, 31, 97},
	} {
		for _, order := range []SortOrder{Ascending, Descending} {
			b.Run(fmt.Sprintf("%s/order=%d", tc.name, order), func(b *testing.B) {
				ctx, mem := newBenchKernelCtx(b)
				col := makeChunkedFloat64Split(b, mem, rows, tc.chunks, tc.nulls, tc.nans)
				columns := []*arrow.Chunked{col}
				keys := []SortKey{{Order: order, NullPlacement: NullsAtEnd}}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := SortIndices(ctx, columns, keys)
					if err != nil {
						b.Fatal(err)
					}
					result.Release()
				}
			})
		}
	}
}
