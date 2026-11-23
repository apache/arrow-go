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

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkCheckedRandomTake(b *testing.B) {
	const rows = 1 << 18
	mem := memory.DefaultAllocator
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	indices := make([]uint32, rows/2)
	state := uint32(1)
	for i := range indices {
		state = state*1664525 + 1013904223
		indices[i] = state >> 14
	}
	valuesBuilder := array.NewInt64Builder(mem)
	valuesBuilder.AppendValues(values, nil)
	input := valuesBuilder.NewInt64Array()
	valuesBuilder.Release()
	defer input.Release()
	indicesBuilder := array.NewUint32Builder(mem)
	indicesBuilder.AppendValues(indices, nil)
	selection := indicesBuilder.NewUint32Array()
	indicesBuilder.Release()
	defer selection.Release()

	ctx := context.Background()
	opts := compute.TakeOptions{BoundsCheck: true}
	b.ReportAllocs()
	b.SetBytes(int64(len(indices) * arrow.Int64SizeBytes))
	for b.Loop() {
		out, err := compute.TakeArrayOpts(ctx, input, selection, opts)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
