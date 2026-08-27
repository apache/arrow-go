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

package compute

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkIterateExecSpans(b *testing.B) {
	for _, length := range []int64{1, 16, 256, 4096} {
		b.Run(fmt.Sprintf("array/%d", length), func(b *testing.B) {
			batch := newExecSpanBenchmarkBatch(b, length, 1, false)
			benchmarkIterateExecSpans(b, batch, DefaultMaxChunkSize)
		})

		b.Run(fmt.Sprintf("binary/%d", length), func(b *testing.B) {
			batch := newExecSpanBenchmarkBatch(b, length, 2, false)
			benchmarkIterateExecSpans(b, batch, DefaultMaxChunkSize)
		})

		b.Run(fmt.Sprintf("scalar-array/%d", length), func(b *testing.B) {
			batch := newExecSpanBenchmarkBatch(b, length, 1, true)
			benchmarkIterateExecSpans(b, batch, DefaultMaxChunkSize)
		})
	}

	b.Run("all-scalars", func(b *testing.B) {
		batch := &ExecBatch{
			Values: []Datum{NewDatum(int32(1)), NewDatum(int32(2))},
			Len:    1,
		}
		benchmarkIterateExecSpans(b, batch, DefaultMaxChunkSize)
	})

	b.Run("chunked-control", func(b *testing.B) {
		batch := newExecSpanBenchmarkBatch(b, 4096, 1, false)
		arr := batch.Values[0].(*ArrayDatum).Value
		chunk := array.MakeFromData(arr)
		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{chunk})
		chunk.Release()
		b.Cleanup(chunked.Release)
		batch.Values[0] = &ChunkedDatum{Value: chunked}
		benchmarkIterateExecSpans(b, batch, DefaultMaxChunkSize)
	})
}

func benchmarkIterateExecSpans(b *testing.B, batch *ExecBatch, maxChunkSize int64) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, iter, err := iterateExecSpans(batch, maxChunkSize, true)
		if err != nil {
			b.Fatal(err)
		}

		span, pos, ok := iter()
		if !ok || pos != batch.Len || span.Len != batch.Len {
			b.Fatalf("unexpected first span: len=%d pos=%d ok=%t", span.Len, pos, ok)
		}

		_, pos, ok = iter()
		if ok || pos != batch.Len {
			b.Fatalf("unexpected second span: pos=%d ok=%t", pos, ok)
		}
	}
}

func newExecSpanBenchmarkBatch(tb testing.TB, length int64, arrayCount int, addScalar bool) *ExecBatch {
	tb.Helper()
	values := make([]Datum, 0, arrayCount+1)
	for i := 0; i < arrayCount; i++ {
		builder := array.NewInt32Builder(memory.DefaultAllocator)
		builder.Reserve(int(length))
		for j := int64(0); j < length; j++ {
			builder.Append(int32(i) + int32(j))
		}
		arr := builder.NewInt32Array()
		builder.Release()
		tb.Cleanup(arr.Release)
		values = append(values, &ArrayDatum{Value: arr.Data()})
	}

	if addScalar {
		values = append(values, NewDatum(int32(1)))
	}
	return &ExecBatch{Values: values, Len: length}
}
