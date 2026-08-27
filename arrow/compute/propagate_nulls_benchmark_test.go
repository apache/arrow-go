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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkPropagateNullsScratch(b *testing.B) {
	cases := []struct {
		name          string
		inputCount    int
		nullableInput bool
	}{
		{name: "all-valid/1", inputCount: 1},
		{name: "all-valid/2", inputCount: 2},
		{name: "nullable/1", inputCount: 1, nullableInput: true},
		{name: "nullable/2", inputCount: 2, nullableInput: true},
		{name: "nullable/4", inputCount: 4, nullableInput: true},
		{name: "nullable/5", inputCount: 5, nullableInput: true},
		{name: "nullable/8", inputCount: 8, nullableInput: true},
		{name: "nullable/9", inputCount: 9, nullableInput: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			batch := newPropagateNullsBenchmarkBatch(tc.inputCount, tc.nullableInput)
			out := newPropagateNullsBenchmarkOutput()
			ctx := &exec.KernelCtx{}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := propagateNulls(ctx, batch, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newPropagateNullsBenchmarkBatch(inputCount int, nullableInput bool) *exec.ExecSpan {
	const length int64 = 64

	batch := &exec.ExecSpan{
		Len:    length,
		Values: make([]exec.ExecValue, inputCount),
	}
	bitmap := []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for i := range batch.Values {
		batch.Values[i].Array.Type = arrow.PrimitiveTypes.Int32
		batch.Values[i].Array.Len = length
		if nullableInput {
			batch.Values[i].Array.Nulls = 1
			batch.Values[i].Array.Buffers[0].Buf = bitmap
		}
	}
	return batch
}

func newPropagateNullsBenchmarkOutput() *exec.ArraySpan {
	out := &exec.ArraySpan{
		Type: arrow.PrimitiveTypes.Int32,
		Len:  64,
	}
	out.Buffers[0].Buf = make([]byte, 8)
	return out
}

func BenchmarkPropagateNullsSmallArrayAdd(b *testing.B) {
	const length = 16
	values := make([]int32, length)
	valid := make([]bool, length)
	for i := range valid {
		valid[i] = i != 0
	}

	leftBuilder := array.NewInt32Builder(memory.DefaultAllocator)
	leftBuilder.AppendValues(values, valid)
	left := leftBuilder.NewInt32Array()
	leftBuilder.Release()
	defer left.Release()

	rightBuilder := array.NewInt32Builder(memory.DefaultAllocator)
	rightBuilder.AppendValues(values, nil)
	right := rightBuilder.NewInt32Array()
	rightBuilder.Release()
	defer right.Release()

	leftDatum := NewDatum(left)
	defer leftDatum.Release()
	rightDatum := NewDatum(right)
	defer rightDatum.Release()

	ctx := SetExecCtx(context.Background(), ExecCtx{
		Registry:           GetFunctionRegistry(),
		ChunkSize:          length,
		PreallocContiguous: true,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := CallFunction(ctx, "add", nil, leftDatum, rightDatum)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
