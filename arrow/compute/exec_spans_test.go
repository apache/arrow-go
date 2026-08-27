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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestIterateExecSpansSingleSpan(t *testing.T) {
	const length = 16

	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.AppendValues(make([]int32, length+2), nil)
	full := builder.NewInt32Array()
	builder.Release()
	input := array.NewSlice(full, 1, length+1)
	full.Release()
	defer input.Release()

	batch := &ExecBatch{
		Values: []Datum{
			&ArrayDatum{Value: input.Data()},
			NewDatum(int32(5)),
		},
		Len: int64(input.Len()),
	}

	allScalars, iter, err := iterateExecSpans(batch, DefaultMaxChunkSize, true)
	require.NoError(t, err)
	require.False(t, allScalars)

	span, pos, ok := iter()
	require.True(t, ok)
	require.EqualValues(t, length, span.Len)
	require.EqualValues(t, length, pos)
	require.EqualValues(t, input.Data().Offset(), span.Values[0].Array.Offset)
	require.EqualValues(t, input.Len(), span.Values[0].Array.Len)
	require.True(t, span.Values[1].IsScalar())

	_, pos, ok = iter()
	require.False(t, ok)
	require.EqualValues(t, length, pos)
}

func TestIterateExecSpansPromotesAllScalars(t *testing.T) {
	batch := &ExecBatch{
		Values: []Datum{NewDatum(int32(1)), NewDatum(int32(2))},
		Len:    1,
	}

	allScalars, iter, err := iterateExecSpans(batch, DefaultMaxChunkSize, true)
	require.NoError(t, err)
	require.True(t, allScalars)

	span, pos, ok := iter()
	require.True(t, ok)
	require.EqualValues(t, 1, span.Len)
	require.EqualValues(t, 1, pos)
	require.True(t, span.Values[0].IsArray())
	require.True(t, span.Values[1].IsArray())

	_, pos, ok = iter()
	require.False(t, ok)
	require.EqualValues(t, 1, pos)
}
