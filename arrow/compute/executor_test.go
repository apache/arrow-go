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
	"github.com/stretchr/testify/require"
)

func TestVectorExecutorWrapResultsReleasesEmptyChunkedOutput(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := array.NewInt32Builder(mem)
	builder.Append(42)
	value := builder.NewInt32Array()
	builder.Release()
	defer value.Release()

	empty := array.NewSlice(value, 0, 0)
	defer empty.Release()
	nonEmpty := array.NewSlice(value, 0, 1)
	defer nonEmpty.Release()

	chunked := arrow.NewChunked(value.DataType(), []arrow.Array{empty, nonEmpty})
	output := make(chan Datum, 1)
	output <- &ChunkedDatum{Value: chunked}
	close(output)

	executor := &vectorExecutor{
		nonAggExecImpl: nonAggExecImpl{
			kernel:  &exec.VectorKernel{OutputChunked: true},
			outType: value.DataType(),
		},
	}

	result := executor.WrapResults(context.Background(), output, true)
	require.NotNil(t, result)
	require.Equal(t, KindChunked, result.Kind())

	resultChunked := result.(*ChunkedDatum).Value
	require.Len(t, resultChunked.Chunks(), 1)
	require.Equal(t, int32(42), resultChunked.Chunk(0).(*array.Int32).Value(0))
	result.Release()
}
