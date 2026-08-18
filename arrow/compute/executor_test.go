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

type signalChunkedDatum struct {
	*ChunkedDatum
	chunksCalled chan struct{}
}

func (d *signalChunkedDatum) Chunks() []arrow.Array {
	close(d.chunksCalled)
	return d.Value.Chunks()
}

func TestVectorExecutorWrapResultsReleasesEmptyArrayOutput(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.String)
	empty := builder.NewArray()
	builder.Release()

	output := make(chan Datum, 1)
	output <- NewDatum(empty)
	close(output)

	executor := &vectorExecutor{
		nonAggExecImpl: nonAggExecImpl{
			kernel:  &exec.VectorKernel{OutputChunked: true},
			outType: arrow.BinaryTypes.String,
		},
	}

	result := executor.WrapResults(context.Background(), output, true)
	require.NotNil(t, result)
	require.Equal(t, KindChunked, result.Kind())
	require.Empty(t, result.(*ChunkedDatum).Value.Chunks())
	result.Release()
	empty.Release()
}

func TestVectorExecutorWrapResultsReleasesChunkedOutputOnCancellation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := array.NewInt32Builder(mem)
	builder.Append(42)
	value := builder.NewInt32Array()
	builder.Release()
	defer value.Release()

	chunked := arrow.NewChunked(value.DataType(), []arrow.Array{value})
	output := make(chan Datum)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := &vectorExecutor{
		nonAggExecImpl: nonAggExecImpl{
			kernel:  &exec.VectorKernel{OutputChunked: true},
			outType: value.DataType(),
		},
	}

	datum := &signalChunkedDatum{
		ChunkedDatum: &ChunkedDatum{Value: chunked},
		chunksCalled: make(chan struct{}),
	}
	result := make(chan Datum, 1)
	go func() {
		result <- executor.WrapResults(ctx, output, true)
	}()

	output <- datum
	<-datum.chunksCalled
	cancel()

	require.Nil(t, <-result)
	close(output)
}
