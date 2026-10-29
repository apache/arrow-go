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

package exec_test

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregateResultScalar(t *testing.T) {
	res := exec.NewScalarResult(scalar.NewInt64Scalar(7))
	assert.True(t, res.IsScalar())
	assert.False(t, res.IsArray())
	assert.Nil(t, res.ArrayData())
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, res.Type()))
	assert.EqualValues(t, 7, res.Scalar().(*scalar.Int64).Value)

	taken := res.Take()
	assert.IsType(t, (*scalar.Int64)(nil), taken)
	assert.False(t, res.IsScalar())
	assert.Nil(t, res.Take())
	assert.Nil(t, res.Type())

	// releasing an emptied result is a no-op, and releasing twice is safe
	res.Release()
	res.Release()
}

func TestAggregateResultArrayOwnership(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewFloat64Builder(mem)
	bldr.AppendValues([]float64{1, 2, 3}, nil)
	arr := bldr.NewArray()
	bldr.Release()

	// the result takes over the single reference held by the array data
	data := arr.Data()
	data.Retain()
	arr.Release()

	res := exec.NewArrayResult(data)
	assert.True(t, res.IsArray())
	assert.False(t, res.IsScalar())
	assert.Nil(t, res.Scalar())
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Float64, res.Type()))

	res.Release()
	assert.False(t, res.IsArray())
	res.Release()
}

func TestAggregateResultReleasesBufferBackedScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)
	buf.Resize(5)
	copy(buf.Bytes(), "hello")

	res := exec.NewScalarResult(scalar.NewStringScalarFromBuffer(buf))
	// the scalar took its own reference on the buffer
	buf.Release()

	assert.Equal(t, "hello", res.Scalar().(*scalar.String).String())
	res.Release()
}

// counterKernel is a minimal aggregate kernel used to exercise the framework
// without going through the compute executor.
type counterState struct {
	n        int64
	mergeErr error
}

func newCounterKernel(cleanups *int, mergeErr error) exec.ScalarAggKernel {
	k := exec.NewScalarAggKernel(
		[]exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Int64)},
		exec.NewOutputType(arrow.PrimitiveTypes.Int64),
		func(*exec.KernelCtx, exec.KernelInitArgs) (exec.KernelState, error) {
			return &counterState{mergeErr: mergeErr}, nil
		},
		func(ctx *exec.KernelCtx, span *exec.ExecSpan) error {
			ctx.State.(*counterState).n += span.Len
			return nil
		},
		func(_ *exec.KernelCtx, src, dst exec.KernelState) error {
			d := dst.(*counterState)
			if d.mergeErr != nil {
				return d.mergeErr
			}
			d.n += src.(*counterState).n
			return nil
		},
		func(ctx *exec.KernelCtx) (*exec.AggregateResult, error) {
			return exec.NewScalarResult(scalar.NewInt64Scalar(ctx.State.(*counterState).n)), nil
		})
	k.CleanupFn = func(*exec.KernelCtx, exec.KernelState) error {
		*cleanups++
		return nil
	}
	return k
}

func TestScalarAggKernelDefaults(t *testing.T) {
	var cleanups int
	k := newCounterKernel(&cleanups, nil)

	assert.False(t, k.IsOrdered())
	assert.NotNil(t, k.GetInitFn())
	require.NotNil(t, k.GetSig())
	assert.True(t, k.GetSig().MatchesInputs([]arrow.DataType{arrow.PrimitiveTypes.Int64}))

	k.Ordered = true
	assert.True(t, k.IsOrdered())

	// a kernel without a cleanup function cleans up successfully
	plain := exec.NewScalarAggKernel(nil, exec.NewOutputType(arrow.Null), nil, nil, nil, nil)
	assert.NoError(t, plain.Cleanup(&exec.KernelCtx{}, nil))
}

func TestScalarAggMergeAll(t *testing.T) {
	var cleanups int
	k := newCounterKernel(&cleanups, nil)
	ctx := &exec.KernelCtx{Kernel: &k}

	states := []exec.KernelState{
		&counterState{n: 1}, &counterState{n: 2}, &counterState{n: 3},
	}
	merged, err := exec.MergeAll(&k, ctx, states)
	require.NoError(t, err)
	assert.EqualValues(t, 6, merged.(*counterState).n)
	// everything but the state that was merged into has been cleaned up
	assert.Equal(t, 2, cleanups)

	cleanups = 0
	merged, err = exec.MergeAll(&k, ctx, nil)
	assert.NoError(t, err)
	assert.Nil(t, merged)
	assert.Zero(t, cleanups)
}

func TestScalarAggMergeAllCleansUpOnError(t *testing.T) {
	errMerge := errors.New("merge failed")
	var cleanups int
	k := newCounterKernel(&cleanups, errMerge)
	ctx := &exec.KernelCtx{Kernel: &k}

	states := []exec.KernelState{
		&counterState{n: 1, mergeErr: errMerge}, &counterState{n: 2}, &counterState{n: 3},
	}
	merged, err := exec.MergeAll(&k, ctx, states)
	assert.ErrorIs(t, err, errMerge)
	assert.Nil(t, merged)
	// every state given to MergeAll is cleaned up exactly once
	assert.Equal(t, len(states), cleanups)
}
