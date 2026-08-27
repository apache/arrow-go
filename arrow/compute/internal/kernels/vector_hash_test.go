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

package kernels

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDictionaryEncodeStateResetAfterFinalize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ctx := &exec.KernelCtx{Ctx: exec.WithAllocator(context.Background(), mem)}
	inputBuilder := array.NewStringBuilder(mem)
	inputBuilder.AppendValues([]string{"foo", "bar", "foo"}, nil)
	input := inputBuilder.NewStringArray()
	inputBuilder.Release()
	defer input.Release()

	state, err := getHashInit(arrow.STRING, initDictionaryEncode)(ctx, exec.KernelInitArgs{
		Inputs:  []arrow.DataType{arrow.BinaryTypes.String},
		Options: DictionaryEncodeOptions{},
	})
	require.NoError(t, err)
	ctx.State = state
	hash := state.(HashState)

	inputSpan := &exec.ArraySpan{}
	inputSpan.SetMembers(input.Data())
	outputType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: input.DataType(),
	}

	encodeOnce := func() {
		require.NoError(t, hash.Append(ctx, inputSpan))
		result := &exec.ArraySpan{Type: outputType}
		require.NoError(t, hash.Flush(result))

		results, err := dictionaryEncodeFinalize(ctx, []*exec.ArraySpan{result})
		require.NoError(t, err)
		for _, result := range results {
			result.Release()
		}
	}

	encodeOnce()
	stateImpl := state.(*regularHashState)
	require.True(t, stateImpl.memoReleased)

	require.NoError(t, hash.Reset())
	require.False(t, stateImpl.memoReleased)

	encodeOnce()
}
