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

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDictionaryEncodeFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := [][]byte{
		{0, 0, 0, 1},
		{0, 0, 0, 2},
		{0, 0, 0, 1},
		{0, 0, 0, 3},
		{0, 0, 0, 2},
		{0, 0, 0, 3},
	}
	valid := []bool{true, true, true, false, true, true}

	builder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
	builder.AppendValues(values, valid)
	input := builder.NewFixedSizeBinaryArray()
	builder.Release()
	defer input.Release()

	ctx := compute.WithAllocator(context.Background(), mem)
	result, err := compute.DictionaryEncodeArray(ctx, compute.DictionaryEncodeOptions{}, input)
	require.NoError(t, err)
	defer result.Release()

	encoded := result.(*array.Dictionary)
	require.Equal(t, []int32{0, 1, 0, 0, 1, 2}, encoded.Indices().(*array.Int32).Int32Values())
	require.Equal(t, 1, encoded.NullN())
	require.Equal(t, 3, encoded.Dictionary().Len())

	dict := encoded.Dictionary().(*array.FixedSizeBinary)
	require.Equal(t, values[0], dict.Value(0))
	require.Equal(t, values[1], dict.Value(1))
	require.Equal(t, values[3], dict.Value(2))
}
