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

package array_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestFixedWidthBuilderAppendNulls(t *testing.T) {
	builders := []struct {
		name string
		new  func(memory.Allocator) array.Builder
	}{
		{"int32", func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) }},
		{"float16", func(mem memory.Allocator) array.Builder { return array.NewFloat16Builder(mem) }},
		{"decimal128", func(mem memory.Allocator) array.Builder {
			return array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 38})
		}},
		{"timestamp", func(mem memory.Allocator) array.Builder {
			return array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
		}},
		{"month-interval", func(mem memory.Allocator) array.Builder { return array.NewMonthIntervalBuilder(mem) }},
		{"day-time-interval", func(mem memory.Allocator) array.Builder { return array.NewDayTimeIntervalBuilder(mem) }},
		{"month-day-nano-interval", func(mem memory.Allocator) array.Builder {
			return array.NewMonthDayNanoIntervalBuilder(mem)
		}},
		{"boolean", func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) }},
		{"fixed-size-binary", func(mem memory.Allocator) array.Builder {
			return array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 16})
		}},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			instance := builder.new(mem)
			defer instance.Release()

			instance.AppendNulls(0)
			instance.AppendNulls(-1)
			require.Zero(t, instance.Len())
			require.Zero(t, instance.Cap())
			require.Zero(t, instance.NullN())

			instance.AppendEmptyValue()
			instance.AppendNulls(10)
			instance.AppendEmptyValue()
			require.Equal(t, 12, instance.Len())
			require.Equal(t, 10, instance.NullN())

			result := instance.NewArray()
			defer result.Release()
			require.True(t, result.IsValid(0))
			for idx := 1; idx <= 10; idx++ {
				require.True(t, result.IsNull(idx))
			}
			require.True(t, result.IsValid(11))
		})
	}
}

func TestFixedWidthBuilderAppendNullsClearsReusedValidity(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewInt32Builder(mem)
	defer builder.Release()
	builder.AppendEmptyValues(16)
	builder.Resize(3)
	builder.AppendNulls(10)

	result := builder.NewInt32Array()
	defer result.Release()
	require.Equal(t, 13, result.Len())
	require.Equal(t, 10, result.NullN())
	for idx := 0; idx < 3; idx++ {
		require.True(t, result.IsValid(idx))
	}
	for idx := 3; idx < result.Len(); idx++ {
		require.True(t, result.IsNull(idx))
	}
}

func TestFixedWidthBuilderAppendNullsPowerOfTwoCapacity(t *testing.T) {
	const (
		count    = 1 << 16
		byteSize = 16
	)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	intBuilder := array.NewInt32Builder(mem)
	intBuilder.AppendNulls(count)
	require.Equal(t, count, intBuilder.Cap())
	intBuilder.Release()

	fixedBinaryBuilder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: byteSize})
	fixedBinaryBuilder.AppendNulls(count)
	require.Equal(t, count, fixedBinaryBuilder.Cap())

	result := fixedBinaryBuilder.NewFixedSizeBinaryArray()
	require.Equal(t, count*byteSize, result.Data().Buffers()[1].Len())
	require.Equal(t, count*byteSize, result.Data().Buffers()[1].Cap())
	result.Release()
	fixedBinaryBuilder.Release()
}

func TestFixedSizeBinaryBuilderAppendNullsAcrossGrowthBoundary(t *testing.T) {
	const (
		count    = 1 << 16
		byteSize = 16
	)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: byteSize})
	builder.AppendNulls(count)
	builder.AppendNulls(count)

	result := builder.NewFixedSizeBinaryArray()
	require.Equal(t, count*2, result.Len())
	require.Equal(t, count*2, result.NullN())
	require.Equal(t, count*2*byteSize, result.Data().Buffers()[1].Cap())
	result.Release()
	builder.Release()
}
