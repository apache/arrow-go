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

func TestFixedWidthBuilderAppendEmptyValues(t *testing.T) {
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
		{"month-interval", func(mem memory.Allocator) array.Builder {
			return array.NewMonthIntervalBuilder(mem)
		}},
		{"day-time-interval", func(mem memory.Allocator) array.Builder {
			return array.NewDayTimeIntervalBuilder(mem)
		}},
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

			instance.AppendEmptyValues(0)
			instance.AppendEmptyValues(-1)
			require.Zero(t, instance.Len())
			require.Zero(t, instance.Cap())
			require.Zero(t, instance.NullN())

			instance.AppendEmptyValue()
			instance.AppendEmptyValues(10)
			instance.AppendEmptyValue()
			require.Equal(t, 12, instance.Len())
			require.Zero(t, instance.NullN())

			result := instance.NewArray()
			defer result.Release()
			require.Equal(t, 12, result.Len())
			require.Zero(t, result.NullN())
			for i := 0; i < result.Len(); i++ {
				require.True(t, result.IsValid(i))
			}

			values := result.Data().Buffers()[1]
			require.NotNil(t, values)
			require.Equal(t, make([]byte, values.Len()), values.Bytes())
		})
	}
}

func TestFixedWidthBuilderAppendEmptyValuesClearsReusedStorage(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		values := make([]int32, 16)
		for i := range values {
			values[i] = int32(i + 1)
		}
		builder.AppendValues(values, nil)
		builder.Resize(3)
		builder.AppendEmptyValues(10)

		result := builder.NewInt32Array()
		defer result.Release()
		require.Equal(t, 13, result.Len())
		require.Zero(t, result.NullN())
		for i := 0; i < result.Len(); i++ {
			require.True(t, result.IsValid(i))
			if i < 3 {
				require.Equal(t, int32(i+1), result.Value(i))
			} else {
				require.Zero(t, result.Value(i))
			}
		}
	})

	t.Run("boolean", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		values := make([]bool, 16)
		for i := range values {
			values[i] = true
		}
		builder.AppendValues(values, nil)
		builder.Resize(3)
		builder.AppendEmptyValues(10)

		result := builder.NewBooleanArray()
		defer result.Release()
		require.Equal(t, 13, result.Len())
		require.Zero(t, result.NullN())
		for i := 0; i < result.Len(); i++ {
			require.True(t, result.IsValid(i))
			require.Equal(t, i < 3, result.Value(i))
		}
	})
}
