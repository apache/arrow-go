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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package kernels

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeBooleanFilter(t *testing.T, values []bool, valid []bool, mem memory.Allocator) arrow.Array {
	t.Helper()
	if valid != nil {
		require.Len(t, valid, len(values))
	}

	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()
	for i, value := range values {
		if valid != nil && !valid[i] {
			bldr.AppendNull()
		} else {
			bldr.Append(value)
		}
	}
	return bldr.NewArray()
}

func makeSlicedBooleanFilter(t *testing.T, values []bool, valid []bool, prefix int, mem memory.Allocator) arrow.Array {
	t.Helper()
	allValues := make([]bool, len(values)+2*prefix)
	for i := range allValues {
		allValues[i] = true
	}
	copy(allValues[prefix:], values)

	var allValid []bool
	if valid != nil {
		allValid = make([]bool, len(allValues))
		for i := range allValid {
			allValid[i] = true
		}
		copy(allValid[prefix:], valid)
	}

	base := makeBooleanFilter(t, allValues, allValid, mem)
	sliced := array.NewSlice(base, int64(prefix), int64(prefix+len(values)))
	base.Release()
	return sliced
}

func makeIndexValues[T arrow.IntType | arrow.UintType](selected []int) []T {
	values := make([]T, len(selected))
	for i, value := range selected {
		values[i] = T(value)
	}
	return values
}

func assertTakeIndices[T arrow.IntType | arrow.UintType](t *testing.T, data arrow.ArrayData, wantValues []T, wantValid []bool) {
	t.Helper()
	require.Equal(t, arrow.GetDataType[T]().ID(), data.DataType().ID())
	require.Equal(t, len(wantValues), data.Len())
	require.NotNil(t, data.Buffers()[1])

	values := arrow.GetData[T](data.Buffers()[1].Bytes())
	values = values[data.Offset() : data.Offset()+data.Len()]
	for i, want := range wantValues {
		if wantValid == nil || wantValid[i] {
			assert.Equal(t, want, values[i], "value at index %d", i)
		}
	}

	if wantValid == nil {
		require.Nil(t, data.Buffers()[0])
		require.Zero(t, data.NullN())
		return
	}

	require.NotNil(t, data.Buffers()[0])
	nulls := 0
	for i, want := range wantValid {
		if !want {
			nulls++
		}
		got := bitutil.BitIsSet(data.Buffers()[0].Bytes(), data.Offset()+i)
		assert.Equal(t, want, got, "validity at index %d", i)
	}
	require.Equal(t, nulls, data.NullN())
}

func getTakeIndicesForTest[T arrow.IntType | arrow.UintType](mem memory.Allocator, filter arrow.Array, nullSelect NullSelectionBehavior) arrow.ArrayData {
	var span exec.ArraySpan
	span.SetMembers(filter.Data())
	return getTakeIndices[T](mem, &span, nullSelect)
}

func TestGetTakeIndicesBatchedRanges(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const (
		length = 192
		prefix = 3
	)

	values := make([]bool, length)
	for i := range values {
		values[i] = i < 64 || i >= 128
	}

	selected := make([]int, 0, 128)
	for i, value := range values {
		if value {
			selected = append(selected, i)
		}
	}

	nullableValid := make([]bool, length)
	for i := range nullableValid {
		nullableValid[i] = true
	}
	nullableValid[140] = false

	dropSelected := make([]int, 0, len(selected)-1)
	for _, value := range selected {
		if value != 140 {
			dropSelected = append(dropSelected, value)
		}
	}
	emitValid := make([]bool, len(selected))
	for i, value := range selected {
		emitValid[i] = value != 140
	}

	tests := []struct {
		name       string
		valid      []bool
		nullSelect NullSelectionBehavior
		selected   []int
		validOut   []bool
	}{
		{
			name:       "non_nullable_runs",
			nullSelect: DropNulls,
			selected:   selected,
		},
		{
			name:       "nullable_drop_nulls",
			valid:      nullableValid,
			nullSelect: DropNulls,
			selected:   dropSelected,
		},
		{
			name:       "nullable_emit_nulls",
			valid:      nullableValid,
			nullSelect: EmitNulls,
			selected:   selected,
			validOut:   emitValid,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := makeSlicedBooleanFilter(t, values, tc.valid, prefix, mem)
			defer filter.Release()

			t.Run("uint16", func(t *testing.T) {
				result := getTakeIndicesForTest[uint16](mem, filter, tc.nullSelect)
				defer result.Release()
				assertTakeIndices(t, result, makeIndexValues[uint16](tc.selected), tc.validOut)
			})
			t.Run("uint32", func(t *testing.T) {
				result := getTakeIndicesForTest[uint32](mem, filter, tc.nullSelect)
				defer result.Release()
				assertTakeIndices(t, result, makeIndexValues[uint32](tc.selected), tc.validOut)
			})
		})
	}
}

func TestGetTakeIndicesUint32Coverage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const length = 65536
	values := make([]bool, length)
	want := make([]uint32, 0, length/2)
	for i := range values {
		mask := byte(i / 8)
		values[i] = mask&(1<<uint(i%8)) != 0
		if values[i] {
			want = append(want, uint32(i))
		}
	}

	tests := []struct {
		name   string
		filter arrow.Array
		want   []uint32
	}{
		{
			name:   "all_masks",
			filter: makeBooleanFilter(t, values, nil, mem),
			want:   want,
		},
		{
			name:   "aligned_offset",
			filter: makeSlicedBooleanFilter(t, values, nil, 8, mem),
			want:   want,
		},
		{
			name:   "unaligned_offset",
			filter: makeSlicedBooleanFilter(t, values, nil, 1, mem),
			want:   want,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.filter.Release()
			var span exec.ArraySpan
			span.SetMembers(tc.filter.Data())

			result, err := GetTakeIndices(mem, &span, DropNulls)
			require.NoError(t, err)
			defer result.Release()
			require.Equal(t, arrow.PrimitiveTypes.Uint32.ID(), result.DataType().ID())
			got := arrow.GetData[uint32](result.Buffers()[1].Bytes())
			require.Len(t, got, len(tc.want))
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("value at index %d: got %d, want %d", i, got[i], tc.want[i])
				}
			}
		})
	}

	t.Run("nullable_filter_uses_scalar_path", func(t *testing.T) {
		valid := make([]bool, length)
		for i := range valid {
			valid[i] = true
		}
		valid[8] = false
		filter := makeBooleanFilter(t, values, valid, mem)
		defer filter.Release()
		var span exec.ArraySpan
		span.SetMembers(filter.Data())

		wantDrop := make([]uint32, 0, len(want)-1)
		for _, value := range want {
			if value != 8 {
				wantDrop = append(wantDrop, value)
			}
		}
		result, err := GetTakeIndices(mem, &span, DropNulls)
		require.NoError(t, err)
		assertTakeIndices(t, result, wantDrop, nil)
		result.Release()

		result, err = GetTakeIndices(mem, &span, EmitNulls)
		require.NoError(t, err)
		defer result.Release()
		wantValid := make([]bool, len(want))
		for i := range wantValid {
			wantValid[i] = true
		}
		for i, value := range want {
			if value == 8 {
				wantValid[i] = false
			}
		}
		assertTakeIndices(t, result, want, wantValid)
	})

	t.Run("tail_ignores_padding_bits", func(t *testing.T) {
		const tailLength = int64(length + 3)
		data := make([]byte, int(bitutil.BytesForBits(tailLength)))
		wantTail := make([]uint32, 0, len(want))
		for i := int64(0); i < tailLength; i++ {
			mask := byte(i / 8)
			if mask&(1<<uint(i%8)) != 0 {
				bitutil.SetBit(data, int(i))
				wantTail = append(wantTail, uint32(i))
			}
		}
		data[len(data)-1] |= 0xe0

		filterData := array.NewData(arrow.FixedWidthTypes.Boolean, int(tailLength), []*memory.Buffer{
			nil,
			memory.NewBufferBytes(data),
		}, nil, 0, 0)
		defer filterData.Release()
		var span exec.ArraySpan
		span.SetMembers(filterData)

		result, err := GetTakeIndices(mem, &span, DropNulls)
		require.NoError(t, err)
		defer result.Release()
		got := arrow.GetData[uint32](result.Buffers()[1].Bytes())
		require.Len(t, got, len(wantTail))
		for i := range got {
			if got[i] != wantTail[i] {
				t.Fatalf("value at index %d: got %d, want %d", i, got[i], wantTail[i])
			}
		}
	})

	for _, tc := range []struct {
		name   string
		length int
		wantID arrow.Type
	}{
		{name: "uint16_boundary", length: 65534, wantID: arrow.UINT16},
		{name: "uint32_boundary", length: 65535, wantID: arrow.UINT32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := make([]bool, tc.length)
			for i := range values {
				values[i] = true
			}
			filter := makeBooleanFilter(t, values, nil, mem)
			defer filter.Release()
			var span exec.ArraySpan
			span.SetMembers(filter.Data())

			result, err := GetTakeIndices(mem, &span, DropNulls)
			require.NoError(t, err)
			defer result.Release()
			require.Equal(t, tc.wantID, result.DataType().ID())
			require.Equal(t, tc.length, result.Len())
		})
	}
}
