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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// makeConcatDictionaryChunks reuses the same dictionary Data for all chunks
// selecting the same values entry, including when the indices differ.
func makeConcatDictionaryChunks(mem memory.Allocator, valueSets [][]string, sequence []int, rows int, nullable, sliced bool) []arrow.Array {
	dictionaries := make([]arrow.Array, len(valueSets))
	for i, values := range valueSets {
		builder := array.NewStringBuilder(mem)
		builder.AppendValues(values, nil)
		dictionaries[i] = builder.NewArray()
		builder.Release()
	}
	defer func() {
		for _, dict := range dictionaries {
			dict.Release()
		}
	}()

	dtype := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	out := make([]arrow.Array, 0, len(sequence))
	for chunkIndex, dictIndex := range sequence {
		builder := array.NewInt32Builder(mem)
		for row := 0; row < rows; row++ {
			if nullable && (row+chunkIndex)%11 == 0 {
				builder.AppendNull()
			} else {
				builder.Append(int32((row + chunkIndex) % len(valueSets[dictIndex])))
			}
		}
		indices := builder.NewArray()
		builder.Release()
		dictArray := array.NewDictionaryArray(dtype, indices, dictionaries[dictIndex])
		indices.Release()

		if sliced && rows > 1 {
			start := int64(chunkIndex % 2)
			slice := array.NewSlice(dictArray, start, int64(rows))
			dictArray.Release()
			dictArray = slice.(*array.Dictionary)
		}
		out = append(out, dictArray)
	}
	return out
}

func releaseConcatDictionaryChunks(chunks []arrow.Array) {
	for _, chunk := range chunks {
		chunk.Release()
	}
}

func TestConcatenateDictionaryReuse(t *testing.T) {
	base := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	reversed := []string{"epsilon", "gamma", "alpha", "zeta", "beta"}
	tests := []struct {
		name      string
		valueSets [][]string
		sequence  []int
		rows      int
		nullable  bool
		sliced    bool
	}{
		{"shared", [][]string{base}, []int{0, 0, 0, 0}, 29, false, false},
		{"shared_sliced_nulls", [][]string{base}, []int{0, 0, 0, 0, 0}, 29, true, true},
		{"equal_distinct_data", [][]string{base, append([]string(nil), base...)}, []int{0, 1, 0, 1}, 29, true, false},
		{"mixed_repeated", [][]string{base, reversed}, []int{0, 1, 0, 1, 1, 0, 1, 0}, 37, true, true},
		{"mixed_empty_chunks", [][]string{base, reversed}, []int{0, 1, 0, 1}, 0, false, false},
		{"single_chunk", [][]string{base}, []int{0}, 31, true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			inputs := makeConcatDictionaryChunks(mem, tc.valueSets, tc.sequence, tc.rows, tc.nullable, tc.sliced)
			defer releaseConcatDictionaryChunks(inputs)
			expected := make([]string, 0)
			for _, in := range inputs {
				for i := 0; i < in.Len(); i++ {
					expected = append(expected, in.ValueStr(i))
				}
			}

			combined, err := array.Concatenate(inputs, mem)
			require.NoError(t, err)
			defer combined.Release()
			require.NoError(t, array.ValidateFull(combined))
			require.Equal(t, len(expected), combined.Len())
			for i, want := range expected {
				require.Equal(t, want, combined.ValueStr(i), "row %d", i)
			}
		})
	}
}

// BenchmarkConcatenateDictionaryReuse measures the complete public concatenate
// call with shared dictionaries and repeated dictionary versions. Run with:
// go test ./arrow/array -run '^$' -bench '^BenchmarkConcatenateDictionaryReuse$' -benchmem
func BenchmarkConcatenateDictionaryReuse(b *testing.B) {
	base := make([]string, 8192)
	for i := range base {
		base[i] = fmt.Sprintf("dictionary-value-%06d", i)
	}
	changed := append([]string(nil), base...)
	for i := 0; i < len(changed); i += 16 {
		changed[i] = fmt.Sprintf("new-value-%06d", i)
	}
	second := append([]string(nil), base...)
	for i := 1; i < len(second); i += 16 {
		second[i] = fmt.Sprintf("another-new-value-%06d", i)
	}

	cases := []struct {
		name      string
		valueSets [][]string
		sequence  []int
	}{
		{"shared_dictionary", [][]string{base}, []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"repeated_mixed_dictionaries", [][]string{base, changed, second}, []int{0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			mem := memory.NewGoAllocator()
			inputs := makeConcatDictionaryChunks(mem, tc.valueSets, tc.sequence, 256, true, false)
			defer releaseConcatDictionaryChunks(inputs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				output, err := array.Concatenate(inputs, mem)
				if err != nil {
					b.Fatal(err)
				}
				output.Release()
			}
		})
	}
}
