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

package encoding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpacedExpand(t *testing.T) {
	tests := []struct {
		name   string
		valid  []bool
		values []int32
		offset int64
	}{
		{name: "empty"},
		{
			name:   "all valid",
			valid:  []bool{true, true, true, true},
			values: []int32{10, 11, 12, 13},
		},
		{
			name:   "all null",
			valid:  []bool{false, false, false, false},
			values: nil,
		},
		{
			name:   "trailing null",
			valid:  []bool{true, true, true, false},
			values: []int32{10, 11, 12},
		},
		{
			name:   "leading null",
			valid:  []bool{false, true, true, true},
			values: []int32{10, 11, 12},
		},
		{
			name:   "middle null",
			valid:  []bool{true, false, true, true},
			values: []int32{10, 11, 12},
		},
		{
			name:   "clustered nulls",
			valid:  []bool{true, true, false, false, true, true},
			values: []int32{10, 11, 12, 13},
		},
		{
			name:   "alternating",
			valid:  []bool{true, false, true, false, true, false},
			values: []int32{10, 11, 12},
		},
		{
			name:   "offset",
			valid:  []bool{false, true, true, false, true},
			values: []int32{10, 11, 12},
			offset: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, countValid(tt.valid), len(tt.values))

			validBits := make([]byte, bitutil.BytesForBits(tt.offset+int64(len(tt.valid))))
			for i, valid := range tt.valid {
				if valid {
					bitutil.SetBit(validBits, int(tt.offset)+i)
				}
			}

			buffer := make([]int32, len(tt.valid))
			copy(buffer, tt.values)
			n := spacedExpand(buffer, len(tt.valid)-len(tt.values), validBits, tt.offset)
			require.Equal(t, len(tt.valid), n)

			valueIndex := 0
			for i, valid := range tt.valid {
				if valid {
					assert.Equal(t, tt.values[valueIndex], buffer[i], "value at position %d", i)
					valueIndex++
				}
			}
		})
	}
}

func countValid(values []bool) int {
	var count int
	for _, valid := range values {
		if valid {
			count++
		}
	}
	return count
}

type spacedExpandBenchmarkCase struct {
	name      string
	validBits []byte
	nullCount int
}

func newSpacedExpandBenchmarkCase(name string, valid []bool) spacedExpandBenchmarkCase {
	validBits := make([]byte, bitutil.BytesForBits(int64(len(valid))))
	for i, isValid := range valid {
		if isValid {
			bitutil.SetBit(validBits, i)
		}
	}

	return spacedExpandBenchmarkCase{
		name:      name,
		validBits: validBits,
		nullCount: len(valid) - countValid(valid),
	}
}

func benchmarkValidityCases(numValues int) []spacedExpandBenchmarkCase {
	valid := make([]bool, numValues)
	for i := range valid {
		valid[i] = i != numValues-1
	}

	cases := []spacedExpandBenchmarkCase{
		newSpacedExpandBenchmarkCase("TrailingNull", valid),
	}

	for i := range valid {
		valid[i] = i != numValues*9/10
	}
	cases = append(cases, newSpacedExpandBenchmarkCase("LateNull", valid))

	clusterLen := numValues / 100
	clusterStart := numValues - clusterLen - 1
	for i := range valid {
		valid[i] = i < clusterStart || i >= clusterStart+clusterLen
	}
	cases = append(cases, newSpacedExpandBenchmarkCase("ClusteredNulls", valid))

	state := uint32(1)
	for i := range valid {
		state = state*1664525 + 1013904223
		valid[i] = state%10 != 0
	}
	cases = append(cases, newSpacedExpandBenchmarkCase("Random10PctNulls", valid))

	for i := range valid {
		valid[i] = i%2 == 0
	}
	cases = append(cases, newSpacedExpandBenchmarkCase("Alternating", valid))

	return cases
}

func benchmarkSpacedExpand[T parquet.ColumnTypes](b *testing.B, input []T, validBits []byte, nullCount int) {
	b.Helper()
	const batchSize = 32
	buffers := make([][]T, batchSize)
	for i := range buffers {
		buffers[i] = make([]T, len(input))
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()

	for i := 0; i < b.N; {
		batch := min(batchSize, b.N-i)
		b.StopTimer()
		for j := 0; j < batch; j++ {
			copy(buffers[j], input)
		}
		b.StartTimer()
		for j := 0; j < batch; j++ {
			if n := spacedExpand(buffers[j], nullCount, validBits, 0); n != len(input) {
				b.Fatalf("spacedExpand returned %d, want %d", n, len(input))
			}
		}
		i += batch
	}
}

func BenchmarkSpacedExpand(b *testing.B) {
	const numValues = 1 << 16

	for _, tc := range benchmarkValidityCases(numValues) {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("Int32", func(b *testing.B) {
				input := make([]int32, numValues)
				for i := range input[:numValues-tc.nullCount] {
					input[i] = int32(i)
				}
				benchmarkSpacedExpand(b, input, tc.validBits, tc.nullCount)
			})

			b.Run("ByteArray", func(b *testing.B) {
				input := make([]parquet.ByteArray, numValues)
				value := parquet.ByteArray([]byte("value"))
				for i := range input[:numValues-tc.nullCount] {
					input[i] = value
				}
				benchmarkSpacedExpand(b, input, tc.validBits, tc.nullCount)
			})
		})
	}
}
