// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRleBatchIndicesMatchesScalar(t *testing.T) {
	for _, width := range []int{0, 1, 4, 8, 16, 17, 31} {
		maxValue := int32((uint32(1) << width) - 1)
		patterns := map[string][]int32{
			"empty":            nil,
			"single":           {maxValue},
			"constant":         make([]int32, 1024),
			"alternating":      make([]int32, 1024),
			"random":           make([]int32, 1024),
			"literal boundary": make([]int32, 63*8+17),
		}
		rng := rand.New(rand.NewPCG(0, 0))
		for i := range patterns["constant"] {
			patterns["constant"][i] = maxValue
			patterns["alternating"][i] = int32(i%2) & maxValue
			patterns["random"][i] = int32(rng.Uint32() & uint32(maxValue))
		}
		for i := range patterns["literal boundary"] {
			patterns["literal boundary"][i] = maxValue
			if i < 63*8-3 {
				patterns["literal boundary"][i] = int32(i%2) & maxValue
			}
		}
		for _, runLength := range []int{8, 9, 16, 32} {
			values := make([]int32, 1024)
			for i := range values {
				values[i] = int32((i/runLength)%2) & maxValue
			}
			patterns[fmt.Sprintf("consecutive runs=%d", runLength)] = values
		}
		for offset := range 8 {
			for _, runLength := range []int{7, 8, 9, 32} {
				values := make([]int32, offset+runLength+9)
				for i := range values {
					values[i] = int32(i%2) & maxValue
				}
				for i := offset; i < offset+runLength; i++ {
					values[i] = maxValue
				}
				patterns[fmt.Sprintf("offset=%d/run=%d", offset, runLength)] = values
			}
		}
		for name, values := range patterns {
			t.Run(fmt.Sprintf("width=%d/%s", width, name), func(t *testing.T) {
				outputSize := MaxRLEBufferSize(width, len(values)) + MinRLEBufferSize(width)
				scalarOutput := make([]byte, outputSize)
				scalar := NewRleEncoder(NewWriterAtBuffer(scalarOutput), width)
				for _, value := range values {
					require.NoError(t, scalar.Put(uint64(value)))
				}
				scalarSize := scalar.Flush()
				for _, mode := range []string{"whole", "chunked", "interleaved"} {
					t.Run(mode, func(t *testing.T) {
						output := make([]byte, outputSize)
						batch := NewRleEncoder(NewWriterAtBuffer(output), width)
						for range 2 {
							batch.Clear()
							n, err := batch.PutBatchIndices(nil)
							require.NoError(t, err)
							require.Zero(t, n)
							chunkSizes := []int{1, 7, 8, 9, 31}
							for offset, chunk := 0, 0; offset < len(values); chunk++ {
								end := len(values)
								if mode != "whole" {
									end = min(offset+chunkSizes[chunk%len(chunkSizes)], end)
								}
								if mode == "interleaved" && chunk%3 == 0 {
									for _, value := range values[offset:end] {
										require.NoError(t, batch.Put(uint64(value)))
									}
								} else {
									n, err := batch.PutBatchIndices(values[offset:end])
									require.NoError(t, err)
									require.Equal(t, end-offset, n)
								}
								offset = end
							}
							size := batch.Flush()
							require.Equal(t, scalarOutput[:scalarSize], output[:size])
							decoded := make([]uint64, len(values))
							decoder := NewRleDecoder(bytes.NewReader(output[:size]), width)
							n, err = decoder.GetBatch(decoded)
							require.NoError(t, err)
							require.Equal(t, len(values), n)
							for i, value := range values {
								require.Equal(t, uint64(value), decoded[i])
							}
						}
					})
				}
			})
		}
	}
}

func TestRleBatchIndicesSplitsMaximumRepeatedRun(t *testing.T) {
	for _, initialCount := range []int32{math.MaxInt32 - 4, math.MaxInt32} {
		t.Run(fmt.Sprint(initialCount), func(t *testing.T) {
			output := make([]byte, 32)
			enc := NewRleEncoder(NewWriterAtBuffer(output), 1)
			enc.curVal = 1
			enc.repCount = initialCount
			n, err := enc.PutBatchIndices([]int32{1, 1, 1, 1, 1, 1})
			require.NoError(t, err)
			require.Equal(t, 6, n)
			want := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1, byte((int64(initialCount) + 6 - math.MaxInt32) * 2), 1}
			require.Equal(t, want, output[:enc.Flush()])
		})
	}
}

func TestRleBatchIndicesMaximumRunAtBatchBoundary(t *testing.T) {
	output := make([]byte, 16)
	enc := NewRleEncoder(NewWriterAtBuffer(output), 1)
	enc.curVal = 1
	enc.repCount = math.MaxInt32 - 4
	n, err := enc.PutBatchIndices([]int32{1, 1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, 4, n)
	n, err = enc.PutBatchIndices([]int32{0})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1, 2, 0}, output[:enc.Flush()])
}

func TestRleBatchIndicesWriteErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		repeated int32
		values   []int32
		encoded  int
	}{
		{"literal header", 0, []int32{0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 8},
		{"repeated run", 8, []int32{0}, 0},
		{"maximum repeated run", math.MaxInt32 - 1, []int32{1, 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := NewRleEncoder(NewWriterAtBuffer(nil), 1)
			enc.curVal = 1
			enc.repCount = tc.repeated
			n, err := enc.PutBatchIndices(tc.values)
			require.Error(t, err)
			require.Equal(t, tc.encoded, n)
		})
	}
}

func TestRleBatchIndicesLiteralWriteErrorsMatchScalar(t *testing.T) {
	for _, width := range []int{1, 8, 17, 31} {
		values := make([]int32, 520)
		mask := int32((uint32(1) << width) - 1)
		for i := range values {
			values[i] = int32(i*1234567) & mask
		}
		for _, capacity := range []int{0, 1, 2, 8, 16, 63, 64, 128, 503} {
			t.Run(fmt.Sprintf("width=%d/capacity=%d", width, capacity), func(t *testing.T) {
				scalarOutput, batchOutput := make([]byte, capacity), make([]byte, capacity)
				scalar := NewRleEncoder(NewWriterAtBuffer(scalarOutput), width)
				scalarCount := 0
				var scalarErr error
				for _, value := range values {
					if scalarErr = scalar.Put(uint64(value)); scalarErr != nil {
						break
					}
					scalarCount++
				}
				batch := NewRleEncoder(NewWriterAtBuffer(batchOutput), width)
				n, err := batch.PutBatchIndices(values)
				require.Equal(t, scalarErr, err)
				require.Equal(t, scalarCount, n)
				require.Equal(t, scalarOutput, batchOutput)
				require.Equal(t, scalar.buffer, batch.buffer)
				require.Equal(t, scalar.curVal, batch.curVal)
				require.Equal(t, scalar.repCount, batch.repCount)
				require.Equal(t, scalar.litCount, batch.litCount)
				require.Equal(t, scalar.literalIndicatorOffset, batch.literalIndicatorOffset)
			})
		}
	}
}
