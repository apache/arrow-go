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

package utils

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

// previousPutBatchLevels retains the implementation before the packed-literal
// fast path so the benchmarks measure the change on the same machine/build.
func previousPutBatchLevels(r *RleEncoder, values []int16) (int, error) {
	encoded := 0
	for encoded < len(values) {
		value := values[encoded]
		if r.repCount >= 8 && r.curVal == uint64(value) {
			runEnd := encoded + 1
			for runEnd < len(values) && values[runEnd] == value {
				runEnd++
			}

			runLength := min(runEnd-encoded, int(math.MaxInt32-r.repCount))
			r.repCount += int32(runLength)
			encoded += runLength
			if r.repCount == math.MaxInt32 && encoded < len(values) && values[encoded] == value {
				if !r.flushRepeated() {
					return encoded, fmt.Errorf("failed to flush repeated value")
				}
			}
			continue
		}
		if r.repCount == 0 && len(r.buffer) == 0 && len(values)-encoded >= 8 && values[encoded+7] == value {
			runEnd := encoded + 1
			for runEnd < len(values) && values[runEnd] == value {
				runEnd++
			}
			if runEnd-encoded >= 8 {
				r.curVal = uint64(value)
				if r.litCount != 0 {
					r.repCount = 8
					if err := r.flushLiteral(true); err != nil {
						return encoded, err
					}
					encoded += 8
				}
				runLength := min(runEnd-encoded, int(math.MaxInt32-r.repCount))
				r.repCount += int32(runLength)
				encoded += runLength
				continue
			}
		}

		batchEnd := min(len(values), encoded+8-len(r.buffer))
		for encoded < batchEnd {
			if err := r.Put(uint64(values[encoded])); err != nil {
				return encoded, err
			}
			encoded++
		}
	}
	return encoded, nil
}

func makePackedLevelValues(width, n int, pattern string) []int16 {
	levels := make([]int16, n)
	mask := uint32((1 << min(width, 15)) - 1)
	seed := uint32(0x12345678)
	for i := range levels {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		switch pattern {
		case "random":
			levels[i] = int16(seed & mask)
		case "alternating":
			levels[i] = int16(uint32(i%2) & mask)
		case "mostly_defined":
			if i%20 != 0 {
				levels[i] = int16(mask)
			}
		case "repeated":
			if (i/13)%2 != 0 {
				levels[i] = int16(mask)
			}
		case "boundary":
			levels[i] = int16(uint32(i*17+i/3) & mask)
		}
	}
	return levels
}

func TestRleBatchLevelsPackedMatchesScalar(t *testing.T) {
	for _, width := range []int{0, 1, 2, 3, 4, 8, 9, 15, 16} {
		for _, pattern := range []string{"random", "alternating", "mostly_defined", "repeated", "boundary"} {
			values := makePackedLevelValues(width, 63*8+17, pattern)
			t.Run(fmt.Sprintf("width=%d/%s", width, pattern), func(t *testing.T) {
				size := MaxRLEBufferSize(width, len(values)) + MinRLEBufferSize(width) + 32
				expected := make([]byte, size)
				scalar := NewRleEncoder(NewWriterAtBuffer(expected), width)
				for _, value := range values {
					if err := scalar.Put(uint64(value)); err != nil {
						t.Fatal(err)
					}
				}
				expected = expected[:scalar.Flush()]

				for _, mode := range []string{"whole", "chunked", "interleaved"} {
					t.Run(mode, func(t *testing.T) {
						got := make([]byte, size)
						enc := NewRleEncoder(NewWriterAtBuffer(got), width)
						chunkSizes := []int{1, 7, 8, 9, 31, 65}
						for offset, chunk := 0, 0; offset < len(values); chunk++ {
							end := len(values)
							if mode != "whole" {
								end = min(end, offset+chunkSizes[chunk%len(chunkSizes)])
							}
							if mode == "interleaved" && chunk%3 == 0 {
								for _, value := range values[offset:end] {
									if err := enc.Put(uint64(value)); err != nil {
										t.Fatal(err)
									}
								}
							} else {
								n, err := enc.PutBatchLevels(values[offset:end])
								if err != nil {
									t.Fatal(err)
								}
								if n != end-offset {
									t.Fatalf("encoded %d, want %d", n, end-offset)
								}
							}
							offset = end
						}
						got = got[:enc.Flush()]
						if !bytes.Equal(got, expected) {
							t.Fatalf("encoded bytes differ from scalar at width %d, %s (%d vs %d bytes)", width, pattern, len(got), len(expected))
						}
						decoded := make([]uint64, len(values))
						r := NewRleDecoder(bytes.NewReader(got), width)
						n, err := r.GetBatch(decoded)
						if err != nil || n != len(values) {
							t.Fatalf("decoded %d values, err %v", n, err)
						}
						for i, value := range values {
							if decoded[i] != uint64(value) {
								t.Fatalf("decoded[%d] = %d, want %d", i, decoded[i], value)
							}
						}
					})
				}
			})
		}
	}
}

func TestRleBatchLevelsPackedWriteErrors(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		values := makePackedLevelValues(width, 520, "alternating")
		for _, capacity := range []int{0, 1, 2, 8, 16, 63, 64, 128, 503} {
			t.Run(fmt.Sprintf("width=%d/capacity=%d", width, capacity), func(t *testing.T) {
				expected, got := make([]byte, capacity), make([]byte, capacity)
				scalar := NewRleEncoder(NewWriterAtBuffer(expected), width)
				scalarCount := 0
				var scalarErr error
				for _, value := range values {
					if scalarErr = scalar.Put(uint64(value)); scalarErr != nil {
						break
					}
					scalarCount++
				}
				enc := NewRleEncoder(NewWriterAtBuffer(got), width)
				n, err := enc.PutBatchLevels(values)
				if (err != nil) != (scalarErr != nil) || n != scalarCount {
					t.Fatalf("count/error: got (%d, %v), want (%d, %v)", n, err, scalarCount, scalarErr)
				}
				if !bytes.Equal(got, expected) {
					t.Fatal("partial encoded output differs")
				}
			})
		}
	}
}

// Keep error offsets and partial bytes consistent with the previous batch path.
// The batch path intentionally reports some errors at an earlier count than
// calling Put for each value, so compare the same APIs here.
func TestRleBatchLevelsPackedBatchWriteErrors(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		for _, pattern := range []string{"random", "mostly_defined", "repeated", "alternating"} {
			values := makePackedLevelValues(width, 520, pattern)
			for _, capacity := range []int{0, 1, 2, 8, 16, 63, 64, 128, 503} {
				t.Run(fmt.Sprintf("width=%d/%s/capacity=%d", width, pattern, capacity), func(t *testing.T) {
					beforeOutput, afterOutput := make([]byte, capacity), make([]byte, capacity)
					previous := NewRleEncoder(NewWriterAtBuffer(beforeOutput), width)
					current := NewRleEncoder(NewWriterAtBuffer(afterOutput), width)
					oldCount, oldErr := previousPutBatchLevels(previous, values)
					newCount, newErr := current.PutBatchLevels(values)
					if oldCount != newCount || (oldErr != nil) != (newErr != nil) || !bytes.Equal(beforeOutput, afterOutput) {
						t.Fatalf("previous (%d, %v) vs current (%d, %v), bytes equal: %t", oldCount, oldErr, newCount, newErr, bytes.Equal(beforeOutput, afterOutput))
					}
				})
			}
		}
	}
}

func TestRleBatchLevelsPackedInvalidValues(t *testing.T) {
	// Invalid levels preserve the existing batch writer's behavior, including
	// negative input, which used to be cast directly to uint64.
	for _, width := range []int{1, 2, 4, 8} {
		values := []int16{0, 1, 0, 1, 0, 1, 0, -1, 0, 1, 0, 1, 0, 1, 0, 1024}
		size := MaxRLEBufferSize(width, len(values)) + MinRLEBufferSize(width) + 32
		beforeOutput, afterOutput := make([]byte, size), make([]byte, size)
		previous := NewRleEncoder(NewWriterAtBuffer(beforeOutput), width)
		current := NewRleEncoder(NewWriterAtBuffer(afterOutput), width)
		oldCount, oldErr := previousPutBatchLevels(previous, values)
		newCount, newErr := current.PutBatchLevels(values)
		if oldCount != newCount || (oldErr != nil) != (newErr != nil) ||
			!bytes.Equal(beforeOutput[:previous.Flush()], afterOutput[:current.Flush()]) {
			t.Fatalf("width %d: invalid-level output changed", width)
		}
	}
}

func BenchmarkRleBatchLevelsPacked(b *testing.B) {
	for _, width := range []int{1, 2, 4, 8, 16} {
		for _, pattern := range []string{"random", "alternating", "mostly_defined", "repeated"} {
			for _, size := range []int{1024, 65536} {
				values := makePackedLevelValues(width, size, pattern)
				b.Run(fmt.Sprintf("width=%d/%s/n=%d", width, pattern, size), func(b *testing.B) {
					for _, impl := range []struct {
						name string
						run  func(*RleEncoder, []int16) (int, error)
					}{
						{"before", previousPutBatchLevels},
						{"after", (*RleEncoder).PutBatchLevels},
					} {
						b.Run(impl.name, func(b *testing.B) {
							output := make([]byte, MaxRLEBufferSize(width, len(values))+MinRLEBufferSize(width)+32)
							enc := NewRleEncoder(NewWriterAtBuffer(output), width)
							b.SetBytes(int64(len(values) * 2))
							b.ReportAllocs()
							for b.Loop() {
								enc.Clear()
								n, err := impl.run(enc, values)
								if err != nil || n != len(values) {
									b.Fatalf("encoded %d levels: %v", n, err)
								}
								if got := enc.Flush(); got > len(output) {
									b.Fatalf("encoded %d bytes into %d byte output", got, len(output))
								}
							}
						})
					}
				})
			}
		}
	}
}
