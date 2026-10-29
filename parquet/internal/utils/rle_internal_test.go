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
	"testing"
)

func TestRleEncoderMaximumRepeatedRun(t *testing.T) {
	buf := make([]byte, 16)
	enc := NewRleEncoder(NewWriterAtBuffer(buf), 1)
	enc.curVal = 1
	enc.repCount = math.MaxInt32

	if n := enc.Flush(); n != 6 {
		t.Fatalf("encoded %d bytes, want 6", n)
	}
	want := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1}
	for i, value := range want {
		if buf[i] != value {
			t.Fatalf("byte %d = %#x, want %#x", i, buf[i], value)
		}
	}
}

func TestRleEncoderBatchSplitsMaximumRepeatedRun(t *testing.T) {
	buf := make([]byte, 16)
	enc := NewRleEncoder(NewWriterAtBuffer(buf), 1)
	enc.curVal = 1
	enc.repCount = math.MaxInt32 - 4

	n, err := enc.PutBatchLevels([]int16{1, 1, 1, 1, 1, 1})
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("encoded %d values, want 6", n)
	}
	if n := enc.Flush(); n != 8 {
		t.Fatalf("encoded %d bytes, want 8", n)
	}

	want := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1, 4, 1}
	for i, value := range want {
		if buf[i] != value {
			t.Fatalf("byte %d = %#x, want %#x", i, buf[i], value)
		}
	}
}

func TestRleEncoderBatchReportsValuesBeforeFlushError(t *testing.T) {
	enc := NewRleEncoder(NewWriterAtBuffer(nil), 1)
	values := []int16{0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	n, err := enc.PutBatchLevels(values)
	if err == nil {
		t.Fatal("expected a write error")
	}
	if n != 8 {
		t.Fatalf("encoded %d values, want 8", n)
	}
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

// Compare partial output and errors against encoding each level separately.
func TestRleBatchLevelsPackedBatchWriteErrors(t *testing.T) {
	// When a repeated run follows literals, the batch API flushes the literal
	// header before consuming the next eight values. Put has already accepted
	// seven of those values when it encounters the same error. Keep explicit
	// batch counts for these mostly_defined cases (width -> capacity -> count).
	flushErrorCounts := map[int]map[int]int{
		1: {0: 8, 8: 48, 16: 88, 64: 328},
		2: {0: 8},
		4: {0: 8, 63: 188},
	}
	for _, width := range []int{1, 2, 4, 8} {
		for _, pattern := range []string{"random", "mostly_defined", "repeated", "alternating"} {
			values := makePackedLevelValues(width, 520, pattern)
			for _, capacity := range []int{0, 1, 2, 8, 16, 63, 64, 128, 503} {
				t.Run(fmt.Sprintf("width=%d/%s/capacity=%d", width, pattern, capacity), func(t *testing.T) {
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
					current := NewRleEncoder(NewWriterAtBuffer(got), width)
					n, err := current.PutBatchLevels(values)
					wantCount := scalarCount
					if count, ok := flushErrorCounts[width][capacity]; pattern == "mostly_defined" && ok {
						wantCount = count
						if err == nil {
							t.Fatal("expected a literal header write error")
						}
					}
					if n != wantCount || (err != nil) != (scalarErr != nil) || !bytes.Equal(expected, got) {
						t.Fatalf("batch (%d, %v), want count %d, scalar (%d, %v), bytes equal: %t", n, err, wantCount, scalarCount, scalarErr, bytes.Equal(expected, got))
					}
				})
			}
		}
	}
}

func TestRleBatchLevelsPackedInvalidValues(t *testing.T) {
	// Invalid levels preserve the per-value writer's behavior, including
	// negative input cast directly to uint64.
	for _, width := range []int{1, 2, 4, 8} {
		values := []int16{0, 1, 0, 1, 0, 1, 0, -1, 0, 1, 0, 1, 0, 1, 0, 1024}
		size := MaxRLEBufferSize(width, len(values)) + MinRLEBufferSize(width) + 32
		expected, got := make([]byte, size), make([]byte, size)
		scalar := NewRleEncoder(NewWriterAtBuffer(expected), width)
		scalarCount := 0
		var scalarErr error
		for _, value := range values {
			if scalarErr = scalar.Put(uint64(value)); scalarErr != nil {
				break
			}
			scalarCount++
		}
		current := NewRleEncoder(NewWriterAtBuffer(got), width)
		n, err := current.PutBatchLevels(values)
		if scalarCount != n || (scalarErr != nil) != (err != nil) ||
			!bytes.Equal(expected[:scalar.Flush()], got[:current.Flush()]) {
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
					output := make([]byte, MaxRLEBufferSize(width, len(values))+MinRLEBufferSize(width)+32)
					enc := NewRleEncoder(NewWriterAtBuffer(output), width)
					b.SetBytes(int64(len(values) * 2))
					b.ReportAllocs()
					for b.Loop() {
						enc.Clear()
						n, err := enc.PutBatchLevels(values)
						if err != nil || n != len(values) {
							b.Fatalf("encoded %d levels: %v", n, err)
						}
						if got := enc.Flush(); got > len(output) {
							b.Fatalf("encoded %d bytes into %d byte output", got, len(output))
						}
					}
				})
			}
		}
	}
}
