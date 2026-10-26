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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func deltaLengthBatchValues(n int, pattern string) []parquet.ByteArray {
	values := make([]parquet.ByteArray, n)
	for i := range values {
		var length int
		switch pattern {
		case "empty":
			length = 0
		case "fixed16":
			length = 16
		case "varied":
			length = (i * 17) % 129
		}
		value := make([]byte, length)
		for j := range value {
			value[j] = byte((i + j) % 251)
		}
		values[i] = value
	}
	return values
}

func encodeDeltaLengthBatchValues(values []parquet.ByteArray) []byte {
	enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
		false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer enc.Release()
	enc.Put(values)
	buf, err := enc.FlushValues()
	if err != nil {
		panic(err)
	}
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func TestDeltaLengthByteArrayDecoderMixedBatchOperations(t *testing.T) {
	for _, n := range []int{1, 17, 257, 4096} {
		for _, pattern := range []string{"empty", "fixed16", "varied"} {
			t.Run(fmt.Sprintf("%d/%s", n, pattern), func(t *testing.T) {
				values := deltaLengthBatchValues(n, pattern)
				encoded := encodeDeltaLengthBatchValues(values)
				dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					nil, memory.DefaultAllocator).(*DeltaLengthByteArrayDecoder)

				// Run twice to cover reuse of the decoder's scratch storage.
				for repeat := 0; repeat < 2; repeat++ {
					require.NoError(t, dec.SetData(n, encoded))
					pos := 0
					steps := []struct {
						discard bool
						count   int
					}{
						{false, 1},
						{true, 2},
						{false, 9},
						{true, 13},
						{false, 257},
						{true, 511},
						{false, 2048},
					}
					for _, step := range steps {
						count := min(step.count, n-pos)
						if step.discard {
							skipped, err := dec.Discard(count)
							require.NoError(t, err)
							require.Equal(t, count, skipped)
						} else {
							out := make([]parquet.ByteArray, count)
							decoded, err := dec.Decode(out)
							require.NoError(t, err)
							require.Equal(t, count, decoded)
							for i, value := range out {
								require.True(t, bytes.Equal(values[pos+i], value), "value %d", pos+i)
							}
						}
						pos += count
					}
					remaining := n - pos
					out := make([]parquet.ByteArray, remaining)
					decoded, err := dec.Decode(out)
					require.NoError(t, err)
					require.Equal(t, remaining, decoded)
					for i, value := range out {
						require.True(t, bytes.Equal(values[pos+i], value), "value %d", pos+i)
					}
					require.Equal(t, 0, dec.nvals)
					decoded, err = dec.Decode(make([]parquet.ByteArray, 1))
					require.NoError(t, err)
					require.Zero(t, decoded)
					skipped, err := dec.Discard(1)
					require.NoError(t, err)
					require.Zero(t, skipped)
				}
			})
		}
	}
}

func BenchmarkDeltaLengthByteArrayBatchDecode(b *testing.B) {
	benchmarkDeltaLengthByteArrayBatch(b, false)
}

func BenchmarkDeltaLengthByteArrayBatchDiscard(b *testing.B) {
	benchmarkDeltaLengthByteArrayBatch(b, true)
}

func benchmarkDeltaLengthByteArrayBatch(b *testing.B, discard bool) {
	for _, n := range []int{4096, 65536} {
		for _, pattern := range []string{"empty", "fixed16", "varied"} {
			values := deltaLengthBatchValues(n, pattern)
			encoded := encodeDeltaLengthBatchValues(values)
			var totalBytes int
			for _, v := range values {
				totalBytes += len(v)
			}
			for _, batchSize := range []int{256, n} {
				b.Run(fmt.Sprintf("%d/%s/batch-%d", n, pattern, batchSize), func(b *testing.B) {
					dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
						nil, memory.DefaultAllocator).(*DeltaLengthByteArrayDecoder)
					out := make([]parquet.ByteArray, batchSize)
					b.SetBytes(int64(totalBytes))
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						if err := dec.SetData(n, encoded); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
						remaining := n
						for remaining > 0 {
							count := min(batchSize, remaining)
							var decoded int
							var err error
							if discard {
								decoded, err = dec.Discard(count)
							} else {
								decoded, err = dec.Decode(out[:count])
							}
							if err != nil || decoded != count {
								b.Fatalf("processed %d of %d values: %v", decoded, count, err)
							}
							remaining -= count
						}
					}
				})
			}
		}
	}
}
