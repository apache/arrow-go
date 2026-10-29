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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestDeltaBitPackConstantBlockWireFormat(t *testing.T) {
	t.Run("full positive block", func(t *testing.T) {
		values := make([]int32, 129)
		for i := range values {
			values[i] = 10 + int32(i)*2
		}

		enc := NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, false, nil, memory.DefaultAllocator).(*deltaBitPackEncoder[int32])
		defer enc.Release()
		enc.Put(values)

		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()

		// Block size 128, 4 miniblocks, 129 values, first value 10,
		// minimum delta 2, then four zero bit widths and no payload.
		require.Equal(t, []byte{0x80, 0x01, 0x04, 0x81, 0x01, 0x14, 0x04, 0, 0, 0, 0}, buf.Bytes())
	})

	t.Run("partial negative block", func(t *testing.T) {
		values := []int64{5, 4, 3, 2, 1, 0, -1}
		enc := NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, nil, memory.DefaultAllocator).(*deltaBitPackEncoder[int64])
		defer enc.Release()
		enc.Put(values)

		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()

		// The trailing miniblocks are also written as zero-width.
		require.Equal(t, []byte{0x80, 0x01, 0x04, 0x07, 0x0a, 0x01, 0, 0, 0, 0}, buf.Bytes())
	})
}

func testDeltaBitPackBlockRoundTrip[T int32 | int64](t *testing.T, typ parquet.Type) {
	t.Helper()

	encode := func(parts ...[]T) []byte {
		t.Helper()
		enc := NewEncoder(typ, parquet.Encodings.DeltaBinaryPacked, false, nil, memory.DefaultAllocator).(*deltaBitPackEncoder[T])
		defer enc.Release()
		for _, part := range parts {
			enc.Put(part)
		}
		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()
		return append([]byte(nil), buf.Bytes()...)
	}

	verify := func(t *testing.T, values []T) {
		t.Helper()
		n := len(values)
		wire := encode(values)
		split := max(1, n/2)
		require.Equal(t, wire, encode(values[:1], values[1:split], values[split:]))

		dec := NewDecoder(typ, parquet.Encodings.DeltaBinaryPacked, nil, memory.DefaultAllocator).(*deltaBitPackDecoder[T])
		require.NoError(t, dec.SetData(n, wire))
		out := make([]T, n)
		got, err := dec.Decode(out)
		require.NoError(t, err)
		require.Equal(t, n, got)
		require.Equal(t, values, out)
	}

	for _, n := range []int{1, 2, 31, 32, 33, 127, 128, 129, 130, 255, 257, 1001} {
		for _, pattern := range []string{"constant", "zero", "sparse", "alternating", "random", "overflow"} {
			t.Run(fmt.Sprintf("%s/%d", pattern, n), func(t *testing.T) {
				values := make([]T, n)
				rng := rand.New(rand.NewSource(int64(n)))
				switch pattern {
				case "overflow":
					raw := int64(math.MaxInt32 - 2)
					if typ == parquet.Types.Int64 {
						raw = int64(math.MaxInt64 - 2)
					}
					values[0] = T(raw)
				default:
					values[0] = T(11)
				}

				for i := 1; i < n; i++ {
					switch pattern {
					case "constant":
						values[i] = values[i-1] + T(3)
					case "zero":
						values[i] = values[i-1]
					case "sparse":
						step := T(3)
						if i%128 == 90 {
							step = 7
						}
						values[i] = values[i-1] + step
					case "alternating":
						values[i] = values[i-1] + T(i%2+1)
					case "random":
						values[i] = T(rng.Uint64())
					case "overflow":
						values[i] = values[i-1] + T(7)
					}
				}

				verify(t, values)
			})
		}
	}
	// The last delta in a block must be included in constant-block detection.
	// The 3-value higher case is [0, 3, 10]; 129 values fill one delta block.
	for _, n := range []int{3, 129, 130, 257} {
		for _, outlier := range []struct {
			name string
			step T
		}{
			{name: "higher", step: 7},
			{name: "lower", step: -7},
		} {
			t.Run(fmt.Sprintf("last-delta-%s/%d", outlier.name, n), func(t *testing.T) {
				values := make([]T, n)
				outlierIndex := n - 1
				if n == 130 {
					// The outlier ends the first block, with another delta after it.
					outlierIndex = 128
				}
				for i := 1; i < n; i++ {
					step := T(3)
					if i == outlierIndex {
						step = outlier.step
					}
					values[i] = values[i-1] + step
				}
				verify(t, values)
			})
		}
	}
}

func TestDeltaBitPackConstantAndMixedBlocksInt32(t *testing.T) {
	testDeltaBitPackBlockRoundTrip[int32](t, parquet.Types.Int32)
}

func TestDeltaBitPackConstantAndMixedBlocksInt64(t *testing.T) {
	testDeltaBitPackBlockRoundTrip[int64](t, parquet.Types.Int64)
}
