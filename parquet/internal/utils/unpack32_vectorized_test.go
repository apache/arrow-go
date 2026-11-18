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

package utils_test

import (
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/stretchr/testify/require"
)

// packLSB packs values at width bits each, least significant bit first, one
// bit at a time.
func packLSB(values []uint32, width int) []byte {
	packed := make([]byte, (len(values)*width+7)/8)
	bit := 0
	for _, v := range values {
		for b := range width {
			packed[bit/8] |= byte(v>>b&1) << (bit % 8)
			bit++
		}
	}
	return packed
}

func TestUnpack32Vectorized(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for width := 1; width <= 32; width++ {
		values := make([]uint32, 1024)
		for i := range values {
			values[i] = rng.Uint32() >> (32 - width)
		}
		packed := packLSB(values, width)

		for _, count := range []int{0, 1, 31, 32, 33, 64, 1000, 1024} {
			out := make([]uint32, count)
			n := utils.Unpack32Vectorized(packed, out, width)
			// A platform without a vectorized unpacker unpacks nothing.
			// One with it takes every whole group of 32.
			if n != 0 {
				require.Equal(t, count/32*32, n, "width=%d count=%d", width, count)
			}
			require.Equal(t, values[:n], out[:n], "width=%d count=%d", width, count)
		}

		// The input holds fewer whole groups than the output has room for.
		out := make([]uint32, 1024)
		short := packed[:3*32*width/8+1]
		if n := utils.Unpack32Vectorized(short, out, width); n != 0 {
			require.Equal(t, 3*32, n, "width=%d", width)
			require.Equal(t, values[:n], out[:n], "width=%d", width)
		}
	}

	// Widths the 32-bit unpacker cannot serve.
	out := make([]uint32, 64)
	for _, width := range []int{-1, 0, 33, 64} {
		require.Zero(t, utils.Unpack32Vectorized(make([]byte, 1024), out, width), "width=%d", width)
	}
}
