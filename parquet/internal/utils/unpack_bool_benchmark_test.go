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
	"github.com/stretchr/testify/assert"
)

func TestBytesToBoolsCorrectness(t *testing.T) {
	for _, size := range []int{1, 2, 3, 7, 8, 15, 16, 17, 31, 32, 64, 100, 256, 1024} {
		in := make([]byte, size)
		for i := range in {
			in[i] = byte(rand.IntN(256))
		}
		out := make([]bool, size*8)
		utils.BytesToBools(in, out)

		for i, b := range in {
			for j := 0; j < 8; j++ {
				expected := (b & (1 << j)) != 0
				assert.Equalf(t, expected, out[8*i+j],
					"size=%d byte=%d bit=%d: input=0x%02x", size, i, j, b)
			}
		}
	}
}

func BenchmarkBytesToBools(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096, 16384} {
		in := make([]byte, size)
		for i := range in {
			in[i] = byte(rand.IntN(256))
		}
		out := make([]bool, size*8)

		b.Run("bytes="+bToStr(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				utils.BytesToBools(in, out)
			}
		})
	}
}

func bToStr(n int) string {
	switch {
	case n >= 16384:
		return "16K"
	case n >= 4096:
		return "4K"
	case n >= 1024:
		return "1K"
	case n >= 256:
		return "256"
	default:
		return "64"
	}
}
