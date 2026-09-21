// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func BenchmarkDictEncoderWriteIndices(b *testing.B) {
	for _, length := range []int{64, 65536} {
		for _, pattern := range []struct {
			name                   string
			cardinality, runLength int
		}{
			{"constant", 1, 65536},
			{"runs_8", 256, 8},
			{"runs_32", 256, 32},
			{"runs_256", 256, 256},
			{"random_16", 16, 0},
			{"random_256", 256, 0},
			{"alternating", 2, 1},
		} {
			b.Run(fmt.Sprintf("%s/indices=%d", pattern.name, length), func(b *testing.B) {
				indices := make([]int32, length)
				rng := rand.New(rand.NewPCG(0, 0))
				for i := range indices {
					if pattern.runLength == 0 {
						indices[i] = int32(rng.IntN(pattern.cardinality))
					} else {
						indices[i] = int32((i / pattern.runLength) % pattern.cardinality)
					}
				}
				enc := makeDictIndicesEncoder(b, pattern.cardinality)
				enc.idxValues = indices
				output := make([]byte, enc.EstimatedDataEncodedSize())
				b.ReportAllocs()
				b.SetBytes(int64(length * 4))
				for b.Loop() {
					enc.idxValues = indices
					if _, err := enc.WriteIndices(output); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
