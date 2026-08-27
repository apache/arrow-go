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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkBinaryViewToBinaryMaterialization(b *testing.B) {
	for _, valueLen := range []int{4, 32, 256} {
		for _, nullEvery := range []int{0, 10, 2} {
			name := fmt.Sprintf("value-len=%d/null-every=%d", valueLen, nullEvery)
			b.Run(name, func(b *testing.B) {
				const count = 64 * 1024

				values := make([][]byte, count)
				valid := make([]bool, count)
				value := []byte(strings.Repeat("x", valueLen))
				for i := range values {
					values[i] = value
					valid[i] = nullEvery == 0 || i%nullEvery != 0
				}

				builder := array.NewBinaryViewBuilder(memory.DefaultAllocator)
				builder.AppendValues(values, valid)
				input := builder.NewArray()
				builder.Release()
				defer input.Release()

				opts := compute.SafeCastOptions(arrow.BinaryTypes.Binary)
				b.SetBytes(int64(count * valueLen))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					out, err := compute.CastArray(context.Background(), input, opts)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}
