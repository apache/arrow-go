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

package encoding_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func BenchmarkPlainEncodingFixedLenByteArray(b *testing.B) {
	for _, width := range []int{4, 16, 32} {
		for _, nvalues := range []int{1, 16, 1024} {
			for _, puts := range []int{1, 64, 1024} {
				for _, withNulls := range []bool{false, true} {
					validity := "all-valid"
					if withNulls {
						validity = "sparse-nil"
					}
					name := fmt.Sprintf("width=%d/rows=%d/puts=%d/validity=%s", width, nvalues, puts, validity)
					values := makeFixedLenByteArrayValues(nvalues, width, withNulls)

					b.Run(name, func(b *testing.B) {
						col := schema.NewColumn(schema.NewFixedLenByteArrayNode("fixedlenbytearray", parquet.Repetitions.Required, int32(width), -1), 0, 0)
						encoder := encoding.NewEncoder(parquet.Types.FixedLenByteArray, parquet.Encodings.Plain,
							false, col, memory.DefaultAllocator).(encoding.FixedLenByteArrayEncoder)
						defer encoder.Release()

						b.SetBytes(int64(width * nvalues * puts))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							for j := 0; j < puts; j++ {
								encoder.Put(values)
							}
							buf, err := encoder.FlushValues()
							if err != nil {
								b.Fatal(err)
							}
							buf.Release()
						}
					})
				}
			}
		}
	}
}

func makeFixedLenByteArrayValues(nvalues, width int, withNulls bool) []parquet.FixedLenByteArray {
	values := make([]parquet.FixedLenByteArray, nvalues)
	for i := range values {
		if withNulls && i%8 == 0 {
			continue
		}

		values[i] = make(parquet.FixedLenByteArray, width)
		for j := range values[i] {
			values[i][j] = byte(j)
		}
	}
	return values
}
