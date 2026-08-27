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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

type booleanSpacedEncoder interface {
	TypedEncoder
	PutSpaced([]bool, []byte, int64)
}

func BenchmarkPlainBooleanEncoderPutSpaced(b *testing.B) {
	benchmarkBooleanEncoderPutSpaced(b, parquet.Encodings.Plain)
}

func BenchmarkRleBooleanEncoderPutSpaced(b *testing.B) {
	benchmarkBooleanEncoderPutSpaced(b, parquet.Encodings.RLE)
}

func benchmarkBooleanEncoderPutSpaced(b *testing.B, enc parquet.Encoding) {
	patterns := []struct {
		name  string
		valid func(int) bool
	}{
		{name: "all_valid", valid: func(int) bool { return true }},
		{name: "ten_percent_null", valid: func(i int) bool { return i%10 != 0 }},
		{name: "fifty_percent_null", valid: func(i int) bool { return i%2 != 0 }},
		{name: "ninety_percent_null", valid: func(i int) bool { return i%10 == 0 }},
	}

	for _, length := range []int{1024, 64 * 1024, 1024 * 1024} {
		for _, pattern := range patterns {
			b.Run(fmt.Sprintf("length_%d/%s", length, pattern.name), func(b *testing.B) {
				values := make([]bool, length)
				for i := range values {
					values[i] = i%3 == 0
				}
				validBits := make([]byte, bitutil.BytesForBits(int64(length)))
				for i := range length {
					if pattern.valid(i) {
						bitutil.SetBit(validBits, i)
					}
				}

				encoder := NewEncoder(
					parquet.Types.Boolean, enc, false, nil, memory.DefaultAllocator,
				).(booleanSpacedEncoder)
				defer encoder.Release()

				encoder.PutSpaced(values, validBits, 0)
				flushBooleanEncoder(b, encoder)

				b.ReportAllocs()
				b.SetBytes(int64(length))
				b.ResetTimer()
				for b.Loop() {
					encoder.PutSpaced(values, validBits, 0)
					flushBooleanEncoder(b, encoder)
				}
			})
		}
	}
}

func flushBooleanEncoder(b *testing.B, encoder booleanSpacedEncoder) {
	buf, err := encoder.FlushValues()
	if err != nil {
		b.Fatal(err)
	}
	buf.Release()

	if enc, ok := encoder.(*RleBooleanEncoder); ok {
		enc.bufferedValues = enc.bufferedValues[:0]
	}
}
