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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func BenchmarkPlainDecoderDecodeSpaced(b *testing.B) {
	const size = 1 << 16

	patterns := []struct {
		name  string
		valid func(int) bool
	}{
		{name: "trailing_null", valid: func(i int) bool { return i != size-1 }},
		{name: "leading_null", valid: func(i int) bool { return i != 0 }},
		{name: "clustered_nulls", valid: func(i int) bool { return i < size/2-32 || i >= size/2+32 }},
		{name: "random_10pct_nulls", valid: randomValidity(size, 10)},
		{name: "alternating", valid: func(i int) bool { return i%2 == 0 }},
	}

	for _, pattern := range patterns {
		pattern := pattern
		b.Run(fmt.Sprintf("int32/%s", pattern.name), func(b *testing.B) {
			data, validBits, nullCount := newPlainInt32SpacedInput(size, pattern.valid)
			out := make([]int32, size)
			dec := NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator).(Int32Decoder)

			b.ReportAllocs()
			b.SetBytes(int64(size * arrow.Int32SizeBytes))
			b.ResetTimer()
			for b.Loop() {
				if err := dec.SetData(size-nullCount, data); err != nil {
					b.Fatal(err)
				}
				n, err := dec.DecodeSpaced(out, nullCount, validBits, 0)
				if err != nil {
					b.Fatal(err)
				}
				if n != size {
					b.Fatalf("expected %d values, got %d", size, n)
				}
			}
		})
	}
}

func newPlainInt32SpacedInput(size int, valid func(int) bool) (data, validBits []byte, nullCount int) {
	validBits = make([]byte, bitutil.BytesForBits(int64(size)))
	values := make([]int32, 0, size)
	for i := 0; i < size; i++ {
		if valid(i) {
			bitutil.SetBit(validBits, i)
			values = append(values, int32(i))
		} else {
			nullCount++
		}
	}

	enc := NewEncoder(parquet.Types.Int32, parquet.Encodings.Plain, false, nil, memory.DefaultAllocator).(Int32Encoder)
	enc.Put(values)
	buf, _ := enc.FlushValues()
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...), validBits, nullCount
}

func randomValidity(size, nullPercent int) func(int) bool {
	state := uint32(1)
	valid := make([]bool, size)
	for i := range valid {
		state = state*1664525 + 1013904223
		valid[i] = state%100 >= uint32(nullPercent)
	}
	return func(i int) bool { return valid[i] }
}
