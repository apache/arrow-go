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

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var binaryEqualityResult bool

func BenchmarkBinaryEquality(b *testing.B) {
	const length = 64 * 1024

	types := []arrow.BinaryDataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.LargeString,
	}

	for _, dtype := range types {
		b.Run(dtype.Name(), func(b *testing.B) {
			for _, valueLen := range []int{8, 32, 128, 1024} {
				benchmarkBinaryEqualityCase(b, dtype, length, valueLen, nil, "equal", -1)
			}

			for _, tc := range []struct {
				name          string
				valid         func(int) bool
				mismatchIndex int
			}{
				{name: "nulls_10_percent", valid: func(i int) bool { return i%10 != 0 }, mismatchIndex: -1},
				{name: "nulls_50_percent", valid: func(i int) bool { return i%2 != 0 }, mismatchIndex: -1},
				{name: "nulls_50_percent_clustered", valid: func(i int) bool { return i >= length/2 }, mismatchIndex: -1},
				{name: "mismatch_first", mismatchIndex: 0},
				{name: "mismatch_middle", mismatchIndex: length / 2},
				{name: "mismatch_last", mismatchIndex: length - 1},
				{name: "different_length", mismatchIndex: length / 2},
			} {
				benchmarkBinaryEqualityCase(b, dtype, length, 32, tc.valid, tc.name, tc.mismatchIndex)
			}
		})
	}
}

func benchmarkBinaryEqualityCase(
	b *testing.B, dtype arrow.BinaryDataType, length, valueLen int, validValue func(int) bool, name string, mismatchIndex int,
) {
	b.Helper()

	values := makeBinaryEqualityValues(length, valueLen)
	rightValues := append([]string(nil), values...)
	valid := make([]bool, length)
	for i := range valid {
		valid[i] = validValue == nil || validValue(i)
	}
	if mismatchIndex >= 0 {
		if name == "different_length" {
			rightValues[mismatchIndex] += "x"
		} else {
			value := []byte(rightValues[mismatchIndex])
			value[0]++
			rightValues[mismatchIndex] = string(value)
		}
	}

	mem := memory.NewGoAllocator()
	left := makeBinaryEqualityArray(mem, dtype, values, valid)
	right := makeBinaryEqualityArray(mem, dtype, rightValues, valid)
	b.Cleanup(func() {
		left.Release()
		right.Release()
	})

	b.Run(fmt.Sprintf("%s/value_len_%d", name, valueLen), func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(length * valueLen))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binaryEqualityResult = array.Equal(left, right)
		}
	})
}

func makeBinaryEqualityValues(length, valueLen int) []string {
	values := make([]string, length)
	value := make([]byte, valueLen)
	for i := range values {
		for j := range value {
			value[j] = byte(i*31 + j*17)
		}
		values[i] = string(value)
	}
	return values
}
