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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package array_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

var booleanEqualityResult bool

func BenchmarkBooleanEquality(b *testing.B) {
	for _, length := range []int{64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("len_%d", length), func(b *testing.B) {
			for _, tc := range []struct {
				name          string
				valid         func(int) bool
				mismatchIndex int
				offset        int
			}{
				{name: "all_valid_equal", mismatchIndex: -1},
				{name: "clustered_10_percent_null", valid: func(i int) bool { return i >= length/10 }, mismatchIndex: -1},
				{name: "periodic_10_percent_null", valid: func(i int) bool { return i%10 != 0 }, mismatchIndex: -1},
				{name: "alternating_null", valid: func(i int) bool { return i%2 != 0 }, mismatchIndex: -1},
				{name: "mismatch_first", mismatchIndex: 0},
				{name: "mismatch_last", mismatchIndex: length - 1},
				{name: "unaligned_equal", mismatchIndex: -1, offset: 3},
			} {
				benchmarkBooleanEqualityCase(b, length, tc.valid, tc.name, tc.mismatchIndex, tc.offset)
			}
		})
	}
}

func benchmarkBooleanEqualityCase(
	b *testing.B, length int, validValue func(int) bool, name string, mismatchIndex, offset int,
) {
	b.Helper()

	totalLength := length + offset
	leftValues := make([]bool, totalLength)
	rightValues := make([]bool, totalLength)
	valid := make([]bool, totalLength)
	for i := range totalLength {
		leftValues[i] = i%3 == 0
		rightValues[i] = leftValues[i]
		valid[i] = i < offset || validValue == nil || validValue(i-offset)
	}
	if mismatchIndex >= 0 {
		rightValues[offset+mismatchIndex] = !rightValues[offset+mismatchIndex]
	}

	leftBase := makeBooleanEqualityArray(leftValues, valid)
	rightBase := makeBooleanEqualityArray(rightValues, valid)
	left := array.NewSlice(leftBase, int64(offset), int64(totalLength))
	right := array.NewSlice(rightBase, int64(offset), int64(totalLength))
	b.Cleanup(func() {
		left.Release()
		right.Release()
		leftBase.Release()
		rightBase.Release()
	})

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			booleanEqualityResult = array.Equal(left, right)
		}
	})
}
