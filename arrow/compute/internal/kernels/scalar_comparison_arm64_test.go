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

//go:build go1.18 && arm64 && !noasm && !appengine

package kernels

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func testNeonComparison[T arrow.NumericType](t *testing.T, left, right []T, leftScalar, rightScalar T) {
	t.Helper()

	operations := []struct {
		name string
		op   CompareOperator
		fn   func(T, T) bool
	}{
		{"equal", CmpEQ, func(l, r T) bool { return l == r }},
		{"not_equal", CmpNE, func(l, r T) bool { return l != r }},
		{"greater", CmpGT, func(l, r T) bool { return l > r }},
		{"greater_equal", CmpGE, func(l, r T) bool { return l >= r }},
	}

	leftBytes := arrow.GetBytes(left)
	rightBytes := arrow.GetBytes(right)
	leftScalarBytes := arrow.GetBytes([]T{leftScalar})
	rightScalarBytes := arrow.GetBytes([]T{rightScalar})
	width := int(unsafe.Sizeof(T(0)))
	lengths := []int{0, 1, 2, 7, 8, 9, 15, 16, 17, 23, 24, 25}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			cmp := genCompareKernel[T](operation.op)
			for _, shape := range []string{"array_array", "array_scalar", "scalar_array"} {
				t.Run(shape, func(t *testing.T) {
					for offset := 0; offset < 8; offset++ {
						for _, length := range lengths {
							t.Run(fmt.Sprintf("offset_%d/length_%d", offset, length), func(t *testing.T) {
								out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(int64(offset+length))))
								expected := append([]byte(nil), out...)
								for i := 0; i < length; i++ {
									var result bool
									switch shape {
									case "array_array":
										result = operation.fn(left[i], right[i])
									case "array_scalar":
										result = operation.fn(left[i], rightScalar)
									case "scalar_array":
										result = operation.fn(leftScalar, right[i])
									}
									bitutil.SetBitTo(expected, offset+i, result)
								}

								switch shape {
								case "array_array":
									cmp.funcAA(leftBytes[:length*width], rightBytes[:length*width], out, offset)
								case "array_scalar":
									cmp.funcAS(leftBytes[:length*width], rightScalarBytes, out, offset)
								case "scalar_array":
									cmp.funcSA(leftScalarBytes, rightBytes[:length*width], out, offset)
								}

								if !bytes.Equal(expected, out) {
									t.Fatalf("expected %08b, got %08b", expected, out)
								}
							})
						}
					}
				})
			}
		})
	}
}

func TestNeonComparisons(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		left := make([]int32, 32)
		right := make([]int32, 32)
		for i := range left {
			left[i] = int32((i*37)%17 - 8)
			right[i] = int32((i*19)%13 - 6)
		}
		left[0], left[1] = -1<<31, 1<<31-1
		right[0], right[1] = 1<<31-1, -1<<31
		testNeonComparison(t, left, right, int32(-3), int32(4))
	})

	t.Run("uint32", func(t *testing.T) {
		left := make([]uint32, 32)
		right := make([]uint32, 32)
		for i := range left {
			left[i] = uint32(i * 37)
			right[i] = uint32(i*19 + 3)
		}
		left[0], left[1], left[2] = 0, 1<<31, ^uint32(0)
		right[0], right[1], right[2] = ^uint32(0), 1<<31, 1
		testNeonComparison(t, left, right, uint32(1<<31), uint32(7))
	})

	t.Run("int64", func(t *testing.T) {
		left := make([]int64, 32)
		right := make([]int64, 32)
		for i := range left {
			left[i] = int64(i*37 - 400)
			right[i] = int64(i*19 - 200)
		}
		left[0], left[1] = -1<<63, 1<<63-1
		right[0], right[1] = 1<<63-1, -1<<63
		testNeonComparison(t, left, right, int64(-3), int64(4))
	})

	t.Run("uint64", func(t *testing.T) {
		left := make([]uint64, 32)
		right := make([]uint64, 32)
		for i := range left {
			left[i] = uint64(i * 37)
			right[i] = uint64(i*19 + 3)
		}
		left[0], left[1], left[2] = 0, 1<<63, ^uint64(0)
		right[0], right[1], right[2] = ^uint64(0), 1<<63, 1
		testNeonComparison(t, left, right, uint64(1<<63), uint64(7))
	})

	t.Run("float32", func(t *testing.T) {
		leftPattern := []float32{float32(math.NaN()), float32(math.Inf(-1)), -1.5, float32(math.Copysign(0, -1)), 0, 1.5, float32(math.Inf(1)), float32(math.NaN())}
		rightPattern := []float32{float32(math.NaN()), -float32(math.Inf(1)), -1.5, 0, float32(math.Copysign(0, -1)), 2.5, float32(math.Inf(1)), 3}
		left := make([]float32, 32)
		right := make([]float32, 32)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, float32(math.NaN()), float32(1.5))
	})

	t.Run("float64", func(t *testing.T) {
		leftPattern := []float64{math.NaN(), math.Inf(-1), -1.5, math.Copysign(0, -1), 0, 1.5, math.Inf(1), math.NaN()}
		rightPattern := []float64{math.NaN(), -math.Inf(1), -1.5, 0, math.Copysign(0, -1), 2.5, math.Inf(1), 3}
		left := make([]float64, 32)
		right := make([]float64, 32)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, math.NaN(), 1.5)
	})
}
