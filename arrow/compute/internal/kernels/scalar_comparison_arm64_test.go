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

const neonComparisonTestLength = 1024

func testNeonComparison[T arrow.NumericType](t *testing.T, left, right []T, leftScalar, rightScalar T) {
	t.Helper()

	operations := []struct {
		name string
		op   CompareOperator
	}{
		{"equal", CmpEQ},
		{"not_equal", CmpNE},
		{"greater", CmpGT},
		{"greater_equal", CmpGE},
	}

	leftBytes := arrow.GetBytes(left)
	rightBytes := arrow.GetBytes(right)
	leftScalarBytes := arrow.GetBytes([]T{leftScalar})
	rightScalarBytes := arrow.GetBytes([]T{rightScalar})
	width := int(unsafe.Sizeof(T(0)))
	lengths := []int{0, 1, 2, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 127, 128, 129, neonComparisonTestLength}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			cmp := genCompareKernel[T](operation.op)
			fallback := genGoCompareKernel(getCmpOp[T](operation.op))
			for _, shape := range []string{"array_array", "array_scalar", "scalar_array"} {
				t.Run(shape, func(t *testing.T) {
					for offset := 0; offset < 8; offset++ {
						for _, length := range lengths {
							t.Run(fmt.Sprintf("offset_%d/length_%d", offset, length), func(t *testing.T) {
								out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(int64(offset+length))))
								expected := append([]byte(nil), out...)

								switch shape {
								case "array_array":
									fallback.funcAA(leftBytes[:length*width], rightBytes[:length*width], expected, offset)
								case "array_scalar":
									fallback.funcAS(leftBytes[:length*width], rightScalarBytes, expected, offset)
								case "scalar_array":
									fallback.funcSA(leftScalarBytes, rightBytes[:length*width], expected, offset)
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
		leftPattern := []int32{-1 << 31, -17, -1, 0, 1, 17, 1<<31 - 1, 42, -42, 3, 9, -9, 5, -5, 2, -2}
		rightPattern := []int32{1<<31 - 1, -17, 0, 1, -1, -42, -1 << 31, 42, 4, -3, 9, -10, -5, 5, -2, 2}
		left := make([]int32, neonComparisonTestLength)
		right := make([]int32, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, int32(-3), int32(4))
	})

	t.Run("uint32", func(t *testing.T) {
		leftPattern := []uint32{0, 1, 2, 1<<31 - 1, 1 << 31, ^uint32(0), 42, 7, 100, 3, 19, 5, 77, 11, 12, 13}
		rightPattern := []uint32{^uint32(0), 1, 3, 1 << 31, 1<<31 - 1, 0, 42, 8, 99, 4, 19, 6, 78, 10, 13, 12}
		left := make([]uint32, neonComparisonTestLength)
		right := make([]uint32, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, uint32(1<<31), uint32(7))
	})

	t.Run("int8", func(t *testing.T) {
		leftPattern := []int8{-1 << 7, -17, -1, 0, 1, 17, 1<<7 - 1, 42, -42, 3, 9, -9, 5, -5, 2, -2}
		rightPattern := []int8{1<<7 - 1, -17, 0, 1, -1, -42, -1 << 7, 42, 4, -3, 9, -10, -5, 5, -2, 2}
		left := make([]int8, neonComparisonTestLength)
		right := make([]int8, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, int8(-3), int8(4))
	})

	t.Run("uint8", func(t *testing.T) {
		leftPattern := []uint8{0, 1, 2, 1<<7 - 1, 1 << 7, ^uint8(0), 42, 7, 100, 3, 19, 5, 77, 11, 12, 13}
		rightPattern := []uint8{^uint8(0), 1, 3, 1 << 7, 1<<7 - 1, 0, 42, 8, 99, 4, 19, 6, 78, 10, 13, 12}
		left := make([]uint8, neonComparisonTestLength)
		right := make([]uint8, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, uint8(1<<7), uint8(7))
	})

	t.Run("int16", func(t *testing.T) {
		leftPattern := []int16{-1 << 15, -17, -1, 0, 1, 17, 1<<15 - 1, 42, -42, 3, 9, -9, 5, -5, 2, -2}
		rightPattern := []int16{1<<15 - 1, -17, 0, 1, -1, -42, -1 << 15, 42, 4, -3, 9, -10, -5, 5, -2, 2}
		left := make([]int16, neonComparisonTestLength)
		right := make([]int16, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, int16(-3), int16(4))
	})

	t.Run("uint16", func(t *testing.T) {
		leftPattern := []uint16{0, 1, 2, 1<<15 - 1, 1 << 15, ^uint16(0), 42, 7, 100, 3, 19, 5, 77, 11, 12, 13}
		rightPattern := []uint16{^uint16(0), 1, 3, 1 << 15, 1<<15 - 1, 0, 42, 8, 99, 4, 19, 6, 78, 10, 13, 12}
		left := make([]uint16, neonComparisonTestLength)
		right := make([]uint16, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, uint16(1<<15), uint16(7))
	})

	t.Run("int64", func(t *testing.T) {
		leftPattern := []int64{-1 << 63, -17, -1, 0, 1, 17, 1<<63 - 1, 42, -42, 3, 9, -9, 5, -5, 2, -2}
		rightPattern := []int64{1<<63 - 1, -17, 0, 1, -1, -42, -1 << 63, 42, 4, -3, 9, -10, -5, 5, -2, 2}
		left := make([]int64, neonComparisonTestLength)
		right := make([]int64, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, int64(-3), int64(4))
	})

	t.Run("uint64", func(t *testing.T) {
		leftPattern := []uint64{0, 1, 2, 1<<63 - 1, 1 << 63, ^uint64(0), 42, 7, 100, 3, 19, 5, 77, 11, 12, 13}
		rightPattern := []uint64{^uint64(0), 1, 3, 1 << 63, 1<<63 - 1, 0, 42, 8, 99, 4, 19, 6, 78, 10, 13, 12}
		left := make([]uint64, neonComparisonTestLength)
		right := make([]uint64, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, uint64(1<<63), uint64(7))
	})

	t.Run("float32", func(t *testing.T) {
		leftPattern := []float32{float32(math.NaN()), float32(math.Inf(-1)), -1.5, float32(math.Copysign(0, -1)), 0, 1.5, float32(math.Inf(1)), float32(math.NaN())}
		rightPattern := []float32{float32(math.NaN()), -float32(math.Inf(1)), -1.5, 0, float32(math.Copysign(0, -1)), 2.5, float32(math.Inf(1)), 3}
		left := make([]float32, neonComparisonTestLength)
		right := make([]float32, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, float32(math.NaN()), float32(1.5))
	})

	t.Run("float64", func(t *testing.T) {
		leftPattern := []float64{math.NaN(), math.Inf(-1), -1.5, math.Copysign(0, -1), 0, 1.5, math.Inf(1), math.NaN()}
		rightPattern := []float64{math.NaN(), -math.Inf(1), -1.5, 0, math.Copysign(0, -1), 2.5, math.Inf(1), 3}
		left := make([]float64, neonComparisonTestLength)
		right := make([]float64, neonComparisonTestLength)
		for i := range left {
			left[i] = leftPattern[i%len(leftPattern)]
			right[i] = rightPattern[i%len(rightPattern)]
		}
		testNeonComparison(t, left, right, math.NaN(), 1.5)
	})
}

func TestNeonComparisonBitPacking(t *testing.T) {
	t.Run("int8", testNeonComparisonBitPacking[int8])
	t.Run("uint8", testNeonComparisonBitPacking[uint8])
	t.Run("int16", testNeonComparisonBitPacking[int16])
	t.Run("uint16", testNeonComparisonBitPacking[uint16])
	t.Run("int32", testNeonComparisonBitPacking[int32])
	t.Run("uint32", testNeonComparisonBitPacking[uint32])
	t.Run("int64", testNeonComparisonBitPacking[int64])
	t.Run("uint64", testNeonComparisonBitPacking[uint64])
	t.Run("float32", testNeonComparisonBitPacking[float32])
	t.Run("float64", testNeonComparisonBitPacking[float64])
}

func testNeonComparisonBitPacking[T arrow.NumericType](t *testing.T) {
	const n = 256 * 8
	left, right := make([]T, n), make([]T, n)
	for mask := 0; mask < 256; mask++ {
		for bit := 0; bit < 8; bit++ {
			left[mask*8+bit] = T(mask >> bit & 1)
			right[mask*8+bit] = T(mask >> (7 - bit) & 1)
		}
	}
	leftBytes, rightBytes := arrow.GetBytes(left), arrow.GetBytes(right)
	scalar := arrow.GetBytes([]T{0})
	for _, op := range []CompareOperator{CmpEQ, CmpNE, CmpGT, CmpGE} {
		got, want := genCompareKernel[T](op), genGoCompareKernel(getCmpOp[T](op))
		for _, shape := range []struct {
			name        string
			got, want   binaryKernel
			left, right []byte
		}{
			{"array_array", got.funcAA, want.funcAA, leftBytes, rightBytes},
			{"array_scalar", got.funcAS, want.funcAS, leftBytes, scalar},
			{"scalar_array", got.funcSA, want.funcSA, scalar, rightBytes},
		} {
			output := bytes.Repeat([]byte{0xa5}, n/8+2)
			expected := bytes.Clone(output)
			shape.want(shape.left, shape.right, expected[1:len(expected)-1], 0)
			shape.got(shape.left, shape.right, output[1:len(output)-1], 0)
			if !bytes.Equal(output, expected) {
				t.Fatalf("%s %s: expected %08b, got %08b", op, shape.name, expected, output)
			}
		}
	}
}
