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
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sys/cpu"
)

func checkNeonBinary[T arrow.NumericType](t *testing.T, typ arrow.Type, op ArithmeticOp, left, right []T, want func(T, T) T) {
	t.Helper()

	got := make([]T, len(left))
	wantValues := make([]T, len(left))
	for i := range left {
		wantValues[i] = want(left[i], right[i])
	}
	arithmeticNeon(typ, op, arrow.GetBytes(left), arrow.GetBytes(right), arrow.GetBytes(got), len(left))
	if !reflect.DeepEqual(got, wantValues) {
		t.Fatalf("array-array: got %v, want %v", got, wantValues)
	}

	var scalar T
	if len(right) != 0 {
		scalar = right[0]
	}
	wantValues = make([]T, len(left))
	for i := range left {
		wantValues[i] = want(left[i], scalar)
	}
	arithmeticArrScalarNeon(typ, op, arrow.GetBytes(left), unsafe.Pointer(&scalar), arrow.GetBytes(got), len(left))
	if !reflect.DeepEqual(got, wantValues) {
		t.Fatalf("array-scalar: got %v, want %v", got, wantValues)
	}

	if len(left) != 0 {
		scalar = left[0]
	}
	wantValues = make([]T, len(right))
	for i := range right {
		wantValues[i] = want(scalar, right[i])
	}
	arithmeticScalarArrNeon(typ, op, unsafe.Pointer(&scalar), arrow.GetBytes(right), arrow.GetBytes(got), len(right))
	if !reflect.DeepEqual(got[:len(right)], wantValues) {
		t.Fatalf("scalar-array: got %v, want %v", got[:len(right)], wantValues)
	}
}

func checkNeonUnary[T arrow.NumericType](t *testing.T, typ arrow.Type, op ArithmeticOp, input []T, want func(T) T) {
	t.Helper()

	got := make([]T, len(input))
	wantValues := make([]T, len(input))
	for i, value := range input {
		wantValues[i] = want(value)
	}
	arithmeticUnaryNeon(typ, op, arrow.GetBytes(input), arrow.GetBytes(got), len(input))
	if !reflect.DeepEqual(got, wantValues) {
		t.Fatalf("got %v, want %v", got, wantValues)
	}
}

func TestNeonArithmeticBinary(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	lengths := []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33}
	for _, n := range lengths {
		t.Run(fmt.Sprintf("length=%d", n), func(t *testing.T) {
			int32Left := make([]int32, n)
			int32Right := make([]int32, n)
			uint32Left := make([]uint32, n)
			uint32Right := make([]uint32, n)
			int64Left := make([]int64, n)
			int64Right := make([]int64, n)
			uint64Left := make([]uint64, n)
			uint64Right := make([]uint64, n)
			float32Left := make([]float32, n)
			float32Right := make([]float32, n)
			float64Left := make([]float64, n)
			float64Right := make([]float64, n)
			for i := 0; i < n; i++ {
				int32Left[i], int32Right[i] = int32(i*3-10), int32(i+2)
				uint32Left[i], uint32Right[i] = uint32(i*3+10), uint32(i+2)
				int64Left[i], int64Right[i] = int64(i*3-10), int64(i+2)
				uint64Left[i], uint64Right[i] = uint64(i*3+10), uint64(i+2)
				float32Left[i], float32Right[i] = float32(i)+0.25, float32(i)*0.5+1.5
				float64Left[i], float64Right[i] = float64(i)+0.25, float64(i)*0.5+1.5
			}

			for _, op := range []ArithmeticOp{OpAdd, OpSub} {
				checkNeonBinary(t, arrow.INT32, op, int32Left, int32Right, func(a, b int32) int32 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
				checkNeonBinary(t, arrow.UINT32, op, uint32Left, uint32Right, func(a, b uint32) uint32 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
				checkNeonBinary(t, arrow.INT64, op, int64Left, int64Right, func(a, b int64) int64 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
				checkNeonBinary(t, arrow.UINT64, op, uint64Left, uint64Right, func(a, b uint64) uint64 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
				checkNeonBinary(t, arrow.FLOAT32, op, float32Left, float32Right, func(a, b float32) float32 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
				checkNeonBinary(t, arrow.FLOAT64, op, float64Left, float64Right, func(a, b float64) float64 {
					if op == OpAdd {
						return a + b
					}
					return a - b
				})
			}

			checkNeonBinary(t, arrow.INT32, OpMul, int32Left, int32Right, func(a, b int32) int32 { return a * b })
			checkNeonBinary(t, arrow.UINT32, OpMul, uint32Left, uint32Right, func(a, b uint32) uint32 { return a * b })
			checkNeonBinary(t, arrow.FLOAT32, OpMul, float32Left, float32Right, func(a, b float32) float32 { return a * b })
			checkNeonBinary(t, arrow.FLOAT64, OpMul, float64Left, float64Right, func(a, b float64) float64 { return a * b })
		})
	}
}

func TestNeonArithmeticUnary(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	lengths := []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33}
	for _, n := range lengths {
		t.Run(fmt.Sprintf("length=%d", n), func(t *testing.T) {
			int32Values := make([]int32, n)
			uint32Values := make([]uint32, n)
			int64Values := make([]int64, n)
			uint64Values := make([]uint64, n)
			float32Values := make([]float32, n)
			float64Values := make([]float64, n)
			for i := 0; i < n; i++ {
				int32Values[i] = []int32{0, 1, -1, -1 << 31, 1<<31 - 1}[i%5]
				uint32Values[i] = []uint32{0, 1, ^uint32(0), 2, 17}[i%5]
				int64Values[i] = []int64{0, 1, -1, -1 << 63, 1<<63 - 1}[i%5]
				uint64Values[i] = []uint64{0, 1, ^uint64(0), 2, 17}[i%5]
				float32Values[i] = []float32{0, float32(math.Copysign(0, -1)), 1.5, -2.25, 3.75}[i%5]
				float64Values[i] = []float64{0, math.Copysign(0, -1), 1.5, -2.25, 3.75}[i%5]
			}

			checkNeonUnary(t, arrow.INT32, OpAbsoluteValue, int32Values, func(v int32) int32 {
				if v < 0 {
					return -v
				}
				return v
			})
			checkNeonUnary(t, arrow.INT32, OpNegate, int32Values, func(v int32) int32 { return -v })
			checkNeonUnary(t, arrow.UINT32, OpAbsoluteValue, uint32Values, func(v uint32) uint32 { return v })
			checkNeonUnary(t, arrow.UINT32, OpNegate, uint32Values, func(v uint32) uint32 { return -v })
			checkNeonUnary(t, arrow.INT64, OpAbsoluteValue, int64Values, func(v int64) int64 {
				if v < 0 {
					return -v
				}
				return v
			})
			checkNeonUnary(t, arrow.INT64, OpNegate, int64Values, func(v int64) int64 { return -v })
			checkNeonUnary(t, arrow.UINT64, OpAbsoluteValue, uint64Values, func(v uint64) uint64 { return v })
			checkNeonUnary(t, arrow.UINT64, OpNegate, uint64Values, func(v uint64) uint64 { return -v })
			checkNeonUnary(t, arrow.FLOAT32, OpAbsoluteValue, float32Values, func(v float32) float32 {
				return math.Float32frombits(math.Float32bits(v) &^ (uint32(1) << 31))
			})
			checkNeonUnary(t, arrow.FLOAT32, OpNegate, float32Values, func(v float32) float32 { return -v })
			checkNeonUnary(t, arrow.FLOAT64, OpAbsoluteValue, float64Values, func(v float64) float64 {
				return math.Float64frombits(math.Float64bits(v) &^ (uint64(1) << 63))
			})
			checkNeonUnary(t, arrow.FLOAT64, OpNegate, float64Values, func(v float64) float64 { return -v })
		})
	}
}

func TestNeonArithmeticWrapping(t *testing.T) {
	if !cpu.ARM64.HasASIMD {
		t.Skip("ARM64 SIMD is not available")
	}

	const (
		minInt32 = -1 << 31
		maxInt32 = 1<<31 - 1
		minInt64 = -1 << 63
		maxInt64 = 1<<63 - 1
	)

	int32Left := []int32{maxInt32, minInt32, -1, 12345, -12345}
	int32Right := []int32{2, -1, maxInt32, -7, 2}
	uint32Left := []uint32{^uint32(0), 0, 1, 12345, 17}
	uint32Right := []uint32{2, ^uint32(0), ^uint32(0), 7, 2}
	int64Left := []int64{maxInt64, minInt64, -1, 12345, -12345}
	int64Right := []int64{2, -1, maxInt64, -7, 2}
	uint64Left := []uint64{^uint64(0), 0, 1, 12345, 17}
	uint64Right := []uint64{2, ^uint64(0), ^uint64(0), 7, 2}

	for _, op := range []ArithmeticOp{OpAdd, OpSub} {
		checkNeonBinary(t, arrow.INT32, op, int32Left, int32Right, func(a, b int32) int32 {
			if op == OpAdd {
				return a + b
			}
			return a - b
		})
		checkNeonBinary(t, arrow.UINT32, op, uint32Left, uint32Right, func(a, b uint32) uint32 {
			if op == OpAdd {
				return a + b
			}
			return a - b
		})
		checkNeonBinary(t, arrow.INT64, op, int64Left, int64Right, func(a, b int64) int64 {
			if op == OpAdd {
				return a + b
			}
			return a - b
		})
		checkNeonBinary(t, arrow.UINT64, op, uint64Left, uint64Right, func(a, b uint64) uint64 {
			if op == OpAdd {
				return a + b
			}
			return a - b
		})
	}
	checkNeonBinary(t, arrow.INT32, OpMul, int32Left, int32Right, func(a, b int32) int32 { return a * b })
	checkNeonBinary(t, arrow.UINT32, OpMul, uint32Left, uint32Right, func(a, b uint32) uint32 { return a * b })
}

func BenchmarkNeonArithmetic(b *testing.B) {
	if !cpu.ARM64.HasASIMD {
		b.Skip("ARM64 SIMD is not available")
	}

	const n = 1 << 20
	benchInt64 := func(b *testing.B, shape string, neon bool) {
		left := make([]int64, n)
		right := make([]int64, n)
		out := make([]int64, n)
		for i := range left {
			left[i] = int64(i)
			right[i] = int64(i + 1)
		}
		scalar := int64(7)
		b.SetBytes(int64(n * 8))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if neon {
				switch shape {
				case "array-scalar":
					arithmeticArrScalarNeon(arrow.INT64, OpAdd, arrow.GetBytes(left), unsafe.Pointer(&scalar), arrow.GetBytes(out), n)
				case "scalar-array":
					arithmeticScalarArrNeon(arrow.INT64, OpAdd, unsafe.Pointer(&scalar), arrow.GetBytes(right), arrow.GetBytes(out), n)
				default:
					arithmeticNeon(arrow.INT64, OpAdd, arrow.GetBytes(left), arrow.GetBytes(right), arrow.GetBytes(out), n)
				}
			} else {
				switch shape {
				case "array-scalar":
					for j, value := range left {
						out[j] = value + scalar
					}
				case "scalar-array":
					for j, value := range right {
						out[j] = scalar + value
					}
				default:
					for j, value := range left {
						out[j] = value + right[j]
					}
				}
			}
		}
		b.StopTimer()
		runtime.KeepAlive(out)
	}

	benchFloat64 := func(b *testing.B, op ArithmeticOp, neon bool) {
		left := make([]float64, n)
		right := make([]float64, n)
		out := make([]float64, n)
		for i := range left {
			left[i] = float64(i) + 0.25
			right[i] = float64(i) + 1.5
		}
		b.SetBytes(int64(n * 8))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if neon {
				arithmeticNeon(arrow.FLOAT64, op, arrow.GetBytes(left), arrow.GetBytes(right), arrow.GetBytes(out), n)
			} else {
				for j, value := range left {
					switch op {
					case OpAdd:
						out[j] = value + right[j]
					case OpMul:
						out[j] = value * right[j]
					}
				}
			}
		}
		b.StopTimer()
		runtime.KeepAlive(out)
	}

	for _, shape := range []string{"array-array", "array-scalar", "scalar-array"} {
		shape := shape
		b.Run("int64/add/"+shape+"/neon", func(b *testing.B) { benchInt64(b, shape, true) })
		b.Run("int64/add/"+shape+"/scalar", func(b *testing.B) { benchInt64(b, shape, false) })
	}
	for _, op := range []struct {
		name string
		op   ArithmeticOp
	}{
		{"add", OpAdd},
		{"mul", OpMul},
	} {
		op := op
		b.Run("float64/"+op.name+"/array-array/neon", func(b *testing.B) { benchFloat64(b, op.op, true) })
		b.Run("float64/"+op.name+"/array-array/scalar", func(b *testing.B) { benchFloat64(b, op.op, false) })
	}
}
