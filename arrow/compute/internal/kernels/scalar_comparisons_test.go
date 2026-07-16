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

//go:build go1.18

package kernels

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func TestPrimitiveComparisonsWithOutputOffset(t *testing.T) {
	const maxLength = 65

	left := make([]int32, maxLength)
	right := make([]int32, maxLength)
	for i := range left {
		left[i] = int32((i*7 + 1) % 11)
		right[i] = int32((i*5 + 3) % 11)
	}

	type operation struct {
		op   CompareOperator
		eval func(int32, int32) bool
	}
	operations := []operation{
		{CmpEQ, func(l, r int32) bool { return l == r }},
		{CmpNE, func(l, r int32) bool { return l != r }},
		{CmpGT, func(l, r int32) bool { return l > r }},
		{CmpGE, func(l, r int32) bool { return l >= r }},
	}
	// The assembly wrappers require non-empty slices to obtain data pointers.
	implementations := []struct {
		name       string
		minLength  int
		makeKernel func(CompareOperator) *CompareData
	}{
		{"pure_go", 0, func(op CompareOperator) *CompareData {
			return genGoCompareKernel(getCmpOp[int32](op))
		}},
		{"dispatch", 1, func(op CompareOperator) *CompareData {
			return genCompareKernel[int32](op)
		}},
	}

	leftBytes := arrow.Int32Traits.CastToBytes(left)
	rightBytes := arrow.Int32Traits.CastToBytes(right)
	leftScalar := int32(6)
	rightScalar := int32(4)
	leftScalarBytes := arrow.Int32Traits.CastToBytes([]int32{leftScalar})
	rightScalarBytes := arrow.Int32Traits.CastToBytes([]int32{rightScalar})

	for _, implementation := range implementations {
		t.Run(implementation.name, func(t *testing.T) {
			for _, operation := range operations {
				t.Run(operation.op.String(), func(t *testing.T) {
					cmp := implementation.makeKernel(operation.op)
					for _, shape := range []string{"array_array", "array_scalar", "scalar_array"} {
						t.Run(shape, func(t *testing.T) {
							for offset := 0; offset < 8; offset++ {
								t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
									for length := implementation.minLength; length <= maxLength; length++ {
										out := bytes.Repeat([]byte{0xa5}, int(bitutil.BytesForBits(int64(offset+length))))
										expected := append([]byte(nil), out...)

										for i := 0; i < length; i++ {
											var result bool
											switch shape {
											case "array_array":
												result = operation.eval(left[i], right[i])
											case "array_scalar":
												result = operation.eval(left[i], rightScalar)
											case "scalar_array":
												result = operation.eval(leftScalar, right[i])
											}
											bitutil.SetBitTo(expected, offset+i, result)
										}

										switch shape {
										case "array_array":
											cmp.funcAA(leftBytes[:length*4], rightBytes[:length*4], out, offset)
										case "array_scalar":
											cmp.funcAS(leftBytes[:length*4], rightScalarBytes, out, offset)
										case "scalar_array":
											cmp.funcSA(leftScalarBytes, rightBytes[:length*4], out, offset)
										}

										if !bytes.Equal(expected, out) {
											t.Fatalf("length %d: expected %08b, got %08b", length, expected, out)
										}
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
