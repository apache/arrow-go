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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrayEqualOptionalValidityBitmap(t *testing.T) {
	for _, length := range []int{0, 1, 7, 8, 9, 63, 64, 65, 129} {
		for _, offset := range []int{0, 3, 8, 63, 65} {
			for _, empty := range []bool{false, true} {
				for _, nulls := range []int{0, array.UnknownNullCount} {
					t.Run(fmt.Sprintf("len=%d/offset=%d/empty=%t/nulls=%d", length, offset, empty, nulls), func(t *testing.T) {
						var missing *memory.Buffer
						if empty {
							missing = memory.NewBufferBytes([]byte{})
							defer missing.Release()
						}
						left := makeBooleanValidityEqualityArray(length, 5, missing, 0)
						defer left.Release()

						bitmap := make([]byte, bitutil.BytesForBits(int64(offset+length+1)))
						for i := range length {
							bitutil.SetBit(bitmap, offset+i)
						}
						validity := memory.NewBufferBytes(bitmap)
						defer validity.Release()
						right := makeBooleanValidityEqualityArray(length, offset, validity, nulls)
						defer right.Release()

						assert.True(t, array.Equal(left, right))
						assert.True(t, array.Equal(right, left))
						assert.True(t, array.ApproxEqual(left, right))
						assert.True(t, array.ApproxEqual(right, left))
					})
				}
			}
		}
	}
}

func TestArrayEqualOptionalValidityBitmapMismatch(t *testing.T) {
	const length = 129
	for _, offset := range []int{0, 3, 65} {
		for _, nullIndex := range []int{0, 7, 63, 64, length - 1} {
			for _, nulls := range []int{0, array.UnknownNullCount} {
				t.Run(fmt.Sprintf("offset=%d/null=%d/nulls=%d", offset, nullIndex, nulls), func(t *testing.T) {
					left := makeBooleanValidityEqualityArray(length, 0, nil, 0)
					defer left.Release()
					bitmap := make([]byte, bitutil.BytesForBits(int64(offset+length)))
					for i := range length {
						bitutil.SetBit(bitmap, offset+i)
					}
					bitutil.ClearBit(bitmap, offset+nullIndex)
					validity := memory.NewBufferBytes(bitmap)
					defer validity.Release()
					right := makeBooleanValidityEqualityArray(length, offset, validity, nulls)
					defer right.Release()

					assert.False(t, array.Equal(left, right))
					assert.False(t, array.Equal(right, left))
					assert.False(t, array.ApproxEqual(left, right))
					assert.False(t, array.ApproxEqual(right, left))
				})
			}
		}
	}
}

func makeBooleanValidityEqualityArray(length, offset int, validity *memory.Buffer, nulls int) *array.Boolean {
	values := memory.NewBufferBytes(make([]byte, bitutil.BytesForBits(int64(offset+length))))
	defer values.Release()
	data := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{validity, values}, nil, nulls, offset)
	defer data.Release()
	return array.NewBooleanData(data)
}
