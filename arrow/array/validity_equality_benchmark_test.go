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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkArrayEqualValidityBitmap(b *testing.B) {
	for _, dtype := range []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int64,
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.String,
	} {
		for _, length := range []int{64, 1024 * 1024} {
			for _, layout := range []struct {
				name                    string
				leftBitmap, rightBitmap bool
				rightOffset             int
			}{
				{"both", true, true, 0},
				{"mixed", false, true, 0},
				{"mixed_unaligned", false, true, 3},
				{"neither", false, false, 0},
			} {
				b.Run(fmt.Sprintf("%s/len=%d/%s", dtype.Name(), length, layout.name), func(b *testing.B) {
					left := makeValidityEqualityBenchmarkArray(dtype, length, layout.leftBitmap)
					defer left.Release()
					rightBase := makeValidityEqualityBenchmarkArray(dtype, length+layout.rightOffset, layout.rightBitmap)
					defer rightBase.Release()
					right := array.NewSlice(rightBase, int64(layout.rightOffset), int64(length+layout.rightOffset))
					defer right.Release()
					b.Run("Equal", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if !array.Equal(left, right) {
								b.Fatal("expected equal arrays")
							}
						}
					})
					b.Run("ApproxEqual", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if !array.ApproxEqual(left, right) {
								b.Fatal("expected approximately equal arrays")
							}
						}
					})
				})
			}
		}
	}
}

func makeValidityEqualityBenchmarkArray(dtype arrow.DataType, length int, withBitmap bool) arrow.Array {
	buffers := make([]*memory.Buffer, 2)
	if withBitmap {
		buffers[0] = memory.NewBufferBytes(bytes.Repeat([]byte{0xff}, int(bitutil.BytesForBits(int64(length)))))
	}
	switch dtype.ID() {
	case arrow.BOOL:
		buffers[1] = memory.NewBufferBytes(make([]byte, bitutil.BytesForBits(int64(length))))
	case arrow.INT8:
		buffers[1] = memory.NewBufferBytes(make([]byte, length))
	case arrow.INT64:
		buffers[1] = memory.NewBufferBytes(make([]byte, length*8))
	case arrow.BINARY, arrow.STRING:
		offsets := make([]int32, length+1)
		for i := range offsets {
			offsets[i] = int32(i * 8)
		}
		buffers[1] = memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
		buffers = append(buffers, memory.NewBufferBytes(bytes.Repeat([]byte("abcdefgh"), length)))
	}
	data := array.NewData(dtype, length, buffers, nil, 0, 0)
	defer data.Release()
	for _, buffer := range buffers {
		if buffer != nil {
			buffer.Release()
		}
	}
	return array.MakeFromData(data)
}
