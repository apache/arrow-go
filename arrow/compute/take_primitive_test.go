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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestPrimitiveTakeAccessPatterns(t *testing.T) {
	const nvalues = 97
	for _, valueType := range numericTypes {
		t.Run(valueType.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(context.Background(), mem)
			valueStrings := make([]string, nvalues+6)
			for i := range valueStrings {
				valueStrings[i] = "null"
			}
			for i := 0; i < nvalues; i++ {
				valueStrings[i+3] = fmt.Sprint((i*13 + 7) % nvalues)
			}
			full, _, err := array.FromJSON(mem, valueType, strings.NewReader("["+strings.Join(valueStrings, ",")+"]"))
			require.NoError(t, err)
			values := array.NewSlice(full, 3, nvalues+3)
			full.Release()
			defer values.Release()
			require.Zero(t, values.NullN())

			for _, size := range []int{0, 1, 3, 4, 5, 31, 32, 33, 255, 256, 257} {
				for _, order := range []string{"sorted", "reverse", "random", "repeated"} {
					t.Run(fmt.Sprintf("size=%d/%s", size, order), func(t *testing.T) {
						indexStrings := make([]string, size+2)
						indexStrings[0], indexStrings[size+1] = "99", "99"
						wantStrings := make([]string, size)
						for i := 0; i < size; i++ {
							idx := 0
							switch order {
							case "sorted":
								idx = i * (nvalues - 1) / size
							case "reverse":
								idx = (nvalues - 1) - i*(nvalues-1)/size
							case "random":
								idx = (i*37 + 19) % nvalues
							case "repeated":
								if (i/4)%2 == 0 {
									idx = nvalues - 1
								}
							}
							indexStrings[i+1] = fmt.Sprint(idx)
							wantStrings[i] = valueStrings[idx+3]
						}
						expected, _, err := array.FromJSON(mem, valueType, strings.NewReader("["+strings.Join(wantStrings, ",")+"]"))
						require.NoError(t, err)
						defer expected.Release()

						for _, indexType := range chunkedTakeIndexTypes {
							t.Run(indexType.String(), func(t *testing.T) {
								indicesFull := makeChunkedTakeIndexArray(t, mem, indexType, indexStrings, nil)
								indices := array.NewSlice(indicesFull, 1, int64(size+1))
								indicesFull.Release()
								defer indices.Release()
								actual, err := compute.TakeArray(ctx, values, indices)
								require.NoError(t, err)
								defer actual.Release()
								require.True(t, array.Equal(expected, actual))
								require.Zero(t, actual.NullN())
								require.Empty(t, actual.NullBitmapBytes())
							})
						}
					})
				}
			}
		})
	}
}

func TestPrimitiveTakeFloatBits(t *testing.T) {
	for _, tc := range []struct {
		typ arrow.DataType
		raw []byte
	}{
		{arrow.PrimitiveTypes.Float32, arrow.Uint32Traits.CastToBytes([]uint32{
			0, 0, 0x80000000, 0x7f800000, 0xff800000, 0x7fc12345, 0x7f812345, 1, 0xffc12345, 0,
		})},
		{arrow.PrimitiveTypes.Float64, arrow.Uint64Traits.CastToBytes([]uint64{
			0, 0, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000,
			0x7ff8123456789abc, 0x7ff0123456789abc, 1, 0xfff8123456789abc, 0,
		})},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(context.Background(), mem)
			width := tc.typ.(arrow.FixedWidthDataType).Bytes()
			buffer := memory.NewResizableBuffer(mem)
			buffer.Resize(len(tc.raw))
			copy(buffer.Bytes(), tc.raw)
			data := array.NewData(tc.typ, len(tc.raw)/width, []*memory.Buffer{nil, buffer}, nil, 0, 0)
			buffer.Release()
			full := array.MakeFromData(data)
			data.Release()
			values := array.NewSlice(full, 1, 9)
			full.Release()
			defer values.Release()

			for _, order := range []string{"random", "reverse"} {
				t.Run(order, func(t *testing.T) {
					bldr := array.NewInt32Builder(mem)
					bldr.Append(99)
					var expected []byte
					for i := 0; i < 33; i++ {
						idx := (i*5 + 3) % values.Len()
						if order == "reverse" {
							idx = values.Len() - 1 - i*values.Len()/33
						}
						bldr.Append(int32(idx))
						expected = append(expected, tc.raw[(idx+1)*width:(idx+2)*width]...)
					}
					bldr.Append(99)
					indicesFull := bldr.NewArray()
					bldr.Release()
					indices := array.NewSlice(indicesFull, 1, 34)
					indicesFull.Release()
					defer indices.Release()
					actual, err := compute.TakeArray(ctx, values, indices)
					require.NoError(t, err)
					defer actual.Release()
					start := actual.Data().Offset() * width
					require.Equal(t, expected, actual.Data().Buffers()[1].Bytes()[start:start+len(expected)])
				})
			}
		})
	}
}
