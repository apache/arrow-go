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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newBooleanCastBenchmarkArray(mem memory.Allocator, typ arrow.DataType, size int, zeroFraction float64) arrow.Array {
	builder := array.NewBuilder(mem, typ)
	builder.Reserve(size)
	for i := 0; i < size; i++ {
		if float64(i)/float64(size) < zeroFraction {
			if err := builder.AppendValueFromString("0"); err != nil {
				panic(err)
			}
		} else if err := builder.AppendValueFromString("1"); err != nil {
			panic(err)
		}
	}
	result := builder.NewArray()
	builder.Release()
	return result
}

func BenchmarkNumericToBoolCast(b *testing.B) {
	for _, typ := range []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	} {
		width := int64(typ.(arrow.FixedWidthDataType).Bytes())
		for _, size := range []int{64, 1024, 65536, 1_000_000} {
			for _, zeroFraction := range []float64{0, 0.5, 1} {
				b.Run(fmt.Sprintf("type=%s/size=%d/zeros=%s", typ, size, strconv.FormatFloat(zeroFraction, 'f', -1, 64)), func(b *testing.B) {
					mem := memory.NewGoAllocator()
					input := newBooleanCastBenchmarkArray(mem, typ, size, zeroFraction)
					defer input.Release()
					opts := compute.DefaultCastOptions(true)
					opts.ToType = arrow.FixedWidthTypes.Boolean
					ctx := context.Background()

					b.ReportAllocs()
					b.SetBytes(int64(size) * width)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						output, err := compute.CastArray(ctx, input, opts)
						if err != nil {
							b.Fatal(err)
						}
						output.Release()
					}
				})
			}
		}
	}
}
