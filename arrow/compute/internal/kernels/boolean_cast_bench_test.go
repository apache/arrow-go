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

//go:build go1.24

package kernels

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

func benchmarkNumericToBoolSIMD[T arrow.NumericType](b *testing.B, typ arrow.Type) {
	for _, size := range []int{64, 1024, 65536} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			values := numericToBoolBoundaryValues[T](size)
			out := make([]byte, (size+7)/8)
			b.ReportAllocs()
			b.SetBytes(int64(size) * int64(unsafe.Sizeof(T(0))))
			for b.Loop() {
				if err := numericToBoolSIMD(typ, nil, values, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNumericToBoolSIMD(b *testing.B) {
	b.Run("int32", func(b *testing.B) { benchmarkNumericToBoolSIMD[int32](b, arrow.INT32) })
	b.Run("int64", func(b *testing.B) { benchmarkNumericToBoolSIMD[int64](b, arrow.INT64) })
	b.Run("float32", func(b *testing.B) { benchmarkNumericToBoolSIMD[float32](b, arrow.FLOAT32) })
	b.Run("float64", func(b *testing.B) { benchmarkNumericToBoolSIMD[float64](b, arrow.FLOAT64) })
}
