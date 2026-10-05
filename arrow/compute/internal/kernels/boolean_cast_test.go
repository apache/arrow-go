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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func checkNumericToBoolSIMD[T arrow.NumericType](t *testing.T, typ arrow.Type, values []T) {
	t.Helper()

	var zero T
	for _, fill := range []byte{0, 0xff} {
		nbytes := (len(values) + 7) / 8
		backing := bytes.Repeat([]byte{fill}, nbytes+1)
		backing[nbytes] = 0xa5
		out := backing[:nbytes]
		want := bytes.Repeat([]byte{fill}, nbytes)
		for i, value := range values {
			bitutil.SetBitTo(want, i, value != zero)
		}
		if err := numericToBoolSIMD(typ, nil, values, out); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, want) {
			t.Fatalf("fill=%#x: output = %08b, want %08b", fill, out, want)
		}
		if backing[nbytes] != 0xa5 {
			t.Fatal("cast wrote beyond the output bitmap")
		}
	}
}

func numericToBoolBoundaryValues[T arrow.NumericType](length int) []T {
	values := make([]T, length)
	for i := range values {
		if i%3 != 0 {
			values[i] = T(1)
		}
	}
	return values
}

func TestNumericToBoolSIMDBoundaries(t *testing.T) {
	types := []struct {
		name string
		run  func(*testing.T, int)
	}{
		{
			name: "int8",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.INT8, numericToBoolBoundaryValues[int8](n))
			},
		},
		{
			name: "uint8",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.UINT8, numericToBoolBoundaryValues[uint8](n))
			},
		},
		{
			name: "int16",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.INT16, numericToBoolBoundaryValues[int16](n))
			},
		},
		{
			name: "uint16",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.UINT16, numericToBoolBoundaryValues[uint16](n))
			},
		},
		{
			name: "int32",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.INT32, numericToBoolBoundaryValues[int32](n))
			},
		},
		{
			name: "uint32",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.UINT32, numericToBoolBoundaryValues[uint32](n))
			},
		},
		{
			name: "int64",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.INT64, numericToBoolBoundaryValues[int64](n))
			},
		},
		{
			name: "uint64",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.UINT64, numericToBoolBoundaryValues[uint64](n))
			},
		},
		{
			name: "float32",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.FLOAT32, numericToBoolBoundaryValues[float32](n))
			},
		},
		{
			name: "float64",
			run: func(t *testing.T, n int) {
				checkNumericToBoolSIMD(t, arrow.FLOAT64, numericToBoolBoundaryValues[float64](n))
			},
		},
	}

	for _, size := range []int{0, 7, 8, 9, 15, 16, 17, 31, 32, 33} {
		for _, typ := range types {
			t.Run(fmt.Sprintf("%s/size=%d", typ.name, size), func(t *testing.T) {
				typ.run(t, size)
			})
		}
	}
}

func TestNumericToBoolSIMDFloatSpecialValues(t *testing.T) {
	checkNumericToBoolSIMD(t, arrow.FLOAT32, []float32{
		0, float32(math.Copysign(0, -1)), float32(math.NaN()),
		float32(math.Inf(1)), float32(math.Inf(-1)), 1, -1, 0, 1,
	})
	checkNumericToBoolSIMD(t, arrow.FLOAT64, []float64{
		0, math.Copysign(0, -1), math.NaN(), math.Inf(1),
		math.Inf(-1), 1, -1, 0, 1, math.NaN(),
	})
}
