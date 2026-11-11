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

// Package decimal256 provides the Decimal256 API of [decimal] under its original
// names. Its types and functions are aliases of and forwarders to [decimal].
package decimal256

import (
	"math/big"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

const (
	MaxPrecision = 76
	MaxScale     = 76
)

// Num is an alias of [decimal.Decimal256].
type Num = decimal.Decimal256

func GetMaxValue(prec int32) Num { return decimal.Decimal256MaxValue(prec) }

// New returns a new signed 256-bit integer value where x1 contains
// the highest bits with the rest of the values in order down to the
// lowest bits
//
//	ie: New(1, 2, 3, 4) returns with the elements in little-endian order
//	    {4, 3, 2, 1} but each value is still represented as the native endianness
func New(x1, x2, x3, x4 uint64) Num { return decimal.NewDecimal256(x1, x2, x3, x4) }

func FromDecimal128(n decimal128.Num) Num { return decimal.Decimal256FromDecimal128(n) }

func FromU64(v uint64) Num { return decimal.Decimal256FromU64(v) }

func FromI64(v int64) Num { return decimal.Decimal256FromI64(v) }

func FromString(v string, prec, scale int32) (n Num, err error) {
	return decimal.Decimal256FromString(v, prec, scale)
}

func FromFloat32(v float32, prec, scale int32) (Num, error) {
	return decimal.Decimal256FromFloat32(v, prec, scale)
}

func FromFloat64(v float64, prec, scale int32) (Num, error) {
	return decimal.Decimal256FromFloat(v, prec, scale)
}

// FromBigInt converts a big.Int to a Num. It panics if v.BitLen() > 255.
func FromBigInt(v *big.Int) (n Num) { return decimal.Decimal256FromBigInt(v) }

// Max returns the largest Decimal256 that was passed in the arguments
func Max(first Num, rest ...Num) Num { return decimal.Decimal256Max(first, rest...) }

// Min returns the smallest Decimal256 that was passed in the arguments
func Min(first Num, rest ...Num) Num { return decimal.Decimal256Min(first, rest...) }

func GetScaleMultiplier(pow int) Num { return decimal.Decimal256ScaleMultiplier(pow) }

func GetHalfScaleMultiplier(pow int) Num { return decimal.Decimal256HalfScaleMultiplier(pow) }

// Calling every method of Num here keeps their inline bodies in the export data of
// this package. Without it, packages that import decimal256 but not decimal stop
// inlining them, see https://github.com/golang/go/issues/82474. Remove it when the
// minimum supported Go version includes the fix.
var _ = func(n Num) {
	_ = n.Abs()
	_ = n.Add(*new(Num))
	_ = n.Array()
	_ = n.BigInt()
	_ = n.Cmp(*new(Num))
	_, _ = n.Div(*new(Num))
	_ = n.FitsInPrecision(0)
	_ = n.Greater(*new(Num))
	_ = n.GreaterEqual(*new(Num))
	_ = n.IncreaseScaleBy(0)
	_ = n.Less(*new(Num))
	_ = n.LessEqual(*new(Num))
	_ = n.LowBits()
	_ = n.Mul(*new(Num))
	_ = n.Negate()
	_ = n.Pow(*new(Num))
	_ = n.ReduceScaleBy(0, false)
	_, _ = n.Rescale(0, 0)
	_ = n.Sign()
	_ = n.Sub(*new(Num))
	_ = n.ToBigFloat(0)
	_ = n.ToFloat32(0)
	_ = n.ToFloat64(0)
	_ = n.ToString(0)
}
