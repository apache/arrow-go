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

package encoding

// ALP implementations must use identical scaling arithmetic. Rounding decides
// whether a value is packed or stored verbatim as an exception, so even a
// one-bit difference can change the page contents.

import (
	"math"

	"github.com/apache/arrow-go/v18/parquet"
)

const (
	alpMagicFloat32 = float32(12582912.0)         // 2^22 + 2^23
	alpMagicFloat64 = float64(6755399441055744.0) // 2^51 + 2^52

	// The largest and smallest values of each type that convert to an integer
	// of the matching width. They stop short of the integer limits because
	// neither type can represent every integer that close to them.
	alpEncodingUpperFloat32 = float32(2147483520.0)
	alpEncodingLowerFloat32 = float32(-2147483520.0)
	alpEncodingUpperFloat64 = float64(9223372036854774784.0)
	alpEncodingLowerFloat64 = float64(-9223372036854774784.0)
)

type alpFloat interface {
	float32 | float64
}

// The exponent and factor index these tables, whose lengths define the search
// space. A float32 multiplier above 1e10 pushes every value outside int32.
//
// Scaling multiplies by a tabulated 10^-i rather than dividing by 10^i. The two
// differ by up to one unit in the last place, and every ALP implementation has
// to pick the same one or they decode each other's pages to different values.
var (
	alpFloatPow10 = [...]float32{
		1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	}
	alpFloatNegPow10 = [...]float32{
		1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10,
	}
	alpDoublePow10 = [...]float64{
		1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
		1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
	}
	alpDoubleNegPow10 = [...]float64{
		1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
		1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
	}
)

func alpMagic[T alpFloat]() T {
	var z T
	switch any(z).(type) {
	case float32:
		return T(alpMagicFloat32)
	case float64:
		return T(alpMagicFloat64)
	}
	panic("parquet: ALP: unsupported float type")
}

func alpPow10[T alpFloat](i int) T {
	var z T
	switch any(z).(type) {
	case float32:
		return T(alpFloatPow10[i])
	case float64:
		return T(alpDoublePow10[i])
	}
	panic("parquet: ALP: unsupported float type")
}

func alpNegPow10[T alpFloat](i int) T {
	var z T
	switch any(z).(type) {
	case float32:
		return T(alpFloatNegPow10[i])
	case float64:
		return T(alpDoubleNegPow10[i])
	}
	panic("parquet: ALP: unsupported float type")
}

func alpNumExponents[T alpFloat]() int {
	var z T
	switch any(z).(type) {
	case float32:
		return len(alpFloatPow10)
	case float64:
		return len(alpDoublePow10)
	}
	panic("parquet: ALP: unsupported float type")
}

// The frame of reference and exception values use the physical type's width.
func alpEncodedBytes[T alpFloat]() int {
	var z T
	switch any(z).(type) {
	case float32:
		return 4
	case float64:
		return 8
	}
	panic("parquet: ALP: unsupported float type")
}

// alpEncodingLimits returns the scaled range that converts to an integer. The
// bounds stop short of the integer limits because floats cannot represent every
// integer near them.
func alpEncodingLimits[T alpFloat]() (lo, hi T) {
	var z T
	switch any(z).(type) {
	case float32:
		return T(alpEncodingLowerFloat32), T(alpEncodingUpperFloat32)
	case float64:
		return T(alpEncodingLowerFloat64), T(alpEncodingUpperFloat64)
	}
	panic("parquet: ALP: unsupported float type")
}

func alpParquetType[T alpFloat]() parquet.Type {
	var z T
	switch any(z).(type) {
	case float32:
		return parquet.Types.Float
	case float64:
		return parquet.Types.Double
	}
	panic("parquet: ALP: unsupported float type")
}

func alpFloatToBits[T alpFloat](v T) uint64 {
	switch f := any(v).(type) {
	case float32:
		return uint64(math.Float32bits(f))
	case float64:
		return math.Float64bits(f)
	}
	panic("parquet: ALP: unsupported float type")
}

func alpFloatFromBits[T alpFloat](b uint64) T {
	var z T
	switch any(z).(type) {
	case float32:
		return T(math.Float32frombits(uint32(b)))
	case float64:
		return T(math.Float64frombits(b))
	}
	panic("parquet: ALP: unsupported float type")
}

// alpScaling holds the constants of one exponent and factor pair. Encoding and
// decoding look them up once per vector rather than once per value.
type alpScaling[T alpFloat] struct {
	// Encoding multiplies by 10^exponent then 10^-factor; decoding multiplies
	// by 10^factor then 10^-exponent.
	encExp, encFac T
	decFac, decExp T

	lo, hi, magic T
	// The range of the integer type a vector stores: int32 for FLOAT, int64
	// for DOUBLE.
	intMin, intMax int64
}

func newALPScaling[T alpFloat](exponent, factor int) alpScaling[T] {
	lo, hi := alpEncodingLimits[T]()
	intMin, intMax := int64(math.MinInt64), int64(math.MaxInt64)
	if alpEncodedBytes[T]() == 4 {
		intMin, intMax = math.MinInt32, math.MaxInt32
	}
	return alpScaling[T]{
		encExp: alpPow10[T](exponent),
		encFac: alpNegPow10[T](factor),
		decFac: alpPow10[T](factor),
		decExp: alpNegPow10[T](exponent),
		lo:     lo,
		hi:     hi,
		magic:  alpMagic[T](),
		intMin: intMin,
		intMax: intMax,
	}
}

// round adds and subtracts a magic constant, which rounds to the nearest
// integer without a library call.
func (s *alpScaling[T]) round(v T) int64 {
	if v >= 0 {
		return int64((v + s.magic) - s.magic)
	}
	return int64((v - s.magic) + s.magic)
}

// encode returns the integer v scales to. NaNs, infinities, out-of-range
// values, and negative zero cannot round-trip through an integer, so they get a
// sentinel that cannot decode to the original value.
func (s *alpScaling[T]) encode(v T) int64 {
	// The conversion keeps the product from fusing with the addition in round,
	// so every platform rounds the same product.
	scaled := T(v * s.encExp * s.encFac)
	if scaled != scaled || scaled > s.hi || scaled < s.lo || (scaled == 0 && math.Signbit(float64(scaled))) {
		return int64(s.hi)
	}
	return s.round(scaled)
}

// decode is the normative ALP decoding. The multiplication order is part of
// the format; reassociation can change the final bit.
func (s *alpScaling[T]) decode(encoded int64) T {
	return T(encoded) * s.decFac * s.decExp
}

// tryEncode reports whether v survives the required round trip. Values that
// fail are stored verbatim as exceptions.
//
// Rounding a scaled value just under the limit can still reach 2^31, which a
// FLOAT vector cannot store: readers rebuild its integers in int32. Such a
// value is an exception even though it would round-trip in int64.
func (s *alpScaling[T]) tryEncode(v T) (encoded int64, ok bool) {
	encoded = s.encode(v)
	if encoded < s.intMin || encoded > s.intMax {
		return encoded, false
	}
	return encoded, s.decode(encoded) == v
}
