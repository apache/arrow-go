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

// The arithmetic that turns a float into the integer ALP stores, and back.
//
// Every implementation has to compute these products the same way, down to the
// last bit: a value that survives the round trip is packed, and one that does
// not is stored verbatim as an exception, so a reader that rounds differently
// disagrees with the writer about which values the page even contains.

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

// alpFloat is the set of column types ALP encodes.
type alpFloat interface {
	float32 | float64
}

// The exponent and the factor index these tables, so the lengths set the search
// space: a float32 multiplier never needs more than 1e10, because a larger one
// pushes every value out of int32.
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

// The helpers below give the generic code the one constant or width that
// differs between float32 and float64. Each switch resolves at compile time
// once the compiler instantiates the function.

// alpMagic returns the constant that rounds a value of type T when added to it
// and then subtracted again.
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

// alpPow10 returns 10^i as a value of type T.
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

// alpNegPow10 returns 10^-i as a value of type T.
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

// alpNumExponents returns how many exponents the search may try for type T.
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

// alpEncodedBytes returns the width ALP writes an integer of type T at. The
// frame of reference and the exception values both use that width.
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

// alpEncodingLimits returns the range a scaled value of type T has to stay
// inside to convert to an integer. The bounds are the largest and smallest
// values of T that survive the conversion, which fall short of the integer
// type's own limits because T cannot represent every integer near them.
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

// alpParquetType returns the physical type a column of type T has.
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

// alpFloatToBits returns the bit pattern of v, so that callers can compare two
// values without treating negative zero as equal to zero.
func alpFloatToBits[T alpFloat](v T) uint64 {
	switch f := any(v).(type) {
	case float32:
		return uint64(math.Float32bits(f))
	case float64:
		return math.Float64bits(f)
	}
	panic("parquet: ALP: unsupported float type")
}

// alpFloatFromBits is the inverse of alpFloatToBits.
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

// alpFastRound rounds v to the nearest integer. Adding and then subtracting a
// large constant discards the fractional bits, which is quicker than a library
// call and is what the reference implementation does.
func alpFastRound[T alpFloat](v T) int64 {
	magic := alpMagic[T]()
	if v >= 0 {
		return int64((v + magic) - magic)
	}
	return int64((v - magic) + magic)
}

// alpImpossibleToEncode reports whether a scaled value has no integer form: a
// NaN, either infinity, a magnitude past what the integer type holds, or a
// negative zero, whose sign the round trip would drop.
func alpImpossibleToEncode[T alpFloat](v T) bool {
	lo, hi := alpEncodingLimits[T]()
	f := float64(v)
	return math.IsNaN(f) || v > hi || v < lo || (f == 0 && math.Signbit(f))
}

// alpNumberToInt rounds a scaled value, or returns the upper encoding limit for
// one that cannot be rounded. The limit acts as a sentinel: decoding it cannot
// return the original value, so the caller finds the value in the exceptions.
func alpNumberToInt[T alpFloat](v T) int64 {
	if alpImpossibleToEncode(v) {
		_, hi := alpEncodingLimits[T]()
		return int64(hi)
	}
	return alpFastRound(v)
}

// alpEncode scales v and rounds it to the integer ALP stores.
func alpEncode[T alpFloat](v T, exponent, factor int) int64 {
	return alpNumberToInt(v * alpPow10[T](exponent) * alpNegPow10[T](factor))
}

// alpDecode is the inverse of alpEncode. The order of the two multiplications
// is part of the format: reassociating them changes the last bit of the result.
func alpDecode[T alpFloat](encoded int64, exponent, factor int) T {
	return T(encoded) * alpPow10[T](factor) * alpNegPow10[T](exponent)
}

// alpTryEncode scales v and reports whether the round trip returns it unchanged.
// A value that fails has to be stored verbatim as an exception, which is the
// case for anything with no integer form: a NaN is never equal to itself, and an
// infinity, an out-of-range magnitude, or a negative zero all encode to the
// sentinel, which decodes to something else.
func alpTryEncode[T alpFloat](v T, exponent, factor int) (encoded int64, ok bool) {
	encoded = alpEncode(v, exponent, factor)
	return encoded, alpDecode[T](encoded, exponent, factor) == v
}
