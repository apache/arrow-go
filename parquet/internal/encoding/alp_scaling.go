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

	"github.com/apache/arrow-go/v18/arrow"
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

// alpScale64 scales every value to the integer alpScaling.encode gives it and
// checks, as tryEncode does, that the integer decodes back to the value. It
// writes all the integers to encoded and the positions of the values that
// fail, in order, to excPos. Both must hold len(values). It returns how many
// failed.
//
// This is the loop every value of a column goes through, so it is written for
// one type and avoids what alpScaling cannot. Nothing in it branches on the
// data: an exception costs what any other value costs, where a branch is
// mispredicted wherever the two mix. And it leaves the range of the integers
// to alpIntRange, because tracking a minimum and a maximum here would make
// every value wait for the one before.
func alpScale64(values []float64, encExp, encFac, decFac, decExp float64, encoded []int64, excPos []uint16) (numExc int) {
	encoded = encoded[:len(values)]
	excPos = excPos[:len(values)]
	// The same values as integers, so that comparing bits and taking the sign
	// do not have to move each one out of a floating-point register.
	raw := arrow.Uint64Traits.CastFromBytes(arrow.Float64Traits.CastToBytes(values))[:len(values)]
	magicBits := math.Float64bits(alpMagicFloat64)
	for i, v := range values {
		bits := raw[i]
		// The conversion keeps the product from fusing with the addition
		// below, so every platform rounds the same product.
		scaled := float64(v * encExp * encFac)
		// The magic constant takes the value's sign, which is the scaled
		// value's sign too: adding and subtracting it then rounds as
		// alpScaling.round does.
		magic := math.Float64frombits(magicBits | bits&(1<<63))
		e := int64((scaled + magic) - magic)
		// A NaN fails the comparison too.
		if !(math.Abs(scaled) <= alpEncodingUpperFloat64) {
			e = int64(alpEncodingUpperFloat64)
		}
		encoded[i] = e
		excPos[numExc] = uint16(i)
		// Comparing bits rather than values also rejects -0.0, which scales
		// to 0 and decodes to 0.0.
		if math.Float64bits(float64(e)*decFac*decExp) != bits {
			numExc++
		}
	}
	return numExc
}

// alpScale32 is the FLOAT form of alpScale64.
func alpScale32(values []float32, encExp, encFac, decFac, decExp float32, encoded []int64, excPos []uint16) (numExc int) {
	encoded = encoded[:len(values)]
	excPos = excPos[:len(values)]
	raw := arrow.Uint32Traits.CastFromBytes(arrow.Float32Traits.CastToBytes(values))[:len(values)]
	magicBits := math.Float32bits(alpMagicFloat32)
	for i, v := range values {
		bits := raw[i]
		scaled := float32(v * encExp * encFac)
		magic := math.Float32frombits(magicBits | bits&(1<<31))
		e := int64((scaled + magic) - magic)
		if !(float32(math.Abs(float64(scaled))) <= alpEncodingUpperFloat32) {
			e = int64(alpEncodingUpperFloat32)
		}
		encoded[i] = e
		excPos[numExc] = uint16(i)
		// An integer outside int32 is an exception as well. The two
		// differences are combined so that there is one test, which compiles
		// to no branch, rather than two.
		if (e^int64(int32(e)))|int64(math.Float32bits(float32(e)*decFac*decExp)^bits) != 0 {
			numExc++
		}
	}
	return numExc
}

// alpScale runs the loop of T's width with the constants of one exponent and
// factor pair.
func alpScale[T alpFloat](values []T, exponent, factor int, encoded []int64, excPos []uint16) (numExc int) {
	switch values := any(values).(type) {
	case []float64:
		return alpScale64(values, alpDoublePow10[exponent], alpDoubleNegPow10[factor],
			alpDoublePow10[factor], alpDoubleNegPow10[exponent], encoded, excPos)
	case []float32:
		return alpScale32(values, alpFloatPow10[exponent], alpFloatNegPow10[factor],
			alpFloatPow10[factor], alpFloatNegPow10[exponent], encoded, excPos)
	}
	panic("parquet: ALP: unsupported float type")
}

// alpIntRange returns the smallest and the largest of the integers alpScale
// wrote, leaving out those at the exception positions, of which there must be
// fewer than there are integers. It overwrites each exception's integer with
// the smallest.
func alpIntRange(encoded []int64, excPos []uint16) (minValue, maxValue int64) {
	if len(excPos) > 0 {
		// The exceptions are in order, so the first position that is not its
		// own index in excPos holds an integer that counts. Being within the
		// range, it stands in for the exceptions while the range is found.
		keep := 0
		for keep < len(excPos) && int(excPos[keep]) == keep {
			keep++
		}
		fill := encoded[keep]
		for _, pos := range excPos {
			encoded[pos] = fill
		}
	}

	// Four of each, so that an integer does not wait for its neighbors.
	min0, min1, min2, min3 := int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64)
	max0, max1, max2, max3 := int64(math.MinInt64), int64(math.MinInt64), int64(math.MinInt64), int64(math.MinInt64)
	i := 0
	for ; i+4 <= len(encoded); i += 4 {
		q := encoded[i : i+4 : i+4]
		min0, max0 = min(min0, q[0]), max(max0, q[0])
		min1, max1 = min(min1, q[1]), max(max1, q[1])
		min2, max2 = min(min2, q[2]), max(max2, q[2])
		min3, max3 = min(min3, q[3]), max(max3, q[3])
	}
	for ; i < len(encoded); i++ {
		min0, max0 = min(min0, encoded[i]), max(max0, encoded[i])
	}
	minValue, maxValue = min(min0, min1, min2, min3), max(max0, max1, max2, max3)

	// An exception still takes up a slot among the packed integers. The
	// smallest integer packs to zero there, whatever the width.
	for _, pos := range excPos {
		encoded[pos] = minValue
	}
	return minValue, maxValue
}
