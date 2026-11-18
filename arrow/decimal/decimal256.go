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

package decimal

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/internal/debug"
	"github.com/apache/arrow-go/v18/arrow/internal/decimalstr"
)

const (
	maxPrecision256 = 76
	maxScale256     = 76
)

func Decimal256MaxValue(prec int32) Decimal256 {
	return scaleMultipliers256[prec].Sub(Decimal256FromU64(1))
}

type Decimal256 struct {
	// arr[0] is the lowest bits, arr[3] is the highest bits
	arr [4]uint64
}

// NewDecimal256 returns a new signed 256-bit integer value where x1 contains
// the highest bits with the rest of the values in order down to the
// lowest bits
//
//	ie: NewDecimal256(1, 2, 3, 4) returns with the elements in little-endian order
//	    {4, 3, 2, 1} but each value is still represented as the native endianness
func NewDecimal256(x1, x2, x3, x4 uint64) Decimal256 {
	return Decimal256{[4]uint64{x4, x3, x2, x1}}
}

func (n Decimal256) Array() [4]uint64 { return n.arr }

func (n Decimal256) LowBits() uint64 { return n.arr[0] }

// Like Decimal256FromI64, this stays a single composite literal.
func Decimal256FromDecimal128(n decimal128.Num) Decimal256 {
	top := uint64(n.HighBits() >> 63)
	return Decimal256{[4]uint64{n.LowBits(), uint64(n.HighBits()), top, top}}
}

func Decimal256FromU64(v uint64) Decimal256 {
	return Decimal256{[4]uint64{v, 0, 0, 0}}
}

// Keep this a single composite literal. Callers that inline it then build the
// value in place instead of copying it through stack temporaries.
func Decimal256FromI64(v int64) Decimal256 {
	s := uint64(v >> 63)
	return Decimal256{[4]uint64{uint64(v), s, s, s}}
}

func (n Decimal256) Negate() Decimal256 {
	var carry uint64 = 1
	for i := range n.arr {
		n.arr[i] = ^n.arr[i] + carry
		if n.arr[i] != 0 {
			carry = 0
		}
	}
	return n
}

func (n Decimal256) Add(rhs Decimal256) Decimal256 {
	var carry uint64
	for i, v := range n.arr {
		n.arr[i], carry = bits.Add64(v, rhs.arr[i], carry)
	}
	return n
}

func (n Decimal256) Sub(rhs Decimal256) Decimal256 {
	return n.Add(rhs.Negate())
}

func (n Decimal256) Mul(rhs Decimal256) Decimal256 {
	b := n.BigInt()
	return Decimal256FromBigInt(b.Mul(b, rhs.BigInt()))
}

func (n Decimal256) Div(rhs Decimal256) (res, rem Decimal256) {
	b := n.BigInt()
	out, remainder := b.QuoRem(b, rhs.BigInt(), &big.Int{})
	return Decimal256FromBigInt(out), Decimal256FromBigInt(remainder)
}

func (n Decimal256) Pow(rhs Decimal256) Decimal256 {
	b := n.BigInt()
	return Decimal256FromBigInt(b.Exp(b, rhs.BigInt(), nil))
}

func Decimal256FromString(v string, prec, scale int32) (n Decimal256, err error) {
	if lit, ok := decimalstr.Parse(v); ok {
		return fromDecimalLiteral256(lit, prec, scale)
	}
	return fromFloatLiteral256(v, prec, scale)
}

func fromDecimalLiteral256(lit decimalstr.Parsed, prec, scale int32) (n Decimal256, err error) {
	if scale < 0 {
		// Reject an unscaled magnitude that would not fit in signed 256-bit
		// storage, before using the scale.
		if lit.AtLeastTwoPower(0, 255) {
			return Decimal256{}, errors.New("bitlen too large for decimal256")
		}
		if scale < -maxScale256 {
			return Decimal256{}, fmt.Errorf("scale %d out of range for decimal256, must be between %d and %d", scale, -maxScale256, maxScale256)
		}
	} else if scale > maxScale256 {
		return Decimal256{}, fmt.Errorf("scale %d out of range for decimal256, must be between %d and %d", scale, -maxScale256, maxScale256)
	} else if lit.AtLeastTwoPower(int64(scale), 256) {
		return Decimal256{}, errors.New("bitlen too large for decimal256")
	}

	val, ok := lit.ScaleAndRound(int64(scale))
	if !ok || val.BitLen() > 255 {
		return Decimal256{}, errors.New("bitlen too large for decimal256")
	}
	n = Decimal256FromBigInt(val)
	if !n.FitsInPrecision(prec) {
		err = fmt.Errorf("value %v doesn't fit in precision %d", n, prec)
	}
	return
}

func fromFloatLiteral256(v string, prec, scale int32) (n Decimal256, err error) {
	// time for some math!
	// Our input precision means "number of digits of precision" but the
	// math/big library refers to precision in floating point terms
	// where it refers to the "number of bits of precision in the mantissa".
	// So we need to figure out how many bits we should use for precision,
	// based on the input precision. Too much precision and we aren't rounding
	// when we should. Too little precision and we round when we shouldn't.
	//
	// In general, the number of decimal digits you get from a given number
	// of bits will be:
	//
	//	digits = log[base 10](2^nbits)
	//
	// it thus follows that:
	//
	//	digits = nbits * log[base 10](2)
	//  nbits = digits / log[base 10](2)
	//
	// So we need to account for our scale since we're going to be multiplying
	// by 10^scale in order to get the integral value we're actually going to use
	// So to get our number of bits we do:
	//
	// 	(prec + scale + 1) / log[base10](2)
	//
	// Finally, we still have a sign bit, so we -1 to account for the sign bit.
	// Aren't floating point numbers fun?
	var precInBits = uint(math.Round(float64(prec+scale+1)/math.Log10(2))) + 1

	var out *big.Float
	out, _, err = big.ParseFloat(v, 10, 255, big.ToNearestEven)
	if err != nil {
		return
	}
	if out.IsInf() {
		return Decimal256{}, fmt.Errorf("cannot convert %s to decimal256", v)
	}

	if scale < 0 {
		// Check the magnitude before converting, which would allocate as many
		// bits as the value has.
		if out.MantExp(nil) > 255 {
			return Decimal256{}, errors.New("bitlen too large for decimal256")
		}
		if scale < -maxScale256 {
			return Decimal256{}, fmt.Errorf("scale %d out of range for decimal256, must be between %d and %d", scale, -maxScale256, maxScale256)
		}
	} else if scale > maxScale256 {
		return Decimal256{}, fmt.Errorf("scale %d out of range for decimal256, must be between %d and %d", scale, -maxScale256, maxScale256)
	}

	if scale != 0 {
		if scale > 0 {
			out.Mul(out, (&big.Float{}).SetInt(scaleMultipliers256[scale].BigInt())).SetPrec(precInBits)
		} else {
			out.Quo(out, (&big.Float{}).SetInt(scaleMultipliers256[-scale].BigInt())).SetPrec(precInBits)
		}
		// Reject before rounding and converting, which would allocate as many
		// bits as the value has.
		if scale > 0 && (out.IsInf() || out.MantExp(nil) > 256) {
			return Decimal256{}, errors.New("bitlen too large for decimal256")
		}
	}
	val := decimalstr.RoundScaled(out, v, int64(scale))
	if val.BitLen() > 255 {
		return Decimal256{}, errors.New("bitlen too large for decimal256")
	}
	n = Decimal256FromBigInt(val)
	if !n.FitsInPrecision(prec) {
		err = fmt.Errorf("value %v doesn't fit in precision %d", n, prec)
	}
	return
}

func Decimal256FromFloat32(v float32, prec, scale int32) (Decimal256, error) {
	debug.Assert(prec > 0 && prec <= 76, "invalid precision for converting to decimal256")

	if math.IsInf(float64(v), 0) {
		return Decimal256{}, fmt.Errorf("cannot convert %f to decimal256", v)
	}

	if v < 0 {
		dec, err := fromPositiveFloat32_256(-v, prec, scale)
		if err != nil {
			return dec, err
		}
		return dec.Negate(), nil
	}
	return fromPositiveFloat32_256(v, prec, scale)
}

func Decimal256FromFloat(v float64, prec, scale int32) (Decimal256, error) {
	debug.Assert(prec > 0 && prec <= 76, "invalid precision for converting to decimal256")

	if math.IsInf(v, 0) {
		return Decimal256{}, fmt.Errorf("cannot convert %f to decimal256", v)
	}

	if v < 0 {
		dec, err := fromPositiveFloat64_256(-v, prec, scale)
		if err != nil {
			return dec, err
		}
		return dec.Negate(), nil
	}
	return fromPositiveFloat64_256(v, prec, scale)
}

// this has to exist despite sharing some code with fromPositiveFloat64_256
// because if we don't do the casts back to float32 in between each
// step, we end up with a significantly different answer!
// Aren't floating point values so much fun?
//
// example value to use:
//
//	v := float32(1.8446746e+15)
//
// You'll end up with a different values if you do:
//
//	Decimal256FromFloat(float64(v), 20, 4)
//
// vs
//
//	Decimal256FromFloat32(v, 20, 4)
//
// because float64(v) == 1844674629206016 rather than 1844674600000000
func fromPositiveFloat32_256(v float32, prec, scale int32) (Decimal256, error) {
	val, err := scalePositiveFloat64_256(float64(v), prec, scale)
	if err != nil {
		return Decimal256{}, err
	}

	v = float32(val)
	if math.IsInf(float64(v), 1) {
		// the scaled value is past the float32 range but can still fit
		// in a decimal256, so split it as a float64 instead
		return splitPositiveFloat64_256(val), nil
	}

	var arr [4]float32
	arr[3] = float32(math.Floor(math.Ldexp(float64(v), -192)))
	v -= float32(math.Ldexp(float64(arr[3]), 192))
	arr[2] = float32(math.Floor(math.Ldexp(float64(v), -128)))
	v -= float32(math.Ldexp(float64(arr[2]), 128))
	arr[1] = float32(math.Floor(math.Ldexp(float64(v), -64)))
	v -= float32(math.Ldexp(float64(arr[1]), 64))
	arr[0] = v

	debug.Assert(arr[3] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[3] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[2] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[2] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[1] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[1] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[0] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[0] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	return Decimal256{[4]uint64{uint64(arr[0]), uint64(arr[1]), uint64(arr[2]), uint64(arr[3])}}, nil
}

func scalePositiveFloat64_256(v float64, prec, scale int32) (float64, error) {
	var pscale float64
	if scale >= -76 && scale <= 76 {
		pscale = float64PowersOfTen256[scale+76]
	} else {
		pscale = math.Pow10(int(scale))
	}

	v *= pscale
	v = math.RoundToEven(v)
	maxabs := float64PowersOfTen256[prec+76]
	if v <= -maxabs || v >= maxabs {
		return 0, fmt.Errorf("cannot convert %f to decimal256(precision=%d, scale=%d): overflow",
			v, prec, scale)
	}
	return v, nil
}

func fromPositiveFloat64_256(v float64, prec, scale int32) (Decimal256, error) {
	val, err := scalePositiveFloat64_256(v, prec, scale)
	if err != nil {
		return Decimal256{}, err
	}

	return splitPositiveFloat64_256(val), nil
}

// splitPositiveFloat64_256 converts an already scaled and rounded non-negative
// float64 into the 64-bit words of a Decimal256.
func splitPositiveFloat64_256(val float64) Decimal256 {
	var arr [4]float64
	arr[3] = math.Floor(math.Ldexp(val, -192))
	val -= math.Ldexp(arr[3], 192)
	arr[2] = math.Floor(math.Ldexp(val, -128))
	val -= math.Ldexp(arr[2], 128)
	arr[1] = math.Floor(math.Ldexp(val, -64))
	val -= math.Ldexp(arr[1], 64)
	arr[0] = val

	debug.Assert(arr[3] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[3] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[2] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[2] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[1] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[1] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	debug.Assert(arr[0] >= 0, "bad conversion float64 to decimal256")
	debug.Assert(arr[0] < 1.8446744073709552e+19, "bad conversion float64 to decimal256") // 2**64
	return Decimal256{[4]uint64{uint64(arr[0]), uint64(arr[1]), uint64(arr[2]), uint64(arr[3])}}
}

func (n Decimal256) tofloat64Positive(scale int32) float64 {
	const (
		twoTo64  float64 = 1.8446744073709552e+19
		twoTo128 float64 = 3.402823669209385e+38
		twoTo192 float64 = 6.277101735386681e+57
	)

	x := float64(n.arr[3]) * twoTo192
	x += float64(n.arr[2]) * twoTo128
	x += float64(n.arr[1]) * twoTo64
	x += float64(n.arr[0])

	if scale >= -76 && scale <= 76 {
		return x * float64PowersOfTen256[-scale+76]
	}

	return x * math.Pow10(-int(scale))
}

func (n Decimal256) ToFloat32(scale int32) float32 { return float32(n.ToFloat64(scale)) }

func (n Decimal256) ToFloat64(scale int32) float64 {
	if n.Sign() < 0 {
		return -n.Negate().tofloat64Positive(scale)
	}
	return n.tofloat64Positive(scale)
}

func (n Decimal256) ToBigFloat(scale int32) *big.Float {
	if scale < -maxScale256 || scale > maxScale256 {
		panic("arrow/decimal256: scale out of range")
	}
	f := (&big.Float{}).SetInt(n.BigInt())
	if scale < 0 {
		f.SetPrec(256).Mul(f, (&big.Float{}).SetInt(scaleMultipliers256[-scale].BigInt()))
	} else {
		f.SetPrec(256).Quo(f, (&big.Float{}).SetInt(scaleMultipliers256[scale].BigInt()))
	}
	return f
}

func (n Decimal256) Sign() int {
	if n == (Decimal256{}) {
		return 0
	}
	return int(1 | (int64(n.arr[3]) >> 63))
}

// Decimal256FromBigInt converts a big.Int to a Decimal256. It panics if v.BitLen() > 255.
func Decimal256FromBigInt(v *big.Int) (n Decimal256) {
	bitlen := v.BitLen()
	if bitlen > 255 {
		panic("arrow/decimal256: cannot represent value larger than 256bits")
	} else if bitlen == 0 {
		return
	}

	b := v.Bits()
	if bits.UintSize == 32 {
		// Each 64-bit limb contains two native-sized big.Int words.
		for i, word := range b {
			n.arr[i/2] |= uint64(word) << (32 * (i % 2))
		}
	} else {
		for i, word := range b {
			n.arr[i] = uint64(word)
		}
	}
	if v.Sign() < 0 {
		return n.Negate()
	}
	return
}

func toBigIntPositive256(n Decimal256) *big.Int {
	if bits.UintSize == 32 {
		return new(big.Int).SetBits([]big.Word{
			big.Word(n.arr[0]), big.Word(n.arr[0] >> 32),
			big.Word(n.arr[1]), big.Word(n.arr[1] >> 32),
			big.Word(n.arr[2]), big.Word(n.arr[2] >> 32),
			big.Word(n.arr[3]), big.Word(n.arr[3] >> 32),
		})
	}
	return new(big.Int).SetBits([]big.Word{big.Word(n.arr[0]), big.Word(n.arr[1]), big.Word(n.arr[2]), big.Word(n.arr[3])})
}

func (n Decimal256) BigInt() *big.Int {
	if n.Sign() < 0 {
		b := toBigIntPositive256(n.Negate())
		return b.Neg(b)
	}
	return toBigIntPositive256(n)
}

// Greater returns true if the value represented by n is > other
func (n Decimal256) Greater(other Decimal256) bool {
	return other.Less(n)
}

// GreaterEqual returns true if the value represented by n is >= other
func (n Decimal256) GreaterEqual(other Decimal256) bool {
	return !n.Less(other)
}

// Less returns true if the value represented by n is < other
func (n Decimal256) Less(other Decimal256) bool {
	switch {
	case n.arr[3] != other.arr[3]:
		return int64(n.arr[3]) < int64(other.arr[3])
	case n.arr[2] != other.arr[2]:
		return n.arr[2] < other.arr[2]
	case n.arr[1] != other.arr[1]:
		return n.arr[1] < other.arr[1]
	}
	return n.arr[0] < other.arr[0]
}

// LessEqual returns true if the value represented by n is <= other
func (n Decimal256) LessEqual(other Decimal256) bool {
	return !n.Greater(other)
}

// Decimal256Max returns the largest Decimal256 that was passed in the arguments
func Decimal256Max(first Decimal256, rest ...Decimal256) Decimal256 {
	answer := first
	for _, number := range rest {
		if number.Greater(answer) {
			answer = number
		}
	}
	return answer
}

// Decimal256Min returns the smallest Decimal256 that was passed in the arguments
func Decimal256Min(first Decimal256, rest ...Decimal256) Decimal256 {
	answer := first
	for _, number := range rest {
		if number.Less(answer) {
			answer = number
		}
	}
	return answer
}

// Cmp compares the numbers represented by n and other and returns:
//
//	+1 if n > other
//	 0 if n == other
//	-1 if n < other
func (n Decimal256) Cmp(other Decimal256) int {
	switch {
	case n.Greater(other):
		return 1
	case n.Less(other):
		return -1
	}
	return 0
}

func (n Decimal256) IncreaseScaleBy(increase int32) Decimal256 {
	debug.Assert(increase >= 0, "invalid amount to increase scale by")
	debug.Assert(increase <= 76, "invalid amount to increase scale by")

	v := scaleMultipliers256[increase].BigInt()
	return Decimal256FromBigInt(v.Mul(n.BigInt(), v))
}

func (n Decimal256) ReduceScaleBy(reduce int32, round bool) Decimal256 {
	debug.Assert(reduce >= 0, "invalid amount to reduce scale by")
	debug.Assert(reduce <= 76, "invalid amount to reduce scale by")

	if reduce == 0 {
		return n
	}

	divisor := scaleMultipliers256[reduce].BigInt()
	result, remainder := divisor.QuoRem(n.BigInt(), divisor, new(big.Int))
	if round {
		divisorHalf := scaleMultipliersHalf256[reduce]
		if remainder.Abs(remainder).Cmp(divisorHalf.BigInt()) != -1 {
			result.Add(result, big.NewInt(int64(n.Sign())))
		}
	}
	return Decimal256FromBigInt(result)
}

func (n Decimal256) rescaleWouldCauseDataLoss(deltaScale int64, multiplier Decimal256) (out Decimal256, loss bool) {
	if deltaScale < 0 {
		var remainder Decimal256
		out, remainder = n.Div(multiplier)
		return out, remainder != Decimal256{}
	}

	result := n.BigInt()
	result.Mul(result, multiplier.BigInt())
	if result.BitLen() > 255 {
		return Decimal256{}, true
	}
	return Decimal256FromBigInt(result), false
}

func (n Decimal256) Rescale(original, newscale int32) (out Decimal256, err error) {
	if original == newscale || n == (Decimal256{}) {
		return n, nil
	}

	deltaScale := int64(newscale) - int64(original)
	absDeltaScale := deltaScale
	if absDeltaScale < 0 {
		absDeltaScale = -absDeltaScale
	}
	// Scaling any nonzero value by 10^77 or more overflows or drops every digit.
	if absDeltaScale >= int64(len(scaleMultipliers256)) {
		return Decimal256{}, errors.New("rescale data loss")
	}

	multiplier := scaleMultipliers256[absDeltaScale]
	var wouldHaveLoss bool
	out, wouldHaveLoss = n.rescaleWouldCauseDataLoss(deltaScale, multiplier)
	if wouldHaveLoss {
		err = errors.New("rescale data loss")
	}
	return
}

func (n Decimal256) Abs() Decimal256 {
	switch n.Sign() {
	case -1:
		return n.Negate()
	}
	return n
}

func (n Decimal256) FitsInPrecision(prec int32) bool {
	debug.Assert(prec > 0, "precision must be > 0")
	debug.Assert(prec <= 76, "precision must be <= 76")
	return n.Abs().Less(scaleMultipliers256[prec])
}

func (n Decimal256) ToString(scale int32) string {
	if scale < -maxScale256 || scale > maxScale256 {
		panic("arrow/decimal256: scale out of range")
	}

	value := n.BigInt().String()
	digits := len(value)
	if value[0] == '-' {
		digits--
	}
	if digits > maxPrecision256 {
		return n.ToBigFloat(scale).Text('f', int(scale))
	}
	if scale <= 0 {
		if scale < 0 && value != "0" {
			value += strings.Repeat("0", int(-scale))
		}
		return value
	}

	negative := value[0] == '-'
	if negative {
		value = value[1:]
	}
	places := int(scale)
	var result string
	if len(value) <= places {
		result = "0." + strings.Repeat("0", places-len(value)) + value
	} else {
		at := len(value) - places
		result = value[:at] + "." + value[at:]
	}
	if negative {
		return "-" + result
	}
	return result
}

func Decimal256ScaleMultiplier(pow int) Decimal256 { return scaleMultipliers256[pow] }

func Decimal256HalfScaleMultiplier(pow int) Decimal256 { return scaleMultipliersHalf256[pow] }

var (
	scaleMultipliers256 = [...]Decimal256{
		Decimal256FromU64(1),
		Decimal256FromU64(10),
		Decimal256FromU64(100),
		Decimal256FromU64(1000),
		Decimal256FromU64(10000),
		Decimal256FromU64(100000),
		Decimal256FromU64(1000000),
		Decimal256FromU64(10000000),
		Decimal256FromU64(100000000),
		Decimal256FromU64(1000000000),
		Decimal256FromU64(10000000000),
		Decimal256FromU64(100000000000),
		Decimal256FromU64(1000000000000),
		Decimal256FromU64(10000000000000),
		Decimal256FromU64(100000000000000),
		Decimal256FromU64(1000000000000000),
		Decimal256FromU64(10000000000000000),
		Decimal256FromU64(100000000000000000),
		Decimal256FromU64(1000000000000000000),
		NewDecimal256(0, 0, 0, 10000000000000000000),
		NewDecimal256(0, 0, 5, 7766279631452241920),
		NewDecimal256(0, 0, 54, 3875820019684212736),
		NewDecimal256(0, 0, 542, 1864712049423024128),
		NewDecimal256(0, 0, 5421, 200376420520689664),
		NewDecimal256(0, 0, 54210, 2003764205206896640),
		NewDecimal256(0, 0, 542101, 1590897978359414784),
		NewDecimal256(0, 0, 5421010, 15908979783594147840),
		NewDecimal256(0, 0, 54210108, 11515845246265065472),
		NewDecimal256(0, 0, 542101086, 4477988020393345024),
		NewDecimal256(0, 0, 5421010862, 7886392056514347008),
		NewDecimal256(0, 0, 54210108624, 5076944270305263616),
		NewDecimal256(0, 0, 542101086242, 13875954555633532928),
		NewDecimal256(0, 0, 5421010862427, 9632337040368467968),
		NewDecimal256(0, 0, 54210108624275, 4089650035136921600),
		NewDecimal256(0, 0, 542101086242752, 4003012203950112768),
		NewDecimal256(0, 0, 5421010862427522, 3136633892082024448),
		NewDecimal256(0, 0, 54210108624275221, 12919594847110692864),
		NewDecimal256(0, 0, 542101086242752217, 68739955140067328),
		NewDecimal256(0, 0, 5421010862427522170, 687399551400673280),
		NewDecimal256(0, 2, 17316620476856118468, 6873995514006732800),
		NewDecimal256(0, 29, 7145508105175220139, 13399722918938673152),
		NewDecimal256(0, 293, 16114848830623546549, 4870020673419870208),
		NewDecimal256(0, 2938, 13574535716559052564, 11806718586779598848),
		NewDecimal256(0, 29387, 6618148649623664334, 7386721425538678784),
		NewDecimal256(0, 293873, 10841254275107988496, 80237960548581376),
		NewDecimal256(0, 2938735, 16178822382532126880, 802379605485813760),
		NewDecimal256(0, 29387358, 14214271235644855872, 8023796054858137600),
		NewDecimal256(0, 293873587, 13015503840481697412, 6450984253743169536),
		NewDecimal256(0, 2938735877, 1027829888850112811, 9169610316303040512),
		NewDecimal256(0, 29387358770, 10278298888501128114, 17909126868192198656),
		NewDecimal256(0, 293873587705, 10549268516463523069, 13070572018536022016),
		NewDecimal256(0, 2938735877055, 13258964796087472617, 1578511669393358848),
		NewDecimal256(0, 29387358770557, 3462439444907864858, 15785116693933588480),
		NewDecimal256(0, 293873587705571, 16177650375369096972, 10277214349659471872),
		NewDecimal256(0, 2938735877055718, 14202551164014556797, 10538423128046960640),
		NewDecimal256(0, 29387358770557187, 12898303124178706663, 13150510911921848320),
		NewDecimal256(0, 293873587705571876, 18302566799529756941, 2377900603251621888),
		NewDecimal256(0, 2938735877055718769, 17004971331911604867, 5332261958806667264),
		NewDecimal256(1, 10940614696847636083, 4029016655730084128, 16429131440647569408),
		NewDecimal256(15, 17172426599928602752, 3396678409881738056, 16717361816799281152),
		NewDecimal256(159, 5703569335900062977, 15520040025107828953, 1152921504606846976),
		NewDecimal256(1593, 1695461137871974930, 7626447661401876602, 11529215046068469760),
		NewDecimal256(15930, 16954611378719749304, 2477500319180559562, 4611686018427387904),
		NewDecimal256(159309, 3525417123811528497, 6328259118096044006, 9223372036854775808),
		NewDecimal256(1593091, 16807427164405733357, 7942358959831785217, 0),
		NewDecimal256(15930919, 2053574980671369030, 5636613303479645706, 0),
		NewDecimal256(159309191, 2089005733004138687, 1025900813667802212, 0),
		NewDecimal256(1593091911, 2443313256331835254, 10259008136678022120, 0),
		NewDecimal256(15930919111, 5986388489608800929, 10356360998232463120, 0),
		NewDecimal256(159309191113, 4523652674959354447, 11329889613776873120, 0),
		NewDecimal256(1593091911132, 8343038602174441244, 2618431695511421504, 0),
		NewDecimal256(15930919111324, 9643409726906205977, 7737572881404663424, 0),
		NewDecimal256(159309191113245, 4200376900514301694, 3588752519208427776, 0),
		NewDecimal256(1593091911132452, 5110280857723913709, 17440781118374726144, 0),
		NewDecimal256(15930919111324522, 14209320429820033867, 8387114520361296896, 0),
		NewDecimal256(159309191113245227, 12965995782233477362, 10084168908774762496, 0),
		NewDecimal256(1593091911132452277, 532749306367912313, 8607968719199866880, 0),
	}

	scaleMultipliersHalf256 = [...]Decimal256{
		Decimal256FromU64(0),
		Decimal256FromU64(5),
		Decimal256FromU64(50),
		Decimal256FromU64(500),
		Decimal256FromU64(5000),
		Decimal256FromU64(50000),
		Decimal256FromU64(500000),
		Decimal256FromU64(5000000),
		Decimal256FromU64(50000000),
		Decimal256FromU64(500000000),
		Decimal256FromU64(5000000000),
		Decimal256FromU64(50000000000),
		Decimal256FromU64(500000000000),
		Decimal256FromU64(5000000000000),
		Decimal256FromU64(50000000000000),
		Decimal256FromU64(500000000000000),
		Decimal256FromU64(5000000000000000),
		Decimal256FromU64(50000000000000000),
		Decimal256FromU64(500000000000000000),
		Decimal256FromU64(5000000000000000000),
		NewDecimal256(0, 0, 2, 13106511852580896768),
		NewDecimal256(0, 0, 27, 1937910009842106368),
		NewDecimal256(0, 0, 271, 932356024711512064),
		NewDecimal256(0, 0, 2710, 9323560247115120640),
		NewDecimal256(0, 0, 27105, 1001882102603448320),
		NewDecimal256(0, 0, 271050, 10018821026034483200),
		NewDecimal256(0, 0, 2710505, 7954489891797073920),
		NewDecimal256(0, 0, 27105054, 5757922623132532736),
		NewDecimal256(0, 0, 271050543, 2238994010196672512),
		NewDecimal256(0, 0, 2710505431, 3943196028257173504),
		NewDecimal256(0, 0, 27105054312, 2538472135152631808),
		NewDecimal256(0, 0, 271050543121, 6937977277816766464),
		NewDecimal256(0, 0, 2710505431213, 14039540557039009792),
		NewDecimal256(0, 0, 27105054312137, 11268197054423236608),
		NewDecimal256(0, 0, 271050543121376, 2001506101975056384),
		NewDecimal256(0, 0, 2710505431213761, 1568316946041012224),
		NewDecimal256(0, 0, 27105054312137610, 15683169460410122240),
		NewDecimal256(0, 0, 271050543121376108, 9257742014424809472),
		NewDecimal256(0, 0, 2710505431213761085, 343699775700336640),
		NewDecimal256(0, 1, 8658310238428059234, 3436997757003366400),
		NewDecimal256(0, 14, 12796126089442385877, 15923233496324112384),
		NewDecimal256(0, 146, 17280796452166549082, 11658382373564710912),
		NewDecimal256(0, 1469, 6787267858279526282, 5903359293389799424),
		NewDecimal256(0, 14693, 12532446361666607975, 3693360712769339392),
		NewDecimal256(0, 146936, 14643999174408770056, 40118980274290688),
		NewDecimal256(0, 1469367, 17312783228120839248, 401189802742906880),
		NewDecimal256(0, 14693679, 7107135617822427936, 4011898027429068800),
		NewDecimal256(0, 146936793, 15731123957095624514, 3225492126871584768),
		NewDecimal256(0, 1469367938, 9737286981279832213, 13808177195006296064),
		NewDecimal256(0, 14693679385, 5139149444250564057, 8954563434096099328),
		NewDecimal256(0, 146936793852, 14498006295086537342, 15758658046122786816),
		NewDecimal256(0, 1469367938527, 15852854434898512116, 10012627871551455232),
		NewDecimal256(0, 14693679385278, 10954591759308708237, 7892558346966794240),
		NewDecimal256(0, 146936793852785, 17312197224539324294, 5138607174829735936),
		NewDecimal256(0, 1469367938527859, 7101275582007278398, 14492583600878256128),
		NewDecimal256(0, 14693679385278593, 15672523598944129139, 15798627492815699968),
		NewDecimal256(0, 146936793852785938, 9151283399764878470, 10412322338480586752),
		NewDecimal256(0, 1469367938527859384, 17725857702810578241, 11889503016258109440),
		NewDecimal256(0, 14693679385278593849, 11237880364719817872, 8214565720323784704),
		NewDecimal256(7, 17809585336819077184, 1698339204940869028, 8358680908399640576),
		NewDecimal256(79, 12075156704804807296, 16983392049408690284, 9799832789158199296),
		NewDecimal256(796, 10071102605790763273, 3813223830700938301, 5764607523034234880),
		NewDecimal256(7965, 8477305689359874652, 1238750159590279781, 2305843009213693952),
		NewDecimal256(79654, 10986080598760540056, 12387501595902797811, 4611686018427387904),
		NewDecimal256(796545, 17627085619057642486, 13194551516770668416, 9223372036854775808),
		NewDecimal256(7965459, 10250159527190460323, 2818306651739822853, 0),
		NewDecimal256(79654595, 10267874903356845151, 9736322443688676914, 0),
		NewDecimal256(796545955, 10445028665020693435, 5129504068339011060, 0),
		NewDecimal256(7965459555, 12216566281659176272, 14401552535971007368, 0),
		NewDecimal256(79654595556, 11485198374334453031, 14888316843743212368, 0),
		NewDecimal256(796545955566, 4171519301087220622, 1309215847755710752, 0),
		NewDecimal256(7965459555662, 4821704863453102988, 13092158477557107520, 0),
		NewDecimal256(79654595556622, 11323560487111926655, 1794376259604213888, 0),
		NewDecimal256(796545955566226, 2555140428861956854, 17943762596042138880, 0),
		NewDecimal256(7965459555662261, 7104660214910016933, 13416929297035424256, 0),
		NewDecimal256(79654595556622613, 15706369927971514489, 5042084454387381248, 0),
		NewDecimal256(796545955566226138, 9489746690038731964, 13527356396454709248, 0),
	}

	float64PowersOfTen256 = [...]float64{
		1e-76, 1e-75, 1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66, 1e-65,
		1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57, 1e-56, 1e-55, 1e-54, 1e-53,
		1e-52, 1e-51, 1e-50, 1e-49, 1e-48, 1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41,
		1e-40, 1e-39, 1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
		1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17,
		1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5,
		1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7,
		1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
		1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
		1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42, 1e43,
		1e44, 1e45, 1e46, 1e47, 1e48, 1e49, 1e50, 1e51, 1e52, 1e53, 1e54, 1e55,
		1e56, 1e57, 1e58, 1e59, 1e60, 1e61, 1e62, 1e63, 1e64, 1e65, 1e66, 1e67,
		1e68, 1e69, 1e70, 1e71, 1e72, 1e73, 1e74, 1e75, 1e76,
	}
)
