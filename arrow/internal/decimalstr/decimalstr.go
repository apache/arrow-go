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

// Package decimalstr holds helpers shared by the decimal packages for
// working with decimal string literals.
package decimalstr

import (
	"math"
	"math/big"
	"strconv"
)

// maxDigits bounds the number of integer digits ScaleAndRound will
// materialize. It is well above the 77 digits a 256-bit value can hold, so it
// only guards callers that skipped their own magnitude check.
const maxDigits = 1024

// maxPow10 is the largest exponent kept in pow10Table. It covers Decimal256
// scales and the borderline magnitude checks; larger powers are computed on
// demand only after the digit bound has already accepted the result size.
const maxPow10 = 128

var (
	bigOne     = big.NewInt(1)
	pow10Table [maxPow10 + 1]*big.Int
	twoPow32   = new(big.Int).Lsh(big.NewInt(1), 32)
	twoPow64   = new(big.Int).Lsh(big.NewInt(1), 64)
	twoPow127  = new(big.Int).Lsh(big.NewInt(1), 127)
	twoPow128  = new(big.Int).Lsh(big.NewInt(1), 128)
	twoPow255  = new(big.Int).Lsh(big.NewInt(1), 255)
	twoPow256  = new(big.Int).Lsh(big.NewInt(1), 256)
)

func init() {
	pow10Table[0] = big.NewInt(1)
	ten := big.NewInt(10)
	for i := 1; i <= maxPow10; i++ {
		pow10Table[i] = new(big.Int).Mul(pow10Table[i-1], ten)
	}
}

func pow10(n int) *big.Int {
	if n <= maxPow10 {
		return pow10Table[n]
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func twoPower(bits int) *big.Int {
	switch bits {
	case 32:
		return twoPow32
	case 64:
		return twoPow64
	case 127:
		return twoPow127
	case 128:
		return twoPow128
	case 255:
		return twoPow255
	case 256:
		return twoPow256
	default:
		return new(big.Int).Lsh(big.NewInt(1), uint(bits))
	}
}

// Parsed is a plain decimal literal: an optional sign, digits with an
// optional fraction, and an optional decimal exponent. intPart and fracPart
// are slices of the original string.
type Parsed struct {
	neg               bool
	intPart, fracPart string
	exp               int64
}

// Parse reports whether s is a plain decimal literal and, if so, returns
// its parts. Inputs such as a p exponent, a hex prefix, Inf or NaN are
// rejected so callers can fall back to their own conversion.
func Parse(s string) (p Parsed, ok bool) {
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		p.neg = s[0] == '-'
		s = s[1:]
	}

	intEnd := 0
	for intEnd < len(s) && isDigit(s[intEnd]) {
		intEnd++
	}
	p.intPart, s = s[:intEnd], s[intEnd:]

	if len(s) > 0 && s[0] == '.' {
		fracEnd := 1
		for fracEnd < len(s) && isDigit(s[fracEnd]) {
			fracEnd++
		}
		p.fracPart, s = s[1:fracEnd], s[fracEnd:]
	}
	if len(p.intPart)+len(p.fracPart) == 0 {
		return Parsed{}, false
	}

	if len(s) > 0 {
		if s[0] != 'e' && s[0] != 'E' {
			return Parsed{}, false
		}
		digits := s[1:]
		if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
			digits = digits[1:]
		}
		if len(digits) == 0 {
			return Parsed{}, false
		}
		for i := 0; i < len(digits); i++ {
			if !isDigit(digits[i]) {
				return Parsed{}, false
			}
		}
		var err error
		if p.exp, err = strconv.ParseInt(s[1:], 10, 64); err != nil {
			return Parsed{}, false
		}
	}
	return p, true
}

// ScaleAndRound returns the decimal literal s multiplied by 10^scale and
// rounded to an integer, with ties rounded away from zero.
//
// It works on the decimal digits of s, so an exact halfway value such as
// "0.15" or "0.35" is always recognized as a tie. Parsing into a binary
// float first would leave it just above or just below one half, and the
// rounding direction would depend on that error.
//
// ok is false when s is not a plain decimal literal or when the result
// would have more than maxDigits digits; callers should then fall back to
// their own conversion. Any number of dropped fraction digits is accepted.
// Callers are expected to have bounded the magnitude of the result already.
func ScaleAndRound(s string, scale int64) (val *big.Int, ok bool) {
	p, ok := Parse(s)
	if !ok {
		return nil, false
	}
	return p.ScaleAndRound(scale)
}

// ScaleAndRound returns this literal multiplied by 10^scale and rounded
// to an integer, with ties rounded away from zero.
//
// The coefficient is built from the integer and fractional parts separately
// and scaled with a powers-of-ten table, so the hot path does not join
// those parts into a new string or call big.Int.Exp.
func (p Parsed) ScaleAndRound(scale int64) (val *big.Int, ok bool) {
	fracLen := int64(len(p.fracPart))
	const limit = int64(1) << 62
	if p.exp > limit || p.exp < -limit || scale > limit || scale < -limit ||
		fracLen > limit {
		return nil, false
	}
	shift, ok := add2(p.exp, scale)
	if !ok {
		return nil, false
	}
	shift, ok = add2(shift, -fracLen)
	if !ok {
		return nil, false
	}

	digits := significantDigitCount(p.intPart, p.fracPart)
	val = new(big.Int)
	if digits == 0 {
		return val, true
	}

	if shift >= 0 {
		if int64(digits)+shift > maxDigits {
			return nil, false
		}
		if !setCoeff(val, p.intPart, p.fracPart) {
			return nil, false
		}
		if shift > 0 {
			val.Mul(val, pow10(int(shift)))
		}
	} else {
		down := -shift
		// Every significant digit is to the right of the rounding place,
		// so the first dropped digit is an implied zero and the result is 0.
		if int64(digits) < down {
			return val, true
		}
		if int64(digits)-down > maxDigits {
			return nil, false
		}
		// Ties round away from zero, so only the first dropped digit
		// matters. Cut the digit string instead of dividing, which keeps
		// the cost independent of how many fraction digits are dropped.
		intLen := int64(len(p.intPart))
		keep := intLen + int64(len(p.fracPart)) - down
		var first byte
		if keep < intLen {
			if !setCoeff(val, p.intPart[:keep], "") {
				return nil, false
			}
			first = p.intPart[keep]
		} else {
			if !setCoeff(val, p.intPart, p.fracPart[:keep-intLen]) {
				return nil, false
			}
			first = p.fracPart[keep-intLen]
		}
		if first >= '5' {
			val.Add(val, bigOne)
		}
	}

	if p.neg {
		val.Neg(val)
	}
	return val, true
}

// AtLeastTwoPower reports whether the absolute value of this literal
// multiplied by 10^scale is at least 2^bits. The comparison uses the number
// of integer digits after scaling and only materializes a coefficient on
// the borderline, so a huge exponent does not allocate in proportion to
// its size.
func (p Parsed) AtLeastTwoPower(scale int64, bits int) bool {
	if bits <= 0 {
		return significantDigitCount(p.intPart, p.fracPart) > 0
	}
	e, ok := scaledExp(p, scale)
	if !ok {
		// Overflow of the combined exponent: a huge positive power is at
		// least 2^bits, a huge negative power is a fraction.
		return p.exp > 0 || scale > 0
	}
	d := significantDigitCount(p.intPart, p.fracPart)
	if d == 0 {
		return false
	}
	// |x| is in [10^{d+e-1}, 10^{d+e}).
	// 2^bits is in [10^{td-1}, 10^td), where td = floor(bits*log10(2))+1.
	sum, ok := add2(int64(d), e)
	if !ok {
		return e > 0
	}
	td := int64(decimalDigitsOfTwoPower(bits))
	if sum-1 >= td {
		return true
	}
	if sum <= td-1 {
		return false
	}

	// Borderline: compare coeff * 10^max(e,0) with 2^bits * 10^max(-e,0).
	coeff := new(big.Int)
	if !setCoeff(coeff, p.intPart, p.fracPart) || coeff.Sign() == 0 {
		return false
	}
	lhs := coeff
	rhs := new(big.Int).Set(twoPower(bits))
	if e > 0 {
		if e > maxDigits {
			return true
		}
		lhs = new(big.Int).Mul(coeff, pow10(int(e)))
	} else if e < 0 {
		down := -e
		if down > maxDigits {
			return false
		}
		rhs.Mul(rhs, pow10(int(down)))
	}
	return lhs.Cmp(rhs) >= 0
}

func scaledExp(p Parsed, scale int64) (int64, bool) {
	e, ok := add2(p.exp, scale)
	if !ok {
		return 0, false
	}
	return add2(e, -int64(len(p.fracPart)))
}

func decimalDigitsOfTwoPower(bits int) int {
	switch bits {
	case 32:
		return 10
	case 64:
		return 20
	case 127, 128:
		return 39
	case 255:
		return 77
	case 256:
		return 78
	default:
		return int(math.Floor(float64(bits)*math.Log10(2))) + 1
	}
}

func setCoeff(dst *big.Int, intPart, fracPart string) bool {
	if len(fracPart) == 0 {
		if isAllZeros(intPart) {
			dst.SetInt64(0)
			return true
		}
		_, ok := dst.SetString(intPart, 10)
		return ok
	}
	if isAllZeros(intPart) {
		if isAllZeros(fracPart) {
			dst.SetInt64(0)
			return true
		}
		_, ok := dst.SetString(fracPart, 10)
		return ok
	}
	if _, ok := dst.SetString(intPart, 10); !ok {
		return false
	}
	dst.Mul(dst, pow10(len(fracPart)))
	if !isAllZeros(fracPart) {
		var frac big.Int
		if _, ok := frac.SetString(fracPart, 10); !ok {
			return false
		}
		dst.Add(dst, &frac)
	}
	return true
}

func significantDigitCount(intPart, fracPart string) int {
	i := 0
	for i < len(intPart) && intPart[i] == '0' {
		i++
	}
	if i < len(intPart) {
		return len(intPart) - i + len(fracPart)
	}
	j := 0
	for j < len(fracPart) && fracPart[j] == '0' {
		j++
	}
	return len(fracPart) - j
}

func isAllZeros(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

func add2(a, b int64) (int64, bool) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, false
	}
	return c, true
}

var pt5 = big.NewFloat(0.5)

// RoundScaled returns the integer nearest to scaled, with ties rounded away
// from zero. scaled must hold the literal s multiplied by 10^scale; callers
// use it to reject results that are too large before calling.
//
// The result is computed exactly from the digits of s when s is a plain
// decimal literal, and from scaled otherwise.
func RoundScaled(scaled *big.Float, s string, scale int64) *big.Int {
	// A magnitude below 1/4 rounds to zero. Returning early skips the
	// rounding below, which allocates in proportion to the gap between the
	// value's exponent and 0.5's.
	if scaled.MantExp(nil) < -1 {
		return new(big.Int)
	}
	if val, ok := ScaleAndRound(s, scale); ok {
		return val
	}

	out := new(big.Float).Copy(scaled)
	if out.Signbit() {
		out.Sub(out, pt5)
	} else {
		out.Add(out, pt5)
	}
	val, _ := out.Int(nil)
	return val
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }
