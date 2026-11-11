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
	"math/big"
	"strconv"
)

// maxDigits bounds the number of integer digits ScaleAndRound will
// materialize. It is well above the 77 digits a 256-bit value can hold, so it
// only guards callers that skipped their own magnitude check.
const maxDigits = 1024

// ScaleAndRound returns the decimal literal s multiplied by 10^scale and
// rounded to an integer, with ties rounded away from zero.
//
// It works on the decimal digits of s, so an exact halfway value such as
// "0.15" or "0.35" is always recognized as a tie. Parsing into a binary
// float first would leave it just above or just below one half, and the
// rounding direction would depend on that error.
//
// ok is false when s is not a plain decimal literal (an optional sign,
// digits with an optional fraction, and an optional decimal exponent) or
// when the result would have more than maxDigits digits; callers should then
// fall back to their own conversion. Callers are expected to have bounded
// the magnitude of the result already.
func ScaleAndRound(s string, scale int64) (val *big.Int, ok bool) {
	neg := false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		neg = s[0] == '-'
		s = s[1:]
	}

	intEnd := 0
	for intEnd < len(s) && isDigit(s[intEnd]) {
		intEnd++
	}
	intPart, rest := s[:intEnd], s[intEnd:]

	var fracPart string
	if len(rest) > 0 && rest[0] == '.' {
		fracEnd := 1
		for fracEnd < len(rest) && isDigit(rest[fracEnd]) {
			fracEnd++
		}
		fracPart, rest = rest[1:fracEnd], rest[fracEnd:]
	}
	if len(intPart)+len(fracPart) == 0 {
		return nil, false
	}

	var exp int64
	if len(rest) > 0 {
		if rest[0] != 'e' && rest[0] != 'E' {
			return nil, false
		}
		digits := rest[1:]
		if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
			digits = digits[1:]
		}
		if len(digits) == 0 {
			return nil, false
		}
		for i := 0; i < len(digits); i++ {
			if !isDigit(digits[i]) {
				return nil, false
			}
		}
		var err error
		if exp, err = strconv.ParseInt(rest[1:], 10, 64); err != nil {
			return nil, false
		}
	}

	// The value is digits * 10^shift, where digits is the significand with
	// the decimal point removed.
	digits := trimLeadingZeros(intPart + fracPart)
	if len(digits) == 0 {
		return new(big.Int), true
	}
	const limit = int64(1) << 62
	if exp > limit || exp < -limit || scale > limit || scale < -limit ||
		int64(len(fracPart)) > limit {
		return nil, false
	}
	shift := exp - int64(len(fracPart)) + scale

	val = new(big.Int)
	if shift >= 0 {
		if int64(len(digits))+shift > maxDigits {
			return nil, false
		}
		val.SetString(digits, 10)
		val.Mul(val, new(big.Int).Exp(big.NewInt(10), big.NewInt(shift), nil))
	} else {
		// Drop the last -shift digits. With ties away from zero, only the
		// first dropped digit decides whether to round up; when every digit
		// is dropped and keep is negative, that digit is an implied zero.
		keep := int64(len(digits)) + shift
		if keep > maxDigits {
			return nil, false
		}
		if keep > 0 {
			val.SetString(digits[:keep], 10)
		}
		if keep >= 0 && digits[keep] >= '5' {
			val.Add(val, big.NewInt(1))
		}
	}

	if neg {
		val.Neg(val)
	}
	return val, true
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

func trimLeadingZeros(s string) string {
	i := 0
	for i < len(s) && s[i] == '0' {
		i++
	}
	return s[i:]
}
