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

// Package pow10 scales big.Float values by powers of ten with bounded work, for
// the decimal packages, which accept any int32 scale.
package pow10

import "math/big"

// Mul sets f to f * 10^exp, rounded to f's precision, and returns f. 10^exp is
// applied as an exact 2^exp and then 5^|exp|, computed at prec bits by
// repeated squaring so the work stays bounded. Splitting the power keeps it
// from overflowing before the result does. A result outside big.Float's
// exponent range becomes ±Inf or 0.
//
// The squarings lose fewer than 32 bits of the power's precision for any
// int32 exponent, so the power's relative error is below 2^-(prec-32). The
// result adds one rounding to f's precision, so it is within one unit in the
// last place whenever prec is at least 64 bits above f's precision.
func Mul(f *big.Float, exp int64, prec uint) *big.Float {
	if f.Sign() == 0 {
		// 5^|exp| may overflow to infinity, and 0 * Inf panics.
		return f
	}

	k := exp
	if k < 0 {
		k = -k
	}
	pow := new(big.Float).SetPrec(prec).SetInt64(1)
	for base := new(big.Float).SetPrec(prec).SetInt64(5); k > 0; k >>= 1 {
		if k&1 == 1 {
			pow.Mul(pow, base)
		}
		base.Mul(base, base)
	}

	f.SetMantExp(f, int(exp))
	if exp < 0 {
		return f.Quo(f, pow)
	}
	return f.Mul(f, pow)
}
