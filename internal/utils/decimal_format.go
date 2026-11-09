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

package utils

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// FormatDecimalG formats an Arrow decimal in 'g' style. At absolute scales
// above 1000, it rounds the exact coefficient and shifts the exponent,
// avoiding conversion work quadratic in the scale. Exact ties round to even.
func FormatDecimalG[T interface{ ToBigFloat(int32) *big.Float }](n T, precision, scale int32) string {
	if scale >= -1000 && scale <= 1000 {
		return n.ToBigFloat(scale).Text('g', int(precision))
	}
	coef := n.ToBigFloat(0)
	if coef.Sign() == 0 {
		return coef.Text('g', int(precision))
	}
	// No decimal type has more than 77 coefficient digits, and a negative
	// precision keeps 'g's shortest formatting.
	p := min(int(precision), 77)
	if p > 0 {
		p--
	}
	mant, exp, _ := strings.Cut(coef.Text('e', p), "e")
	e, _ := strconv.ParseInt(exp, 10, 64)
	return fmt.Sprintf("%se%+03d", strings.TrimRight(strings.TrimRight(mant, "0"), "."), e-int64(scale))
}
