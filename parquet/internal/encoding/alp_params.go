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

// Choosing the scaling for a vector.
//
// The encoder tries exponent and factor pairs and keeps the pair whose packed
// output would be smallest. Two things keep the search affordable: it reads a
// sample of the vector rather than all of it, and after the first few vectors it
// only retries the pairs those vectors chose.

import (
	"math"
	"math/bits"
)

const (
	// alpSamplerVectors is how many vectors get the full exponent and factor
	// search before the encoder falls back to the combinations those vectors
	// chose. alpMaxPresetCombinations caps how many it keeps.
	alpSamplerVectors        = 8
	alpMaxPresetCombinations = 5

	// alpSamplerSamples is how many values of a vector the search looks at, and
	// alpPresetGiveUpAfter is how many shortlisted scalings may fail to improve
	// on the best before the per-vector search stops looking.
	alpSamplerSamples    = 256
	alpPresetGiveUpAfter = 4
)

// alpEncodingParams is the scaling one vector is encoded with: ALP multiplies
// each value by 10^exponent, then by 10^-factor.
type alpEncodingParams struct {
	exponent int
	factor   int
}

// alpSample appends equidistant values of a vector to out and returns it. The
// scaling search reads the sample instead of the whole vector: the values of a
// vector share a decimal precision or they do not, and a sample of a few hundred
// answers that as well as a thousand does.
func alpSample[T alpFloat](values []T, out []T) []T {
	stride := max(1, (len(values)+alpSamplerSamples-1)/alpSamplerSamples)
	for i := 0; i < len(values); i += stride {
		out = append(out, values[i])
	}
	return out
}

// alpExceptionBits is what one exception costs: the value at its full width,
// plus the position that patches it back in.
func alpExceptionBits[T alpFloat]() int {
	return 8*alpEncodedBytes[T]() + 16
}

// alpEstimateSizeBits reports what a scaling would cost the given values, and
// how many of them it encodes. The cost counts the packed integers at the width
// the frame of reference leaves, plus every exception.
func alpEstimateSizeBits[T alpFloat](values []T, exponent, factor int) (sizeBits int64, numEncodable int) {
	minEncoded, maxEncoded := int64(math.MaxInt64), int64(math.MinInt64)
	numExceptions := 0
	for _, v := range values {
		e, ok := alpTryEncode(v, exponent, factor)
		if !ok {
			numExceptions++
			continue
		}
		minEncoded, maxEncoded = min(minEncoded, e), max(maxEncoded, e)
	}

	sizeBits = int64(numExceptions) * int64(alpExceptionBits[T]())
	numEncodable = len(values) - numExceptions
	if numEncodable == 0 {
		return sizeBits, 0
	}
	// The subtraction is unsigned because the two ends of an int64 column are
	// further apart than an int64 reaches.
	width := bits.Len64(uint64(maxEncoded) - uint64(minEncoded))
	return sizeBits + int64(len(values))*int64(width), numEncodable
}

// alpBetterParams reports whether one candidate scaling beats another. The
// smaller estimate wins; a tie goes to the larger exponent, then to the larger
// factor. The ALP paper states those tie-breaks without giving a reason
// (Afroozeh et al., SIGMOD 2023, section 3.1.2), and following them keeps this
// encoder picking the same scaling as the other implementations.
func alpBetterParams(a alpEncodingParams, aBits int64, b alpEncodingParams, bBits int64) bool {
	switch {
	case aBits != bBits:
		return aBits < bBits
	case a.exponent != b.exponent:
		return a.exponent > b.exponent
	default:
		return a.factor > b.factor
	}
}

// alpFindBestParams returns the cheapest scaling over every exponent and factor
// pair. The factor never exceeds the exponent, because scaling by a net power
// below one would move the decimal point the wrong way.
func alpFindBestParams[T alpFloat](values []T) alpEncodingParams {
	maxExponent := alpNumExponents[T]() - 1
	// The fallback treats the values as integers: an exponent and a factor of
	// equal size cancel, so whatever is already integral packs and the rest
	// become exceptions. It stands when no scaling encodes enough to compare.
	best := alpEncodingParams{exponent: maxExponent, factor: maxExponent}
	bestBits := int64(math.MaxInt64)

	for e := range maxExponent + 1 {
		for f := range e + 1 {
			sizeBits, numEncodable := alpEstimateSizeBits(values, e, f)
			// A scaling that turns almost everything into an exception tells us
			// nothing about how wide the rest would pack.
			if numEncodable < 2 {
				continue
			}
			candidate := alpEncodingParams{exponent: e, factor: f}
			if alpBetterParams(candidate, sizeBits, best, bestBits) {
				best, bestBits = candidate, sizeBits
			}
		}
	}
	return best
}

// alpFindBestParamsWithPresets returns whichever shortlisted scaling is cheapest
// for the given values. The shortlist is ordered by how often each scaling won
// during sampling, so the search stops once alpPresetGiveUpAfter of them in a
// row fail to improve on the best.
func alpFindBestParamsWithPresets[T alpFloat](values []T, presets []alpEncodingParams) alpEncodingParams {
	best, bestBits := presets[0], int64(math.MaxInt64)
	notImproved := 0

	for _, p := range presets {
		// Only the size counts here. Every shortlisted scaling already encoded
		// enough values during sampling to be worth trying.
		sizeBits, _ := alpEstimateSizeBits(values, p.exponent, p.factor)
		if sizeBits >= bestBits {
			notImproved++
			if notImproved == alpPresetGiveUpAfter {
				break
			}
			continue
		}
		best, bestBits = p, sizeBits
		notImproved = 0
	}
	return best
}
