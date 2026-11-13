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

// The encoder samples each vector and chooses the exponent and factor that
// minimize its encoded size. After the initial sampling window, it searches
// only the combinations that won.

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

	// While the shortlist does no better than PLAIN, a full search checks
	// whether the data has changed scale. It first searches alpResearchProbeSamples
	// of the sampled values and stops there if nothing beats PLAIN. After a
	// search that finds nothing better, the encoder skips alpMinResearchWait
	// such vectors before the next, and the wait doubles up to
	// alpMaxResearchWait, so data ALP can't compress pays for few searches.
	alpResearchProbeSamples = 32
	alpMinResearchWait      = 32
	alpMaxResearchWait      = 512
)

// alpEncodingParams is the scaling one vector is encoded with: ALP multiplies
// each value by 10^exponent, then by 10^-factor.
type alpEncodingParams struct {
	exponent int
	factor   int
}

// alpSamplePosition returns where in a vector of n values the i-th sampled
// value is, for i up to (n-1)/stride with stride from alpSampleStride. There is
// one from each stride-wide block, and the position within the block shifts
// from one block to the next, so data that repeats with the stride's period,
// such as a NaN at every 128th value, can't fill the whole sample.
func alpSamplePosition(i, stride, n int) int {
	block := i * stride
	if idx := block + i%stride; idx < n {
		return idx
	}
	return block
}

// alpSampleStride returns the block width that samples up to alpSamplerSamples
// of n values.
func alpSampleStride(n int) int {
	return max(1, (n+alpSamplerSamples-1)/alpSamplerSamples)
}

// alpSample appends the sampled values to out, up to alpSamplerSamples of them.
func alpSample[T alpFloat](values []T, out []T) []T {
	stride := alpSampleStride(len(values))
	for i := 0; i*stride < len(values); i++ {
		out = append(out, values[alpSamplePosition(i, stride, len(values))])
	}
	return out
}

// alpSampledSizeBits is alpEstimateSizeBits over the sample of a vector that
// is scaled already: encoded holds the integer of every value, and excPos the
// positions of the exceptions, in order. It also returns the size of the
// sample. Reading the sample's size off the scaled vector costs no arithmetic
// on the values, which scaling the sample a second time would.
func alpSampledSizeBits[T alpFloat](encoded []int64, excPos []uint16) (sizeBits int64, sampled int) {
	stride := alpSampleStride(len(encoded))
	minEncoded, maxEncoded := int64(math.MaxInt64), int64(math.MinInt64)
	numExceptions := 0
	for ; sampled*stride < len(encoded); sampled++ {
		idx := alpSamplePosition(sampled, stride, len(encoded))
		// The sampled positions only move forward, as the exceptions do.
		for len(excPos) > 0 && int(excPos[0]) < idx {
			excPos = excPos[1:]
		}
		if len(excPos) > 0 && int(excPos[0]) == idx {
			numExceptions++
			continue
		}
		minEncoded, maxEncoded = min(minEncoded, encoded[idx]), max(maxEncoded, encoded[idx])
	}

	sizeBits = int64(numExceptions) * int64(alpExceptionBits[T]())
	if numExceptions < sampled {
		width := bits.Len64(uint64(maxEncoded) - uint64(minEncoded))
		sizeBits += int64(sampled) * int64(width)
	}
	return sizeBits, sampled
}

// alpExceptionBits is what one exception costs: the value at its full width,
// plus the position that patches it back in.
func alpExceptionBits[T alpFloat]() int {
	return 8*alpEncodedBytes[T]() + 16
}

// alpEstimateBlock is how many values a size estimate scales between checks of
// whether the scaling can still matter.
const alpEstimateBlock = 32

// alpEstimateSizeBits estimates the encoded size for a parameter pair and
// reports how many values it can encode. The cost includes the packed integers
// at the width left by the frame of reference and every exception.
func alpEstimateSizeBits[T alpFloat](values []T, exponent, factor int) (sizeBits int64, numEncodable int) {
	return alpEstimateSizeBitsWithin(values, exponent, factor, math.MaxInt64)
}

// alpEstimateSizeBitsWithin is alpEstimateSizeBits for a caller with no use
// for a scaling larger than limit. Neither the exceptions nor the packing width
// can shrink as more values are scaled, so the size of the values scaled so far
// is a lower bound on the whole. Once that passes limit the scan stops, and
// reports a size above limit with numEncodable 0. Most scalings turn nearly
// every value into an exception, so most scans stop within a block or two of a
// search that would otherwise scale every value for every scaling.
func alpEstimateSizeBitsWithin[T alpFloat](values []T, exponent, factor int, limit int64) (sizeBits int64, numEncodable int) {
	var (
		encoded [alpEstimateBlock]int64
		excPos  [alpEstimateBlock]uint16
	)
	excBits := int64(alpExceptionBits[T]())
	minEncoded, maxEncoded := int64(math.MaxInt64), int64(math.MinInt64)
	numExceptions := 0
	for len(values) > numEncodable+numExceptions {
		start := numEncodable + numExceptions
		block := values[start:min(start+alpEstimateBlock, len(values))]
		n := alpScale(block, exponent, factor, encoded[:], excPos[:])
		numExceptions += n
		numEncodable += len(block) - n
		if n < len(block) {
			lo, hi := alpIntRange(encoded[:len(block)], excPos[:n])
			minEncoded, maxEncoded = min(minEncoded, lo), max(maxEncoded, hi)
		}

		sizeBits = int64(numExceptions) * excBits
		if numEncodable > 0 {
			// The encoded range can exceed int64, so compute its width in
			// unsigned arithmetic.
			width := bits.Len64(uint64(maxEncoded) - uint64(minEncoded))
			sizeBits += int64(len(values)) * int64(width)
		}
		if sizeBits > limit {
			return sizeBits, 0
		}
	}
	return sizeBits, numEncodable
}

// For equal-size estimates, the ALP paper prefers the larger exponent,
// then the larger factor (Afroozeh et al., SIGMOD 2023, section 3.1.2).
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
// pair, and its estimated size in bits. The factor never exceeds the exponent,
// because scaling by a net power below one would move the decimal point the
// wrong way. The size is math.MaxInt64 when no scaling encodes enough values
// to compare.
func alpFindBestParams[T alpFloat](values []T) (alpEncodingParams, int64) {
	maxExponent := alpNumExponents[T]() - 1
	// The fallback treats the values as integers: an exponent and a factor of
	// equal size cancel, so whatever is already integral packs and the rest
	// become exceptions. It stands when no scaling encodes enough to compare.
	best := alpEncodingParams{exponent: maxExponent, factor: maxExponent}
	bestBits := int64(math.MaxInt64)

	for e := range maxExponent + 1 {
		for f := range e + 1 {
			// A scaling the same size as the best can still win the tie, so
			// only one that is larger is cut short.
			sizeBits, numEncodable := alpEstimateSizeBitsWithin(values, e, f, bestBits)
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
	return best, bestBits
}

// alpFindBestParamsWithPresets returns whichever shortlisted scaling is cheapest
// for the given values, and its estimated size in bits. The shortlist is
// ordered by how often each scaling won during sampling, so the search stops
// once alpPresetGiveUpAfter of them in a row fail to improve on the best.
func alpFindBestParamsWithPresets[T alpFloat](values []T, presets []alpEncodingParams) (alpEncodingParams, int64) {
	best, bestBits := presets[0], int64(math.MaxInt64)
	notImproved := 0

	for _, p := range presets {
		// Only the size counts here. Every shortlisted scaling already encoded
		// enough values during sampling to be worth trying.
		// Here a tie goes to the earlier scaling, so one that can only
		// match the best is cut short too.
		sizeBits, _ := alpEstimateSizeBitsWithin(values, p.exponent, p.factor, bestBits-1)
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
	return best, bestBits
}
