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

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alpCountTestExceptions counts the values a scaling cannot encode.
func alpCountTestExceptions[T float32 | float64](values []T, params alpEncodingParams) int {
	count := 0
	s := newALPScaling[T](params.exponent, params.factor)
	for _, v := range values {
		if _, ok := s.tryEncode(v); !ok {
			count++
		}
	}
	return count
}

// assertCheapestParams fails if any exponent and factor pair estimates smaller
// than the one the search returned.
func assertCheapestParams[T float32 | float64](t *testing.T, values []T, got alpEncodingParams) {
	t.Helper()
	gotBits, _ := alpEstimateSizeBits(values, got.exponent, got.factor)
	for e := range alpNumExponents[T]() {
		for f := range e + 1 {
			bits, numEncodable := alpEstimateSizeBits(values, e, f)
			if numEncodable < 2 {
				continue
			}
			assert.LessOrEqual(t, gotBits, bits,
				"exponent %d factor %d estimates %d bits, below the chosen %d/%d at %d",
				e, f, bits, got.exponent, got.factor, gotBits)
		}
	}
}

func TestALPFindBestFloat32Params(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89, 10.11, 12.13}
	params, _ := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params), "two decimal places always encode")
	assertCheapestParams(t, values, params)
	assert.Equal(t, 2, params.exponent-params.factor, "the values scale by 10^2")
}

// TestALPFindBestFloat64Params covers a case where the exponent is not the count
// of decimal places. Scaling 4.56 by 10^2 gives an integer that decodes to
// 4.5600000000000005, so the search has to reach a larger pair that does not
// lose the last bit.
func TestALPFindBestFloat64Params(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89, 10.11, 12.13}
	params, _ := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assertCheapestParams(t, values, params)
	assert.Equal(t, 2, params.exponent-params.factor, "the values scale by 10^2")
}

func TestALPFindBestParamsWithExceptions(t *testing.T) {
	values := []float32{
		1.23, 4.56, float32(math.Inf(1)), 7.89,
		alpNegZero[float32](), 10.11,
	}
	params, _ := alpFindBestParams(values)
	assert.Equal(t, 2, alpCountTestExceptions(values, params),
		"the infinity and the negative zero are the only exceptions")
	assertCheapestParams(t, values, params)
}

// TestALPFindBestParamsPrefersNarrowPacking pins the reason the search weighs
// estimated size rather than exceptions alone: both pairs below encode every
// value, and the one with the smaller factor packs 30 bits wider.
func TestALPFindBestParamsPrefersNarrowPacking(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89}
	wide, _ := alpEstimateSizeBits(values, 14, 0)
	narrow, _ := alpEstimateSizeBits(values, 14, 12)
	assert.Less(t, narrow, wide)

	params, _ := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assertCheapestParams(t, values, params)
}

func TestALPFindBestFloat32ParamsWithPresets(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{3, 0}, {2, 0}, {1, 0}}
	params, _ := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	// 10^3 encodes the values too, but three digits wider than it has to.
	assert.Equal(t, 2, params.exponent)
	assert.Equal(t, 0, params.factor)
}

func TestALPFindBestFloat64ParamsWithPresets(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{2, 0}, {14, 12}, {16, 12}}
	params, _ := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assert.Equal(t, 14, params.exponent)
	assert.Equal(t, 12, params.factor)
}

// TestALPFindBestParamsWithPresetsStopsImproving covers the early exit: the
// shortlist is ordered by how often each scaling won, so once several in a row
// have failed to beat the best, the rest are not worth estimating.
func TestALPFindBestParamsWithPresetsStopsImproving(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{2, 0}, {3, 0}, {4, 0}, {5, 0}, {6, 0}}
	require.GreaterOrEqual(t, len(presets), alpPresetGiveUpAfter+1)

	params, _ := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 2, params.exponent)
}

func TestALPFindBestParamsIntegerData(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0, 100.0, 200.0}
	params, _ := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assert.Equal(t, params.exponent, params.factor,
		"integers need no scaling, so the exponent and the factor cancel")
}

// TestALPFindBestParamsAllExceptions covers a vector no scaling can encode. The
// search returns the pair that leaves the values as they are, and the encoder
// writes all of them verbatim.
func TestALPFindBestParamsAllExceptions(t *testing.T) {
	inf := float32(math.Inf(1))
	negZero := alpNegZero[float32]()
	values := []float32{inf, float32(math.Inf(-1)), negZero, inf}

	params, _ := alpFindBestParams(values)
	assert.Equal(t, len(values), alpCountTestExceptions(values, params))
	assert.Equal(t, alpNumExponents[float32]()-1, params.exponent)
	assert.Equal(t, params.exponent, params.factor)
}

func TestALPBetterParams(t *testing.T) {
	a := alpEncodingParams{exponent: 3, factor: 1}
	b := alpEncodingParams{exponent: 2, factor: 2}
	assert.True(t, alpBetterParams(a, 100, b, 200), "fewer bits wins")
	assert.False(t, alpBetterParams(a, 200, b, 100))
	assert.True(t, alpBetterParams(a, 100, b, 100), "a tie goes to the larger exponent")
	assert.True(t, alpBetterParams(alpEncodingParams{exponent: 2, factor: 2}, 100,
		alpEncodingParams{exponent: 2, factor: 1}, 100), "then to the larger factor")
}

func TestALPSample(t *testing.T) {
	values := make([]float64, 4*alpSamplerSamples)
	for i := range values {
		values[i] = float64(i)
	}

	sample := alpSample(values, nil)
	assert.Len(t, sample, alpSamplerSamples)
	assert.Equal(t, float64(0), sample[0])
	assert.Equal(t, float64(5), sample[1], "one value from each 4-value block, shifted one place per block")
	assert.Equal(t, float64(4*4+0), sample[4], "the shift wraps around within the block")

	// Fewer values than the sample size means every value is sampled.
	short := alpSample(values[:10], nil)
	assert.Equal(t, values[:10], short)
	assert.Empty(t, alpSample(values[:0], nil))
}

// TestALPSamplePeriodicData covers data that repeats with the sampling stride.
// Sampling the same position of every block would see only the NaNs and pick
// a scaling that turns every value into an exception.
func TestALPSamplePeriodicData(t *testing.T) {
	const n = 32768 // a stride of 128
	values := make([]float64, n)
	for i := range values {
		values[i] = float64(i%1000) / 100
		if i%128 == 0 {
			values[i] = math.NaN()
		}
	}

	sample := alpSample(values, nil)
	nans := 0
	for _, v := range sample {
		if math.IsNaN(v) {
			nans++
		}
	}
	assert.Equal(t, 2, nans, "only blocks where the shift lands on 0 sample a NaN")

	params, _ := alpFindBestParams(sample)
	assert.Equal(t, 2, params.exponent-params.factor, "two decimal places, got %+v", params)
}

// A size estimate that is cut short must not change what a search concludes:
// within the limit it is the full estimate, and past the limit it says so.
func TestALPEstimateSizeBitsWithin(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	// More than one block of values, and not a whole number of blocks.
	values := alpTestAwkwardValues[float64](rng, 5*alpEstimateBlock+7)
	for e := range alpNumExponents[float64]() {
		for f := range e + 1 {
			fullBits, fullEncodable := alpEstimateSizeBits(values, e, f)
			for _, limit := range []int64{0, fullBits / 2, fullBits - 1, fullBits, fullBits + 1, math.MaxInt64} {
				bits, numEncodable := alpEstimateSizeBitsWithin(values, e, f, limit)
				if fullBits <= limit {
					require.Equal(t, fullBits, bits, "exponent %d factor %d limit %d", e, f, limit)
					require.Equal(t, fullEncodable, numEncodable, "exponent %d factor %d limit %d", e, f, limit)
					continue
				}
				require.Greater(t, bits, limit, "exponent %d factor %d limit %d", e, f, limit)
				require.Zero(t, numEncodable, "exponent %d factor %d limit %d", e, f, limit)
			}
		}
	}
}

// The size of a vector's sample read off the scaled vector is the size
// estimated by scaling the sample.
func TestALPSampledSizeBits(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	for _, count := range []int{1, 5, 255, 256, 257, 1000, 1024, 4096} {
		values := alpTestAwkwardValues[float64](rng, count)
		// A NaN at every stride-th value is the pattern the sample's shifting
		// positions are there for.
		for i := 0; i < count; i += alpSampleStride(count) {
			values[i] = math.NaN()
		}
		sample := alpSample(values, nil)
		encoded, excPos := make([]int64, count), make([]uint16, count)
		for e := range alpNumExponents[float64]() {
			for f := range e + 1 {
				numExc := alpScale(values, e, f, encoded, excPos)
				if numExc < count {
					alpIntRange(encoded, excPos[:numExc])
				}
				wantBits, _ := alpEstimateSizeBits(sample, e, f)
				bits, sampled := alpSampledSizeBits[float64](encoded, excPos[:numExc])
				require.Equal(t, len(sample), sampled, "count %d", count)
				require.Equal(t, wantBits, bits, "count %d exponent %d factor %d", count, e, f)
			}
		}
	}
}
