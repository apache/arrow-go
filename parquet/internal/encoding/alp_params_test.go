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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alpCountTestExceptions counts the values a scaling cannot encode.
func alpCountTestExceptions[T float32 | float64](values []T, params alpEncodingParams) int {
	count := 0
	for _, v := range values {
		if alpDecode[T](alpEncode(v, params.exponent, params.factor), params.exponent, params.factor) != v {
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

func TestAlpFindBestFloat32Params(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89, 10.11, 12.13}
	params := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params), "two decimal places always encode")
	assertCheapestParams(t, values, params)
	assert.Equal(t, 2, params.exponent-params.factor, "the values scale by 10^2")
}

// TestAlpFindBestFloat64Params covers a case where the exponent is not the count
// of decimal places. Scaling 4.56 by 10^2 gives an integer that decodes to
// 4.5600000000000005, so the search has to reach a larger pair that does not
// lose the last bit.
func TestAlpFindBestFloat64Params(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89, 10.11, 12.13}
	params := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assertCheapestParams(t, values, params)
	assert.Equal(t, 2, params.exponent-params.factor, "the values scale by 10^2")
}

func TestAlpFindBestParamsWithExceptions(t *testing.T) {
	values := []float32{
		1.23, 4.56, float32(math.Inf(1)), 7.89,
		alpNegZero[float32](), 10.11,
	}
	params := alpFindBestParams(values)
	assert.Equal(t, 2, alpCountTestExceptions(values, params),
		"the infinity and the negative zero are the only exceptions")
	assertCheapestParams(t, values, params)
}

// TestAlpFindBestParamsPrefersNarrowPacking pins the reason the search weighs
// estimated size rather than exceptions alone: both pairs below encode every
// value, and the one with the smaller factor packs 30 bits wider.
func TestAlpFindBestParamsPrefersNarrowPacking(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89}
	wide, _ := alpEstimateSizeBits(values, 14, 0)
	narrow, _ := alpEstimateSizeBits(values, 14, 12)
	assert.Less(t, narrow, wide)

	params := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assertCheapestParams(t, values, params)
}

func TestAlpFindBestFloat32ParamsWithPresets(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{3, 0}, {2, 0}, {1, 0}}
	params := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	// 10^3 encodes the values too, but three digits wider than it has to.
	assert.Equal(t, 2, params.exponent)
	assert.Equal(t, 0, params.factor)
}

func TestAlpFindBestFloat64ParamsWithPresets(t *testing.T) {
	values := []float64{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{2, 0}, {14, 12}, {16, 12}}
	params := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assert.Equal(t, 14, params.exponent)
	assert.Equal(t, 12, params.factor)
}

// TestAlpFindBestParamsWithPresetsStopsImproving covers the early exit: the
// shortlist is ordered by how often each scaling won, so once several in a row
// have failed to beat the best, the rest are not worth estimating.
func TestAlpFindBestParamsWithPresetsStopsImproving(t *testing.T) {
	values := []float32{1.23, 4.56, 7.89}
	presets := []alpEncodingParams{{2, 0}, {3, 0}, {4, 0}, {5, 0}, {6, 0}}
	require.GreaterOrEqual(t, len(presets), alpPresetGiveUpAfter+1)

	params := alpFindBestParamsWithPresets(values, presets)
	assert.Equal(t, 2, params.exponent)
}

func TestAlpFindBestParamsIntegerData(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0, 100.0, 200.0}
	params := alpFindBestParams(values)
	assert.Equal(t, 0, alpCountTestExceptions(values, params))
	assert.Equal(t, params.exponent, params.factor,
		"integers need no scaling, so the exponent and the factor cancel")
}

// TestAlpFindBestParamsAllExceptions covers a vector no scaling can encode. The
// search returns the pair that leaves the values as they are, and the encoder
// writes all of them verbatim.
func TestAlpFindBestParamsAllExceptions(t *testing.T) {
	inf := float32(math.Inf(1))
	negZero := alpNegZero[float32]()
	values := []float32{inf, float32(math.Inf(-1)), negZero, inf}

	params := alpFindBestParams(values)
	assert.Equal(t, len(values), alpCountTestExceptions(values, params))
	assert.Equal(t, alpNumExponents[float32]()-1, params.exponent)
	assert.Equal(t, params.exponent, params.factor)
}

func TestAlpBetterParams(t *testing.T) {
	a := alpEncodingParams{exponent: 3, factor: 1}
	b := alpEncodingParams{exponent: 2, factor: 2}
	assert.True(t, alpBetterParams(a, 100, b, 200), "fewer bits wins")
	assert.False(t, alpBetterParams(a, 200, b, 100))
	assert.True(t, alpBetterParams(a, 100, b, 100), "a tie goes to the larger exponent")
	assert.True(t, alpBetterParams(alpEncodingParams{exponent: 2, factor: 2}, 100,
		alpEncodingParams{exponent: 2, factor: 1}, 100), "then to the larger factor")
}

func TestAlpSample(t *testing.T) {
	values := make([]float64, alpDefaultVectorSize)
	for i := range values {
		values[i] = float64(i)
	}

	sample := alpSample(values, nil)
	assert.Len(t, sample, alpSamplerSamples)
	assert.Equal(t, float64(0), sample[0])
	assert.Equal(t, float64(4), sample[1], "the stride spreads the sample over the vector")

	// Fewer values than the sample size means every value is sampled.
	short := alpSample(values[:10], nil)
	assert.Equal(t, values[:10], short)
	assert.Empty(t, alpSample(values[:0], nil))
}
