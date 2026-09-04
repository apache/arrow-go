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

// ALP (Adaptive Lossless floating-Point) encoding for FLOAT and DOUBLE columns.
//
// ALP multiplies each value by a power of ten, rounds the product to an integer,
// and stores the integers as differences from the smallest one, bit packed. A
// value that does not survive that round trip is stored verbatim as an
// exception. The encoder chooses the exponent and factor for every vector of
// 1024 values; after the first few vectors it keeps only the combinations that
// won so far and tries those, which is far cheaper than the full search.
//
// A page begins with a 7 byte header: compression mode, integer encoding, log2
// of the vector size, then the value count as a little-endian uint32. An array
// of little-endian uint32 offsets follows, one per vector, each counted from the
// start of that array. Every vector then holds:
//
//	exponent (1 byte), factor (1 byte), exception count (uint16)
//	frame of reference (int32 for FLOAT, int64 for DOUBLE), bit width (1 byte)
//	the bit-packed differences from the frame of reference
//	the exception positions (uint16 each), then the exception values
//
// Reference: https://dl.acm.org/doi/10.1145/3626717

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

const (
	alpCompressionMode    = 0 // ALP
	alpIntegerEncodingFOR = 0 // FOR bit packing
	alpHeaderSize         = 7
	alpDefaultVectorSize  = 1024
	alpDefaultLogVector   = 10
	alpMaxLogVectorSize   = 15
	alpMinLogVectorSize   = 3

	alpInfoSize = 4 // exponent(1) + factor(1) + num_exceptions(2)

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

	alpMagicFloat32 = float32(12582912.0)         // 2^22 + 2^23
	alpMagicFloat64 = float64(6755399441055744.0) // 2^51 + 2^52

	// The largest and smallest values of each type that convert to an integer
	// of the matching width. They stop short of the integer limits because
	// neither type can represent every integer that close to them.
	alpEncodingUpperFloat32 = float32(2147483520.0)
	alpEncodingLowerFloat32 = float32(-2147483520.0)
	alpEncodingUpperFloat64 = float64(9223372036854774784.0)
	alpEncodingLowerFloat64 = float64(-9223372036854774784.0)

	alpNegZeroFloat32Bits = uint32(0x80000000)
	alpNegZeroFloat64Bits = uint64(0x8000000000000000)
)

// alpFloat is the set of column types ALP encodes.
type alpFloat interface {
	float32 | float64
}

// The exponent and the factor index these tables, so the lengths set the search
// space: a float32 multiplier never needs more than 1e10, because a larger one
// pushes every value out of int32.
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

// The helpers below give the generic code the one constant or width that
// differs between float32 and float64. Each switch resolves at compile time
// once the compiler instantiates the function.

// alpMagic returns the constant that rounds a value of type T when added to it
// and then subtracted again.
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

// alpPow10 returns 10^i as a value of type T.
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

// alpNegPow10 returns 10^-i as a value of type T.
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

// alpNumExponents returns how many exponents the search may try for type T.
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

// alpEncodedBytes returns the width ALP stores integers of type T at. The frame
// of reference and the exception values both use it.
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

// alpEncodingLimits returns the range a scaled value of type T has to stay
// inside to convert to an integer. The bounds are the largest and smallest
// values of T that survive the conversion, which fall short of the integer
// type's own limits because T cannot represent every integer near them.
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

// alpFloatToBits returns the bit pattern of v, so that callers can compare two
// values without treating negative zero as equal to zero.
func alpFloatToBits[T alpFloat](v T) uint64 {
	switch f := any(v).(type) {
	case float32:
		return uint64(math.Float32bits(f))
	case float64:
		return math.Float64bits(f)
	}
	panic("parquet: ALP: unsupported float type")
}

// alpFloatFromBits is the inverse of alpFloatToBits.
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

// alpFastRound rounds v to the nearest integer. Adding and then subtracting a
// large constant discards the fractional bits, which is quicker than a library
// call and is what the reference implementation does.
func alpFastRound[T alpFloat](v T) int64 {
	magic := alpMagic[T]()
	if v >= 0 {
		return int64((v + magic) - magic)
	}
	return int64((v - magic) + magic)
}

// alpImpossibleToEncode reports whether a scaled value has no integer form: a
// NaN, either infinity, a magnitude past what the integer type holds, or a
// negative zero, whose sign the round trip would drop.
func alpImpossibleToEncode[T alpFloat](v T) bool {
	lo, hi := alpEncodingLimits[T]()
	f := float64(v)
	return math.IsNaN(f) || v > hi || v < lo || (f == 0 && math.Signbit(f))
}

// alpNumberToInt rounds a scaled value, or returns the upper encoding limit for
// one that cannot be rounded. The limit acts as a sentinel: decoding it cannot
// return the original value, so the caller finds the value in the exceptions.
func alpNumberToInt[T alpFloat](v T) int64 {
	if alpImpossibleToEncode(v) {
		_, hi := alpEncodingLimits[T]()
		return int64(hi)
	}
	return alpFastRound(v)
}

// alpEncode scales v and rounds it to the integer ALP stores.
func alpEncode[T alpFloat](v T, exponent, factor int) int64 {
	return alpNumberToInt(v * alpPow10[T](exponent) * alpNegPow10[T](factor))
}

// alpDecode is the inverse of alpEncode. The order of the two multiplications
// is part of the format: reassociating them changes the last bit of the result.
func alpDecode[T alpFloat](encoded int64, exponent, factor int) T {
	return T(encoded) * alpPow10[T](factor) * alpNegPow10[T](exponent)
}

// alpIsBasicException reports whether v can never be encoded, whatever the
// exponent and factor. NaN and the infinities have no integer form, and
// negative zero would come back as positive zero.
func alpIsBasicException[T alpFloat](v T) bool {
	f := float64(v)
	return math.IsNaN(f) || math.IsInf(f, 0) || (f == 0 && math.Signbit(f))
}

// alpIsException reports whether v has to be stored verbatim under the given
// exponent and factor, which is the case when the round trip does not return it
// unchanged. A NaN is never equal to itself, and a negative zero comes back
// from the sentinel as a large number, so both compare unequal here.
func alpIsException[T alpFloat](v T, exponent, factor int) bool {
	return alpDecode[T](alpEncode(v, exponent, factor), exponent, factor) != v
}

// alpBitWidth returns the number of bits needed to hold v.
func alpBitWidth(v uint64) int {
	return bits.Len64(v)
}

// alpPackedSize returns the byte size of count values packed at bitWidth bits.
func alpPackedSize(count, bitWidth int) int {
	return (count*bitWidth + 7) / 8
}

// alpAppendPackedBits appends values to dst, packed at bitWidth bits each,
// least significant bit first.
func alpAppendPackedBits(dst []byte, values []uint64, bitWidth int) []byte {
	if bitWidth == 0 || len(values) == 0 {
		return dst
	}

	// BitWriter flushes eight bytes at a time, so it needs room past the last
	// packed byte. The return drops those bytes again.
	start, size := len(dst), alpPackedSize(len(values), bitWidth)
	dst = slices.Grow(dst, size+8)[:start+size+8]
	clear(dst[start:])

	bw := utils.NewBitWriter(utils.NewWriterAtBuffer(dst[start:]))
	for _, v := range values {
		bw.WriteValue(v, uint(bitWidth))
	}
	bw.Flush(true)
	return dst[:start+size]
}

// alpUnpackBits reverses alpAppendPackedBits into out, which must hold as many
// values as were packed. The reader is reset onto packed, so one reader serves
// every vector of a page.
func alpUnpackBits(br *utils.BitReader, packed *bytes.Reader, data []byte, out []uint64, bitWidth int) error {
	if bitWidth == 0 {
		clear(out)
		return nil
	}

	packed.Reset(data)
	br.Reset(packed)
	n, err := br.GetBatch(uint(bitWidth), out)
	if err != nil {
		return err
	}
	if n != len(out) {
		return fmt.Errorf("parquet: ALP unpacked %d of %d values", n, len(out))
	}
	return nil
}

// alpEncodingParams is the scaling one vector is encoded with.
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
		e := alpEncode(v, exponent, factor)
		if alpDecode[T](e, exponent, factor) != v {
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
	width := alpBitWidth(uint64(maxEncoded) - uint64(minEncoded))
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
func alpFindBestParamsWithPresets[T alpFloat](values []T, presets [][2]int) alpEncodingParams {
	best := alpEncodingParams{exponent: presets[0][0], factor: presets[0][1]}
	bestBits := int64(math.MaxInt64)
	notImproved := 0

	for _, p := range presets {
		sizeBits, _ := alpEstimateSizeBits(values, p[0], p[1])
		if sizeBits >= bestBits {
			notImproved++
			if notImproved == alpPresetGiveUpAfter {
				break
			}
			continue
		}
		best, bestBits = alpEncodingParams{exponent: p[0], factor: p[1]}, sizeBits
		notImproved = 0
	}
	return best
}

// alpPresetKey packs an exponent and factor into one map key.
func alpPresetKey(exponent, factor int) uint32 {
	return uint32(exponent)<<16 | uint32(factor)
}

type alpEncoder[T alpFloat] struct {
	encoder

	vectorSize    int
	logVectorSize int

	vectorBuf  []T
	bufCount   int
	totalCount int

	encodedBuf  []byte
	vectorSizes []int

	// Scratch reused by every vector, so encoding allocates nothing per vector.
	encodedScratch []int64
	deltaScratch   []uint64
	excPositions   []uint16
	excValues      []T
	sampleScratch  []T

	vectorsProcessed int
	cachedPresets    [][2]int
	presetCounts     map[uint32]int
}

func newAlpEncoder[T alpFloat](e format.Encoding, descr *schema.Column, mem memory.Allocator) *alpEncoder[T] {
	return &alpEncoder[T]{
		encoder:        newEncoderBase(e, descr, mem),
		vectorSize:     alpDefaultVectorSize,
		logVectorSize:  alpDefaultLogVector,
		vectorBuf:      make([]T, alpDefaultVectorSize),
		encodedScratch: make([]int64, alpDefaultVectorSize),
		deltaScratch:   make([]uint64, alpDefaultVectorSize),
		sampleScratch:  make([]T, 0, alpSamplerSamples),
		presetCounts:   make(map[uint32]int),
	}
}

func (enc *alpEncoder[T]) Type() parquet.Type {
	var z T
	switch any(z).(type) {
	case float32:
		return parquet.Types.Float
	case float64:
		return parquet.Types.Double
	}
	panic("parquet: ALP: unsupported float type")
}

func (enc *alpEncoder[T]) Put(in []T) {
	for len(in) > 0 {
		n := copy(enc.vectorBuf[enc.bufCount:], in)
		enc.bufCount += n
		enc.totalCount += n
		in = in[n:]

		if enc.bufCount == enc.vectorSize {
			enc.encodeVector(enc.vectorBuf[:enc.bufCount])
			enc.bufCount = 0
		}
	}
}

func (enc *alpEncoder[T]) PutSpaced(in []T, validBits []byte, validBitsOffset int64) {
	nbuf := make([]T, len(in))
	nvalid := spacedCompress(in, nbuf, validBits, validBitsOffset)
	enc.Put(nbuf[:nvalid])
}

// findParams picks the scaling for one vector and records what it picked, so
// that the first alpSamplerVectors vectors seed the preset list.
func (enc *alpEncoder[T]) findParams(values []T) alpEncodingParams {
	sample := alpSample(values, enc.sampleScratch[:0])

	if enc.cachedPresets != nil {
		if len(enc.cachedPresets) == 1 {
			return alpEncodingParams{exponent: enc.cachedPresets[0][0], factor: enc.cachedPresets[0][1]}
		}
		return alpFindBestParamsWithPresets(sample, enc.cachedPresets)
	}

	params := alpFindBestParams(sample)
	enc.presetCounts[alpPresetKey(params.exponent, params.factor)]++
	return params
}

func (enc *alpEncoder[T]) encodeVector(values []T) {
	params := enc.findParams(values)

	enc.vectorsProcessed++
	if enc.cachedPresets == nil && enc.vectorsProcessed >= alpSamplerVectors {
		enc.buildPresetCache()
	}

	encoded := enc.encodedScratch[:len(values)]
	enc.excPositions = enc.excPositions[:0]
	enc.excValues = enc.excValues[:0]

	// A value is an exception when the round trip does not return it unchanged,
	// which this pass finds out by doing the round trip.
	minValue := int64(math.MaxInt64)
	for i, v := range values {
		e := alpEncode(v, params.exponent, params.factor)
		if alpDecode[T](e, params.exponent, params.factor) != v {
			enc.excPositions = append(enc.excPositions, uint16(i))
			enc.excValues = append(enc.excValues, v)
			continue
		}
		encoded[i] = e
		minValue = min(e, minValue)
	}

	if len(enc.excValues) == len(values) {
		// Nothing encoded, so nothing constrains the frame of reference.
		minValue = 0
	}
	// An exception still takes up a slot among the packed integers. Filling the
	// slot with the frame of reference itself costs no bits, so the exceptions
	// cannot widen the packing.
	for _, pos := range enc.excPositions {
		encoded[pos] = minValue
	}

	// The difference between two int64 values can exceed int64, so take it
	// unsigned: the wrapped result is the difference the decoder adds back.
	deltas := enc.deltaScratch[:len(values)]
	var maxDelta uint64
	for i, e := range encoded {
		deltas[i] = uint64(e) - uint64(minValue)
		maxDelta = max(deltas[i], maxDelta)
	}
	bitWidth := alpBitWidth(maxDelta)

	startLen := len(enc.encodedBuf)
	enc.encodedBuf = append(enc.encodedBuf, byte(params.exponent), byte(params.factor))
	enc.encodedBuf = binary.LittleEndian.AppendUint16(enc.encodedBuf, uint16(len(enc.excPositions)))
	enc.encodedBuf = alpAppendInt[T](enc.encodedBuf, uint64(minValue))
	enc.encodedBuf = append(enc.encodedBuf, byte(bitWidth))
	enc.encodedBuf = alpAppendPackedBits(enc.encodedBuf, deltas, bitWidth)

	for _, pos := range enc.excPositions {
		enc.encodedBuf = binary.LittleEndian.AppendUint16(enc.encodedBuf, pos)
	}
	for _, v := range enc.excValues {
		enc.encodedBuf = alpAppendInt[T](enc.encodedBuf, alpFloatToBits(v))
	}

	enc.vectorSizes = append(enc.vectorSizes, len(enc.encodedBuf)-startLen)
}

// alpAppendInt appends v to dst little endian, at the width ALP stores integers
// of type T at.
func alpAppendInt[T alpFloat](dst []byte, v uint64) []byte {
	if alpEncodedBytes[T]() == 4 {
		return binary.LittleEndian.AppendUint32(dst, uint32(v))
	}
	return binary.LittleEndian.AppendUint64(dst, v)
}

// alpReadInt reads what alpAppendInt wrote, sign extending it.
func alpReadInt[T alpFloat](src []byte) int64 {
	if alpEncodedBytes[T]() == 4 {
		return int64(int32(binary.LittleEndian.Uint32(src)))
	}
	return int64(binary.LittleEndian.Uint64(src))
}

// alpReadFloat reads one exception value.
func alpReadFloat[T alpFloat](src []byte) T {
	if alpEncodedBytes[T]() == 4 {
		return alpFloatFromBits[T](uint64(binary.LittleEndian.Uint32(src)))
	}
	return alpFloatFromBits[T](binary.LittleEndian.Uint64(src))
}

// buildPresetCache keeps the exponent and factor pairs the sampled vectors
// chose most often, ordered by how often they won.
func (enc *alpEncoder[T]) buildPresetCache() {
	type preset struct {
		key   uint32
		count int
	}

	sorted := make([]preset, 0, len(enc.presetCounts))
	for k, v := range enc.presetCounts {
		sorted = append(sorted, preset{k, v})
	}
	// The counts come out of a map, so the exponent and the factor break ties:
	// without them the shortlist, and the encoded bytes, would differ between
	// two runs over the same input.
	slices.SortStableFunc(sorted, func(a, b preset) int {
		if a.count != b.count {
			return b.count - a.count
		}
		return int(b.key) - int(a.key)
	})

	numPresets := min(len(sorted), alpMaxPresetCombinations)
	enc.cachedPresets = make([][2]int, numPresets)
	for i, p := range sorted[:numPresets] {
		enc.cachedPresets[i][0] = int(p.key >> 16)
		enc.cachedPresets[i][1] = int(p.key & 0xFFFF)
	}
}

func (enc *alpEncoder[T]) EstimatedDataEncodedSize() int64 {
	// The buffered values are not encoded yet, so charge them the width they
	// would take under a bit packing that saved nothing.
	return int64(len(enc.encodedBuf)) + int64(enc.bufCount*alpEncodedBytes[T]())
}

func (enc *alpEncoder[T]) FlushValues() (Buffer, error) {
	if enc.bufCount > 0 {
		enc.encodeVector(enc.vectorBuf[:enc.bufCount])
		enc.bufCount = 0
	}

	if enc.totalCount == 0 {
		return enc.encoder.FlushValues()
	}

	offsetArraySize := len(enc.vectorSizes) * 4
	enc.sink.Reserve(alpHeaderSize + offsetArraySize + len(enc.encodedBuf))

	header := make([]byte, 0, alpHeaderSize+offsetArraySize)
	header = append(header, alpCompressionMode, alpIntegerEncodingFOR, byte(enc.logVectorSize))
	header = binary.LittleEndian.AppendUint32(header, uint32(enc.totalCount))

	offset := uint32(offsetArraySize)
	for _, size := range enc.vectorSizes {
		header = binary.LittleEndian.AppendUint32(header, offset)
		offset += uint32(size)
	}

	enc.sink.Write(header)
	enc.sink.Write(enc.encodedBuf)
	return enc.sink.Finish(), nil
}

func (enc *alpEncoder[T]) Reset() {
	enc.encoder.Reset()
	enc.bufCount = 0
	enc.totalCount = 0
	enc.encodedBuf = enc.encodedBuf[:0]
	enc.vectorSizes = enc.vectorSizes[:0]
	enc.vectorsProcessed = 0
	enc.cachedPresets = nil
	enc.presetCounts = make(map[uint32]int)
}

type alpDecoder[T alpFloat] struct {
	decoder

	vectorSize int
	totalCount int
	numVectors int

	vectorOffsets []uint32
	bodyData      []byte

	// The decoder holds one decoded vector at a time, since Decode is free to
	// stop part way through one.
	currentVectorIndex int
	currentIndex       int
	decodedValues      []T
	deltas             []uint64

	// One reader pair, reset onto each vector's packed deltas in turn.
	packedReader *bytes.Reader
	bitReader    *utils.BitReader
}

// newAlpDecoder returns a decoder for a float column written with ALP.
func newAlpDecoder[T alpFloat](e format.Encoding, descr *schema.Column) *alpDecoder[T] {
	packed := bytes.NewReader(nil)
	return &alpDecoder[T]{
		decoder:      newDecoderBase(e, descr),
		packedReader: packed,
		bitReader:    utils.NewBitReader(packed),
	}
}

func (dec *alpDecoder[T]) Type() parquet.Type {
	var z T
	switch any(z).(type) {
	case float32:
		return parquet.Types.Float
	case float64:
		return parquet.Types.Double
	}
	panic("parquet: ALP: unsupported float type")
}

func (dec *alpDecoder[T]) SetData(nvals int, data []byte) error {
	if len(data) < alpHeaderSize {
		return fmt.Errorf("parquet: ALP data too short for header: %d bytes", len(data))
	}

	compressionMode := data[0]
	integerEncoding := data[1]
	logVectorSize := data[2]
	numElements := int32(binary.LittleEndian.Uint32(data[3:7]))

	if compressionMode != alpCompressionMode {
		return fmt.Errorf("parquet: unsupported ALP compression mode: %d", compressionMode)
	}
	if integerEncoding != alpIntegerEncodingFOR {
		return fmt.Errorf("parquet: unsupported ALP integer encoding: %d", integerEncoding)
	}
	if logVectorSize < alpMinLogVectorSize || logVectorSize > alpMaxLogVectorSize {
		return fmt.Errorf("parquet: invalid ALP log vector size: %d, must be between %d and %d",
			logVectorSize, alpMinLogVectorSize, alpMaxLogVectorSize)
	}
	if numElements < 0 {
		return fmt.Errorf("parquet: invalid ALP element count: %d", numElements)
	}

	dec.vectorSize = 1 << logVectorSize
	dec.totalCount = int(numElements)
	dec.numVectors = (dec.totalCount + dec.vectorSize - 1) / dec.vectorSize
	dec.currentIndex = 0
	dec.currentVectorIndex = -1
	dec.nvals = nvals

	offsetArraySize := dec.numVectors * 4
	if len(data) < alpHeaderSize+offsetArraySize {
		return fmt.Errorf("parquet: ALP data too short for offset array: need %d, have %d",
			alpHeaderSize+offsetArraySize, len(data))
	}

	dec.vectorOffsets = make([]uint32, dec.numVectors)
	for i := range dec.vectorOffsets {
		dec.vectorOffsets[i] = binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
	}

	// Vector offsets count from the start of the offset array, so keep the page
	// from there rather than from the start of the data.
	dec.bodyData = data[alpHeaderSize:]
	dec.decodedValues = make([]T, dec.vectorSize)
	dec.deltas = make([]uint64, dec.vectorSize)

	return nil
}

// vectorLength returns how many values the given vector holds. Only the last
// one can be short.
func (dec *alpDecoder[T]) vectorLength(vectorIdx int) int {
	if vectorIdx < dec.numVectors-1 {
		return dec.vectorSize
	}
	if lastLen := dec.totalCount % dec.vectorSize; lastLen != 0 {
		return lastLen
	}
	return dec.vectorSize
}

// decodeVector decodes one vector into dec.decodedValues. It checks the sizes
// it reads, since the page may be truncated or corrupt.
func (dec *alpDecoder[T]) decodeVector(vectorIdx int) error {
	vectorLen := dec.vectorLength(vectorIdx)
	pos := int(dec.vectorOffsets[vectorIdx])
	valueBytes := alpEncodedBytes[T]()

	if pos < 0 || pos+alpInfoSize+valueBytes+1 > len(dec.bodyData) {
		return fmt.Errorf("parquet: ALP vector %d starts past the end of the page", vectorIdx)
	}

	exponent := int(dec.bodyData[pos])
	factor := int(dec.bodyData[pos+1])
	numExceptions := int(binary.LittleEndian.Uint16(dec.bodyData[pos+2:]))
	pos += alpInfoSize

	if exponent >= alpNumExponents[T]() || factor > exponent {
		return fmt.Errorf("parquet: invalid ALP exponent %d and factor %d", exponent, factor)
	}
	if numExceptions > vectorLen {
		return fmt.Errorf("parquet: ALP vector %d claims %d exceptions in %d values",
			vectorIdx, numExceptions, vectorLen)
	}

	frameOfRef := alpReadInt[T](dec.bodyData[pos:])
	bitWidth := int(dec.bodyData[pos+valueBytes])
	pos += valueBytes + 1

	if bitWidth > valueBytes*8 {
		return fmt.Errorf("parquet: invalid ALP bit width: %d", bitWidth)
	}

	packedSize := alpPackedSize(vectorLen, bitWidth)
	exceptionSize := numExceptions * (2 + valueBytes)
	if pos+packedSize+exceptionSize > len(dec.bodyData) {
		return fmt.Errorf("parquet: ALP vector %d runs past the end of the page", vectorIdx)
	}

	deltas := dec.deltas[:vectorLen]
	if err := alpUnpackBits(dec.bitReader, dec.packedReader, dec.bodyData[pos:pos+packedSize], deltas, bitWidth); err != nil {
		return err
	}
	pos += packedSize

	for i, d := range deltas {
		dec.decodedValues[i] = alpDecode[T](int64(d)+frameOfRef, exponent, factor)
	}

	// Positions come first, then the values, so read both in one pass instead
	// of copying the positions out.
	valuePos := pos + numExceptions*2
	for e := range numExceptions {
		idx := binary.LittleEndian.Uint16(dec.bodyData[pos+e*2:])
		if int(idx) >= vectorLen {
			return fmt.Errorf("parquet: ALP exception position %d is outside vector %d", idx, vectorIdx)
		}
		dec.decodedValues[idx] = alpReadFloat[T](dec.bodyData[valuePos+e*valueBytes:])
	}

	return nil
}

func (dec *alpDecoder[T]) Decode(out []T) (int, error) {
	toRead := min(len(out), dec.nvals)

	read := 0
	for read < toRead {
		vectorIdx := dec.currentIndex / dec.vectorSize
		if vectorIdx >= dec.numVectors {
			break
		}

		if vectorIdx != dec.currentVectorIndex {
			if err := dec.decodeVector(vectorIdx); err != nil {
				dec.nvals -= read
				return read, err
			}
			dec.currentVectorIndex = vectorIdx
		}

		indexInVector := dec.currentIndex % dec.vectorSize
		vectorLen := dec.vectorLength(vectorIdx)
		n := copy(out[read:toRead], dec.decodedValues[indexInVector:vectorLen])
		read += n
		dec.currentIndex += n
	}

	dec.nvals -= read
	return read, nil
}

func (dec *alpDecoder[T]) DecodeSpaced(out []T, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toRead := len(out) - nullCount
	valuesRead, err := dec.Decode(out[:toRead])
	if err != nil {
		return valuesRead, err
	}
	if valuesRead != toRead {
		return valuesRead, errors.New("parquet: number of values / definition levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

func (dec *alpDecoder[T]) Discard(n int) (int, error) {
	n = min(n, dec.nvals)
	dec.nvals -= n
	dec.currentIndex += n
	return n, nil
}

type AlpFloat32Encoder = alpEncoder[float32]
type AlpFloat64Encoder = alpEncoder[float64]
type AlpFloat32Decoder = alpDecoder[float32]
type AlpFloat64Decoder = alpDecoder[float64]
