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

// ALP (Adaptive Lossless floating-Point) encoding for FLOAT and DOUBLE types.
// Reference: https://dl.acm.org/doi/10.1145/3626717

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"golang.org/x/xerrors"
)

const (
	alpVersion            = 1
	alpCompressionMode    = 0 // ALP
	alpIntegerEncodingFOR = 0 // kForBitPack
	alpHeaderSize         = 8
	alpDefaultVectorSize  = 1024
	alpDefaultLogVector   = 10
	alpMaxLogVectorSize   = 15
	alpMinLogVectorSize   = 3

	alpInfoSize       = 4 // exponent(1) + factor(1) + num_exceptions(2)
	alpFloatForInfo   = 5 // frame_of_reference(4) + bit_width(1)
	alpDoubleForInfo  = 9 // frame_of_reference(8) + bit_width(1)

	alpFloatMaxExponent  = 10
	alpDoubleMaxExponent = 18

	alpSamplerVectors        = 8
	alpMaxPresetCombinations = 5

	alpMagicFloat32 = float32(12582912.0)     // 2^22 + 2^23
	alpMagicFloat64 = float64(6755399441055744.0) // 2^51 + 2^52

	alpNegZeroFloat32Bits = uint32(0x80000000)
	alpNegZeroFloat64Bits = uint64(0x8000000000000000)
)

var (
	alpFloatPow10 = [11]float32{
		1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	}
	alpDoublePow10 = [19]float64{
		1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
		1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
	}
)
func alpFloatMultiplier(exponent, factor int) float32 {
	m := alpFloatPow10[exponent]
	if factor > 0 {
		m /= alpFloatPow10[factor]
	}
	return m
}

func alpDoubleMultiplier(exponent, factor int) float64 {
	m := alpDoublePow10[exponent]
	if factor > 0 {
		m /= alpDoublePow10[factor]
	}
	return m
}

func alpFastRoundFloat32(v float32) int32 {
	if v >= 0 {
		return int32((v + alpMagicFloat32) - alpMagicFloat32)
	}
	return int32((v - alpMagicFloat32) + alpMagicFloat32)
}

func alpFastRoundFloat64(v float64) int64 {
	if v >= 0 {
		return int64((v + alpMagicFloat64) - alpMagicFloat64)
	}
	return int64((v - alpMagicFloat64) + alpMagicFloat64)
}

func alpIsBasicFloatException(v float32) bool {
	return math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) ||
		math.Float32bits(v) == alpNegZeroFloat32Bits
}

func alpIsBasicDoubleException(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 0) ||
		math.Float64bits(v) == alpNegZeroFloat64Bits
}

func alpIsFloatException(v float32, exponent, factor int) bool {
	if alpIsBasicFloatException(v) {
		return true
	}
	m := alpFloatMultiplier(exponent, factor)
	scaled := float64(v) * float64(m)
	if scaled > math.MaxInt32 || scaled < math.MinInt32 {
		return true
	}
	encoded := alpFastRoundFloat32(v * m)
	decoded := float32(encoded) / m
	return math.Float32bits(v) != math.Float32bits(decoded)
}

func alpIsDoubleException(v float64, exponent, factor int) bool {
	if alpIsBasicDoubleException(v) {
		return true
	}
	m := alpDoubleMultiplier(exponent, factor)
	scaled := v * m
	if scaled > math.MaxInt64 || scaled < math.MinInt64 {
		return true
	}
	encoded := alpFastRoundFloat64(v * m)
	decoded := float64(encoded) / m
	return math.Float64bits(v) != math.Float64bits(decoded)
}

func alpEncodeFloat32(v float32, exponent, factor int) int32 {
	return alpFastRoundFloat32(v * alpFloatMultiplier(exponent, factor))
}

func alpDecodeFloat32(encoded int32, exponent, factor int) float32 {
	return float32(encoded) / alpFloatMultiplier(exponent, factor)
}

func alpEncodeFloat64(v float64, exponent, factor int) int64 {
	return alpFastRoundFloat64(v * alpDoubleMultiplier(exponent, factor))
}

func alpDecodeFloat64(encoded int64, exponent, factor int) float64 {
	return float64(encoded) / alpDoubleMultiplier(exponent, factor)
}

func alpBitWidth32(v uint32) int {
	if v == 0 {
		return 0
	}
	return bits.Len32(v)
}

func alpBitWidth64(v uint64) int {
	if v == 0 {
		return 0
	}
	return bits.Len64(v)
}
func alpPackBits32(values []uint32, count, bitWidth int) []byte {
	if bitWidth == 0 || count == 0 {
		return nil
	}
	packedSize := (count*bitWidth + 7) / 8
	out := make([]byte, packedSize)

	var buf uint64
	bitsInBuf := 0
	byteIdx := 0

	for i := 0; i < count; i++ {
		buf |= uint64(values[i]) << uint(bitsInBuf)
		bitsInBuf += bitWidth

		for bitsInBuf >= 8 {
			out[byteIdx] = byte(buf)
			buf >>= 8
			bitsInBuf -= 8
			byteIdx++
		}
	}

	if bitsInBuf > 0 {
		out[byteIdx] = byte(buf)
	}

	return out
}

func alpUnpackBits32(packed []byte, count, bitWidth int) []uint32 {
	out := make([]uint32, count)
	if bitWidth == 0 || count == 0 {
		return out
	}

	var buf uint64
	bitsInBuf := 0
	byteIdx := 0
	mask := uint64((1 << uint(bitWidth)) - 1)

	for i := 0; i < count; i++ {
		for bitsInBuf < bitWidth && byteIdx < len(packed) {
			buf |= uint64(packed[byteIdx]) << uint(bitsInBuf)
			bitsInBuf += 8
			byteIdx++
		}

		out[i] = uint32(buf & mask)
		buf >>= uint(bitWidth)
		bitsInBuf -= bitWidth
	}

	return out
}

func alpPackBits64(values []uint64, count, bitWidth int) []byte {
	if bitWidth == 0 || count == 0 {
		return nil
	}
	packedSize := (count*bitWidth + 7) / 8
	out := make([]byte, packedSize)

	byteIdx := 0
	bitPos := 0

	for i := 0; i < count; i++ {
		val := values[i]
		remaining := bitWidth

		for remaining > 0 {
			bitIdx := bitPos & 7
			bitsToWrite := remaining
			if bitsToWrite > 8-bitIdx {
				bitsToWrite = 8 - bitIdx
			}
			m := uint64((1 << uint(bitsToWrite)) - 1)
			out[byteIdx] |= byte((val & m) << uint(bitIdx))
			val >>= uint(bitsToWrite)
			bitPos += bitsToWrite
			remaining -= bitsToWrite
			byteIdx = bitPos >> 3
		}
	}

	return out
}

func alpUnpackBits64(packed []byte, count, bitWidth int) []uint64 {
	out := make([]uint64, count)
	if bitWidth == 0 || count == 0 {
		return out
	}

	byteIdx := 0
	bitPos := 0

	for i := 0; i < count; i++ {
		var val uint64
		remaining := bitWidth
		shift := 0

		for remaining > 0 {
			bitIdx := bitPos & 7
			bitsToRead := remaining
			if bitsToRead > 8-bitIdx {
				bitsToRead = 8 - bitIdx
			}
			m := byte((1 << uint(bitsToRead)) - 1)
			val |= uint64((packed[byteIdx]>>uint(bitIdx))&m) << uint(shift)
			shift += bitsToRead
			bitPos += bitsToRead
			remaining -= bitsToRead
			byteIdx = bitPos >> 3
		}

		out[i] = val
	}

	return out
}
type alpEncodingParams struct {
	exponent      int
	factor        int
	numExceptions int
}

func alpFindBestFloat32Params(values []float32, offset, length int) alpEncodingParams {
	best := alpEncodingParams{exponent: 0, factor: 0, numExceptions: length}

	for e := 0; e <= alpFloatMaxExponent; e++ {
		for f := 0; f <= e; f++ {
			exceptions := 0
			for i := 0; i < length; i++ {
				if alpIsFloatException(values[offset+i], e, f) {
					exceptions++
				}
			}
			if exceptions < best.numExceptions {
				best = alpEncodingParams{exponent: e, factor: f, numExceptions: exceptions}
				if best.numExceptions == 0 {
					return best
				}
			}
		}
	}
	return best
}

func alpFindBestFloat32ParamsWithPresets(values []float32, offset, length int, presets [][2]int) alpEncodingParams {
	best := alpEncodingParams{exponent: presets[0][0], factor: presets[0][1], numExceptions: length}

	for _, p := range presets {
		e, f := p[0], p[1]
		exceptions := 0
		for i := 0; i < length; i++ {
			if alpIsFloatException(values[offset+i], e, f) {
				exceptions++
			}
		}
		if exceptions < best.numExceptions {
			best = alpEncodingParams{exponent: e, factor: f, numExceptions: exceptions}
			if best.numExceptions == 0 {
				return best
			}
		}
	}
	return best
}

func alpFindBestFloat64Params(values []float64, offset, length int) alpEncodingParams {
	best := alpEncodingParams{exponent: 0, factor: 0, numExceptions: length}

	for e := 0; e <= alpDoubleMaxExponent; e++ {
		for f := 0; f <= e; f++ {
			exceptions := 0
			for i := 0; i < length; i++ {
				if alpIsDoubleException(values[offset+i], e, f) {
					exceptions++
				}
			}
			if exceptions < best.numExceptions {
				best = alpEncodingParams{exponent: e, factor: f, numExceptions: exceptions}
				if best.numExceptions == 0 {
					return best
				}
			}
		}
	}
	return best
}

func alpFindBestFloat64ParamsWithPresets(values []float64, offset, length int, presets [][2]int) alpEncodingParams {
	best := alpEncodingParams{exponent: presets[0][0], factor: presets[0][1], numExceptions: length}

	for _, p := range presets {
		e, f := p[0], p[1]
		exceptions := 0
		for i := 0; i < length; i++ {
			if alpIsDoubleException(values[offset+i], e, f) {
				exceptions++
			}
		}
		if exceptions < best.numExceptions {
			best = alpEncodingParams{exponent: e, factor: f, numExceptions: exceptions}
			if best.numExceptions == 0 {
				return best
			}
		}
	}
	return best
}
type alpEncoder[T float32 | float64] struct {
	encoder

	vectorSize    int
	logVectorSize int

	vectorBuf  []T
	bufCount   int
	totalCount int

	encodedBuf  []byte
	vectorSizes []int

	vectorsProcessed int
	cachedPresets    [][2]int
	presetCounts     map[uint32]int
}

func newAlpEncoder[T float32 | float64](e format.Encoding, descr *schema.Column, mem memory.Allocator) *alpEncoder[T] {
	return &alpEncoder[T]{
		encoder:       newEncoderBase(e, descr, mem),
		vectorSize:    alpDefaultVectorSize,
		logVectorSize: alpDefaultLogVector,
		vectorBuf:     make([]T, alpDefaultVectorSize),
		presetCounts:  make(map[uint32]int),
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
	panic("unreachable")
}

func (enc *alpEncoder[T]) Put(in []T) {
	for i := 0; i < len(in); {
		space := enc.vectorSize - enc.bufCount
		toCopy := len(in) - i
		if toCopy > space {
			toCopy = space
		}
		copy(enc.vectorBuf[enc.bufCount:], in[i:i+toCopy])
		enc.bufCount += toCopy
		enc.totalCount += toCopy
		i += toCopy

		if enc.bufCount == enc.vectorSize {
			enc.encodeVector(enc.bufCount)
			enc.bufCount = 0
		}
	}
}

func (enc *alpEncoder[T]) PutSpaced(in []T, validBits []byte, validBitsOffset int64) {
	nbuf := make([]T, len(in))
	nvalid := spacedCompress(in, nbuf, validBits, validBitsOffset)
	enc.Put(nbuf[:nvalid])
}

func (enc *alpEncoder[T]) encodeVector(vectorLen int) {
	var z T
	switch any(z).(type) {
	case float32:
		enc.encodeFloat32Vector(vectorLen)
	case float64:
		enc.encodeFloat64Vector(vectorLen)
	}
}

func (enc *alpEncoder[T]) encodeFloat32Vector(vectorLen int) {
	values := make([]float32, vectorLen)
	for i := 0; i < vectorLen; i++ {
		values[i] = float32(any(enc.vectorBuf[i]).(float32))
	}

	var params alpEncodingParams
	if enc.cachedPresets != nil {
		params = alpFindBestFloat32ParamsWithPresets(values, 0, vectorLen, enc.cachedPresets)
	} else {
		params = alpFindBestFloat32Params(values, 0, vectorLen)
		key := uint32(params.exponent<<16) | uint32(params.factor)
		enc.presetCounts[key]++
	}

	enc.vectorsProcessed++
	if enc.cachedPresets == nil && enc.vectorsProcessed >= alpSamplerVectors {
		enc.buildPresetCache()
	}

	encoded := make([]int32, vectorLen)
	excPositions := make([]uint16, 0, params.numExceptions)
	excValues := make([]float32, 0, params.numExceptions)

	var placeholder int32
	for i := 0; i < vectorLen; i++ {
		if !alpIsFloatException(values[i], params.exponent, params.factor) {
			placeholder = alpEncodeFloat32(values[i], params.exponent, params.factor)
			break
		}
	}

	minValue := int32(math.MaxInt32)
	for i := 0; i < vectorLen; i++ {
		if alpIsFloatException(values[i], params.exponent, params.factor) {
			excPositions = append(excPositions, uint16(i))
			excValues = append(excValues, values[i])
			encoded[i] = placeholder
		} else {
			encoded[i] = alpEncodeFloat32(values[i], params.exponent, params.factor)
		}
		if encoded[i] < minValue {
			minValue = encoded[i]
		}
	}

	var maxDelta uint32
	deltas := make([]uint32, vectorLen)
	for i := 0; i < vectorLen; i++ {
		deltas[i] = uint32(encoded[i] - minValue) // wrapping subtraction, result is unsigned
		if deltas[i] > maxDelta {
			maxDelta = deltas[i]
		}
	}

	bitWidth := alpBitWidth32(maxDelta)

	startLen := len(enc.encodedBuf)

	var alpInfo [alpInfoSize]byte
	alpInfo[0] = byte(params.exponent)
	alpInfo[1] = byte(params.factor)
	binary.LittleEndian.PutUint16(alpInfo[2:], uint16(params.numExceptions))
	enc.encodedBuf = append(enc.encodedBuf, alpInfo[:]...)

	var forInfo [alpFloatForInfo]byte
	binary.LittleEndian.PutUint32(forInfo[0:], uint32(minValue))
	forInfo[4] = byte(bitWidth)
	enc.encodedBuf = append(enc.encodedBuf, forInfo[:]...)

	if bitWidth > 0 {
		packed := alpPackBits32(deltas, vectorLen, bitWidth)
		enc.encodedBuf = append(enc.encodedBuf, packed...)
	}

	if params.numExceptions > 0 {
		for _, pos := range excPositions {
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], pos)
			enc.encodedBuf = append(enc.encodedBuf, buf[:]...)
		}
		for _, val := range excValues {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], math.Float32bits(val))
			enc.encodedBuf = append(enc.encodedBuf, buf[:]...)
		}
	}

	enc.vectorSizes = append(enc.vectorSizes, len(enc.encodedBuf)-startLen)
}

func (enc *alpEncoder[T]) encodeFloat64Vector(vectorLen int) {
	values := make([]float64, vectorLen)
	for i := 0; i < vectorLen; i++ {
		values[i] = float64(any(enc.vectorBuf[i]).(float64))
	}

	var params alpEncodingParams
	if enc.cachedPresets != nil {
		params = alpFindBestFloat64ParamsWithPresets(values, 0, vectorLen, enc.cachedPresets)
	} else {
		params = alpFindBestFloat64Params(values, 0, vectorLen)
		key := uint32(params.exponent<<16) | uint32(params.factor)
		enc.presetCounts[key]++
	}

	enc.vectorsProcessed++
	if enc.cachedPresets == nil && enc.vectorsProcessed >= alpSamplerVectors {
		enc.buildPresetCache()
	}

	encoded := make([]int64, vectorLen)
	excPositions := make([]uint16, 0, params.numExceptions)
	excValues := make([]float64, 0, params.numExceptions)

	var placeholder int64
	for i := 0; i < vectorLen; i++ {
		if !alpIsDoubleException(values[i], params.exponent, params.factor) {
			placeholder = alpEncodeFloat64(values[i], params.exponent, params.factor)
			break
		}
	}

	minValue := int64(math.MaxInt64)
	for i := 0; i < vectorLen; i++ {
		if alpIsDoubleException(values[i], params.exponent, params.factor) {
			excPositions = append(excPositions, uint16(i))
			excValues = append(excValues, values[i])
			encoded[i] = placeholder
		} else {
			encoded[i] = alpEncodeFloat64(values[i], params.exponent, params.factor)
		}
		if encoded[i] < minValue {
			minValue = encoded[i]
		}
	}

	var maxDelta uint64
	deltas := make([]uint64, vectorLen)
	for i := 0; i < vectorLen; i++ {
		deltas[i] = uint64(encoded[i] - minValue)
		if deltas[i] > maxDelta {
			maxDelta = deltas[i]
		}
	}

	bitWidth := alpBitWidth64(maxDelta)

	startLen := len(enc.encodedBuf)

	var alpInfo [alpInfoSize]byte
	alpInfo[0] = byte(params.exponent)
	alpInfo[1] = byte(params.factor)
	binary.LittleEndian.PutUint16(alpInfo[2:], uint16(params.numExceptions))
	enc.encodedBuf = append(enc.encodedBuf, alpInfo[:]...)

	var forInfo [alpDoubleForInfo]byte
	binary.LittleEndian.PutUint64(forInfo[0:], uint64(minValue))
	forInfo[8] = byte(bitWidth)
	enc.encodedBuf = append(enc.encodedBuf, forInfo[:]...)

	if bitWidth > 0 {
		packed := alpPackBits64(deltas, vectorLen, bitWidth)
		enc.encodedBuf = append(enc.encodedBuf, packed...)
	}

	if params.numExceptions > 0 {
		for _, pos := range excPositions {
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], pos)
			enc.encodedBuf = append(enc.encodedBuf, buf[:]...)
		}
		for _, val := range excValues {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(val))
			enc.encodedBuf = append(enc.encodedBuf, buf[:]...)
		}
	}

	enc.vectorSizes = append(enc.vectorSizes, len(enc.encodedBuf)-startLen)
}

func (enc *alpEncoder[T]) buildPresetCache() {
	type kv struct {
		key   uint32
		count int
	}
	sorted := make([]kv, 0, len(enc.presetCounts))
	for k, v := range enc.presetCounts {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	numPresets := len(sorted)
	if numPresets > alpMaxPresetCombinations {
		numPresets = alpMaxPresetCombinations
	}
	enc.cachedPresets = make([][2]int, numPresets)
	for i := 0; i < numPresets; i++ {
		enc.cachedPresets[i][0] = int(sorted[i].key >> 16)
		enc.cachedPresets[i][1] = int(sorted[i].key & 0xFFFF)
	}
}

func (enc *alpEncoder[T]) EstimatedDataEncodedSize() int64 {
	return int64(len(enc.encodedBuf)) + int64(enc.bufCount)*3
}

func (enc *alpEncoder[T]) FlushValues() (Buffer, error) {
	if enc.bufCount > 0 {
		enc.encodeVector(enc.bufCount)
		enc.bufCount = 0
	}

	if enc.totalCount == 0 {
		return enc.encoder.FlushValues()
	}

	numVectors := len(enc.vectorSizes)
	offsetArraySize := numVectors * 4
	totalSize := alpHeaderSize + offsetArraySize + len(enc.encodedBuf)
	enc.sink.Reserve(totalSize)

	var header [alpHeaderSize]byte
	header[0] = alpVersion
	header[1] = alpCompressionMode
	header[2] = alpIntegerEncodingFOR
	header[3] = byte(enc.logVectorSize)
	binary.LittleEndian.PutUint32(header[4:], uint32(enc.totalCount))
	enc.sink.Write(header[:])

	currentOffset := uint32(offsetArraySize)
	for _, size := range enc.vectorSizes {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], currentOffset)
		enc.sink.Write(buf[:])
		currentOffset += uint32(size)
	}

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

func (enc *alpEncoder[T]) Release() {
	enc.encoder.Release()
}
type alpDecoder[T float32 | float64] struct {
	decoder

	vectorSize    int
	totalCount    int
	numVectors    int

	vectorOffsets   []uint32
	bodyData        []byte
	offsetArraySize int

	currentIndex       int
	currentVectorIndex int
	decodedValues      []T
}

func (dec *alpDecoder[T]) Type() parquet.Type {
	var z T
	switch any(z).(type) {
	case float32:
		return parquet.Types.Float
	case float64:
		return parquet.Types.Double
	}
	panic("unreachable")
}

func (dec *alpDecoder[T]) SetData(nvals int, data []byte) error {
	if len(data) < alpHeaderSize {
		return fmt.Errorf("parquet: ALP data too short for header: %d bytes", len(data))
	}

	version := data[0]
	compressionMode := data[1]
	integerEncoding := data[2]
	logVectorSize := data[3]
	numElements := int32(binary.LittleEndian.Uint32(data[4:8]))

	if version != alpVersion {
		return fmt.Errorf("parquet: unsupported ALP version: %d, expected %d", version, alpVersion)
	}
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

	dec.vectorSize = 1 << uint(logVectorSize)
	dec.totalCount = int(numElements)
	dec.numVectors = (dec.totalCount + dec.vectorSize - 1) / dec.vectorSize
	dec.currentIndex = 0
	dec.currentVectorIndex = -1
	dec.nvals = nvals

	dec.offsetArraySize = dec.numVectors * 4
	offsetEnd := alpHeaderSize + dec.offsetArraySize
	if len(data) < offsetEnd {
		return fmt.Errorf("parquet: ALP data too short for offset array: need %d, have %d",
			offsetEnd, len(data))
	}

	dec.vectorOffsets = make([]uint32, dec.numVectors)
	for i := 0; i < dec.numVectors; i++ {
		dec.vectorOffsets[i] = binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
	}

	dec.bodyData = data[alpHeaderSize:]
	dec.decodedValues = make([]T, dec.vectorSize)

	return nil
}

func (dec *alpDecoder[T]) getVectorLength(vectorIdx int) int {
	if vectorIdx < dec.numVectors-1 {
		return dec.vectorSize
	}
	lastLen := dec.totalCount % dec.vectorSize
	if lastLen == 0 {
		return dec.vectorSize
	}
	return lastLen
}

func (dec *alpDecoder[T]) getVectorDataPos(vectorIdx int) int {
	return int(dec.vectorOffsets[vectorIdx])
}

func (dec *alpDecoder[T]) decodeVector(vectorIdx int) {
	var z T
	switch any(z).(type) {
	case float32:
		dec.decodeFloat32Vector(vectorIdx)
	case float64:
		dec.decodeFloat64Vector(vectorIdx)
	}
}

func (dec *alpDecoder[T]) decodeFloat32Vector(vectorIdx int) {
	vectorLen := dec.getVectorLength(vectorIdx)
	pos := dec.getVectorDataPos(vectorIdx)

	exponent := int(dec.bodyData[pos])
	factor := int(dec.bodyData[pos+1])
	numExceptions := int(binary.LittleEndian.Uint16(dec.bodyData[pos+2:]))
	pos += alpInfoSize

	frameOfRef := int32(binary.LittleEndian.Uint32(dec.bodyData[pos:]))
	bitWidth := int(dec.bodyData[pos+4])
	pos += alpFloatForInfo

	var deltas []uint32
	if bitWidth > 0 {
		packedSize := (vectorLen*bitWidth + 7) / 8
		deltas = alpUnpackBits32(dec.bodyData[pos:pos+packedSize], vectorLen, bitWidth)
		pos += packedSize
	} else {
		deltas = make([]uint32, vectorLen)
	}

	outSlice := any(&dec.decodedValues).(*[]float32)
	for i := 0; i < vectorLen; i++ {
		encoded := int32(deltas[i]) + frameOfRef
		(*outSlice)[i] = alpDecodeFloat32(encoded, exponent, factor)
	}

	if numExceptions > 0 {
		excPositions := make([]uint16, numExceptions)
		for e := 0; e < numExceptions; e++ {
			excPositions[e] = binary.LittleEndian.Uint16(dec.bodyData[pos:])
			pos += 2
		}
		for e := 0; e < numExceptions; e++ {
			bits := binary.LittleEndian.Uint32(dec.bodyData[pos:])
			(*outSlice)[excPositions[e]] = math.Float32frombits(bits)
			pos += 4
		}
	}
}

func (dec *alpDecoder[T]) decodeFloat64Vector(vectorIdx int) {
	vectorLen := dec.getVectorLength(vectorIdx)
	pos := dec.getVectorDataPos(vectorIdx)

	exponent := int(dec.bodyData[pos])
	factor := int(dec.bodyData[pos+1])
	numExceptions := int(binary.LittleEndian.Uint16(dec.bodyData[pos+2:]))
	pos += alpInfoSize

	frameOfRef := int64(binary.LittleEndian.Uint64(dec.bodyData[pos:]))
	bitWidth := int(dec.bodyData[pos+8])
	pos += alpDoubleForInfo

	var deltas []uint64
	if bitWidth > 0 {
		packedSize := (vectorLen*bitWidth + 7) / 8
		deltas = alpUnpackBits64(dec.bodyData[pos:pos+packedSize], vectorLen, bitWidth)
		pos += packedSize
	} else {
		deltas = make([]uint64, vectorLen)
	}

	outSlice := any(&dec.decodedValues).(*[]float64)
	for i := 0; i < vectorLen; i++ {
		encoded := int64(deltas[i]) + frameOfRef
		(*outSlice)[i] = alpDecodeFloat64(encoded, exponent, factor)
	}

	if numExceptions > 0 {
		excPositions := make([]uint16, numExceptions)
		for e := 0; e < numExceptions; e++ {
			excPositions[e] = binary.LittleEndian.Uint16(dec.bodyData[pos:])
			pos += 2
		}
		for e := 0; e < numExceptions; e++ {
			bits := binary.LittleEndian.Uint64(dec.bodyData[pos:])
			(*outSlice)[excPositions[e]] = math.Float64frombits(bits)
			pos += 8
		}
	}
}

func (dec *alpDecoder[T]) Decode(out []T) (int, error) {
	toRead := len(out)
	if toRead > dec.nvals {
		toRead = dec.nvals
	}

	read := 0
	for read < toRead {
		// Determine which vector we're reading from
		vectorIdx := dec.currentIndex / dec.vectorSize
		if vectorIdx >= dec.numVectors {
			break
		}

		// Ensure current vector is decoded
		if vectorIdx != dec.currentVectorIndex {
			dec.decodeVector(vectorIdx)
			dec.currentVectorIndex = vectorIdx
		}

		// Copy from decoded buffer
		indexInVector := dec.currentIndex % dec.vectorSize
		vectorLen := dec.getVectorLength(vectorIdx)
		available := vectorLen - indexInVector
		toCopy := toRead - read
		if toCopy > available {
			toCopy = available
		}

		copy(out[read:], dec.decodedValues[indexInVector:indexInVector+toCopy])
		read += toCopy
		dec.currentIndex += toCopy
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
		return valuesRead, xerrors.New("parquet: number of values / definitions levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

func (dec *alpDecoder[T]) Discard(n int) (int, error) {
	if n > dec.nvals {
		n = dec.nvals
	}
	dec.nvals -= n
	dec.currentIndex += n
	return n, nil
}
type AlpFloat32Encoder = alpEncoder[float32]
type AlpFloat64Encoder = alpEncoder[float64]
type AlpFloat32Decoder = alpDecoder[float32]
type AlpFloat64Decoder = alpDecoder[float64]
