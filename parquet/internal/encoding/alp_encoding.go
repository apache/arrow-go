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
// exception. alp_scaling.go holds that arithmetic and alp_params.go chooses the
// scaling; this file writes and reads the bytes.
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
	// The two page header bytes that name the algorithm. ALP allows other
	// values, and a page carrying one is a page this implementation cannot read.
	alpCompressionMode    = 0 // ALP, rather than ALP for a sparse column
	alpIntegerEncodingFOR = 0 // frame of reference plus bit packing

	alpHeaderSize = 7

	// The vector size this encoder writes. The format allows anything from
	// 2^alpMinLogVectorSize to 2^alpMaxLogVectorSize, and the decoder honours
	// what a page declares.
	alpDefaultVectorSize    = 1024
	alpDefaultLogVectorSize = 10
	alpMinLogVectorSize     = 3
	alpMaxLogVectorSize     = 15
)

// alpVectorHeaderSize returns how many bytes precede a vector's packed
// differences: the exponent, the factor, the exception count, the frame of
// reference and the bit width.
func alpVectorHeaderSize[T alpFloat]() int {
	return 1 + 1 + 2 + alpEncodedBytes[T]() + 1
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

// alpAppendInt appends v to dst little endian, at the width ALP writes an
// integer of type T at.
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

// alpEncoder encodes a float column with ALP, a vector at a time. It buffers
// values until it holds a whole vector, so the values of a page reach the sink
// only when the caller flushes it.
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

	// The scalings the sampled vectors chose. Once the encoder has enough of
	// them, cachedPresets replaces the full search for the rest of the column.
	vectorsProcessed int
	cachedPresets    []alpEncodingParams
	presetCounts     map[alpEncodingParams]int
}

// newAlpEncoder returns an encoder for a float column, with the scratch every
// vector reuses allocated up front.
func newAlpEncoder[T alpFloat](e format.Encoding, descr *schema.Column, mem memory.Allocator) *alpEncoder[T] {
	return &alpEncoder[T]{
		encoder:        newEncoderBase(e, descr, mem),
		vectorSize:     alpDefaultVectorSize,
		logVectorSize:  alpDefaultLogVectorSize,
		vectorBuf:      make([]T, alpDefaultVectorSize),
		encodedScratch: make([]int64, alpDefaultVectorSize),
		deltaScratch:   make([]uint64, alpDefaultVectorSize),
		sampleScratch:  make([]T, 0, alpSamplerSamples),
		presetCounts:   make(map[alpEncodingParams]int),
	}
}

func (enc *alpEncoder[T]) Type() parquet.Type {
	return alpParquetType[T]()
}

// Put buffers values and encodes each vector as it fills. A partly filled
// vector waits for FlushValues.
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

// PutSpaced drops the null slots of in and buffers the values that remain.
func (enc *alpEncoder[T]) PutSpaced(in []T, validBits []byte, validBitsOffset int64) {
	nbuf := make([]T, len(in))
	nvalid := spacedCompress(in, nbuf, validBits, validBitsOffset)
	enc.Put(nbuf[:nvalid])
}

// findParams picks the scaling for one vector and records what it picked, so
// that the first alpSamplerVectors vectors seed the preset list.
func (enc *alpEncoder[T]) findParams(values []T) alpEncodingParams {
	sample := alpSample(values, enc.sampleScratch[:0])

	if len(enc.cachedPresets) > 0 {
		return alpFindBestParamsWithPresets(sample, enc.cachedPresets)
	}

	params := alpFindBestParams(sample)
	enc.presetCounts[params]++
	return params
}

// encodeVector appends one encoded vector to enc.encodedBuf and records its
// size, which the page header turns into an offset.
func (enc *alpEncoder[T]) encodeVector(values []T) {
	params := enc.findParams(values)

	enc.vectorsProcessed++
	if enc.cachedPresets == nil && enc.vectorsProcessed >= alpSamplerVectors {
		enc.buildPresetCache()
	}

	encoded := enc.encodedScratch[:len(values)]
	enc.excPositions = enc.excPositions[:0]
	enc.excValues = enc.excValues[:0]

	// One pass encodes and collects the exceptions, since finding out whether a
	// value is an exception means encoding it.
	minValue := int64(math.MaxInt64)
	for i, v := range values {
		e, ok := alpTryEncode(v, params.exponent, params.factor)
		if !ok {
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
	bitWidth := bits.Len64(maxDelta)

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

// buildPresetCache keeps the scalings the sampled vectors chose most often,
// ordered by how often they won.
func (enc *alpEncoder[T]) buildPresetCache() {
	type preset struct {
		params alpEncodingParams
		count  int
	}

	sorted := make([]preset, 0, len(enc.presetCounts))
	for k, v := range enc.presetCounts {
		sorted = append(sorted, preset{k, v})
	}
	// The counts come out of a map, so the exponent and the factor break ties:
	// without them the shortlist, and the encoded bytes, would differ between
	// two runs over the same input.
	slices.SortStableFunc(sorted, func(a, b preset) int {
		switch {
		case a.count != b.count:
			return b.count - a.count
		case a.params.exponent != b.params.exponent:
			return b.params.exponent - a.params.exponent
		default:
			return b.params.factor - a.params.factor
		}
	})

	numPresets := min(len(sorted), alpMaxPresetCombinations)
	enc.cachedPresets = make([]alpEncodingParams, numPresets)
	for i, p := range sorted[:numPresets] {
		enc.cachedPresets[i] = p.params
	}
}

// EstimatedDataEncodedSize returns how large the page would be if it were
// flushed now. The buffered values are not encoded yet, so they count at the
// width a bit packing that saved nothing would give them.
func (enc *alpEncoder[T]) EstimatedDataEncodedSize() int64 {
	return int64(len(enc.encodedBuf)) + int64(enc.bufCount*alpEncodedBytes[T]())
}

// FlushValues writes one page and clears the values it wrote, so that the next
// Put starts a new page. The scaling shortlist survives, because it describes
// the column rather than the page.
//
// A page always carries the header, even when it holds no values: a column of
// optional values can produce a page whose rows are all null, and a reader
// rejects an empty page rather than reading it as no values.
func (enc *alpEncoder[T]) FlushValues() (Buffer, error) {
	if enc.bufCount > 0 {
		enc.encodeVector(enc.vectorBuf[:enc.bufCount])
		enc.bufCount = 0
	}
	defer enc.resetPage()

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

// resetPage drops the encoded vectors of the page just written.
func (enc *alpEncoder[T]) resetPage() {
	enc.totalCount = 0
	enc.encodedBuf = enc.encodedBuf[:0]
	enc.vectorSizes = enc.vectorSizes[:0]
}

// Reset returns the encoder to its state when new, shortlist included, so that
// it can encode an unrelated column.
func (enc *alpEncoder[T]) Reset() {
	enc.encoder.Reset()
	enc.resetPage()
	enc.bufCount = 0
	enc.vectorsProcessed = 0
	enc.cachedPresets = nil
	clear(enc.presetCounts)
}

// alpDecoder reads the pages an ALP encoder wrote. It decodes a whole vector at
// a time and holds it, since a caller is free to stop part way through one.
type alpDecoder[T alpFloat] struct {
	decoder

	vectorSize int
	totalCount int
	numVectors int

	vectorOffsets []uint32
	bodyData      []byte

	// The vector the decoder holds, and how far through the page it has read.
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
	return alpParquetType[T]()
}

// SetData points the decoder at one page and reads its header. It rejects a
// header describing an ALP variant this implementation does not write, and one
// whose vector count needs more bytes than the page holds.
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

	// Grow rather than allocate, so that a column of many pages pays for these
	// once. The offset array is the one that changes size from page to page.
	dec.vectorOffsets = slices.Grow(dec.vectorOffsets[:0], dec.numVectors)[:dec.numVectors]
	for i := range dec.vectorOffsets {
		dec.vectorOffsets[i] = binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
	}

	// Vector offsets count from the start of the offset array, so keep the page
	// from there rather than from the start of the data.
	dec.bodyData = data[alpHeaderSize:]
	dec.decodedValues = slices.Grow(dec.decodedValues[:0], dec.vectorSize)[:dec.vectorSize]
	dec.deltas = slices.Grow(dec.deltas[:0], dec.vectorSize)[:dec.vectorSize]

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

// decodeVector decodes one vector into dec.decodedValues. It checks every size
// and position it reads, since the page may be truncated or corrupt.
func (dec *alpDecoder[T]) decodeVector(vectorIdx int) error {
	vectorLen := dec.vectorLength(vectorIdx)
	valueBytes := alpEncodedBytes[T]()

	// A 32-bit int cannot hold every uint32 offset, so a page claiming a huge
	// one leaves pos negative.
	pos := int(dec.vectorOffsets[vectorIdx])
	if pos < 0 || pos+alpVectorHeaderSize[T]() > len(dec.bodyData) {
		return fmt.Errorf("parquet: ALP vector %d starts past the end of the page", vectorIdx)
	}

	exponent := int(dec.bodyData[pos])
	factor := int(dec.bodyData[pos+1])
	numExceptions := int(binary.LittleEndian.Uint16(dec.bodyData[pos+2:]))
	frameOfRef := alpReadInt[T](dec.bodyData[pos+4:])
	bitWidth := int(dec.bodyData[pos+4+valueBytes])
	pos += alpVectorHeaderSize[T]()

	if exponent >= alpNumExponents[T]() || factor > exponent {
		return fmt.Errorf("parquet: invalid ALP exponent %d and factor %d", exponent, factor)
	}
	if numExceptions > vectorLen {
		return fmt.Errorf("parquet: ALP vector %d claims %d exceptions in %d values",
			vectorIdx, numExceptions, vectorLen)
	}
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

// Decode fills out with as many values as the caller has left to read, decoding
// each vector as it reaches it.
func (dec *alpDecoder[T]) Decode(out []T) (int, error) {
	toRead := min(len(out), dec.nvals)

	read := 0
	// Stop at the last value the page holds, which is not always the end of the
	// last vector: that one is short whenever the count is not a whole number of
	// vectors.
	for read < toRead && dec.currentIndex < dec.totalCount {
		vectorIdx := dec.currentIndex / dec.vectorSize
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
	if read < toRead {
		// The page header and the ALP header disagree about how many values the
		// page holds. Report it rather than return a short read, which the column
		// reader would retry forever, since a read of nothing leaves it where it
		// was.
		return read, fmt.Errorf("parquet: ALP page holds %d values, %d more were requested",
			dec.totalCount, toRead-read)
	}
	return read, nil
}

// DecodeSpaced decodes into out and then spreads the values apart, leaving a
// slot for every null.
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

// Discard skips n values. Skipping over the vector the decoder holds costs
// nothing: the next Decode finds that it needs a different vector and decodes
// that one instead.
func (dec *alpDecoder[T]) Discard(n int) (int, error) {
	n = min(n, dec.nvals)
	dec.nvals -= n
	dec.currentIndex += n
	return n, nil
}

// The names the encoding traits hand out, one per column type.
type (
	AlpFloat32Encoder = alpEncoder[float32]
	AlpFloat64Encoder = alpEncoder[float64]
	AlpFloat32Decoder = alpDecoder[float32]
	AlpFloat64Decoder = alpDecoder[float64]
)
