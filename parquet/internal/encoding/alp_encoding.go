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

// ALP (Adaptive Lossless Floating-Point) encoding for FLOAT and DOUBLE columns.
//
// ALP multiplies each value by a power of ten, rounds the product to an integer,
// and stores bit-packed differences from the smallest integer. A value that does
// not survive that round trip is stored verbatim as an exception. alp_scaling.go
// holds that arithmetic and alp_params.go chooses the scaling; this file writes
// and reads the bytes.
//
// A page begins with a 7-byte header: compression mode, integer encoding, log2
// of the vector size, then the value count as a little-endian int32. An array
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
	// The first two header bytes select the ALP variants. The decoder rejects
	// variants other than the ones this encoder writes.
	alpCompressionMode    = 0 // ALP; other values are reserved for variants such as ALP-RD
	alpIntegerEncodingFOR = 0 // frame of reference plus bit packing

	alpHeaderSize = 7

	// Encoders write 1,024-value vectors unless SetVectorSize says otherwise.
	// The format allows anything from 2^alpMinLogVectorSize to
	// 2^alpMaxLogVectorSize, and the decoder honors what a page declares.
	alpDefaultVectorSize    = 1024
	alpDefaultLogVectorSize = 10
	alpMinLogVectorSize     = 3
	alpMaxLogVectorSize     = 15
)

func alpVectorHeaderSize[T alpFloat]() int {
	return 1 + 1 + 2 + alpEncodedBytes[T]() + 1
}

func alpPackedSize(count, bitWidth int) int {
	return (count*bitWidth + 7) / 8
}

// alpPack packs each integer's distance above minValue into dst at bitWidth
// bits, least significant bit first. The distances collect in words that are
// stored whole, so dst needs room for 8 bytes past the alpPackedSize ones.
//
// A group of 8 values ends on a byte boundary at any width. The integers are
// therefore cut into four runs of whole groups, which are packed side by side:
// no run's word waits for another's. What is left after the four runs is
// packed on its own.
func alpPack(dst []byte, encoded []int64, minValue int64, bitWidth uint) {
	if bitWidth == 0 {
		return
	}
	run := len(encoded) / 4 &^ 7
	if run == 0 {
		alpPackRun(dst, encoded, minValue, bitWidth)
		return
	}
	size := run * int(bitWidth) / 8
	e0, e1, e2, e3 := encoded[:run], encoded[run:2*run], encoded[2*run:3*run], encoded[3*run:4*run]
	d0, d1, d2, d3 := dst[:size], dst[size:2*size], dst[2*size:3*size], dst[3*size:4*size]

	var w0, w1, w2, w3 uint64
	var used uint
	base, pos := uint64(minValue), 0
	for i := range e0 {
		// The distance between two int64 values can exceed int64. Unsigned
		// arithmetic wraps, and the decoder adds the distance back the same way.
		v0, v1, v2, v3 := uint64(e0[i])-base, uint64(e1[i])-base, uint64(e2[i])-base, uint64(e3[i])-base
		w0 |= v0 << used
		w1 |= v1 << used
		w2 |= v2 << used
		w3 |= v3 << used
		used += bitWidth
		if used >= 64 {
			binary.LittleEndian.PutUint64(d0[pos:], w0)
			binary.LittleEndian.PutUint64(d1[pos:], w1)
			binary.LittleEndian.PutUint64(d2[pos:], w2)
			binary.LittleEndian.PutUint64(d3[pos:], w3)
			pos += 8
			used -= 64
			// The bits the full words had no room for.
			rest := bitWidth - used
			w0, w1, w2, w3 = v0>>rest, v1>>rest, v2>>rest, v3>>rest
		}
	}
	// A whole word here would run into the next run, which is already
	// written, so the last bytes of each run go out one at a time.
	for ; pos < size; pos++ {
		d0[pos], d1[pos], d2[pos], d3[pos] = byte(w0), byte(w1), byte(w2), byte(w3)
		w0, w1, w2, w3 = w0>>8, w1>>8, w2>>8, w3>>8
	}
	alpPackRun(dst[4*size:], encoded[4*run:], minValue, bitWidth)
}

// alpPackRun packs one run of integers. The distances collect in a word that
// is stored whole each time it fills, and once more at the end.
func alpPackRun(dst []byte, encoded []int64, minValue int64, bitWidth uint) {
	if len(encoded) == 0 {
		return
	}
	var word uint64
	var used uint
	base, pos := uint64(minValue), 0
	for _, e := range encoded {
		d := uint64(e) - base
		word |= d << used
		used += bitWidth
		if used >= 64 {
			binary.LittleEndian.PutUint64(dst[pos:], word)
			pos += 8
			used -= 64
			word = d >> (bitWidth - used)
		}
	}
	binary.LittleEndian.PutUint64(dst[pos:], word)
}

// alpUnpackBits reverses alpPack into out, which must hold as many
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

func alpReadInt[T alpFloat](src []byte) int64 {
	if alpEncodedBytes[T]() == 4 {
		return int64(int32(binary.LittleEndian.Uint32(src)))
	}
	return int64(binary.LittleEndian.Uint64(src))
}

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

	// Scratch reused across vectors, calls, and pages.
	encodedScratch []int64
	excPositions   []uint16
	sampleScratch  []T
	spacedScratch  []T
	pageHeader     []byte

	vectorsProcessed int
	cachedPresets    []alpEncodingParams
	presetCounts     map[alpEncodingParams]int

	// While the shortlist does no better than PLAIN, the encoder runs a full
	// search in case the data has changed scale. After a search that finds
	// nothing better it skips the next researchWait such vectors, and the
	// wait doubles each time.
	researchWait int
	researchSkip int
}

func newALPEncoder[T alpFloat](e format.Encoding, descr *schema.Column, mem memory.Allocator) *alpEncoder[T] {
	enc := &alpEncoder[T]{
		encoder:       newEncoderBase(e, descr, mem),
		sampleScratch: make([]T, 0, alpSamplerSamples),
		presetCounts:  make(map[alpEncodingParams]int),
		researchWait:  alpMinResearchWait,
	}
	enc.setVectorSize(alpDefaultLogVectorSize)
	return enc
}

func (enc *alpEncoder[T]) setVectorSize(logSize int) {
	enc.logVectorSize = logSize
	enc.vectorSize = 1 << logSize
	enc.vectorBuf = make([]T, enc.vectorSize)
	enc.encodedScratch = make([]int64, enc.vectorSize)
	enc.excPositions = make([]uint16, enc.vectorSize)
}

// SetVectorSize sets how many values each vector holds: a power of two from
// 8 to 32768, 1024 by default. It must be called before the first value is
// put, and Reset keeps it.
func (enc *alpEncoder[T]) SetVectorSize(size int) error {
	if size < 1<<alpMinLogVectorSize || size > 1<<alpMaxLogVectorSize || size&(size-1) != 0 {
		return fmt.Errorf("parquet: ALP vector size must be a power of two from %d to %d, got %d",
			1<<alpMinLogVectorSize, 1<<alpMaxLogVectorSize, size)
	}
	if enc.bufCount > 0 || len(enc.vectorSizes) > 0 {
		return errors.New("parquet: ALP vector size cannot change once values are buffered")
	}
	if size != enc.vectorSize {
		enc.setVectorSize(bits.TrailingZeros(uint(size)))
	}
	return nil
}

func (enc *alpEncoder[T]) Type() parquet.Type {
	return alpParquetType[T]()
}

// Put buffers values and encodes complete vectors. FlushValues handles the
// final partial vector.
func (enc *alpEncoder[T]) Put(in []T) {
	for len(in) > 0 {
		if enc.bufCount == 0 && len(in) >= enc.vectorSize {
			// A whole vector is here already: encode it where it is rather
			// than copy it into the buffer first.
			enc.encodeVector(in[:enc.vectorSize])
			enc.totalCount += enc.vectorSize
			in = in[enc.vectorSize:]
			continue
		}
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
	enc.spacedScratch = slices.Grow(enc.spacedScratch[:0], len(in))[:len(in)]
	nvalid := spacedCompress(in, enc.spacedScratch, validBits, validBitsOffset)
	enc.Put(enc.spacedScratch[:nvalid])
}

// The first alpSamplerVectors vectors seed the preset list.
func (enc *alpEncoder[T]) findParams(values []T) alpEncodingParams {
	sample := alpSample(values, enc.sampleScratch[:0])

	if len(enc.cachedPresets) > 0 {
		params, sizeBits := alpFindBestParamsWithPresets(sample, enc.cachedPresets)
		if full, ok := enc.research(sample, sizeBits); ok {
			return full
		}
		return params
	}

	params, _ := alpFindBestParams(sample)
	enc.presetCounts[params]++
	return params
}

// research checks whether a full search beats a shortlist that does no better
// than PLAIN. The shortlist comes from the first vectors of the chunk, so a
// later change of scale, such as integers giving way to prices, can leave every
// shortlisted scaling turning most values into exceptions. When the full search
// wins, sampling starts over with its result as the first winner.
func (enc *alpEncoder[T]) research(sample []T, presetBits int64) (alpEncodingParams, bool) {
	plainBitsPerValue := int64(8 * alpEncodedBytes[T]())
	plainBits := int64(len(sample)) * plainBitsPerValue
	if presetBits < plainBits {
		return alpEncodingParams{}, false
	}
	if enc.researchSkip > 0 {
		enc.researchSkip--
		return alpEncodingParams{}, false
	}

	// Data no scaling compresses fails a search over a few values as surely as
	// over the whole sample, at a fraction of the cost.
	probe := sample[:min(len(sample), alpResearchProbeSamples)]
	_, probeBits := alpFindBestParams(probe)
	full, fullBits := alpEncodingParams{}, int64(math.MaxInt64)
	if probeBits < int64(len(probe))*plainBitsPerValue {
		full, fullBits = alpFindBestParams(sample)
	}
	if fullBits >= presetBits || fullBits >= plainBits {
		// Nothing compresses this data, so search less often.
		enc.researchSkip = enc.researchWait
		enc.researchWait = min(2*enc.researchWait, alpMaxResearchWait)
		return alpEncodingParams{}, false
	}

	enc.researchWait = alpMinResearchWait
	enc.cachedPresets = nil
	clear(enc.presetCounts)
	enc.vectorsProcessed = 0
	enc.presetCounts[full]++
	return full, true
}

// scaleVector scales values with params into the encoder's scratch: every
// integer, and the positions of the exceptions. It returns how many exceptions
// there are, the frame of reference, and the width the integers pack at.
func (enc *alpEncoder[T]) scaleVector(values []T, params alpEncodingParams) (numExc int, minValue int64, bitWidth int) {
	// Scaling a value also determines whether it is an exception, so one pass
	// collects both the integers and the exceptions.
	encoded := enc.encodedScratch[:len(values)]
	numExc = alpScale(values, params.exponent, params.factor, encoded, enc.excPositions)

	// When nothing encodes, nothing constrains the frame of reference, and a
	// width of zero packs nothing. Otherwise the range leaves each exception's
	// slot holding the frame of reference itself, which costs no bits, so the
	// exceptions cannot widen the packing.
	var maxValue int64
	if numExc < len(values) {
		minValue, maxValue = alpIntRange(encoded, enc.excPositions[:numExc])
	}
	// The difference between two int64 values can exceed int64. Compute it in
	// unsigned arithmetic; the decoder adds the wrapped difference back.
	return numExc, minValue, bits.Len64(uint64(maxValue) - uint64(minValue))
}

func (enc *alpEncoder[T]) encodeVector(values []T) {
	n := len(values)
	var (
		params   alpEncodingParams
		numExc   int
		minValue int64
		bitWidth int
	)
	if len(enc.cachedPresets) == 1 {
		// With one scaling on the shortlist there is nothing to choose, so
		// the vector is scaled straight away. What the sample would have
		// estimated is read off the result, and a sample is only taken when
		// that is no better than PLAIN, to check whether the data has changed
		// scale.
		params = enc.cachedPresets[0]
		numExc, minValue, bitWidth = enc.scaleVector(values, params)
		sampleBits, sampled := alpSampledSizeBits[T](enc.encodedScratch[:n], enc.excPositions[:numExc])
		if sampleBits >= int64(sampled)*int64(8*alpEncodedBytes[T]()) {
			sample := alpSample(values, enc.sampleScratch[:0])
			if full, ok := enc.research(sample, sampleBits); ok {
				params = full
				numExc, minValue, bitWidth = enc.scaleVector(values, params)
			}
		}
	} else {
		params = enc.findParams(values)
		numExc, minValue, bitWidth = enc.scaleVector(values, params)
	}
	encoded := enc.encodedScratch[:n]
	excPositions := enc.excPositions[:numExc]

	enc.vectorsProcessed++
	if enc.cachedPresets == nil && enc.vectorsProcessed >= alpSamplerVectors {
		enc.buildPresetCache()
	}

	// The packing stores whole words, so it needs room for one past the end of
	// the vector. The exceptions overwrite what it leaves after the packed
	// bytes, and the final reslice drops the rest.
	valueBytes := alpEncodedBytes[T]()
	packedSize := alpPackedSize(n, bitWidth)
	size := alpVectorHeaderSize[T]() + packedSize + numExc*(2+valueBytes)
	start := len(enc.encodedBuf)
	enc.encodedBuf = slices.Grow(enc.encodedBuf, size+8)[:start+size]
	out := enc.encodedBuf[start : start+size+8]

	out[0], out[1] = byte(params.exponent), byte(params.factor)
	binary.LittleEndian.PutUint16(out[2:], uint16(numExc))
	if valueBytes == 4 {
		binary.LittleEndian.PutUint32(out[4:], uint32(minValue))
	} else {
		binary.LittleEndian.PutUint64(out[4:], uint64(minValue))
	}
	out[4+valueBytes] = byte(bitWidth)
	out = out[alpVectorHeaderSize[T]():]

	alpPack(out, encoded, minValue, uint(bitWidth))
	out = out[packedSize:]

	for i, pos := range excPositions {
		binary.LittleEndian.PutUint16(out[2*i:], pos)
	}
	out = out[2*numExc:]
	switch values := any(values).(type) {
	case []float64:
		for i, pos := range excPositions {
			binary.LittleEndian.PutUint64(out[8*i:], math.Float64bits(values[pos]))
		}
	case []float32:
		for i, pos := range excPositions {
			binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(values[pos]))
		}
	}

	enc.vectorSizes = append(enc.vectorSizes, size)
}

// buildPresetCache keeps the scaling pairs the sampled vectors chose most often,
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

// EstimatedDataEncodedSize counts the encoded vectors and their offsets, plus
// an upper bound for the vector still being buffered: its offset and header,
// every slot packed at the full integer width, and every value also stored as
// an exception with its 2-byte position. A vector that mixes exceptions with
// wide packed values can approach that bound, so nothing smaller is safe; the
// overestimate is limited to one vector.
func (enc *alpEncoder[T]) EstimatedDataEncodedSize() int64 {
	size := len(enc.encodedBuf) + 4*len(enc.vectorSizes)
	if enc.bufCount > 0 {
		width := alpEncodedBytes[T]()
		size += 4 + alpVectorHeaderSize[T]() + enc.bufCount*(2*width+2)
	}
	return int64(size)
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

	header := append(enc.pageHeader[:0], alpCompressionMode, alpIntegerEncodingFOR, byte(enc.logVectorSize))
	header = binary.LittleEndian.AppendUint32(header, uint32(enc.totalCount))

	offset := uint32(offsetArraySize)
	for _, size := range enc.vectorSizes {
		header = binary.LittleEndian.AppendUint32(header, offset)
		offset += uint32(size)
	}
	enc.pageHeader = header

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
// it can encode an unrelated column. The vector size stays as set.
func (enc *alpEncoder[T]) Reset() {
	enc.encoder.Reset()
	enc.resetPage()
	enc.bufCount = 0
	enc.vectorsProcessed = 0
	enc.cachedPresets = nil
	clear(enc.presetCounts)
	enc.researchWait = alpMinResearchWait
	enc.researchSkip = 0
}

// alpDecoder decodes one ALP vector at a time and keeps it across partial reads.
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

func newALPDecoder[T alpFloat](e format.Encoding, descr *schema.Column) *alpDecoder[T] {
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

// SetData points the decoder at one page and reads its header. It rejects
// unsupported ALP variants and truncated vector-offset tables.
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
	// nvals counts the page's nulls too, so a smaller element count is valid,
	// but a page can't hold more non-null values than it has values.
	if int64(numElements) > int64(nvals) {
		return fmt.Errorf("parquet: ALP page claims %d values, but the page holds at most %d",
			numElements, nvals)
	}

	dec.vectorSize = 1 << logVectorSize
	dec.totalCount = int(numElements)
	// Rounding up by adding vectorSize-1 first could overflow a 32-bit int.
	dec.numVectors = dec.totalCount/dec.vectorSize + min(dec.totalCount%dec.vectorSize, 1)
	dec.currentIndex = 0
	dec.currentVectorIndex = -1
	dec.nvals = nvals

	offsetArraySize := dec.numVectors * 4
	if len(data) < alpHeaderSize+offsetArraySize {
		return fmt.Errorf("parquet: ALP data too short for offset array: need %d, have %d",
			alpHeaderSize+offsetArraySize, len(data))
	}

	// Reuse these buffers across pages. Only the offset count varies by page.
	dec.vectorOffsets = slices.Grow(dec.vectorOffsets[:0], dec.numVectors)[:dec.numVectors]
	for i := range dec.vectorOffsets {
		dec.vectorOffsets[i] = binary.LittleEndian.Uint32(data[alpHeaderSize+i*4:])
		// The first vector starts right after the offset array and each one
		// starts where the previous ends, so offsets strictly increase.
		// decodeVector checks each vector's end against the next offset.
		if i == 0 && int64(dec.vectorOffsets[0]) != int64(offsetArraySize) {
			return fmt.Errorf("parquet: ALP vector 0 offset %d, want %d (the offset array size)",
				dec.vectorOffsets[0], offsetArraySize)
		}
		if i > 0 && dec.vectorOffsets[i] <= dec.vectorOffsets[i-1] {
			return fmt.Errorf("parquet: ALP vector %d offset %d does not follow vector %d offset %d",
				i, dec.vectorOffsets[i], i-1, dec.vectorOffsets[i-1])
		}
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

	// On 32-bit platforms, converting a large uint32 offset can make pos
	// negative, and adding to a large one can overflow, so compare against
	// the length less the header instead.
	pos := int(dec.vectorOffsets[vectorIdx])
	if pos < 0 || pos > len(dec.bodyData)-alpVectorHeaderSize[T]() {
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
	end := pos + packedSize + exceptionSize
	if end > len(dec.bodyData) {
		return fmt.Errorf("parquet: ALP vector %d runs past the end of the page", vectorIdx)
	}
	// Vectors are stored back to back, so this one must end where the next
	// begins. On 32-bit platforms a huge offset converts to a negative int,
	// which can't match.
	if vectorIdx < dec.numVectors-1 && end != int(dec.vectorOffsets[vectorIdx+1]) {
		return fmt.Errorf("parquet: ALP vector %d ends at %d, but vector %d starts at %d",
			vectorIdx, end, vectorIdx+1, dec.vectorOffsets[vectorIdx+1])
	}

	deltas := dec.deltas[:vectorLen]
	if err := alpUnpackBits(dec.bitReader, dec.packedReader, dec.bodyData[pos:pos+packedSize], deltas, bitWidth); err != nil {
		return err
	}
	pos += packedSize

	// The deltas were computed in the encoded integer's width with wrapping
	// arithmetic, so add the frame of reference back the same way.
	s := newALPScaling[T](exponent, factor)
	out := dec.decodedValues[:len(deltas)]
	if valueBytes == 4 {
		ref := uint32(frameOfRef)
		for i, d := range deltas {
			out[i] = s.decode(int64(int32(uint32(d) + ref)))
		}
	} else {
		ref := uint64(frameOfRef)
		for i, d := range deltas {
			out[i] = s.decode(int64(d + ref))
		}
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

// Decode fills out with the remaining requested values, decoding vectors as
// needed.
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

// DecodeSpaced leaves a slot for every null.
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

// Discard skips n values without decoding intervening vectors.
func (dec *alpDecoder[T]) Discard(n int) (int, error) {
	n = min(n, dec.nvals)
	// nvals counts nulls too, so it can exceed what the page encodes. As in
	// Decode, report a shortfall rather than claim to skip values that aren't
	// there.
	skipped := min(n, max(dec.totalCount-dec.currentIndex, 0))
	dec.nvals -= skipped
	dec.currentIndex += skipped
	if skipped < n {
		return skipped, fmt.Errorf("parquet: ALP page holds %d values, %d more were requested",
			dec.totalCount, n-skipped)
	}
	return skipped, nil
}

type (
	// ALPFloat32Encoder encodes float32 values with ALP.
	ALPFloat32Encoder = alpEncoder[float32]
	// ALPFloat64Encoder encodes float64 values with ALP.
	ALPFloat64Encoder = alpEncoder[float64]
	// ALPFloat32Decoder decodes ALP-encoded float32 values.
	ALPFloat32Decoder = alpDecoder[float32]
	// ALPFloat64Decoder decodes ALP-encoded float64 values.
	ALPFloat64Decoder = alpDecoder[float64]
)

// ALPEncoder is implemented by the ALP encoders, so that a column writer can
// set the vector size without knowing the physical type.
type ALPEncoder interface {
	SetVectorSize(size int) error
}

var (
	_ ALPEncoder = (*ALPFloat32Encoder)(nil)
	_ ALPEncoder = (*ALPFloat64Encoder)(nil)
)
