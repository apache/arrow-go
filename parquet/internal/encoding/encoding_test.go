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

package encoding_test

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/internal/testutils"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type nodeFactory func(string, parquet.Repetition, int32) *schema.PrimitiveNode

func createNodeFactory(t reflect.Type) nodeFactory {
	switch t {
	case reflect.TypeOf(true):
		return schema.NewBooleanNode
	case reflect.TypeOf(int32(0)):
		return schema.NewInt32Node
	case reflect.TypeOf(int64(0)):
		return schema.NewInt64Node
	case reflect.TypeOf(parquet.Int96{}):
		return schema.NewInt96Node
	case reflect.TypeOf(float32(0)):
		return schema.NewFloat32Node
	case reflect.TypeOf(float64(0)):
		return schema.NewFloat64Node
	case reflect.TypeOf(parquet.ByteArray{}):
		return schema.NewByteArrayNode
	case reflect.TypeOf(parquet.FixedLenByteArray{}):
		return func(name string, rep parquet.Repetition, field int32) *schema.PrimitiveNode {
			return schema.NewFixedLenByteArrayNode(name, rep, 12, field)
		}
	}
	return nil
}

func initdata(t reflect.Type, drawbuf, decodebuf []byte, nvals, repeats int, heap *memory.Buffer) (interface{}, interface{}) {
	switch t {
	case reflect.TypeOf(true):
		draws := *(*[]bool)(unsafe.Pointer(&drawbuf))
		decode := *(*[]bool)(unsafe.Pointer(&decodebuf))
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(int32(0)):
		draws := arrow.Int32Traits.CastFromBytes(drawbuf)
		decode := arrow.Int32Traits.CastFromBytes(decodebuf)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(int64(0)):
		draws := arrow.Int64Traits.CastFromBytes(drawbuf)
		decode := arrow.Int64Traits.CastFromBytes(decodebuf)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(parquet.Int96{}):
		draws := parquet.Int96Traits.CastFromBytes(drawbuf)
		decode := parquet.Int96Traits.CastFromBytes(decodebuf)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(float32(0)):
		draws := arrow.Float32Traits.CastFromBytes(drawbuf)
		decode := arrow.Float32Traits.CastFromBytes(decodebuf)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(float64(0)):
		draws := arrow.Float64Traits.CastFromBytes(drawbuf)
		decode := arrow.Float64Traits.CastFromBytes(decodebuf)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(parquet.ByteArray{}):
		draws := make([]parquet.ByteArray, nvals*repeats)
		decode := make([]parquet.ByteArray, nvals*repeats)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	case reflect.TypeOf(parquet.FixedLenByteArray{}):
		draws := make([]parquet.FixedLenByteArray, nvals*repeats)
		decode := make([]parquet.FixedLenByteArray, nvals*repeats)
		testutils.InitValues(draws[:nvals], heap)

		for j := 1; j < repeats; j++ {
			for k := 0; k < nvals; k++ {
				draws[nvals*j+k] = draws[k]
			}
		}

		return draws[:nvals*repeats], decode[:nvals*repeats]
	}
	return nil, nil
}

func encode(enc encoding.TypedEncoder, vals interface{}) {
	switch v := vals.(type) {
	case []bool:
		enc.(encoding.BooleanEncoder).Put(v)
	case []int32:
		enc.(encoding.Int32Encoder).Put(v)
	case []int64:
		enc.(encoding.Int64Encoder).Put(v)
	case []parquet.Int96:
		enc.(encoding.Int96Encoder).Put(v)
	case []float32:
		enc.(encoding.Float32Encoder).Put(v)
	case []float64:
		enc.(encoding.Float64Encoder).Put(v)
	case []parquet.ByteArray:
		enc.(encoding.ByteArrayEncoder).Put(v)
	case []parquet.FixedLenByteArray:
		enc.(encoding.FixedLenByteArrayEncoder).Put(v)
	}
}

func encodeSpaced(enc encoding.TypedEncoder, vals interface{}, validBits []byte, validBitsOffset int64) {
	switch v := vals.(type) {
	case []bool:
		enc.(encoding.BooleanEncoder).PutSpaced(v, validBits, validBitsOffset)
	case []int32:
		enc.(encoding.Int32Encoder).PutSpaced(v, validBits, validBitsOffset)
	case []int64:
		enc.(encoding.Int64Encoder).PutSpaced(v, validBits, validBitsOffset)
	case []parquet.Int96:
		enc.(encoding.Int96Encoder).PutSpaced(v, validBits, validBitsOffset)
	case []float32:
		enc.(encoding.Float32Encoder).PutSpaced(v, validBits, validBitsOffset)
	case []float64:
		enc.(encoding.Float64Encoder).PutSpaced(v, validBits, validBitsOffset)
	case []parquet.ByteArray:
		enc.(encoding.ByteArrayEncoder).PutSpaced(v, validBits, validBitsOffset)
	case []parquet.FixedLenByteArray:
		enc.(encoding.FixedLenByteArrayEncoder).PutSpaced(v, validBits, validBitsOffset)
	}
}

func decode(dec encoding.TypedDecoder, out interface{}) (int, error) {
	switch v := out.(type) {
	case []bool:
		return dec.(encoding.BooleanDecoder).Decode(v)
	case []int32:
		return dec.(encoding.Int32Decoder).Decode(v)
	case []int64:
		return dec.(encoding.Int64Decoder).Decode(v)
	case []parquet.Int96:
		return dec.(encoding.Int96Decoder).Decode(v)
	case []float32:
		return dec.(encoding.Float32Decoder).Decode(v)
	case []float64:
		return dec.(encoding.Float64Decoder).Decode(v)
	case []parquet.ByteArray:
		return dec.(encoding.ByteArrayDecoder).Decode(v)
	case []parquet.FixedLenByteArray:
		return dec.(encoding.FixedLenByteArrayDecoder).Decode(v)
	}
	return 0, nil
}

func decodeSpaced(dec encoding.TypedDecoder, out interface{}, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	switch v := out.(type) {
	case []bool:
		return dec.(encoding.BooleanDecoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []int32:
		return dec.(encoding.Int32Decoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []int64:
		return dec.(encoding.Int64Decoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []parquet.Int96:
		return dec.(encoding.Int96Decoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []float32:
		return dec.(encoding.Float32Decoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []float64:
		return dec.(encoding.Float64Decoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []parquet.ByteArray:
		return dec.(encoding.ByteArrayDecoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	case []parquet.FixedLenByteArray:
		return dec.(encoding.FixedLenByteArrayDecoder).DecodeSpaced(v, nullCount, validBits, validBitsOffset)
	}
	return 0, nil
}

type BaseEncodingTestSuite struct {
	suite.Suite

	descr   *schema.Column
	typeLen int
	mem     memory.Allocator
	typ     reflect.Type

	nvalues     int
	heap        *memory.Buffer
	inputBytes  *memory.Buffer
	outputBytes *memory.Buffer
	nodeFactory nodeFactory

	draws     interface{}
	decodeBuf interface{}
}

func (b *BaseEncodingTestSuite) SetupSuite() {
	b.mem = memory.DefaultAllocator
	b.inputBytes = memory.NewResizableBuffer(b.mem)
	b.outputBytes = memory.NewResizableBuffer(b.mem)
	b.heap = memory.NewResizableBuffer(b.mem)
	b.nodeFactory = createNodeFactory(b.typ)
}

func (b *BaseEncodingTestSuite) TearDownSuite() {
	b.inputBytes.Release()
	b.outputBytes.Release()
	b.heap.Release()
}

func (b *BaseEncodingTestSuite) SetupTest() {
	b.descr = schema.NewColumn(b.nodeFactory("name", parquet.Repetitions.Optional, -1), 0, 0)
	b.typeLen = int(b.descr.TypeLength())
}

func (b *BaseEncodingTestSuite) initData(nvalues, repeats int) {
	b.nvalues = nvalues * repeats
	b.inputBytes.ResizeNoShrink(b.nvalues * int(b.typ.Size()))
	b.outputBytes.ResizeNoShrink(b.nvalues * int(b.typ.Size()))
	memory.Set(b.inputBytes.Buf(), 0)
	memory.Set(b.outputBytes.Buf(), 0)

	b.draws, b.decodeBuf = initdata(b.typ, b.inputBytes.Buf(), b.outputBytes.Buf(), nvalues, repeats, b.heap)
}

func (b *BaseEncodingTestSuite) encodeTestData(e parquet.Encoding) (encoding.Buffer, error) {
	enc := encoding.NewEncoder(testutils.TypeToParquetType(b.typ), e, false, b.descr, memory.DefaultAllocator)
	b.Equal(e, enc.Encoding())
	b.Equal(b.descr.PhysicalType(), enc.Type())
	encode(enc, reflect.ValueOf(b.draws).Slice(0, b.nvalues).Interface())
	return enc.FlushValues()
}

func (b *BaseEncodingTestSuite) testDiscardDecodeData(e parquet.Encoding, buf []byte) {
	dec := encoding.NewDecoder(testutils.TypeToParquetType(b.typ), e, b.descr, b.mem)
	b.Equal(e, dec.Encoding())
	b.Equal(b.descr.PhysicalType(), dec.Type())

	dec.SetData(b.nvalues, buf)
	discarded := b.nvalues / 2
	n, err := dec.Discard(discarded)
	b.Require().NoError(err)
	b.Equal(discarded, n)

	decoded, _ := decode(dec, b.decodeBuf)
	b.Equal(b.nvalues-discarded, decoded)
	b.Equal(reflect.ValueOf(b.draws).Slice(discarded, b.nvalues).Interface(), reflect.ValueOf(b.decodeBuf).Slice(0, b.nvalues-discarded).Interface())
}

func (b *BaseEncodingTestSuite) decodeTestData(e parquet.Encoding, buf []byte) {
	dec := encoding.NewDecoder(testutils.TypeToParquetType(b.typ), e, b.descr, b.mem)
	b.Equal(e, dec.Encoding())
	b.Equal(b.descr.PhysicalType(), dec.Type())

	dec.SetData(b.nvalues, buf)
	decoded, _ := decode(dec, b.decodeBuf)
	b.Equal(b.nvalues, decoded)
	b.Equal(reflect.ValueOf(b.draws).Slice(0, b.nvalues).Interface(), reflect.ValueOf(b.decodeBuf).Slice(0, b.nvalues).Interface())

	dec.SetData(b.nvalues, buf)
	decoded = 0
	for i := 0; i < b.nvalues; i += 500 {
		n, err := decode(dec, reflect.ValueOf(b.decodeBuf).Slice(i, i+500).Interface())
		b.Require().NoError(err)
		decoded += n
	}
	b.Equal(b.nvalues, decoded)
	b.Equal(reflect.ValueOf(b.draws).Slice(0, b.nvalues).Interface(), reflect.ValueOf(b.decodeBuf).Slice(0, b.nvalues).Interface())
}

func (b *BaseEncodingTestSuite) encodeTestDataSpaced(e parquet.Encoding, validBits []byte, validBitsOffset int64) (encoding.Buffer, error) {
	enc := encoding.NewEncoder(testutils.TypeToParquetType(b.typ), e, false, b.descr, memory.DefaultAllocator)
	encodeSpaced(enc, reflect.ValueOf(b.draws).Slice(0, b.nvalues).Interface(), validBits, validBitsOffset)
	return enc.FlushValues()
}

func (b *BaseEncodingTestSuite) decodeTestDataSpaced(e parquet.Encoding, nullCount int, buf []byte, validBits []byte, validBitsOffset int64) {
	dec := encoding.NewDecoder(testutils.TypeToParquetType(b.typ), e, b.descr, b.mem)
	dec.SetData(b.nvalues-nullCount, buf)
	decoded, _ := decodeSpaced(dec, b.decodeBuf, nullCount, validBits, validBitsOffset)
	b.Equal(b.nvalues, decoded)

	drawval := reflect.ValueOf(b.draws)
	decodeval := reflect.ValueOf(b.decodeBuf)
	for j := 0; j < b.nvalues; j++ {
		if bitutil.BitIsSet(validBits, int(validBitsOffset)+j) {
			b.Equal(drawval.Index(j).Interface(), decodeval.Index(j).Interface())
		}
	}
}

func (b *BaseEncodingTestSuite) checkRoundTrip(e parquet.Encoding) {
	buf, _ := b.encodeTestData(e)
	defer buf.Release()
	b.decodeTestData(e, buf.Bytes())
	b.testDiscardDecodeData(e, buf.Bytes())
}

func (b *BaseEncodingTestSuite) checkRoundTripSpaced(e parquet.Encoding, validBits []byte, validBitsOffset int64) {
	buf, _ := b.encodeTestDataSpaced(e, validBits, validBitsOffset)
	defer buf.Release()

	nullCount := 0
	for i := 0; i < b.nvalues; i++ {
		if bitutil.BitIsNotSet(validBits, int(validBitsOffset)+i) {
			nullCount++
		}
	}
	b.decodeTestDataSpaced(e, nullCount, buf.Bytes(), validBits, validBitsOffset)
}

func (b *BaseEncodingTestSuite) TestBasicRoundTrip() {
	b.initData(10000, 1)
	b.checkRoundTrip(parquet.Encodings.Plain)
}

func (b *BaseEncodingTestSuite) TestRleBooleanEncodingRoundTrip() {
	switch b.typ {
	case reflect.TypeOf(true):
		b.initData(2000, 200)
		b.checkRoundTrip(parquet.Encodings.RLE)
	default:
		b.T().SkipNow()
	}
}

func (b *BaseEncodingTestSuite) TestDeltaEncodingRoundTrip() {
	b.initData(10000, 1)

	switch b.typ {
	case reflect.TypeOf(int32(0)), reflect.TypeOf(int64(0)):
		b.checkRoundTrip(parquet.Encodings.DeltaBinaryPacked)
	default:
		b.Panics(func() { b.checkRoundTrip(parquet.Encodings.DeltaBinaryPacked) })
	}
}

func (b *BaseEncodingTestSuite) TestDeltaLengthByteArrayRoundTrip() {
	b.initData(10000, 1)

	switch b.typ {
	case reflect.TypeOf(parquet.ByteArray{}):
		b.checkRoundTrip(parquet.Encodings.DeltaLengthByteArray)
	default:
		b.Panics(func() { b.checkRoundTrip(parquet.Encodings.DeltaLengthByteArray) })
	}
}

func (b *BaseEncodingTestSuite) TestDeltaByteArrayRoundTrip() {
	b.initData(10000, 1)

	switch b.typ {
	case reflect.TypeOf(parquet.ByteArray{}):
		b.checkRoundTrip(parquet.Encodings.DeltaByteArray)
	default:
		b.Panics(func() { b.checkRoundTrip(parquet.Encodings.DeltaLengthByteArray) })
	}
}

func (b *BaseEncodingTestSuite) TestByteStreamSplitRoundTrip() {
	b.initData(10000, 1)

	switch b.typ {
	case reflect.TypeOf(float32(0)), reflect.TypeOf(float64(0)), reflect.TypeOf(int32(0)), reflect.TypeOf(int64(0)), reflect.TypeOf(parquet.FixedLenByteArray{}):
		b.checkRoundTrip(parquet.Encodings.ByteStreamSplit)
	default:
		b.Panics(func() { b.checkRoundTrip(parquet.Encodings.ByteStreamSplit) })
	}
}

func (b *BaseEncodingTestSuite) TestSpacedRoundTrip() {
	exec := func(vals, repeats int, validBitsOffset int64, nullProb float64) {
		b.Run(fmt.Sprintf("%d vals %d repeats %d offset %0.3f null", vals, repeats, validBitsOffset, 1-nullProb), func() {
			b.initData(vals, repeats)

			size := int64(b.nvalues) + validBitsOffset
			r := testutils.NewRandomArrayGenerator(1923)
			arr := r.Uint8(size, 0, 100, 1-nullProb)
			validBits := arr.NullBitmapBytes()
			if validBits != nil {
				b.checkRoundTripSpaced(parquet.Encodings.Plain, validBits, validBitsOffset)
				switch b.typ {
				case reflect.TypeOf(false):
					b.checkRoundTripSpaced(parquet.Encodings.RLE, validBits, validBitsOffset)
				case reflect.TypeOf(int32(0)), reflect.TypeOf(int64(0)):
					b.checkRoundTripSpaced(parquet.Encodings.DeltaBinaryPacked, validBits, validBitsOffset)
				case reflect.TypeOf(parquet.ByteArray{}):
					b.checkRoundTripSpaced(parquet.Encodings.DeltaLengthByteArray, validBits, validBitsOffset)
					b.checkRoundTripSpaced(parquet.Encodings.DeltaByteArray, validBits, validBitsOffset)
				}
			}
		})
	}

	const (
		avx512Size    = 64
		simdSize      = avx512Size
		multiSimdSize = simdSize * 33
	)

	for _, nullProb := range []float64{0.001, 0.1, 0.5, 0.9, 0.999} {
		// Test with both size and offset up to 3 simd block
		for i := 1; i < simdSize*3; i++ {
			exec(i, 1, 0, nullProb)
			exec(i, 1, int64(i+1), nullProb)
		}
		// large block and offset
		exec(multiSimdSize, 1, 0, nullProb)
		exec(multiSimdSize+33, 1, 0, nullProb)
		exec(multiSimdSize, 1, 33, nullProb)
		exec(multiSimdSize+33, 1, 33, nullProb)
	}
}

func TestEncoding(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
	}{
		{"Bool", reflect.TypeOf(true)},
		{"Int32", reflect.TypeOf(int32(0))},
		{"Int64", reflect.TypeOf(int64(0))},
		{"Float32", reflect.TypeOf(float32(0))},
		{"Float64", reflect.TypeOf(float64(0))},
		{"Int96", reflect.TypeOf(parquet.Int96{})},
		{"ByteArray", reflect.TypeOf(parquet.ByteArray{})},
		{"FixedLenByteArray", reflect.TypeOf(parquet.FixedLenByteArray{})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite.Run(t, &BaseEncodingTestSuite{typ: tt.typ})
		})
	}
}

type DictionaryEncodingTestSuite struct {
	BaseEncodingTestSuite
}

func (d *DictionaryEncodingTestSuite) encodeTestDataDict(e parquet.Encoding) (dictBuffer, indices encoding.Buffer, numEntries int) {
	enc := encoding.NewEncoder(testutils.TypeToParquetType(d.typ), e, true, d.descr, memory.DefaultAllocator).(encoding.DictEncoder)

	d.Equal(parquet.Encodings.PlainDict, enc.Encoding())
	d.Equal(d.descr.PhysicalType(), enc.Type())
	encode(enc, reflect.ValueOf(d.draws).Slice(0, d.nvalues).Interface())
	dictBuffer = memory.NewResizableBuffer(d.mem)
	dictBuffer.Resize(enc.DictEncodedSize())
	enc.WriteDict(dictBuffer.Bytes())
	indices, _ = enc.FlushValues()
	numEntries = enc.NumEntries()
	return
}

func (d *DictionaryEncodingTestSuite) encodeTestDataDictSpaced(e parquet.Encoding, validBits []byte, validBitsOffset int64) (dictBuffer, indices encoding.Buffer, numEntries int) {
	enc := encoding.NewEncoder(testutils.TypeToParquetType(d.typ), e, true, d.descr, memory.DefaultAllocator).(encoding.DictEncoder)
	d.Equal(d.descr.PhysicalType(), enc.Type())

	encodeSpaced(enc, reflect.ValueOf(d.draws).Slice(0, d.nvalues).Interface(), validBits, validBitsOffset)
	dictBuffer = memory.NewResizableBuffer(d.mem)
	dictBuffer.Resize(enc.DictEncodedSize())
	enc.WriteDict(dictBuffer.Bytes())
	indices, _ = enc.FlushValues()
	numEntries = enc.NumEntries()
	return
}

func (d *DictionaryEncodingTestSuite) checkRoundTrip() {
	dictBuffer, indices, numEntries := d.encodeTestDataDict(parquet.Encodings.Plain)
	defer dictBuffer.Release()
	defer indices.Release()
	validBits := make([]byte, int(bitutil.BytesForBits(int64(d.nvalues)))+1)
	memory.Set(validBits, 255)

	spacedBuffer, indicesSpaced, _ := d.encodeTestDataDictSpaced(parquet.Encodings.Plain, validBits, 0)
	defer spacedBuffer.Release()
	defer indicesSpaced.Release()
	d.Equal(indices.Bytes(), indicesSpaced.Bytes())

	dictDecoder := encoding.NewDecoder(testutils.TypeToParquetType(d.typ), parquet.Encodings.Plain, d.descr, d.mem)
	d.Equal(d.descr.PhysicalType(), dictDecoder.Type())
	dictDecoder.SetData(numEntries, dictBuffer.Bytes())
	decoder := encoding.NewDictDecoder(testutils.TypeToParquetType(d.typ), d.descr, d.mem)
	decoder.SetDict(dictDecoder)
	decoder.SetData(d.nvalues, indices.Bytes())

	decoded, _ := decode(decoder, d.decodeBuf)
	d.Equal(d.nvalues, decoded)
	d.Equal(reflect.ValueOf(d.draws).Slice(0, d.nvalues).Interface(), reflect.ValueOf(d.decodeBuf).Slice(0, d.nvalues).Interface())

	decoder.SetData(d.nvalues, indices.Bytes())
	decoded, _ = decodeSpaced(decoder, d.decodeBuf, 0, validBits, 0)
	d.Equal(d.nvalues, decoded)
	d.Equal(reflect.ValueOf(d.draws).Slice(0, d.nvalues).Interface(), reflect.ValueOf(d.decodeBuf).Slice(0, d.nvalues).Interface())

	decoder.SetData(d.nvalues, indices.Bytes())
	discarded := d.nvalues / 2
	n, err := decoder.Discard(discarded)
	d.Require().NoError(err)
	d.Equal(discarded, n)

	decoded, _ = decode(decoder, d.decodeBuf)
	d.Equal(d.nvalues-discarded, decoded)
	d.Equal(reflect.ValueOf(d.draws).Slice(discarded, d.nvalues).Interface(), reflect.ValueOf(d.decodeBuf).Slice(0, d.nvalues-discarded).Interface())
}

func (d *DictionaryEncodingTestSuite) TestBasicRoundTrip() {
	d.initData(2500, 2)
	d.checkRoundTrip()
}

func TestDictEncoding(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
	}{
		{"Int32", reflect.TypeOf(int32(0))},
		{"Int64", reflect.TypeOf(int64(0))},
		{"Float32", reflect.TypeOf(float32(0))},
		{"Float64", reflect.TypeOf(float64(0))},
		{"ByteArray", reflect.TypeOf(parquet.ByteArray{})},
		{"FixedLenByteArray", reflect.TypeOf(parquet.FixedLenByteArray{})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite.Run(t, &DictionaryEncodingTestSuite{BaseEncodingTestSuite{typ: tt.typ}})
		})
	}
}

func TestWriteDeltaBitPackedInt32(t *testing.T) {
	column := schema.NewColumn(schema.NewInt32Node("int32", parquet.Repetitions.Required, -1), 0, 0)

	tests := []struct {
		name     string
		toencode []int32
		expected []byte
	}{
		{"simple 12345", []int32{1, 2, 3, 4, 5}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
		{"odd vals", []int32{7, 5, 3, 1, 2, 3, 4, 5}, []byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := encoding.NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)

			enc.(encoding.Int32Encoder).Put(tt.toencode)
			buf, _ := enc.FlushValues()
			defer buf.Release()

			assert.Equal(t, tt.expected, buf.Bytes())

			dec := encoding.NewDecoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)

			dec.(encoding.Int32Decoder).SetData(len(tt.toencode), tt.expected)
			out := make([]int32, len(tt.toencode))
			dec.(encoding.Int32Decoder).Decode(out)
			assert.Equal(t, tt.toencode, out)
		})
	}

	t.Run("test progressive decoding", func(t *testing.T) {
		values := make([]int32, 1000)
		testutils.FillRandomInt32(0, values)

		enc := encoding.NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)
		enc.(encoding.Int32Encoder).Put(values)
		buf, _ := enc.FlushValues()
		defer buf.Release()

		dec := encoding.NewDecoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)
		dec.(encoding.Int32Decoder).SetData(len(values), buf.Bytes())

		valueBuf := make([]int32, 100)
		for i, j := 0, len(valueBuf); j <= len(values); i, j = i+len(valueBuf), j+len(valueBuf) {
			dec.(encoding.Int32Decoder).Decode(valueBuf)
			assert.Equalf(t, values[i:j], valueBuf, "indexes %d:%d", i, j)
		}
	})

	t.Run("test decoding multiple pages", func(t *testing.T) {
		values := make([]int32, 1000)
		testutils.FillRandomInt32(0, values)

		enc := encoding.NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)
		enc.(encoding.Int32Encoder).Put(values)
		buf, _ := enc.FlushValues()
		defer buf.Release()

		// Using same Decoder to decode the data.
		dec := encoding.NewDecoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)
		for i := 0; i < 5; i += 1 {
			dec.(encoding.Int32Decoder).SetData(len(values), buf.Bytes())

			valueBuf := make([]int32, 100)
			for i, j := 0, len(valueBuf); j <= len(values); i, j = i+len(valueBuf), j+len(valueBuf) {
				dec.(encoding.Int32Decoder).Decode(valueBuf)
				assert.Equalf(t, values[i:j], valueBuf, "indexes %d:%d", i, j)
			}
		}
	})
}

func TestWriteDeltaBitPackedInt64(t *testing.T) {
	column := schema.NewColumn(schema.NewInt64Node("int64", parquet.Repetitions.Required, -1), 0, 0)

	tests := []struct {
		name     string
		toencode []int64
		expected []byte
	}{
		{"simple 12345", []int64{1, 2, 3, 4, 5}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
		{"odd vals", []int64{7, 5, 3, 1, 2, 3, 4, 5}, []byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)

			enc.(encoding.Int64Encoder).Put(tt.toencode)
			buf, _ := enc.FlushValues()
			defer buf.Release()

			assert.Equal(t, tt.expected, buf.Bytes())

			dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)

			dec.(encoding.Int64Decoder).SetData(len(tt.toencode), tt.expected)
			out := make([]int64, len(tt.toencode))
			dec.(encoding.Int64Decoder).Decode(out)
			assert.Equal(t, tt.toencode, out)
		})
	}

	t.Run("test progressive decoding", func(t *testing.T) {
		values := make([]int64, 1000)
		testutils.FillRandomInt64(0, values)

		enc := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)
		enc.(encoding.Int64Encoder).Put(values)
		buf, _ := enc.FlushValues()
		defer buf.Release()

		dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)
		dec.(encoding.Int64Decoder).SetData(len(values), buf.Bytes())

		valueBuf := make([]int64, 100)
		for i, j := 0, len(valueBuf); j <= len(values); i, j = i+len(valueBuf), j+len(valueBuf) {
			decoded, _ := dec.(encoding.Int64Decoder).Decode(valueBuf)
			assert.Equal(t, len(valueBuf), decoded)
			assert.Equalf(t, values[i:j], valueBuf, "indexes %d:%d", i, j)
		}
	})

	t.Run("GH-37102", func(t *testing.T) {
		values := []int64{
			0, 3000000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 3000000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 3000000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 3000000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0,
		}

		enc := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)
		enc.(encoding.Int64Encoder).Put(values)
		buf, _ := enc.FlushValues()
		defer buf.Release()

		dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)
		dec.(encoding.Int64Decoder).SetData(len(values), buf.Bytes())

		valueBuf := make([]int64, len(values))

		decoded, _ := dec.(encoding.Int64Decoder).Decode(valueBuf)
		assert.Equal(t, len(valueBuf), decoded)
		assert.Equal(t, values, valueBuf)
	})

	t.Run("test decoding multiple pages", func(t *testing.T) {
		values := make([]int64, 1000)
		testutils.FillRandomInt64(0, values)

		enc := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, column, memory.DefaultAllocator)
		enc.(encoding.Int64Encoder).Put(values)
		buf, _ := enc.FlushValues()
		defer buf.Release()

		// Using same Decoder to decode the data.
		dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, column, memory.DefaultAllocator)
		for i := 0; i < 5; i += 1 {
			dec.(encoding.Int64Decoder).SetData(len(values), buf.Bytes())

			valueBuf := make([]int64, 100)
			for i, j := 0, len(valueBuf); j <= len(values); i, j = i+len(valueBuf), j+len(valueBuf) {
				dec.(encoding.Int64Decoder).Decode(valueBuf)
				assert.Equalf(t, values[i:j], valueBuf, "indexes %d:%d", i, j)
			}
		}
	})
}

func TestDeltaLengthByteArrayEncoding(t *testing.T) {
	column := schema.NewColumn(schema.NewByteArrayNode("bytearray", parquet.Repetitions.Required, -1), 0, 0)

	test := []parquet.ByteArray{[]byte("Hello"), []byte("World"), []byte("Foobar"), []byte("ABCDEF")}
	expected := []byte{128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}

	enc := encoding.NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, false, column, memory.DefaultAllocator)
	enc.(encoding.ByteArrayEncoder).Put(test)
	buf, _ := enc.FlushValues()
	defer buf.Release()

	assert.Equal(t, expected, buf.Bytes())

	dec := encoding.NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, column, nil)
	dec.SetData(len(test), expected)
	out := make([]parquet.ByteArray, len(test))
	decoded, _ := dec.(encoding.ByteArrayDecoder).Decode(out)
	assert.Equal(t, len(test), decoded)
	assert.Equal(t, test, out)
}

func TestDeltaByteArrayEncoding(t *testing.T) {
	test := []parquet.ByteArray{[]byte("Hello"), []byte("World"), []byte("Foobar"), []byte("ABCDEF")}
	expected := []byte{128, 1, 4, 4, 0, 0, 0, 0, 0, 0, 128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}

	enc := encoding.NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, false, nil, nil)
	enc.(encoding.ByteArrayEncoder).Put(test)
	buf, _ := enc.FlushValues()
	defer buf.Release()

	assert.Equal(t, expected, buf.Bytes())

	dec := encoding.NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, nil, nil)
	dec.SetData(len(test), expected)
	out := make([]parquet.ByteArray, len(test))
	decoded, _ := dec.(encoding.ByteArrayDecoder).Decode(out)
	assert.Equal(t, len(test), decoded)
	assert.Equal(t, test, out)
}

func TestDeltaBitPacking(t *testing.T) {
	datadir := os.Getenv("ARROW_TEST_DATA")
	if datadir == "" {
		return
	}

	fname := path.Join(datadir, "parquet/timestamp.data")
	require.FileExists(t, fname)
	f, err := os.Open(fname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	values := make([]int64, 0)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		v, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, v)
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	col := schema.NewColumn(schema.MustPrimitive(schema.NewPrimitiveNode("foo", parquet.Repetitions.Required,
		parquet.Types.Int64, -1, -1)), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, false, col, memory.DefaultAllocator).(encoding.Int64Encoder)

	enc.Put(values)
	buf, err := enc.FlushValues()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	dec := encoding.NewDecoder(parquet.Types.Int64, parquet.Encodings.DeltaBinaryPacked, col, memory.DefaultAllocator).(encoding.Int64Decoder)
	dec.SetData(len(values), buf.Bytes())

	ll := len(values)
	for i := 0; i < ll; i += 1024 {
		out := make([]int64, 1024)
		n, err := dec.Decode(out)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, values[:n], out[:n])
		values = values[n:]
	}
	assert.Equal(t, dec.ValuesLeft(), 0)
}

func TestBooleanPlainDecoderAfterFlushing(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)

	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	decSlice := make([]bool, 1)
	bdec := dec.(encoding.BooleanDecoder)

	// Write and extract two different values
	// This is validating that `FlushValues` wholly
	// resets the encoder state.
	benc.Put([]bool{true})
	buf1, err := benc.FlushValues()
	assert.NoError(t, err)

	benc.Put([]bool{false})
	buf2, err := benc.FlushValues()
	assert.NoError(t, err)

	// Decode buf1, expect true
	err = bdec.SetData(1, buf1.Buf())
	assert.NoError(t, err)
	n, err := bdec.Decode(decSlice)
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, decSlice[0], true)

	// Decode buf2, expect false
	err = bdec.SetData(1, buf2.Buf())
	assert.NoError(t, err)
	n, err = bdec.Decode(decSlice)
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	assert.Equal(t, decSlice[0], false)
}

func TestBooleanPlainEncoderPutBitmap(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)

	// Create test bitmap
	bitmapBytes := make([]byte, bitutil.BytesForBits(16))
	expected := []bool{true, false, true, true, false, false, true, false,
		false, true, false, true, false, true, false, true}
	for i, bit := range expected {
		if bit {
			bitutil.SetBit(bitmapBytes, i)
		}
	}

	// Write using PutBitmap
	type bitmapEncoder interface {
		PutBitmap(bitmap []byte, offset int64, length int64)
	}
	if bme, ok := benc.(bitmapEncoder); ok {
		bme.PutBitmap(bitmapBytes, 0, 16)
	} else {
		t.Skip("Encoder does not support PutBitmap")
	}

	// Flush and decode
	buf, err := benc.FlushValues()
	assert.NoError(t, err)

	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(16, buf.Buf())
	assert.NoError(t, err)

	decoded := make([]bool, 16)
	n, err := bdec.Decode(decoded)
	assert.NoError(t, err)
	assert.Equal(t, 16, n)
	assert.Equal(t, expected, decoded)
}

func TestBooleanPlainEncoderPutBitmapUnaligned(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)

	// Create test bitmap with offset
	srcBits := []bool{false, false, true, false, true, true, false, false, true, false}
	bitmapBytes := make([]byte, bitutil.BytesForBits(int64(len(srcBits))))
	for i, bit := range srcBits {
		if bit {
			bitutil.SetBit(bitmapBytes, i)
		}
	}

	// Write 6 bits starting from offset 2
	expected := srcBits[2:8]
	type bitmapEncoder interface {
		PutBitmap(bitmap []byte, offset int64, length int64)
	}
	if bme, ok := benc.(bitmapEncoder); ok {
		bme.PutBitmap(bitmapBytes, 2, 6)
	} else {
		t.Skip("Encoder does not support PutBitmap")
	}

	// Flush and decode
	buf, err := benc.FlushValues()
	assert.NoError(t, err)

	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(6, buf.Buf())
	assert.NoError(t, err)

	decoded := make([]bool, 6)
	n, err := bdec.Decode(decoded)
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, expected, decoded)
}

func TestBooleanPlainDecoderDecodeToBitmap(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)

	// Encode test data
	expected := []bool{true, false, true, true, false, false, true, false,
		false, true, false, true, false, true, false, true}
	benc.Put(expected)
	buf, err := benc.FlushValues()
	assert.NoError(t, err)

	// Decode using DecodeToBitmap
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(16, buf.Buf())
	assert.NoError(t, err)

	outBitmap := make([]byte, bitutil.BytesForBits(16))
	type bitmapDecoder interface {
		DecodeToBitmap(out []byte, outOffset int64, length int) (int, error)
	}
	if bmd, ok := bdec.(bitmapDecoder); ok {
		n, err := bmd.DecodeToBitmap(outBitmap, 0, 16)
		assert.NoError(t, err)
		assert.Equal(t, 16, n)

		// Verify bitmap contents
		for i, expectedBit := range expected {
			actualBit := bitutil.BitIsSet(outBitmap, i)
			assert.Equal(t, expectedBit, actualBit, "bit mismatch at position %d", i)
		}
	} else {
		t.Skip("Decoder does not support DecodeToBitmap")
	}
}

func TestBooleanPlainDecoderDecodeToBitmapUnaligned(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)

	// Encode test data
	expected := []bool{true, false, true, true, false, false}
	benc.Put(expected)
	buf, err := benc.FlushValues()
	assert.NoError(t, err)

	// Decode to unaligned offset
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(6, buf.Buf())
	assert.NoError(t, err)

	outBitmap := make([]byte, bitutil.BytesForBits(10)) // Extra space
	type bitmapDecoder interface {
		DecodeToBitmap(out []byte, outOffset int64, length int) (int, error)
	}
	if bmd, ok := bdec.(bitmapDecoder); ok {
		// Decode starting at bit offset 3
		n, err := bmd.DecodeToBitmap(outBitmap, 3, 6)
		assert.NoError(t, err)
		assert.Equal(t, 6, n)

		// Verify bitmap contents at offset 3
		for i, expectedBit := range expected {
			actualBit := bitutil.BitIsSet(outBitmap, 3+i)
			assert.Equal(t, expectedBit, actualBit, "bit mismatch at position %d", i)
		}

		// Verify bits 0-2 are unmodified (should be false)
		for i := 0; i < 3; i++ {
			assert.False(t, bitutil.BitIsSet(outBitmap, i), "bit at position %d should be false", i)
		}
	} else {
		t.Skip("Decoder does not support DecodeToBitmap")
	}
}

func TestPlainBooleanEncoderPutSpacedBitmapBasic(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)
	
	// Cast to access PutSpacedBitmap
	type bitmapSpacedEncoder interface {
		PutSpacedBitmap(bitmap []byte, bitmapOffset int64, numValues int64, validBits []byte, validBitsOffset int64) int64
	}
	spacedEnc := benc.(bitmapSpacedEncoder)
	
	// Data bitmap: [true, false, true, true, false, false, true, false]
	bitmap := []byte{0b01001101} // LSB first
	validBits := []byte{0xFF}     // all valid
	
	numValid := spacedEnc.PutSpacedBitmap(bitmap, 0, 8, validBits, 0)
	assert.Equal(t, int64(8), numValid)
	
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	
	// Decode and verify
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(8, buf.Bytes())
	require.NoError(t, err)
	
	out := make([]bool, 8)
	n, err := bdec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	
	expected := []bool{true, false, true, true, false, false, true, false}
	assert.Equal(t, expected, out)
}

func TestPlainBooleanEncoderPutSpacedBitmapWithNulls(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)
	
	type bitmapSpacedEncoder interface {
		PutSpacedBitmap(bitmap []byte, bitmapOffset int64, numValues int64, validBits []byte, validBitsOffset int64) int64
	}
	spacedEnc := benc.(bitmapSpacedEncoder)
	
	// Data bitmap: [1,1,1,1,0,0,0,0]
	bitmap := []byte{0b00001111}
	// Valid bits: [1,0,1,0,1,0,1,0] - only positions 0,2,4,6 are valid
	validBits := []byte{0b01010101}
	
	numValid := spacedEnc.PutSpacedBitmap(bitmap, 0, 8, validBits, 0)
	assert.Equal(t, int64(4), numValid, "should encode 4 valid values")
	
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	
	// Decode and verify - should get compressed bitmap (only valid values)
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(4, buf.Bytes())
	require.NoError(t, err)
	
	out := make([]bool, 4)
	n, err := bdec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	
	// Valid positions 0,2,4,6 have data bits 1,1,0,0
	expected := []bool{true, true, false, false}
	assert.Equal(t, expected, out)
}

func TestPlainBooleanEncoderPutSpacedBitmapConsistency(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	
	// Verify PutSpacedBitmap produces same output as PutSpaced with []bool
	bitmap := []byte{0b10110010}
	validBits := []byte{0xFF}
	numValues := int64(8)
	
	// Encode using PutSpacedBitmap
	enc1 := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc1 := enc1.(encoding.BooleanEncoder)
	
	type bitmapSpacedEncoder interface {
		PutSpacedBitmap(bitmap []byte, bitmapOffset int64, numValues int64, validBits []byte, validBitsOffset int64) int64
	}
	spacedEnc1 := benc1.(bitmapSpacedEncoder)
	
	spacedEnc1.PutSpacedBitmap(bitmap, 0, numValues, validBits, 0)
	buf1, err := enc1.FlushValues()
	require.NoError(t, err)
	
	// Encode using PutSpaced with []bool
	enc2 := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
	benc2 := enc2.(encoding.BooleanEncoder)
	
	bools := make([]bool, numValues)
	for i := int64(0); i < numValues; i++ {
		bools[i] = bitutil.BitIsSet(bitmap, int(i))
	}
	benc2.PutSpaced(bools, validBits, 0)
	buf2, err := enc2.FlushValues()
	require.NoError(t, err)
	
	// Both should produce identical encoded output
	assert.Equal(t, buf2.Bytes(), buf1.Bytes(), "encoded output should match")
}

func TestRleBooleanEncoderPutSpacedBitmap(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.RLE, false, descr, memory.DefaultAllocator)
	benc := enc.(encoding.BooleanEncoder)
	
	type bitmapSpacedEncoder interface {
		PutSpacedBitmap(bitmap []byte, bitmapOffset int64, numValues int64, validBits []byte, validBitsOffset int64) int64
	}
	spacedEnc, ok := benc.(bitmapSpacedEncoder)
	require.True(t, ok, "RleBooleanEncoder should implement bitmapSpacedEncoder interface")
	
	// Data bitmap: [true,true,false,true,false,false,true,false]
	bitmap := []byte{0b01001011}
	// Valid bits: [1,0,1,0,1,0,1,0] - positions 0,2,4,6 are valid
	validBits := []byte{0b01010101}
	
	numValid := spacedEnc.PutSpacedBitmap(bitmap, 0, 8, validBits, 0)
	assert.Equal(t, int64(4), numValid, "should encode 4 valid values")
	
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	
	// Decode and verify - should get only the valid positions: [true, false, false, true]
	dec := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE, descr, memory.DefaultAllocator)
	bdec := dec.(encoding.BooleanDecoder)
	err = bdec.SetData(4, buf.Bytes())
	require.NoError(t, err)
	
	out := make([]bool, 4)
	n, err := bdec.Decode(out)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	
	// Valid values are at positions 0,2,4,6 with values true,false,false,true
	expected := []bool{true, false, false, true}
	assert.Equal(t, expected, out)
}

func TestRleBooleanEncoderPutSpacedBitmapConsistency(t *testing.T) {
	descr := schema.NewColumn(schema.NewBooleanNode("bool", parquet.Repetitions.Optional, -1), 0, 0)
	
	// Create test data
	bitmap := []byte{0b10110101}
	validBits := []byte{0b11110011}
	
	// Encode with bitmap method
	enc1 := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.RLE, false, descr, memory.DefaultAllocator)
	benc1 := enc1.(encoding.BooleanEncoder)
	type bitmapSpacedEncoder interface {
		PutSpacedBitmap(bitmap []byte, bitmapOffset int64, numValues int64, validBits []byte, validBitsOffset int64) int64
	}
	spacedEnc := benc1.(bitmapSpacedEncoder)
	numValid := spacedEnc.PutSpacedBitmap(bitmap, 0, 8, validBits, 0)
	buf1, _ := enc1.FlushValues()
	defer buf1.Release()
	
	// Encode with []bool method (reference)
	enc2 := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.RLE, false, descr, memory.DefaultAllocator)
	benc2 := enc2.(encoding.BooleanEncoder)
	spacedValues := make([]bool, 8)
	for i := 0; i < 8; i++ {
		spacedValues[i] = bitutil.BitIsSet(bitmap, i)
	}
	benc2.PutSpaced(spacedValues, validBits, 0)
	buf2, _ := enc2.FlushValues()
	defer buf2.Release()
	
	// Both should produce identical output
	assert.Equal(t, buf2.Bytes(), buf1.Bytes(), "bitmap and []bool methods should produce identical output")
	
	// Decode both and verify they match
	dec1 := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE, descr, memory.DefaultAllocator)
	bdec1 := dec1.(encoding.BooleanDecoder)
	bdec1.SetData(int(numValid), buf1.Bytes())
	
	dec2 := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE, descr, memory.DefaultAllocator)
	bdec2 := dec2.(encoding.BooleanDecoder)
	bdec2.SetData(int(numValid), buf2.Bytes())
	
	out1 := make([]bool, numValid)
	out2 := make([]bool, numValid)
	bdec1.Decode(out1)
	bdec2.Decode(out2)
	
	assert.Equal(t, out2, out1, "decoded values should match")
}
