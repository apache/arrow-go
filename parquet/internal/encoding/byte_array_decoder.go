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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding/streaming"
	pqutils "github.com/apache/arrow-go/v18/parquet/internal/utils"
)

// PlainByteArrayDecoder decodes a data chunk for bytearrays according to
// the plain encoding. The byte arrays will use slices to reference the
// data rather than copying it.
//
// The parquet spec defines Plain encoding for ByteArrays as a 4 byte little
// endian integer containing the length of the bytearray followed by that many
// bytes being the raw data of the byte array.
type PlainByteArrayDecoder struct {
	decoder

	// src drives the streaming path (PageStreamingEnabled) when non-nil.
	src streaming.ValueBuffer
}

// Type returns parquet.Types.ByteArray for this decoder
func (PlainByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.ByteArray
}

func (pbad *PlainByteArrayDecoder) SetSource(nvals int, src streaming.ValueBuffer) {
	pbad.src, pbad.nvals = src, nvals
}

func (pbad *PlainByteArrayDecoder) SetData(nvals int, data []byte) error {
	pbad.src = nil
	return pbad.decoder.SetData(nvals, data)
}

// decodeStreaming fills out by aliasing each value in the buffer (no copy). Recycle at
// entry reclaims the previous batch, so the returned slices are valid only until the
// next Decode call; callers consume or copy them before then.
func (pbad *PlainByteArrayDecoder) decodeStreaming(out []parquet.ByteArray) (int, error) {
	pbad.src.Recycle()
	max := utils.Min(len(out), pbad.nvals)
	for i := 0; i < max; i++ {
		hdr, err := pbad.src.Fill(4)
		if err != nil {
			return i, fmt.Errorf("parquet: reading bytearray: %w", err)
		}
		byteLen := int(int32(binary.LittleEndian.Uint32(hdr[:4])))
		if byteLen < 0 {
			return i, errors.New("parquet: invalid BYTE_ARRAY value")
		}
		pbad.src.Advance(4)
		// Fill rejects a byteLen past the value region before allocating.
		val, err := pbad.src.Fill(byteLen)
		if err != nil {
			return i, fmt.Errorf("parquet: reading bytearray: %w", err)
		}
		// 3-index cap stops appends bleeding into the next value; a zero-length slice
		// of the non-nil buffer stays non-nil (empty value != null).
		out[i] = val[:byteLen:byteLen]
		pbad.src.Advance(byteLen)
	}
	pbad.nvals -= max
	return max, nil
}

func (pbad *PlainByteArrayDecoder) discardStreaming(n int) (int, error) {
	n = min(n, pbad.nvals)
	for i := 0; i < n; i++ {
		pbad.src.Recycle()
		buf, err := pbad.src.Fill(4)
		if err != nil {
			return i, fmt.Errorf("parquet: skipping bytearray: %w", err)
		}
		valueLen := int(int32(binary.LittleEndian.Uint32(buf[:4])))
		if valueLen < 0 {
			return i, errors.New("parquet: invalid BYTE_ARRAY value")
		}
		pbad.src.Advance(4)
		if err := pbad.src.Skip(valueLen); err != nil {
			return i, fmt.Errorf("parquet: skipping bytearray: %w", err)
		}
	}
	pbad.nvals -= n
	return n, nil
}

func (pbad *PlainByteArrayDecoder) Discard(n int) (int, error) {
	if pbad.src != nil {
		return pbad.discardStreaming(n)
	}
	n = min(n, pbad.nvals)
	// we have to skip the length of each value by first checking
	// the length of the value and then skipping that many bytes
	for i := 0; i < n; i++ {
		if len(pbad.data) < 4 {
			return i, errors.New("parquet: eof skipping bytearray values")
		}

		valueLen := int32(binary.LittleEndian.Uint32(pbad.data[:4]))
		if valueLen < 0 {
			return i, errors.New("parquet: invalid BYTE_ARRAY value")
		}

		if int64(len(pbad.data)) < int64(valueLen)+4 {
			return i, errors.New("parquet: eof skipping bytearray values")
		}

		pbad.data = pbad.data[valueLen+4:]
	}

	pbad.nvals -= n
	return n, nil
}

// Decode will populate the slice of bytearrays in full or until the number
// of values is consumed.
//
// Returns the number of values that were decoded.
func (pbad *PlainByteArrayDecoder) Decode(out []parquet.ByteArray) (int, error) {
	if pbad.src != nil {
		return pbad.decodeStreaming(out)
	}

	max := utils.Min(len(out), pbad.nvals)

	for i := 0; i < max; i++ {
		// there should always be at least four bytes which is the length of the
		// next value in the data.
		if len(pbad.data) < 4 {
			return i, errors.New("parquet: eof reading bytearray")
		}

		// the first 4 bytes are a little endian int32 length
		byteLen := int32(binary.LittleEndian.Uint32(pbad.data[:4]))
		if byteLen < 0 {
			return i, errors.New("parquet: invalid BYTE_ARRAY value")
		}

		if int64(len(pbad.data)) < int64(byteLen)+4 {
			return i, errors.New("parquet: eof reading bytearray")
		}

		out[i] = pbad.data[4 : byteLen+4 : byteLen+4]
		pbad.data = pbad.data[byteLen+4:]
	}

	pbad.nvals -= max
	return max, nil
}

// DecodeSpaced is like Decode, but expands the slice out to leave empty values
// where the validBits bitmap has 0s
func (pbad *PlainByteArrayDecoder) DecodeSpaced(out []parquet.ByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toRead := len(out) - nullCount
	valuesRead, err := pbad.Decode(out[:toRead])
	if err != nil {
		return valuesRead, err
	}
	if valuesRead != toRead {
		return valuesRead, errors.New("parquet: number of values / definition levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

// ReserveBinaryData checks the Arrow offset limit before growing the data
// buffer for a payload of required bytes, including bytes already appended.
func ReserveBinaryData(bldr *array.BinaryBuilder, required uint64) error {
	limit := uint64(math.MaxInt)
	if bldr.Type().Layout().Buffers[1].ByteWidth == arrow.Int32SizeBytes {
		limit = min(limit, math.MaxInt32)
	}
	if required > limit {
		return fmt.Errorf("%w: binary payload exceeds Arrow offset capacity", arrow.ErrInvalid)
	}
	if size := int(required); size > bldr.DataCap() {
		capacity := size
		if required <= limit/2 {
			capacity = bitutil.NextPowerOf2(size)
		}
		bldr.ReserveData(capacity - bldr.DataLen())
	}

	return nil
}

// DecodeToBinaryBuilder appends count dictionary values in bounded batches.
// On error it restores the builder to its starting length.
func (d *DictByteArrayDecoder) DecodeToBinaryBuilder(bldr *array.BinaryBuilder, count int) (_ int, err error) {
	if count == 0 {
		return 0, nil
	}
	if count > d.nvals {
		return 0, io.ErrUnexpectedEOF
	}

	dict := d.dictValueDecoder.(*dictConverter[parquet.ByteArray])
	var indices [1024]uint64
	var values [len(indices)][]byte

	start := bldr.Len()
	defer func() {
		if err != nil && bldr.Len() > start {
			bldr.Resize(start)
		}
	}()
	bldr.Reserve(count)
	written := 0
	for written < count {
		batch := min(count-written, len(indices))
		n, err := d.idxDecoder.GetBatch(indices[:batch])
		d.nvals -= n
		if err != nil {
			return 0, err
		}
		if n != batch {
			return 0, io.ErrUnexpectedEOF
		}

		var maxIndex uint64
		for _, index := range indices[:n] {
			maxIndex = max(maxIndex, index)
		}
		if maxIndex > math.MaxInt32 {
			return 0, fmt.Errorf("parquet: dictionary index %d is out of range", maxIndex)
		}
		if err := dict.ensure(pqutils.IndexType(maxIndex)); err != nil {
			return 0, err
		}

		size := uint64(bldr.DataLen())
		for i, index := range indices[:n] {
			value := dict.dict[index]
			values[i] = value
			size += uint64(len(value))
		}
		if err := ReserveBinaryData(bldr, size); err != nil {
			return 0, err
		}
		bldr.AppendValues(values[:n], nil)
		written += n
	}
	return written, nil
}

func (d *DictByteArrayDecoder) InsertDictionary(bldr array.Builder) error {
	conv := d.dictValueDecoder.(*dictConverter[parquet.ByteArray])
	dictLength := conv.dictLen
	if dictLength > 0 {
		if err := conv.ensure(pqutils.IndexType(dictLength - 1)); err != nil {
			return err
		}
	}

	byteArrayData := memory.NewResizableBuffer(d.mem)
	defer byteArrayData.Release()
	byteArrayOffsets := memory.NewResizableBuffer(d.mem)
	defer byteArrayOffsets.Release()

	var totalLen int
	for _, v := range conv.dict {
		totalLen += len(v)
	}
	byteArrayData.ResizeNoShrink(totalLen)
	byteArrayOffsets.ResizeNoShrink((dictLength + 1) * arrow.Int32SizeBytes)

	byteData := byteArrayData.Bytes()
	byteOffsets := arrow.Int32Traits.CastFromBytes(byteArrayOffsets.Bytes())

	var offset int32
	for i, v := range conv.dict {
		n := copy(byteData, v)
		byteData, byteOffsets[i] = byteData[n:], offset
		offset += int32(n)
	}
	byteOffsets[dictLength] = offset

	data := array.NewData(bldr.Type().(*arrow.DictionaryType).ValueType, dictLength,
		[]*memory.Buffer{nil, byteArrayOffsets, byteArrayData}, nil, 0, 0)
	defer data.Release()
	arr := array.NewBinaryData(data)
	defer arr.Release()

	binaryBldr := bldr.(*array.BinaryDictionaryBuilder)
	return binaryBldr.InsertDictValues(arr)
}
