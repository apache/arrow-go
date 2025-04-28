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

package variant

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/google/uuid"
)

type Builder struct {
	buf             bytes.Buffer
	dict            map[string]uint32
	dictKeys        [][]byte
	allowDuplicates bool
}

func (b *Builder) SetAllowDuplicates(allow bool) {
	b.allowDuplicates = allow
}

func (b *Builder) AddKeys(keys []string) (ids []uint32) {
	if b.dict == nil {
		b.dict = make(map[string]uint32)
		b.dictKeys = make([][]byte, 0, len(keys))
	}

	ids = make([]uint32, len(keys))
	for i, key := range keys {
		var ok bool
		if ids[i], ok = b.dict[key]; ok {
			continue
		}

		ids[i] = uint32(len(b.dictKeys))
		b.dict[key] = ids[i]
		b.dictKeys = append(b.dictKeys, unsafe.Slice(unsafe.StringData(key), len(key)))
	}

	return ids
}

func (b *Builder) AddKey(key string) (id uint32) {
	if b.dict == nil {
		b.dict = make(map[string]uint32)
		b.dictKeys = make([][]byte, 0, 16)
	}

	var ok bool
	if id, ok = b.dict[key]; ok {
		return id
	}

	id = uint32(len(b.dictKeys))
	b.dict[key] = id
	b.dictKeys = append(b.dictKeys, unsafe.Slice(unsafe.StringData(key), len(key)))

	return id
}

func (b *Builder) AppendNull() error {
	return b.buf.WriteByte(primitiveHeader(PrimitiveNull))
}

func (b *Builder) AppendBool(v bool) error {
	var t PrimitiveType
	if v {
		t = PrimitiveBoolTrue
	} else {
		t = PrimitiveBoolFalse
	}

	return b.buf.WriteByte(primitiveHeader(t))
}

type primitiveNumeric interface {
	int8 | int16 | int32 | int64 | float32 | float64 |
		arrow.Date32 | arrow.Time64
}

type buffer interface {
	io.Writer
	io.ByteWriter
}

func writeBinary[T string | []byte](w buffer, v T) error {
	var t PrimitiveType
	switch any(v).(type) {
	case string:
		t = PrimitiveString
	case []byte:
		t = PrimitiveBinary
	}

	if err := w.WriteByte(primitiveHeader(t)); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
		return err
	}

	_, err := w.Write([]byte(v))
	return err
}

func writeNumeric[T primitiveNumeric](w buffer, v T) error {
	var t PrimitiveType
	switch any(v).(type) {
	case int8:
		t = PrimitiveInt8
	case int16:
		t = PrimitiveInt16
	case int32:
		t = PrimitiveInt32
	case int64:
		t = PrimitiveInt64
	case float32:
		t = PrimitiveFloat
	case float64:
		t = PrimitiveDouble
	case arrow.Date32:
		t = PrimitiveDate
	case arrow.Time64:
		t = PrimitiveTimeMicrosNTZ
	}

	if err := w.WriteByte(primitiveHeader(t)); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, v)
}

func (b *Builder) AppendInt(v int64) error {
	b.buf.Grow(9)
	switch {
	case v >= math.MinInt8 && v <= math.MaxInt8:
		return writeNumeric(&b.buf, int8(v))
	case v >= math.MinInt16 && v <= math.MaxInt16:
		return writeNumeric(&b.buf, int16(v))
	case v >= math.MinInt32 && v <= math.MaxInt32:
		return writeNumeric(&b.buf, int32(v))
	default:
		return writeNumeric(&b.buf, v)
	}
}

func (b *Builder) AppendFloat32(v float32) error {
	b.buf.Grow(5)
	return writeNumeric(&b.buf, v)
}

func (b *Builder) AppendFloat64(v float64) error {
	b.buf.Grow(9)
	return writeNumeric(&b.buf, v)
}

func (b *Builder) AppendDate(v arrow.Date32) error {
	b.buf.Grow(5)
	return writeNumeric(&b.buf, v)
}

func (b *Builder) AppendTimeMicro(v arrow.Time64) error {
	b.buf.Grow(9)
	return writeNumeric(&b.buf, v)
}

func (b *Builder) AppendTimestamp(v arrow.Timestamp, useMicros, useUTC bool) error {
	b.buf.Grow(9)
	var t PrimitiveType
	if useMicros {
		t = PrimitiveTimestampMicrosNTZ
	} else {
		t = PrimitiveTimestampNanosNTZ
	}

	if useUTC {
		t--
	}

	if err := b.buf.WriteByte(primitiveHeader(t)); err != nil {
		return err
	}

	return binary.Write(&b.buf, binary.LittleEndian, v)
}

func (b *Builder) AppendBinary(v []byte) error {
	b.buf.Grow(5 + len(v))
	return writeBinary(&b.buf, v)
}

func (b *Builder) AppendString(v string) error {
	if len(v) > maxShortStringSize {
		b.buf.Grow(5 + len(v))
		return writeBinary(&b.buf, v)
	}

	b.buf.Grow(1 + len(v))
	if err := b.buf.WriteByte(shortStrHeader(len(v))); err != nil {
		return err
	}

	_, err := b.buf.WriteString(v)
	return err
}

func (b *Builder) AppendUUID(v uuid.UUID) error {
	b.buf.Grow(17)
	if err := b.buf.WriteByte(primitiveHeader(PrimitiveUUID)); err != nil {
		return err
	}

	m, _ := v.MarshalBinary()
	_, err := b.buf.Write(m)
	return err
}

func (b *Builder) AppendDecimal4(scale uint8, v decimal.Decimal32) error {
	b.buf.Grow(6)
	if err := b.buf.WriteByte(primitiveHeader(PrimitiveDecimal4)); err != nil {
		return err
	}

	if err := b.buf.WriteByte(scale); err != nil {
		return err
	}

	return binary.Write(&b.buf, binary.LittleEndian, int32(v))
}

func (b *Builder) AppendDecimal8(scale uint8, v decimal.Decimal64) error {
	b.buf.Grow(10)
	return errors.Join(
		b.buf.WriteByte(primitiveHeader(PrimitiveDecimal8)),
		b.buf.WriteByte(scale),
		binary.Write(&b.buf, binary.LittleEndian, int64(v)),
	)
}

func (b *Builder) AppendDecimal16(scale uint8, v decimal.Decimal128) error {
	b.buf.Grow(18)
	return errors.Join(
		b.buf.WriteByte(primitiveHeader(PrimitiveDecimal16)),
		b.buf.WriteByte(scale),
		binary.Write(&b.buf, binary.LittleEndian, v.LowBits()),
		binary.Write(&b.buf, binary.LittleEndian, v.HighBits()),
	)
}

func (b *Builder) Offset() int {
	return b.buf.Len()
}

func (b *Builder) FinishArray(start int, offsets []int) error {
	var (
		dataSize, sz = b.buf.Len() - start, len(offsets)
		isLarge      = sz > math.MaxUint8
		sizeBytes    = 1
	)

	if isLarge {
		sizeBytes = 4
	}

	if dataSize < 0 {
		return errors.New("invalid array size")
	}

	offsetSize := intSize(dataSize)
	headerSize := 1 + sizeBytes + (sz+1)*int(offsetSize)

	// shift the just written data to make room for the header section
	b.buf.Grow(headerSize)
	av := b.buf.AvailableBuffer()
	if _, err := b.buf.Write(av[:headerSize]); err != nil {
		return err
	}

	bs := b.buf.Bytes()
	copy(bs[start+headerSize:], bs[start:start+dataSize])

	// populate the header
	bs[start] = arrayHeader(isLarge, offsetSize)
	writeOffset(bs[start+1:], sz, uint8(sizeBytes))

	offsetsStart := start + 1 + sizeBytes
	for i, off := range offsets {
		writeOffset(bs[offsetsStart+i*int(offsetSize):], off, offsetSize)
	}
	writeOffset(bs[offsetsStart+sz*int(offsetSize):], dataSize, offsetSize)

	return nil
}

type FieldEntry struct {
	Key    string
	ID     uint32
	Offset int
}

func (b *Builder) NextField(start int, key string) FieldEntry {
	id := b.AddKey(key)
	return FieldEntry{
		Key:    key,
		ID:     id,
		Offset: b.Offset() - start,
	}
}

func (b *Builder) FinishObject(start int, fields []FieldEntry) error {
	slices.SortFunc(fields, func(a, b FieldEntry) int {
		return cmp.Compare(a.Key, b.Key)
	})

	sz := len(fields)
	var maxID uint32
	if sz > 0 {
		maxID = fields[0].ID
	}

	// if a duplicate key is found, one of two things happens:
	// - if allowDuplicates is true, then the field with the greatest
	//    offset value (the last appended field) is kept.
	// - if allowDuplicates is false, then an error is returned
	if b.allowDuplicates {
		distinctPos := 0
		// maintain a list of distinct keys in-place
		for i := 1; i < sz; i++ {
			maxID = max(maxID, fields[i].ID)
			if fields[i].ID == fields[i-1].ID {
				// found a duplicate key. keep the
				// field with a greater offset
				if fields[distinctPos].Offset < fields[i].Offset {
					fields[distinctPos].Offset = fields[i].Offset
				}
			} else {
				// found distinct key, add field to the list
				distinctPos++
				fields[distinctPos] = fields[i]
			}
		}

		if distinctPos+1 < len(fields) {
			sz = distinctPos + 1
			// resize fields to size
			fields = fields[:sz]
			// sort the fields by offsets so that we can move the value
			// data of each field to the new offset without overwriting the
			// fields after it.
			slices.SortFunc(fields, func(a, b FieldEntry) int {
				return cmp.Compare(a.Offset, b.Offset)
			})

			buf := b.buf.Bytes()
			curOffset := 0
			for i := range sz {
				oldOffset := fields[i].Offset
				fieldSize := valueSize(buf[start+oldOffset:])
				copy(buf[start+curOffset:], buf[start+oldOffset:start+oldOffset+fieldSize])
				fields[i].Offset = curOffset
				curOffset += fieldSize
			}
			b.buf.Truncate(start + curOffset)
			// change back to sort order by field keys to meet variant spec
			slices.SortFunc(fields, func(a, b FieldEntry) int {
				return cmp.Compare(a.Key, b.Key)
			})
		}
	} else {
		for i := 1; i < sz; i++ {
			maxID = max(maxID, fields[i].ID)
			if fields[i].Key == fields[i-1].Key {
				return fmt.Errorf("disallowed duplicate key found: %s", fields[i].Key)
			}
		}
	}

	var (
		dataSize           = b.buf.Len() - start
		isLarge            = sz > math.MaxUint8
		sizeBytes          = 1
		idSize, offsetSize = intSize(int(maxID)), intSize(dataSize)
	)

	if isLarge {
		sizeBytes = 4
	}

	if dataSize < 0 {
		return errors.New("invalid object size")
	}

	headerSize := 1 + sizeBytes + sz*int(idSize) + (sz+1)*int(offsetSize)
	// shift the just written data to make room for the header section
	b.buf.Grow(headerSize)
	av := b.buf.AvailableBuffer()
	if _, err := b.buf.Write(av[:headerSize]); err != nil {
		return err
	}

	bs := b.buf.Bytes()
	copy(bs[start+headerSize:], bs[start:start+dataSize])

	// populate the header
	bs[start] = objectHeader(isLarge, idSize, offsetSize)
	writeOffset(bs[start+1:], sz, uint8(sizeBytes))

	idStart := start + 1 + sizeBytes
	offsetStart := idStart + sz*int(idSize)
	for i, field := range fields {
		writeOffset(bs[idStart+i*int(idSize):], int(field.ID), idSize)
		writeOffset(bs[offsetStart+i*int(offsetSize):], field.Offset, offsetSize)
	}
	writeOffset(bs[offsetStart+sz*int(offsetSize):], dataSize, offsetSize)
	return nil
}

func (b *Builder) Build() (Value, error) {
	nkeys := len(b.dictKeys)
	totalDictSize := 0
	for _, k := range b.dictKeys {
		totalDictSize += len(k)
	}

	// determine the number of bytes required per offset entry.
	// the largest offset is the one-past-the-end value, the total size.
	// It's very unlikely that the number of keys could be larger, but
	// incorporate that into the calculation in case of pathological data.
	maxSize := max(totalDictSize, nkeys)
	if maxSize > maxSizeLimit {
		return Value{}, fmt.Errorf("metadata size too large: %d", maxSize)
	}

	offsetSize := intSize(int(maxSize))
	offsetStart := 1 + offsetSize
	stringStart := int(offsetStart) + (nkeys+1)*int(offsetSize)
	metadataSize := stringStart + totalDictSize

	if metadataSize > maxSizeLimit {
		return Value{}, fmt.Errorf("metadata size too large: %d", metadataSize)
	}

	meta := make([]byte, metadataSize)

	meta[0] = supportedVersion | ((offsetSize - 1) << 6)
	if nkeys > 0 && slices.IsSortedFunc(b.dictKeys, bytes.Compare) {
		meta[0] |= 1 << 4
	}
	writeOffset(meta[1:], nkeys, offsetSize)

	curOffset := 0
	for i, k := range b.dictKeys {
		writeOffset(meta[int(offsetStart)+i*int(offsetSize):], curOffset, offsetSize)
		curOffset += copy(meta[stringStart+curOffset:], k)
	}
	writeOffset(meta[int(offsetStart)+nkeys*int(offsetSize):], curOffset, offsetSize)

	return Value{
		value: b.buf.Bytes(),
		meta: Metadata{
			data: meta,
			keys: b.dictKeys,
		},
	}, nil
}
