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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/internal/debug"
	"github.com/google/uuid"
)

//go:generate go tool stringer -type=BasicType -linecomment -output=basic_type_string.go
//go:generate go tool stringer -type=PrimitiveType -linecomment -output=primitive_type_string.go

type BasicType int

const (
	BasicUndefined   BasicType = iota - 1 // Unknown
	BasicPrimitive                        // Primitive
	BasicShortString                      // ShortString
	BasicObject                           // Object
	BasicArray                            // Array
)

func basicTypeFromHeader(hdr byte) BasicType {
	return BasicType(hdr & basicTypeMask)
}

type PrimitiveType int

const (
	PrimitiveInvalid            PrimitiveType = iota - 1 // Unknown
	PrimitiveNull                                        // Null
	PrimitiveBoolTrue                                    // BoolTrue
	PrimitiveBoolFalse                                   // BoolFalse
	PrimitiveInt8                                        // Int8
	PrimitiveInt16                                       // Int16
	PrimitiveInt32                                       // Int32
	PrimitiveInt64                                       // Int64
	PrimitiveDouble                                      // Double
	PrimitiveDecimal4                                    // Decimal32
	PrimitiveDecimal8                                    // Decimal64
	PrimitiveDecimal16                                   // Decimal128
	PrimitiveDate                                        // Date
	PrimitiveTimestampMicros                             // Timestamp(micros)
	PrimitiveTimestampMicrosNTZ                          // TimestampNTZ(micros)
	PrimitiveFloat                                       // Float
	PrimitiveBinary                                      // Binary
	PrimitiveString                                      // String
	PrimitiveTimeMicrosNTZ                               // TimeNTZ(micros)
	PrimitiveTimestampNanos                              // Timestamp(nanos)
	PrimitiveTimestampNanosNTZ                           // TimestampNTZ(nanos)
	PrimitiveUUID                                        // UUID
)

func primitiveTypeFromHeader(hdr byte) PrimitiveType {
	return PrimitiveType((hdr >> basicTypeBits) & typeInfoMask)
}

type Type int

const (
	Object Type = iota
	Array
	Null
	Bool
	Int8
	Int16
	Int32
	Int64
	String
	Double
	Decimal4
	Decimal8
	Decimal16
	Date
	TimestampMicros
	TimestampMicrosNTZ
	Float
	Binary
	Time
	TimestampNanos
	TimestampNanosNTZ
	UUID
)

const (
	versionMask        uint8 = 0x0F
	sortedStrMask      uint8 = 0b10000
	basicTypeMask      uint8 = 0x3
	basicTypeBits      uint8 = 2
	typeInfoMask       uint8 = 0x3F
	hdrSizeBytes             = 1
	minOffsetSizeBytes       = 1
	maxOffsetSizeBytes       = 4

	// mask is applied after shift
	offsetSizeMask     uint8 = 0b11
	offsetSizeBitShift uint8 = 6
	supportedVersion         = 1
	maxShortStringSize       = 0x3F
	maxSizeLimit             = 128 * 1024 * 1024 // 128MB
)

var (
	EmptyMetadataBytes = [3]byte{0x1, 0, 0}
)

type Metadata struct {
	data []byte
	keys [][]byte
}

func NewMetadata(data []byte) (Metadata, error) {
	m := Metadata{data: data}
	if len(data) < hdrSizeBytes+minOffsetSizeBytes*2 {
		return m, fmt.Errorf("invalid variant metadata: too short: size=%d", len(data))
	}

	if m.Version() != supportedVersion {
		return m, fmt.Errorf("invalid variant metadata: unsupported version: %d", m.Version())
	}

	offsetSz := m.OffsetSize()
	if offsetSz < minOffsetSizeBytes || offsetSz > maxOffsetSizeBytes {
		return m, fmt.Errorf("invalid variant metadata: invalid offset size: %d", offsetSz)
	}

	dictSize, err := m.loadDictionary(offsetSz)
	if err != nil {
		return m, err
	}

	if hdrSizeBytes+int(dictSize+1)*int(offsetSz) > len(m.data) {
		return m, fmt.Errorf("invalid variant metadata: offset out of range: %d > %d",
			(dictSize+hdrSizeBytes)*uint32(offsetSz), len(m.data))
	}

	return m, nil
}

func (m *Metadata) Clone() Metadata {
	return Metadata{
		data: bytes.Clone(m.data),
		// shallow copy of the values, but the slice is copied
		// more efficient, and nothing should be mutating the keys
		// so it's probably safe, but something we should keep in mind
		keys: slices.Clone(m.keys),
	}
}

func (m *Metadata) loadDictionary(offsetSz uint8) (uint32, error) {
	if int(offsetSz+hdrSizeBytes) > len(m.data) {
		return 0, errors.New("invalid variant metadata: too short for dictionary size")
	}

	dictSize := readLEU32(m.data[hdrSizeBytes : hdrSizeBytes+offsetSz])
	m.keys = make([][]byte, dictSize)

	if dictSize == 0 {
		return 0, nil
	}

	// first offset is always 0
	offsetStart, offsetPos := uint32(0), hdrSizeBytes+offsetSz
	valuesStart := hdrSizeBytes + (dictSize+2)*uint32(offsetSz)
	for i := range dictSize {
		offsetPos += offsetSz
		end := readLEU32(m.data[offsetPos : offsetPos+offsetSz])

		keySize := end - offsetStart
		valStart := valuesStart + offsetStart
		if valStart+keySize > uint32(len(m.data)) {
			return 0, fmt.Errorf("invalid variant metadata: string data out of range: %d + %d > %d",
				valStart, keySize, len(m.data))
		}
		m.keys[i] = m.data[valStart : valStart+keySize]
		offsetStart += keySize
	}

	return dictSize, nil
}

func (m Metadata) Bytes() []byte { return m.data }

func (m Metadata) Version() uint8        { return m.data[0] & versionMask }
func (m Metadata) SortedAndUnique() bool { return m.data[0]&sortedStrMask != 0 }
func (m Metadata) OffsetSize() uint8 {
	return ((m.data[0] >> offsetSizeBitShift) & offsetSizeMask) + 1
}

func (m Metadata) DictionarySize() uint32 { return uint32(len(m.keys)) }

func (m Metadata) KeyAt(id uint32) (string, error) {
	if id >= uint32(len(m.keys)) {
		return "", fmt.Errorf("invalid variant metadata: id out of range: %d >= %d",
			id, len(m.keys))
	}

	return unsafe.String(&m.keys[id][0], len(m.keys[id])), nil
}

func (m Metadata) IdFor(key string) []uint32 {
	k := unsafe.Slice(unsafe.StringData(key), len(key))

	var ret []uint32
	if m.SortedAndUnique() {
		idx, found := slices.BinarySearchFunc(m.keys, k, bytes.Compare)
		if found {
			ret = append(ret, uint32(idx))
		}

		return ret
	}

	for i, k := range m.keys {
		if bytes.Equal(k, k) {
			ret = append(ret, uint32(i))
		}
	}

	return ret
}

type DecimalValue[T decimal.DecimalTypes] struct {
	Scale uint8
	Value decimal.Num[T]
}

func (v DecimalValue[T]) MarshalJSON() ([]byte, error) {
	return []byte(v.Value.ToString(int32(v.Scale))), nil
}

type ArrayValue struct {
	value []byte
	meta  Metadata

	numElements uint32
	dataStart   uint32
	offsetSize  uint8
	offsetStart uint8
}

func (v ArrayValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Values())
}

func (v ArrayValue) NumElements() uint32 { return v.numElements }

func (v ArrayValue) Values() []Value {
	values := make([]Value, v.numElements)
	for i := range v.numElements {
		idx := uint32(v.offsetStart) + i*uint32(v.offsetSize)
		offset := readLEU32(v.value[idx : idx+uint32(v.offsetSize)])
		values[i] = Value{value: v.value[v.dataStart+offset:], meta: v.meta}
	}
	return values
}

func (v ArrayValue) Value(i uint32) (Value, error) {
	if i >= v.numElements {
		return Value{}, fmt.Errorf("%w: invalid array value: index out of range: %d >= %d",
			arrow.ErrIndex, i, v.numElements)
	}

	idx := uint32(v.offsetStart) + i*uint32(v.offsetSize)
	offset := readLEU32(v.value[idx : idx+uint32(v.offsetSize)])

	return Value{meta: v.meta, value: v.value[v.dataStart+offset:]}, nil
}

type ObjectValue struct {
	value []byte
	meta  Metadata

	numElements uint32
	offsetStart uint32
	dataStart   uint32
	idSize      uint8
	offsetSize  uint8
	idStart     uint8
}

type ObjectField struct {
	Key   string
	Value Value
}

func (v ObjectValue) NumElements() uint32 { return v.numElements }
func (v ObjectValue) ValueByKey(key string) (ObjectField, error) {
	n := v.numElements

	// if total list size is smaller than threshold, linear search will
	// likely be faster than a binary search
	const binarySearchThreshold = 32
	if n < binarySearchThreshold {
		for i := range n {
			idx := uint32(v.idStart) + i*uint32(v.idSize)
			id := readLEU32(v.value[idx : idx+uint32(v.idSize)])
			k, err := v.meta.KeyAt(id)
			if err != nil {
				return ObjectField{}, fmt.Errorf("invalid object value: fieldID at idx %d is not in metadata", idx)
			}
			if k == key {
				idx := uint32(v.offsetStart) + uint32(v.offsetSize)*i
				offset := readLEU32(v.value[idx : idx+uint32(v.offsetSize)])
				return ObjectField{
					Key:   key,
					Value: Value{value: v.value[v.dataStart+offset:], meta: v.meta}}, nil
			}
		}
		return ObjectField{}, arrow.ErrNotFound
	}

	i, j := uint32(0), n
	for i < j {
		mid := (i + j) >> 1
		idx := uint32(v.idStart) + mid*uint32(v.idSize)
		id := readLEU32(v.value[idx : idx+uint32(v.idSize)])
		k, err := v.meta.KeyAt(id)
		if err != nil {
			return ObjectField{}, fmt.Errorf("invalid object value: fieldID at idx %d is not in metadata", idx)
		}

		switch strings.Compare(k, key) {
		case -1:
			i = mid + 1
		case 0:
			idx := uint32(v.offsetStart) + uint32(v.offsetSize)*mid
			offset := readLEU32(v.value[idx : idx+uint32(v.offsetSize)])

			return ObjectField{
				Key:   key,
				Value: Value{value: v.value[v.dataStart+offset:], meta: v.meta}}, nil
		case 1:
			j = mid - 1
		}
	}

	return ObjectField{}, arrow.ErrNotFound
}

func (v ObjectValue) FieldAt(i uint32) (ObjectField, error) {
	if i >= v.numElements {
		return ObjectField{}, fmt.Errorf("%w: invalid object value: index out of range: %d >= %d",
			arrow.ErrIndex, i, v.numElements)
	}

	idx := uint32(v.idStart) + i*uint32(v.idSize)
	id := readLEU32(v.value[idx : idx+uint32(v.idSize)])
	k, err := v.meta.KeyAt(id)
	if err != nil {
		return ObjectField{}, fmt.Errorf("invalid object value: fieldID at idx %d is not in metadata", idx)
	}

	offsetIdx := uint32(v.offsetStart) + i*uint32(v.offsetSize)
	offset := readLEU32(v.value[offsetIdx : offsetIdx+uint32(v.offsetSize)])

	return ObjectField{
		Key:   k,
		Value: Value{value: v.value[v.dataStart+offset:], meta: v.meta}}, nil
}

func (v ObjectValue) Values() iter.Seq2[string, Value] {
	return func(yield func(string, Value) bool) {
		for i := range v.numElements {
			idx := uint32(v.idStart) + i*uint32(v.idSize)
			id := readLEU32(v.value[idx : idx+uint32(v.idSize)])
			k, err := v.meta.KeyAt(id)
			if err != nil {
				return
			}

			offsetIdx := uint32(v.offsetStart) + i*uint32(v.offsetSize)
			offset := readLEU32(v.value[offsetIdx : offsetIdx+uint32(v.offsetSize)])

			if !yield(k, Value{value: v.value[v.dataStart+offset:], meta: v.meta}) {
				return
			}
		}
	}
}

func (v ObjectValue) MarshalJSON() ([]byte, error) {
	// for now we'll use a naive approach and just build a map
	// then marshal it. This is not the most efficient way to do this
	// but it is the simplest and most straightforward.
	mapping := make(map[string]Value)
	maps.Insert(mapping, v.Values())
	return json.Marshal(mapping)
}

type Value struct {
	value []byte
	meta  Metadata
}

func NewWithMetadata(meta Metadata, value []byte) (Value, error) {
	if len(value) == 0 {
		return Value{}, errors.New("invalid variant value: empty")
	}

	return Value{value: value, meta: meta}, nil
}

func New(meta, value []byte) (Value, error) {
	m, err := NewMetadata(meta)
	if err != nil {
		return Value{}, err
	}

	return NewWithMetadata(m, value)
}

func (v Value) Bytes() []byte { return v.value }

func (v Value) Clone() Value { return Value{value: bytes.Clone(v.value)} }

func (v Value) Metadata() Metadata { return v.meta }

func (v Value) BasicType() BasicType {
	return basicTypeFromHeader(v.value[0])
}

func (v Value) Type() Type {
	switch t := v.BasicType(); t {
	case BasicPrimitive:
		switch primType := primitiveTypeFromHeader(v.value[0]); primType {
		case PrimitiveNull:
			return Null
		case PrimitiveBoolTrue, PrimitiveBoolFalse:
			return Bool
		case PrimitiveInt8:
			return Int8
		case PrimitiveInt16:
			return Int16
		case PrimitiveInt32:
			return Int32
		case PrimitiveInt64:
			return Int64
		case PrimitiveDouble:
			return Double
		case PrimitiveDecimal4:
			return Decimal4
		case PrimitiveDecimal8:
			return Decimal8
		case PrimitiveDecimal16:
			return Decimal16
		case PrimitiveDate:
			return Date
		case PrimitiveTimestampMicros:
			return TimestampMicros
		case PrimitiveTimestampMicrosNTZ:
			return TimestampMicrosNTZ
		case PrimitiveFloat:
			return Float
		case PrimitiveBinary:
			return Binary
		case PrimitiveString:
			return String
		case PrimitiveTimeMicrosNTZ:
			return Time
		case PrimitiveTimestampNanos:
			return TimestampNanos
		case PrimitiveTimestampNanosNTZ:
			return TimestampNanosNTZ
		case PrimitiveUUID:
			return UUID
		default:
			panic(fmt.Errorf("invalid primitive type found: %d", primType))
		}
	case BasicShortString:
		return String
	case BasicObject:
		return Object
	case BasicArray:
		return Array
	default:
		panic(fmt.Errorf("invalid basic type found: %d", t))
	}
}

func (v Value) Value() any {
	switch t := v.BasicType(); t {
	case BasicPrimitive:
		switch primType := primitiveTypeFromHeader(v.value[0]); primType {
		case PrimitiveNull:
			return nil
		case PrimitiveBoolTrue:
			return true
		case PrimitiveBoolFalse:
			return false
		case PrimitiveInt8:
			return readExact[int8](v.value[1:])
		case PrimitiveInt16:
			return readExact[int16](v.value[1:])
		case PrimitiveInt32:
			return readExact[int32](v.value[1:])
		case PrimitiveInt64:
			return readExact[int64](v.value[1:])
		case PrimitiveDouble:
			return readExact[float64](v.value[1:])
		case PrimitiveFloat:
			return readExact[float32](v.value[1:])
		case PrimitiveDate:
			return arrow.Date32(readExact[int32](v.value[1:]))
		case PrimitiveTimestampMicros, PrimitiveTimestampMicrosNTZ,
			PrimitiveTimestampNanos, PrimitiveTimestampNanosNTZ:
			return arrow.Timestamp(readExact[int64](v.value[1:]))
		case PrimitiveTimeMicrosNTZ:
			return arrow.Time32(readExact[int32](v.value[1:]))
		case PrimitiveUUID:
			debug.Assert(len(v.value[1:]) == 16, "invalid UUID length")
			return uuid.Must(uuid.FromBytes(v.value[1:]))
		case PrimitiveBinary:
			sz := binary.LittleEndian.Uint32(v.value[1:5])
			return v.value[5 : 5+sz]
		case PrimitiveString:
			sz := binary.LittleEndian.Uint32(v.value[1:5])
			return unsafe.String(&v.value[5], sz)
		case PrimitiveDecimal4:
			scale := uint8(v.value[1])
			val := decimal.Decimal32(readExact[int32](v.value[2:]))
			return DecimalValue[decimal.Decimal32]{Scale: scale, Value: val}
		case PrimitiveDecimal8:
			scale := uint8(v.value[1])
			val := decimal.Decimal64(readExact[int64](v.value[2:]))
			return DecimalValue[decimal.Decimal64]{Scale: scale, Value: val}
		case PrimitiveDecimal16:
			scale := uint8(v.value[1])
			lowBits := readLEU64(v.value[2:10])
			highBits := readExact[int64](v.value[10:])
			return DecimalValue[decimal.Decimal128]{
				Scale: scale,
				Value: decimal128.New(highBits, lowBits),
			}
		}
	case BasicShortString:
		sz := int(v.value[0] >> 2)
		return unsafe.String(&v.value[1], sz)
	case BasicObject:
		valueHdr := (v.value[0] >> basicTypeBits)
		fieldOffsetSz := (valueHdr & 0b11) + 1
		fieldIdSz := ((valueHdr >> 2) & 0b11) + 1
		isLarge := ((valueHdr >> 4) & 0b1) == 1

		var nelemSize uint8 = 1
		if isLarge {
			nelemSize = 4
		}

		debug.Assert(len(v.value) >= int(1+nelemSize), "invalid object value: too short")
		numElements := readLEU32(v.value[1 : 1+nelemSize])
		idStart := uint32(1 + nelemSize)
		offsetStart := idStart + numElements*uint32(fieldIdSz)
		dataStart := offsetStart + (numElements+1)*uint32(fieldOffsetSz)

		debug.Assert(dataStart <= uint32(len(v.value)), "invalid object value: dataStart out of range")
		return ObjectValue{
			value:       v.value,
			meta:        v.meta,
			numElements: numElements,
			offsetStart: offsetStart,
			dataStart:   dataStart,
			idSize:      fieldIdSz,
			offsetSize:  fieldOffsetSz,
			idStart:     uint8(idStart),
		}
	case BasicArray:
		valueHdr := (v.value[0] >> basicTypeBits)
		fieldOffsetSz := (valueHdr & 0b11) + 1
		isLarge := (valueHdr & 0b1) == 1

		var (
			sz                     int
			offsetStart, dataStart int
		)

		if isLarge {
			sz, offsetStart = int(readLEU32(v.value[1:5])), 5
		} else {
			sz, offsetStart = int(v.value[1]), 2
		}

		dataStart = offsetStart + (sz+1)*int(fieldOffsetSz)
		debug.Assert(dataStart <= len(v.value), "invalid array value: dataStart out of range")
		return ArrayValue{
			value:       v.value,
			meta:        v.meta,
			numElements: uint32(sz),
			dataStart:   uint32(dataStart),
			offsetSize:  fieldOffsetSz,
			offsetStart: uint8(offsetStart),
		}
	}

	debug.Assert(false, "unsupported type")
	return nil
}

func (v Value) MarshalJSON() ([]byte, error) {
	result := v.Value()
	switch t := result.(type) {
	case arrow.Date32:
		result = t.FormattedString()
	case arrow.Timestamp:
		switch primType := primitiveTypeFromHeader(v.value[0]); primType {
		case PrimitiveTimestampMicros:
			result = t.ToTime(arrow.Microsecond).Format("2006-01-02 15:04:05.999999Z0700")
		case PrimitiveTimestampMicrosNTZ:
			result = t.ToTime(arrow.Microsecond).In(time.Local).Format("2006-01-02 15:04:05.999999Z0700")
		case PrimitiveTimestampNanos:
			result = t.ToTime(arrow.Nanosecond).Format("2006-01-02 15:04:05.999999999Z0700")
		case PrimitiveTimestampNanosNTZ:
			result = t.ToTime(arrow.Nanosecond).In(time.Local).Format("2006-01-02 15:04:05.999999999Z0700")
		}
	case arrow.Time32:
		result = t.ToTime(arrow.Microsecond).In(time.Local).Format("15:04:05.999999Z0700")
	}

	return json.Marshal(result)
}
