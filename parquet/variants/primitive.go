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

package variants

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
)

// Variant primitive type IDs.
type primitiveType int

const (
	primitiveInvalid            primitiveType = -1
	primitiveNull               primitiveType = 0
	primitiveTrue               primitiveType = 1
	primitiveFalse              primitiveType = 2
	primitiveInt8               primitiveType = 3
	primitiveInt16              primitiveType = 4
	primitiveInt32              primitiveType = 5
	primitiveInt64              primitiveType = 6
	primitiveDouble             primitiveType = 7
	primitiveDecimal4           primitiveType = 8  // TODO
	primitiveDecimal8           primitiveType = 9  // TODO
	primitiveDecimal16          primitiveType = 10 // TODO
	primitiveDate               primitiveType = 11
	primitiveTimestampMicros    primitiveType = 12
	primitiveTimestampNTZMicros primitiveType = 13
	primitiveFloat              primitiveType = 14
	primitiveBinary             primitiveType = 15
	primitiveString             primitiveType = 16
	primitiveTimeNTZ            primitiveType = 17
	primitiveTimestampNanos     primitiveType = 18
	primitiveTimestampNTZNanos  primitiveType = 19
	primitiveUUID               primitiveType = 20
)

func (pt primitiveType) String() string {
	switch pt {
	case primitiveNull:
		return "Null"
	case primitiveFalse, primitiveTrue:
		return "Boolean"
	case primitiveInt8:
		return "Int8"
	case primitiveInt16:
		return "Int16"
	case primitiveInt32:
		return "Int32"
	case primitiveInt64:
		return "Int64"
	case primitiveDouble:
		return "Double"
	case primitiveDecimal4:
		return "Decimal4"
	case primitiveDecimal8:
		return "Decimal8"
	case primitiveDecimal16:
		return "Decimal16"
	case primitiveDate:
		return "Date"
	case primitiveTimestampMicros:
		return "Timestamp(micros)"
	case primitiveTimestampNTZMicros:
		return "TimestampNTZ(micros)"
	case primitiveFloat:
		return "Float"
	case primitiveBinary:
		return "Binary"
	case primitiveString:
		return "String"
	case primitiveTimeNTZ:
		return "TimeNTZ"
	case primitiveTimestampNanos:
		return "Timestamp(nanos)"
	case primitiveTimestampNTZNanos:
		return "TimestampNTZ(nanos)"
	case primitiveUUID:
		return "UUID"
	}
	return "Invalid"
}

func validPrimitiveValue(prim primitiveType) error {
	if prim < primitiveNull || prim > primitiveUUID {
		return fmt.Errorf("invalid primitive type: %d", prim)
	}
	return nil
}

func primitiveFromHeader(hdr byte) (primitiveType, error) {
	// Special case the basic type of Short String and call it a Primitive String.
	bt := BasicTypeFromHeader(hdr)
	if bt == BasicShortString {
		return primitiveString, nil
	} else if bt == BasicPrimitive {
		prim := primitiveType(hdr >> 2)
		if err := validPrimitiveValue(prim); err != nil {
			return primitiveInvalid, err
		}
		return prim, nil
	}
	return primitiveInvalid, fmt.Errorf("header is not of a primitive or short string basic type: %s", bt)
}

func primitiveHeader(prim primitiveType) (byte, error) {
	if err := validPrimitiveValue(prim); err != nil {
		return 0, err
	}
	hdr := byte(prim << 2)
	hdr |= byte(BasicPrimitive)
	return hdr, nil
}

// marshalPrimitive takes in a primitive value, asserts its type, then marshals the data according to the Variant spec
// into the provided writer, returning the number of bytes written.
//
// Time can be provided in various ways- either by a time.Time struct, or by an int64 when the EncodeAs{Date,Time,Timestamp}
// options are provided. By default, timestamps are written as microseconds- to use nanoseconds pass in EncodeTimeAsNanos.
// Timezone information can be determined from a time.Time struct. Otherwise, by default, timestamps will be written with
// local timezone set.
func marshalPrimitive(v any, w io.Writer, opts ...MarshalOpts) (int, error) {
	var allOpts MarshalOpts
	for _, o := range opts {
		allOpts |= o
	}
	switch val := v.(type) {
	case bool:
		return marshalBoolean(val, w), nil
	case int:
		return marshalInt(int64(val), w), nil
	case int8:
		return marshalInt(int64(val), w), nil
	case int16:
		return marshalInt(int64(val), w), nil
	case int32:
		return marshalInt(int64(val), w), nil
	case int64:
		if allOpts&MarshalAsTime != 0 {
			encodeTimestamp(val, allOpts&MarshalTimeNanos != 0, allOpts&MarshalTimeNTZ != 0, w)
		}
		return marshalInt(val, w), nil
	case float32:
		return marshalFloat(val, w), nil
	case float64:
		return marshalDouble(val, w), nil
	case string:
		if allOpts&MarshalAsUUID != 0 {
			return marshalUUID([]byte(val), w), nil
		}
		return marshalString(val, w), nil
	case []byte:
		if allOpts&MarshalAsUUID != 0 {
			return marshalUUID([]byte(val), w), nil
		}
		return marshalBinary(val, w), nil
	case time.Time:
		if allOpts&MarshalAsDate != 0 {
			return marshalDate(val, w), nil
		}
		return marshalTimestamp(val, allOpts&MarshalTimeNanos != 0, w), nil
	}
	if v == nil {
		return marshalNull(w), nil
	}
	return -1, fmt.Errorf("unsupported primitive type")
}

// unmarshals a primitive (or a short string) into dest. dest must be a non-nil pointer to variable that is
// compatible with the Variant value to decode. Some conversions can take place:
//   - Integer values: Higher widths can be decoded into smaller widths so long as they don't overflow. Also,
//     integral values can be decoded into floats (also so long as they don't overflow).
//   - Time/timestamps: Can be decoded into either int64 or time.Time, the latter of which will carry time zone information
//   - Strings and binary: Can be decoded into either string or []byte
//
// If the Variant primitive is of the Null type, dest will be set to its zero value.
func unmarshalPrimitive(raw []byte, offset int, destPtr reflect.Value) error {
	dest := destPtr.Elem()
	kind := dest.Kind()

	if err := checkBounds(raw, offset, offset); err != nil {
		return err
	}

	prim, err := primitiveFromHeader(raw[offset])
	if err != nil {
		return err
	}

	switch prim {
	case primitiveNull:
		dest.Set(reflect.Zero(dest.Type()))
	case primitiveTrue, primitiveFalse:
		if kind != reflect.Bool && kind != reflect.Interface {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
		dest.Set(reflect.ValueOf(prim == primitiveTrue))
	case primitiveInt8, primitiveInt16, primitiveInt32, primitiveInt64:
		iv, err := decodeIntPhysical(raw, offset)
		if err != nil {
			return err
		}
		if kind == reflect.Interface {
			dest.Set(reflect.ValueOf(iv))
		} else if dest.CanInt() {
			if dest.OverflowInt(iv) {
				return fmt.Errorf("int value of %d will overflow dest", iv)
			}
			switch kind {
			case reflect.Int:
				dest.Set(reflect.ValueOf(int(iv)))
			case reflect.Int8:
				dest.Set(reflect.ValueOf(int8(iv)))
			case reflect.Int16:
				dest.Set(reflect.ValueOf(int16(iv)))
			case reflect.Int32:
				dest.Set(reflect.ValueOf(int32(iv)))
			case reflect.Int64:
				dest.Set(reflect.ValueOf(iv))
			default:
				panic("unhandled int value")
			}
		} else if dest.CanFloat() {
			// Converting from an int64 to a float64 can potentially lose precision, but it's still a valid
			// conversion and can be supported here.
			fv := float64(iv)
			if dest.OverflowFloat(fv) {
				return fmt.Errorf("value of %d will overflow dest", iv)
			}
			switch kind {
			case reflect.Float32:
				dest.Set(reflect.ValueOf(float32(fv)))
			case reflect.Float64:
				dest.Set(reflect.ValueOf(fv))
			default:
				panic("unhandled float value")
			}
		} else {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
	case primitiveFloat:
		fv, err := unmarshalFloat(raw, offset)
		if err != nil {
			return err
		}
		if !dest.CanFloat() && kind != reflect.Interface {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
		switch kind {
		case reflect.Float32, reflect.Interface:
			dest.Set(reflect.ValueOf(fv))
		case reflect.Float64:
			dest.Set(reflect.ValueOf(float64(fv)))
		default:
			panic("unhandled float value")
		}
	case primitiveDouble:
		dv, err := unmarshalDouble(raw, offset)
		if err != nil {
			return err
		}
		if !dest.CanFloat() && kind != reflect.Interface {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
		switch kind {
		case reflect.Float32:
			if dest.OverflowFloat(dv) {
				return fmt.Errorf("value of %f will overflow dest", dv)
			}
			dest.Set(reflect.ValueOf(float32(dv)))
		case reflect.Float64, reflect.Interface:
			dest.Set(reflect.ValueOf(dv))
		default:
			panic("unhandled float value")
		}
	case primitiveTimeNTZ, primitiveTimestampMicros, primitiveTimestampNTZMicros,
		primitiveTimestampNanos, primitiveTimestampNTZNanos:
		tsv, err := readUint(raw, offset+1, 8)
		if err != nil {
			return err
		}

		// Time can be decoded into either an int64 (the physical time), or into a time.Time struct.
		// Anything else is invalid.
		if kind == reflect.Int64 || kind == reflect.Interface {
			dest.Set(reflect.ValueOf(int64(tsv)))
		} else if kind == reflect.Uint64 {
			dest.Set(reflect.ValueOf(tsv))
		} else if dest.Type() == reflect.TypeOf(time.Time{}) {
			var t time.Time
			if prim == primitiveTimeNTZ {
				// TimeNTZ for Variants is UTC=false (ie. local timezone) and in microseconds
				t = time.Date(0, 0, 0, 0, 0, 0, 1000*int(tsv), time.Local)
			} else {
				if prim == primitiveTimestampMicros || prim == primitiveTimestampNTZMicros {
					t = time.UnixMicro(int64(tsv))
				} else {
					sec := int64(tsv / 1e9)
					nsec := int64(tsv % 1e9)
					t = time.Unix(sec, nsec)
				}
				if prim == primitiveTimestampMicros || prim == primitiveTimestampNanos {
					t = t.In(time.Local)
				}
			}
			dest.Set(reflect.ValueOf(t))
		} else {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
	case primitiveString:
		str, err := unmarshalString(raw, offset)
		if err != nil {
			return err
		}
		if kind == reflect.String || kind == reflect.Interface {
			dest.Set(reflect.ValueOf(str))
		} else if kind == reflect.Slice && dest.Type().Elem().Kind() == reflect.Uint8 {
			dest.Set(reflect.ValueOf([]byte(str)))
		} else {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
	case primitiveBinary:
		bytes, err := unmarshalBinary(raw, offset)
		if err != nil {
			return err
		}
		if kind == reflect.Slice && dest.Type().Elem().Kind() == reflect.Uint8 || kind == reflect.Interface {
			dest.Set(reflect.ValueOf(bytes))
		} else if kind == reflect.String {
			dest.Set(reflect.ValueOf(string(bytes)))
		} else {
			return fmt.Errorf("cannot decode Variant value of %s into dest %s", prim, kind)
		}
	case primitiveUUID:
		bytes, err := unmarshalUUID(raw, offset)
		if err != nil {
			return err
		}
		if kind == reflect.Slice && dest.Type().Elem().Kind() == reflect.Uint8 || kind == reflect.Interface {
			dest.Set(reflect.ValueOf(bytes))
		} else if kind == reflect.String {
			dest.Set(reflect.ValueOf(string(bytes)))
		} else {
			return fmt.Errorf("cannot decode Variant UUID into dest %s", kind)
		}
	default:
		return fmt.Errorf("unknown primitive: %s", prim)
	}

	return nil
}

func marshalNull(w io.Writer) int {
	hdr, _ := primitiveHeader(primitiveNull)
	w.Write([]byte{hdr})
	return 1
}

func marshalBoolean(b bool, w io.Writer) int {
	var hdr byte
	if b {
		hdr, _ = primitiveHeader(primitiveTrue)
	} else {
		hdr, _ = primitiveHeader(primitiveFalse)
	}
	w.Write([]byte{hdr})
	return 1
}

func unmarshalBoolean(raw []byte, offset int) (bool, error) {
	prim, err := primitiveFromHeader(raw[offset])
	if err != nil {
		return false, err
	}
	return prim == primitiveTrue, nil
}

// Encodes an integer with the appropriate primitive header. This encodes the int
// into the minimal space necessary regardless of the width that's passed in (eg. an
// int64 of value 1 will be encoded into an int8)
func marshalInt(val int64, w io.Writer) int {
	var hdr byte
	var size int
	if val < math.MaxInt8 && val > math.MinInt8 {
		hdr, _ = primitiveHeader(primitiveInt8)
		size = 1
	} else if val < math.MaxInt16 && val > math.MinInt16 {
		hdr, _ = primitiveHeader(primitiveInt16)
		size = 2
	} else if val < math.MaxInt32 && val > math.MinInt32 {
		hdr, _ = primitiveHeader(primitiveInt32)
		size = 4
	} else {
		hdr, _ = primitiveHeader(primitiveInt64)
		size = 8
	}
	w.Write([]byte{hdr})
	encodeNumber(val, size, w)
	return size + 1
}

func decodeIntPhysical(raw []byte, offset int) (int64, error) {
	typ, _ := primitiveFromHeader(raw[offset])
	var size int
	switch typ {
	case primitiveInt8:
		size = 1
	case primitiveInt16:
		size = 2
	case primitiveInt32, primitiveDate:
		size = 4
	case primitiveInt64:
		size = 8
	default:
		return -1, fmt.Errorf("not an integral type: %s", typ)
	}
	val, err := readInt(raw, offset+1, size)
	if err != nil {
		return -1, err
	}

	// Do a conversion dance from the minimal width to int64 to catch
	// negative numbers.
	switch typ {
	case primitiveInt8:
		return int64(int8(val)), nil
	case primitiveInt16:
		return int64(int16(val)), nil
	case primitiveInt32:
		return int64(int32(val)), nil
	default:
		return int64(val), nil
	}
}

func marshalFloat(val float32, w io.Writer) int {
	buf := make([]byte, 5)
	hdr, _ := primitiveHeader(primitiveFloat)
	buf[0] = hdr
	bits := math.Float32bits(val)
	for i := range 4 {
		buf[i+1] = byte(bits)
		bits >>= 8
	}
	w.Write(buf)
	return 5
}

func marshalDouble(val float64, w io.Writer) int {
	buf := make([]byte, 9)
	hdr, _ := primitiveHeader(primitiveDouble)
	buf[0] = hdr
	bits := math.Float64bits(val)
	for i := range 8 {
		buf[i+1] = byte(bits)
		bits >>= 8
	}
	w.Write(buf)
	return 9
}

func unmarshalFloat(raw []byte, offset int) (float32, error) {
	v, err := readUint(raw, offset+1, 4)
	if err != nil {
		return -1, err
	}
	return math.Float32frombits(uint32(v)), nil
}

func unmarshalDouble(raw []byte, offset int) (float64, error) {
	v, err := readUint(raw, offset+1, 8)
	if err != nil {
		return -1, err
	}
	return math.Float64frombits(v), nil
}

func encodePrimitiveBytes(b []byte, w io.Writer) int {
	encodeNumber(int64(len(b)), 4, w)
	w.Write(b)
	return len(b) + 4
}

func marshalString(str string, w io.Writer) int {
	str = strings.ToValidUTF8(str, "\uFFFD")

	// If the string is 63 characters or less, encode this as a short string to save space.
	strlen := len(str)
	if strlen < 0x3F {
		hdr := byte(strlen << 2)
		hdr |= byte(BasicShortString)
		w.Write([]byte{hdr})
		w.Write([]byte(str))
		return 1 + strlen
	}

	// Otherwise, encode this as a basic string.
	hdr, _ := primitiveHeader(primitiveString)
	w.Write([]byte{hdr})
	return 1 + encodePrimitiveBytes([]byte(strings.ToValidUTF8(str, "\uFFFD")), w)
}

func marshalUUID(uuid []byte, w io.Writer) int {
	hdr, _ := primitiveHeader(primitiveUUID)
	w.Write([]byte{hdr})

	// A UUID is 16 bytes. Either pad or truncate to this length.
	if len(uuid) > 16 {
		uuid = uuid[:16]
	} else if pad := 16 - len(uuid); pad > 0 {
		uuid = append(uuid, make([]byte, pad)...)
	}
	w.Write(uuid)
	return 17
}

func unmarshalUUID(raw []byte, offset int) ([]byte, error) {
	if err := checkBounds(raw, offset, offset+17); err != nil {
		return nil, err
	}
	return raw[offset+1 : offset+17], nil
}

func unmarshalString(raw []byte, offset int) (string, error) {
	// Determine if the string is a short string, or a basic string.
	maxPos := len(raw)
	if offset >= maxPos {
		return "", fmt.Errorf("offset is out of bounds: trying to access position %d, max position is %d", offset, maxPos)
	}
	bt := BasicTypeFromHeader(raw[offset])

	if bt == BasicShortString {
		l := int(raw[offset] >> 2)
		endIdx := 1 + l + offset
		if endIdx > maxPos {
			return "", fmt.Errorf("end index is out of bounds: trying to access position %d, max position is %d", endIdx, maxPos)
		}
		return string(raw[offset+1 : endIdx]), nil
	}

	b, err := getBytes(raw, offset+1)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getBytes(raw []byte, offset int) ([]byte, error) {
	l, err := readUint(raw, offset, 4)
	if err != nil {
		return nil, fmt.Errorf("could not read length: %v", err)
	}
	maxIdx := offset + 4 + int(l)
	if len(raw) < maxIdx {
		return nil, fmt.Errorf("bytes are out of bounds")
	}
	return raw[offset+4 : maxIdx], nil
}

func marshalBinary(b []byte, w io.Writer) int {
	hdr, _ := primitiveHeader(primitiveBinary)
	w.Write([]byte{hdr})
	return 1 + encodePrimitiveBytes(b, w)
}

func unmarshalBinary(raw []byte, offset int) ([]byte, error) {
	return getBytes(raw, offset+1)
}

func marshalTimestamp(t time.Time, nanos bool, w io.Writer) int {
	var typ primitiveType
	var ts int64
	ntz := t.Location() == time.UTC
	if nanos {
		ts = t.UnixNano()
		if ntz {
			typ = primitiveTimestampNTZNanos
		} else {
			typ = primitiveTimestampNanos
		}
	} else {
		ts = t.UnixMicro()
		if ntz {
			typ = primitiveTimestampNTZMicros
		} else {
			typ = primitiveTimestampMicros
		}
	}
	hdr, _ := primitiveHeader(typ)
	w.Write([]byte{hdr})
	encodeNumber(ts, 8, w)
	return 9
}

func encodeTimestamp(t int64, nanos, ntz bool, w io.Writer) int {
	var typ primitiveType
	if nanos {
		if ntz {
			typ = primitiveTimestampNTZNanos
		} else {
			typ = primitiveTimestampNanos
		}
	} else {
		if ntz {
			typ = primitiveTimestampNTZMicros
		} else {
			typ = primitiveTimestampMicros
		}
	}
	hdr, _ := primitiveHeader(typ)
	w.Write([]byte{hdr})
	encodeNumber(t, 8, w)
	return 9
}

// func decodeTimestamp(raw []byte, offset int) (int64, error) {
// 	ts, err := readUint(raw, offset+1, 8)
// 	if err != nil {
// 		return -1, err
// 	}
// 	return int64(ts), nil
// }

func unmarshalTimestamp(raw []byte, offset int) (time.Time, error) {
	typ, _ := primitiveFromHeader(raw[offset])
	ts, err := readUint(raw, offset+1, 8)
	if err != nil {
		return time.Time{}, err
	}
	var ret time.Time
	if typ == primitiveTimestampMicros || typ == primitiveTimestampNTZMicros {
		ret = time.UnixMicro(int64(ts))
	} else {
		ret = time.Unix(0, int64(ts))
	}
	if typ == primitiveTimestampNTZMicros || typ == primitiveTimestampNTZNanos {
		ret = ret.UTC()
	} else {
		ret = ret.Local()
	}
	return ret, nil
}

func marshalDate(t time.Time, w io.Writer) int {
	epoch := time.Unix(0, 0)
	since := t.Sub(epoch)
	days := int64(since.Hours() / 24)
	hdr, _ := primitiveHeader(primitiveDate)
	w.Write([]byte{hdr})
	encodeNumber(days, 4, w)
	return 5
}

func unmarshalDate(raw []byte, offset int) (time.Time, error) {
	days, err := readUint(raw, offset+1, 4)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, 0).Add(time.Hour * 24 * time.Duration(days)), nil
}
