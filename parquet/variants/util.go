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
	"reflect"
	"time"
)

// Reads a little-endian encoded uint (betwen 1 and 8 bytes wide) from a raw buffer at a specified
// offset and returns its value. If any part of the read would be out of bounds, this returns an error.
func readUint(raw []byte, offset, size int) (uint64, error) {
	if size < 1 || size > 8 {
		return 0, fmt.Errorf("invalid size, must be in range [1,8]: %d", size)
	}
	if maxPos := offset + size; maxPos > len(raw) {
		return 0, fmt.Errorf("out of bounds: trying to access position %d, max position is %d", maxPos, len(raw))
	}
	var ret uint64
	for i := range size {
		ret |= uint64(raw[i+offset]) << (8 * i)
	}
	return ret, nil
}

// Reads a little-endian encoded integer (between 1 and 8 bytes wide) from a raw buffer at a specified offset.
func readInt(raw []byte, offset, size int) (int64, error) {
	u, err := readUint(raw, offset, size)
	if err != nil {
		return -1, err
	}
	return int64(u), nil
}

func fieldOffsetSize(maxSize int32) int {
	if maxSize < 0xFF {
		return 1
	} else if maxSize < 0xFFFF {
		return 2
	} else if maxSize < 0xFFFFFF {
		return 3
	}
	return 4
}

// Checks that a given range is in the provided raw buffer.
func checkBounds(raw []byte, low, high int) error {
	maxPos := len(raw)
	if low >= maxPos {
		return fmt.Errorf("out of bounds: trying to access position %d, max is %d", low, maxPos)
	}
	if high > maxPos {
		return fmt.Errorf("out of bounds: trying to access position %d, max is %d", high, maxPos)
	}
	return nil
}

// Encodes a number of a specified width in little-endian format and writes to a writer.
func encodeNumber(val int64, size int, w io.Writer) {
	buf := make([]byte, size)
	for i := range size {
		buf[i] = byte(val)
		val >>= 8
	}
	w.Write(buf)
}

func isLarge(numItems int) bool {
	return numItems > 0xFF
}

// Returns the basic type the passed in value should be encoded as, or undefined if it cannot be handled.
func kindFromValue(val any) BasicType {
	if val == nil {
		return BasicPrimitive
	}
	v := reflect.ValueOf(val)

	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Bool,
		reflect.String, reflect.Float32, reflect.Float64:
		return BasicPrimitive
	case reflect.Struct:
		// Time is considered a primitive. All other structs are objects.
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return BasicPrimitive
		}
		return BasicObject
	case reflect.Array, reflect.Slice:
		typ := v.Type()
		if typ.Elem().Kind() == reflect.Uint8 {
			return BasicPrimitive
		}
		return BasicArray
	case reflect.Map:
		// Only maps with string keys are supported.
		typ := v.Type()
		if typ.Key().Kind() == reflect.String {
			return BasicObject
		}
	}
	return BasicUndefined
}

// Returns the nth item (zero indexed) in a serialized list (ie. a serialized Array, or serialized Metadata).
// The offset should be the index of the first offset listing.
func readNthItem(raw []byte, offset, item, offsetSize, numElements int) ([]byte, error) {
	if err := checkBounds(raw, offset, offset); err != nil {
		return nil, err
	}

	if item > numElements {
		return nil, fmt.Errorf("item number is greater than number of elements (%d vs %d)", item, numElements)
	}

	// Calculate the range to return by getting the upper and lower bound of the item.
	lowerBound, err := readUint(raw, offset+item*offsetSize, offsetSize)
	if err != nil {
		return nil, err
	}
	upperBound, err := readUint(raw, offset+(item+1)*offsetSize, offsetSize)
	if err != nil {
		return nil, err
	}
	firstElemIdx := offset + (numElements+1)*offsetSize

	lowIdx := firstElemIdx + int(lowerBound)
	highIdx := firstElemIdx + int(upperBound)

	if err := checkBounds(raw, lowIdx, highIdx); err != nil {
		return nil, err
	}

	return raw[lowIdx:highIdx], nil
}
