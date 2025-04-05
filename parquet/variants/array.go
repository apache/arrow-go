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
	"bytes"
	"fmt"
	"io"
	"reflect"
)

type arrayBuilder struct {
	w          io.Writer
	buf        bytes.Buffer
	numItems   int
	offsets    []int32
	nextOffset int32
	doneCB     doneCB
	mdb        *metadataBuilder
	built      bool
}

func newArrayBuilder(w io.Writer, mdb *metadataBuilder, doneCB doneCB) *arrayBuilder {
	return &arrayBuilder{
		w:      w,
		doneCB: doneCB,
		mdb:    mdb,
	}
}

var _ ArrayBuilder = (*arrayBuilder)(nil)

// Write marshals the provided value into the appropriate Variant type and appends it to this array.
func (a *arrayBuilder) Write(val any, opts ...MarshalOpts) error {
	return writeCommon(val, &a.buf, a.mdb, a.recordOffset)
}

// Appends all elements from a provided slice into this array
func (a *arrayBuilder) fromSlice(sl any, opts ...MarshalOpts) error {
	val := reflect.ValueOf(sl)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		return fmt.Errorf("not a slice: %v", val.Kind())
	}
	for i := range val.Len() {
		item := val.Index(i)
		if err := a.Write(item.Interface(), opts...); err != nil {
			return err
		}
	}
	return nil
}

func (a *arrayBuilder) recordOffset(size int) {
	a.numItems++
	a.offsets = append(a.offsets, a.nextOffset)
	a.nextOffset += int32(size)
}

// Array returns a new ArrayBuilder associated with this array. The marshaled array
// will not be part of the array until the returned ArrayBuilder's Build method is called.
func (a *arrayBuilder) Array() ArrayBuilder {
	ab := newArrayBuilder(&a.buf, a.mdb, a.recordOffset)
	return ab
}

// Object returns a new ObjectBuilder associated with this array. The marshaled object
// will not be part of the array until the returned ObjectBuilder's Build() method is called.
func (a *arrayBuilder) Object() ObjectBuilder {
	ob := newObjectBuilder(&a.buf, a.mdb, a.recordOffset)
	return ob
}

// Build marshals an Array in Variant format and writes its header and value data to the
// underlying Writer. This prepends serialized metadata about the array (ie. its header, number
// of elements, and element offsets) to the runnig data buffer.
func (a *arrayBuilder) Build() error {
	if a.built {
		return errAlreadyBuilt
	}
	large := a.numItems > 0xFF
	offsetSize := fieldOffsetSize(a.nextOffset)

	// Preallocate a buffer for the header, number of items, and the field offsets.
	numItemsSize := 1
	if large {
		numItemsSize = 4
	}
	serializedOffsetSize := (a.numItems + 1) * offsetSize
	serializedDataBuf := bytes.NewBuffer(make([]byte, 0, 1+serializedOffsetSize))

	// Write the header and number of elements in the array
	serializedDataBuf.WriteByte(a.header(large, offsetSize))
	encodeNumber(int64(a.numItems), numItemsSize, serializedDataBuf)

	// Write all of the field offsets, including the final offset which is the first index after all
	// of the array's elements.
	for _, o := range a.offsets {
		encodeNumber(int64(o), offsetSize, serializedDataBuf)
	}
	encodeNumber(int64(a.nextOffset), offsetSize, serializedDataBuf)

	// Calculate the size of this entire array in bytes, then call the recordkeeping callback if configured.
	hdrSize, _ := a.w.Write(serializedDataBuf.Bytes())
	dataSize, _ := a.w.Write(a.buf.Bytes())
	totalSize := hdrSize + dataSize

	if a.doneCB != nil {
		a.doneCB(totalSize)
	}
	return nil
}

func (a *arrayBuilder) header(large bool, offsetSize int) byte {
	// Header is one byte: AAABCCDD
	//  * A: Unused
	//  * B: Is Large: whether there are more than 255 elements in this array or not
	//  * C: Field Offset Size Minus One: the number of bytes (minus one) used to encode each Field Offset
	//  * D: 0x03: the identifier of the Array basic type
	hdr := byte(offsetSize - 1)
	if large {
		hdr |= (1 << 2)
	}
	// Shift the value header over 2 to allow for the lower to bits to
	// denote the array basic type
	hdr <<= 2
	hdr |= byte(BasicArray)
	return hdr
}

type arrayData struct {
	size           int
	numElements    int
	firstOffsetIdx int
	firstDataIdx   int
	offsetWidth    int
}

// Parses array data from a marshaled object (where the different encoded sections start, plus size in bytes
// and number of elements). This also ensures that the entire array exists in the raw buffer.
func getArrayData(raw []byte, offset int) (*arrayData, error) {
	if err := checkBounds(raw, offset, offset); err != nil {
		return nil, err
	}
	hdr := raw[offset]
	if bt := BasicTypeFromHeader(hdr); bt != BasicArray {
		return nil, fmt.Errorf("not an array: %s", bt)
	}

	// Get the size of all encoded metadata fields. Bitshift by two to expose the 5 raw value header bits.
	hdr >>= 2

	offsetWidth := int(hdr&0x03) + 1
	numElementsWidth := 1
	if hdr&0x2 != 0 {
		numElementsWidth = 4
	}

	numElements, err := readUint(raw, offset+1, numElementsWidth)
	if err != nil {
		return nil, fmt.Errorf("could not get number of elements: %v", err)
	}
	firstOffsetIdx := offset + 1 + numElementsWidth // Header plus width of # of elements
	lastOffsetIdx := firstOffsetIdx + int(numElements)*offsetWidth
	firstDataIdx := lastOffsetIdx + offsetWidth

	// Do some bounds checking to ensure that the entire array is present in the raw buffer.
	lastDataOffset, err := readUint(raw, lastOffsetIdx, offsetWidth)
	if err != nil {
		return nil, fmt.Errorf("could not read last offset: %v", err)
	}
	lastDataIdx := firstDataIdx + int(lastDataOffset)
	if err := checkBounds(raw, offset, lastDataIdx); err != nil {
		return nil, fmt.Errorf("array is out of bounds: %v", err)
	}

	return &arrayData{
		size:           lastDataIdx - offset,
		numElements:    int(numElements),
		firstOffsetIdx: firstOffsetIdx,
		firstDataIdx:   firstDataIdx,
		offsetWidth:    offsetWidth,
	}, nil
}

// Unmarshals a Variant array into the provided destination. The destination must be a pointer to either
// a slice, or to the "any" type (which is then populated with []any{}). Any passed in slice will be
// cleared before unmarshaling.
func unmarshalArray(raw []byte, md *decodedMetadata, offset int, dest reflect.Value) error {
	data, err := getArrayData(raw, offset)
	if err != nil {
		return err
	}

	if kind := dest.Kind(); kind != reflect.Pointer {
		return fmt.Errorf("invalid dest, must be non-nil pointer (got kind %s)", kind)
	}
	if dest.IsNil() {
		return fmt.Errorf("invalid dest, must be non-nil pointer")
	}

	destElem := dest.Elem()
	if destElem.Kind() != reflect.Slice && destElem.Kind() != reflect.Interface {
		return fmt.Errorf("invalid dest, must be a pointer to a slice (got pointer to %s)", destElem.Kind())
	}

	// Reset the slice.
	var ret reflect.Value
	if destElem.Kind() == reflect.Slice {
		ret = reflect.MakeSlice(destElem.Type(), 0, data.numElements)
	} else if destElem.Kind() == reflect.Interface {
		ret = reflect.MakeSlice(reflect.TypeOf([]any{}), 0, data.numElements)
	}

	// Iterate through all the elements in the encoded variant.
	for i := range data.numElements {
		elemOffset, err := readUint(raw, data.firstOffsetIdx+data.offsetWidth*i, data.offsetWidth)
		if err != nil {
			return err
		}
		dataIdx := int(elemOffset) + data.firstDataIdx
		if err := checkBounds(raw, dataIdx, dataIdx); err != nil {
			return err
		}

		// Unmarshal the element and append to the slice to return.
		newElemValue := reflect.New(ret.Type().Elem())
		if err := unmarshalCommon(raw, md, dataIdx, newElemValue); err != nil {
			return err
		}
		ret = reflect.Append(ret, newElemValue.Elem())
	}
	destElem.Set(ret)
	return nil
}
