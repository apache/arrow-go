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
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

// Container to keep track of the metadata for a given element in an object. This is mainly
// used so that it's possible to sort by key during build time (as required by spec) and
// not need to go doing a bunch of lookups to find field IDs and offsets.
type objectKey struct {
	fieldID int
	offset  int
	key     string
}

type objectBuilder struct {
	w          io.Writer
	buf        bytes.Buffer
	objKeys    []objectKey
	fieldIDs   map[int]struct{}
	maxFieldID int
	numItems   int
	offsets    []int
	nextOffset int
	doneCB     doneCB
	mdb        *metadataBuilder
	built      bool
}

var _ ObjectBuilder = (*objectBuilder)(nil)

func newObjectBuilder(w io.Writer, mdb *metadataBuilder, doneCB doneCB) *objectBuilder {
	return &objectBuilder{
		w:        w,
		mdb:      mdb,
		doneCB:   doneCB,
		fieldIDs: make(map[int]struct{}),
	}
}

// Write marshals the provided value into the appropriate Variant type and adds it to this object with
// the provided key. Keys must be unique per object (though nested objects may share the same key).
func (o *objectBuilder) Write(key string, val any, opts ...MarshalOpts) error {
	if err := o.checkKey(key); err != nil {
		return err
	}
	return writeCommon(val, &o.buf, o.mdb, func(size int) {
		o.record(key, size)
	})
}

// Extracts field info from a struct field, namely the key name and any options associated with the field.
// If the Variant key name is not present in the `variant` annotation, the struct's field name will be
// used as the key.
//
// This function assumes the field is exported.
func extractFieldInfo(field reflect.StructField) (string, []MarshalOpts) {
	var opts []MarshalOpts

	tag, ok := field.Tag.Lookup("variant")
	if !ok || tag == "" {
		return field.Name, nil
	}

	// Tag is of the form "key_name,comma,separated,flags"
	parts := strings.Split(tag, ",")
	if len(parts) == 1 {
		return tag, nil
	}

	keyName := parts[0]
	if keyName == "" {
		keyName = field.Name
	}

	for _, optStr := range parts[1:] {
		switch strings.ToLower(optStr) {
		case "nanos":
			opts = append(opts, MarshalTimeNanos)
		case "ntz":
			opts = append(opts, MarshalTimeNTZ)
		case "date":
			opts = append(opts, MarshalAsDate)
		case "time":
			opts = append(opts, MarshalAsTime)
		case "timestamp":
			opts = append(opts, MarshalAsTimestamp)
		case "uuid":
			opts = append(opts, MarshalAsUUID)
		}
	}

	return keyName, opts
}

// Creates an object from a struct. Key names are determined by either the struct's field name, or
// by a value in a `variant` field annotation (with the annotation taking precedence).
func (o *objectBuilder) fromStruct(st any) error {
	stVal := reflect.ValueOf(st)

	// Get the underlying struct if this is a pointer to one.
	if stVal.Kind() == reflect.Pointer {
		stVal = stVal.Elem()
	}
	if stVal.Kind() != reflect.Struct {
		return fmt.Errorf("not a struct: %s", stVal.Kind())
	}
	typ := stVal.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get the tag. If not present, use the field name.
		key, opts := extractFieldInfo(field)
		if err := o.Write(key, stVal.Field(i).Interface(), opts...); err != nil {
			return err
		}
	}
	return nil
}

// Creates an object from a string-keyed map.
func (o *objectBuilder) fromMap(m any) error {
	mapVal := reflect.ValueOf(m)

	// Make sure this is a map with string keys.
	if mapVal.Kind() != reflect.Map {
		return fmt.Errorf("not a map: %v", mapVal.Kind())
	}
	if keyKind := mapVal.Type().Key().Kind(); keyKind != reflect.String {
		return fmt.Errorf("map does not have string keys: %v", keyKind)
	}

	for _, keyVal := range mapVal.MapKeys() {
		valVal := mapVal.MapIndex(keyVal)
		if err := o.Write(keyVal.Interface().(string), valVal.Interface()); err != nil {
			return err
		}
	}
	return nil
}

// Keys within a given object must be unique
func (o *objectBuilder) checkKey(key string) error {
	fieldID, ok := o.mdb.KeyID(key)
	if ok {
		if _, ok := o.fieldIDs[fieldID]; ok {
			return fmt.Errorf("mutiple insertion of key %q in object", key)
		}
	}
	return nil
}

// Array returns a new ArrayBuilder associated with this object. The marshaled array will
// not be part of the object until the returned ArrayBuilder's Build method is called.
func (o *objectBuilder) Array(key string) (ArrayBuilder, error) {
	if err := o.checkKey(key); err != nil {
		return nil, err
	}
	ab := newArrayBuilder(&o.buf, o.mdb, func(size int) {
		o.record(key, size)
	})
	return ab, nil
}

// Object returns a new ObjectBuilder associated with this object. The marshaled object will
// not be part of this object until the returned ObjectBuilder's Build method is called.
//
// NB. A nested object can contain a key that also exists in the parent object.
func (o *objectBuilder) Object(key string) (ObjectBuilder, error) {
	if err := o.checkKey(key); err != nil {
		return nil, err
	}
	ob := newObjectBuilder(&o.buf, o.mdb, func(size int) {
		o.record(key, size)
	})
	return ob, nil
}

// Bookkeeping to record information about an elements key, offset, field ID, and
// to keep a running track of the number of items and the max field ID seen.
func (o *objectBuilder) record(key string, size int) {
	o.numItems++
	currOffset := o.nextOffset
	o.offsets = append(o.offsets, currOffset)
	o.nextOffset += size

	fieldID := o.mdb.Add(key)
	o.objKeys = append(o.objKeys, objectKey{
		fieldID: fieldID,
		offset:  currOffset,
		key:     key,
	})
	o.fieldIDs[fieldID] = struct{}{}

	if fieldID > o.maxFieldID {
		o.maxFieldID = fieldID
	}
}

// Build writes the marshaled object to the builders io.Writer. This prepends serialized
// metadata about the object (ie. its header, number of elements, sorted field IDs, and
// offsets) to the running data buffer.
func (o *objectBuilder) Build() error {
	if o.built {
		return errAlreadyBuilt
	}
	numItemsSize := 1
	if isLarge(o.numItems) {
		numItemsSize = 4
	}
	offsetSize := fieldOffsetSize(int32(o.nextOffset))
	fieldIDSize := fieldOffsetSize(int32(o.maxFieldID))

	// Sort the object keys as per the spec.
	sort.Slice(o.objKeys, func(i, j int) bool {
		return strings.Compare(o.objKeys[i].key, o.objKeys[j].key) < 0
	})

	// Preallocate a buffer for the header, number of items, field IDs, and field offsets
	serializedFieldIDSize := fieldIDSize * o.numItems
	serializedFieldOffsetSize := offsetSize * (o.numItems + 1)

	serializedDataBuf := bytes.NewBuffer(make([]byte, 0, 1+numItemsSize+serializedFieldIDSize+serializedFieldOffsetSize))
	serializedDataBuf.WriteByte(o.header(fieldIDSize, offsetSize))

	encodeNumber(int64(o.numItems), numItemsSize, serializedDataBuf)
	for _, k := range o.objKeys {
		encodeNumber(int64(k.fieldID), fieldIDSize, serializedDataBuf)
	}
	for _, k := range o.objKeys {
		encodeNumber(int64(k.offset), offsetSize, serializedDataBuf)
	}
	encodeNumber(int64(o.nextOffset), offsetSize, serializedDataBuf)

	// Write everything to the writer.
	hdrSize, _ := o.w.Write(serializedDataBuf.Bytes())
	dataSize, _ := o.w.Write(o.buf.Bytes())

	totalSize := hdrSize + dataSize
	if o.doneCB != nil {
		o.doneCB(totalSize)
	}
	return nil
}

func (o *objectBuilder) header(fieldIDSize, fieldOffsetSize int) byte {
	// Header is one byte: ABCCDDEE
	//  * A: Unused
	//  * B: Is Large: whether there are more than 255 elements in this object or not.
	//  * C: Field ID Size Minus One: the number of bytes (minus one) used to encode each Field ID
	//  * D: Field Offset Size Minus One: the number of bytes (minus one) used to encode each Field Offset
	//  * E: 0x02: the identifier of the Object basic type
	hdr := byte(fieldOffsetSize - 1)
	hdr |= byte((fieldIDSize - 1) << 2)
	if isLarge(o.numItems) {
		hdr |= byte(1 << 4)
	}

	// Basic type is the lower two bits of the header. Shift the Object specific bits over 2.
	hdr <<= 2
	hdr |= byte(BasicObject)
	return hdr
}

type objectData struct {
	size            int
	numElements     int
	firstFieldIDIdx int
	firstOffsetIdx  int
	firstDataIdx    int
	fieldIDWidth    int
	offsetWidth     int
}

// Parses object data from a marshaled object (where the different encoded sections start,
// plus size in bytes and number of elements), plus ensures that the entire object is present
// in the raw buffer.
func getObjectData(raw []byte, offset int) (*objectData, error) {
	if err := checkBounds(raw, offset, offset); err != nil {
		return nil, err
	}

	hdr := raw[offset]
	if bt := BasicTypeFromHeader(hdr); bt != BasicObject {
		return nil, fmt.Errorf("not an object: %s", bt)
	}

	// Get the size of all encoded metadata fields. Bitshift by two to expose the 5 raw value header bits.
	hdr >>= 2

	offsetWidth := int(hdr&0x03) + 1
	fieldIDWidth := int((hdr>>2)&0x03) + 1

	numElementsWidth := 1
	if hdr&0x08 != 0 {
		numElementsWidth = 4
	}

	numElements, err := readUint(raw, offset+1, numElementsWidth)
	if err != nil {
		return nil, fmt.Errorf("could not get number of elements: %v", err)
	}

	firstFieldIDIdx := offset + 1 + numElementsWidth // Header plus width of # of elements
	firstOffsetIdx := firstFieldIDIdx + int(numElements)*fieldIDWidth
	firstDataIdx := firstOffsetIdx + int(numElements+1)*offsetWidth
	lastDataOffset, err := readUint(raw, firstDataIdx-offsetWidth, offsetWidth)
	if err != nil {
		return nil, fmt.Errorf("could not read last offset: %v", err)
	}
	lastDataIdx := firstDataIdx + int(lastDataOffset)

	// Also do some bounds checking to ensure that the entire object is represented in the raw buffer.
	if err := checkBounds(raw, offset, int(lastDataIdx)); err != nil {
		return nil, fmt.Errorf("object is out of bounds: %v", err)
	}
	return &objectData{
		size:            lastDataIdx - offset,
		numElements:     int(numElements),
		firstFieldIDIdx: firstFieldIDIdx,
		firstOffsetIdx:  firstOffsetIdx,
		firstDataIdx:    firstDataIdx,
		fieldIDWidth:    fieldIDWidth,
		offsetWidth:     offsetWidth,
	}, nil
}

// Unmarshals a Variant object into the provided destination. The destination must be a pointer
// to one of three types:
//   - A struct (unmarshal will map the Variant fields to struct fields by name, or contents of the `variant` annotation)
//   - A string-keyed map (the passed in map will be cleared)
//   - The "any" type. This will be returned as a map[string]any
func unmarshalObject(raw []byte, md *decodedMetadata, offset int, destPtr reflect.Value) error {
	data, err := getObjectData(raw, offset)
	if err != nil {
		return err
	}

	if kind := destPtr.Kind(); kind != reflect.Pointer {
		return fmt.Errorf("invalid dest, must be non-nil pointer (got kind %s)", kind)
	}
	if destPtr.IsNil() {
		return errors.New("invalid dest, must be non-nil pointer")
	}

	destElem := destPtr.Elem()

	switch kind := destElem.Kind(); kind {
	case reflect.Struct:
		// Nothing to do.
	case reflect.Interface:
		// Create a new map[string]any
		newMap := reflect.MakeMap(reflect.TypeOf(map[string]any{}))
		destElem.Set(newMap)
		destElem = newMap
	case reflect.Map:
		if keyKind := destElem.Type().Key().Kind(); keyKind != reflect.String {
			return fmt.Errorf("invalid dest map- must have a string for a key, got %s", keyKind)
		}
		// Clear out the map to start fresh.
		destElem.Clear()
	default:
		return fmt.Errorf("invalid kind- must be a string-keyed map, struct, or any, got %s", kind)
	}

	destType := destElem.Type()

	// For slightly faster lookups, preprocess the struct to get a mapping from field name to field ID.
	// We only care about settable struct fields.
	fieldIDMap := make(map[string]int)
	if destElem.Kind() == reflect.Struct {
		for i := range destType.NumField() {
			structField := destElem.Field(i)
			if structField.CanSet() {
				fieldName, _ := extractFieldInfo(destType.Field(i))
				fieldIDMap[fieldName] = i

				// Zero out the field if possible to start fresh.
				structField.Set(reflect.Zero(structField.Type()))
			}
		}
	}

	// Iterate through all elements in the encoded Variant.
	for i := range data.numElements {
		variantFieldID, err := readUint(raw, data.firstFieldIDIdx+data.fieldIDWidth*i, data.fieldIDWidth)
		if err != nil {
			return err
		}
		variantKey, ok := md.At(int(variantFieldID))
		if !ok {
			return fmt.Errorf("key ID %d not present in metadata dictionary", i)
		}

		// Get the new element value depending on whether this is a struct or a map
		var newElemValue reflect.Value
		if destElem.Kind() == reflect.Struct {
			// Get pointer to the field within the struct.
			structFieldID, ok := fieldIDMap[variantKey]
			if !ok {
				continue
			}
			field := destElem.Field(structFieldID)
			newElemValue = field.Addr()
		} else {
			// New element within the map.
			newElemValue = reflect.New(destType.Elem())
		}

		// Set the element value based on what's encoded in the Variant.
		elemOffset, err := readUint(raw, data.firstOffsetIdx+data.offsetWidth*i, data.offsetWidth)
		if err != nil {
			return err
		}
		dataIdx := int(elemOffset) + data.firstDataIdx
		if err := checkBounds(raw, dataIdx, dataIdx); err != nil {
			return err
		}
		if err := unmarshalCommon(raw, md, dataIdx, newElemValue); err != nil {
			return err
		}

		// Structs already have a pointer to the value and are set. For maps, set the value here.
		if destElem.Kind() == reflect.Map {
			destElem.SetMapIndex(reflect.ValueOf(variantKey), newElemValue.Elem())
		}
	}

	return nil
}
