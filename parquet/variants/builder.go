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
)

// ArrayBuilder provides a mechanism to build a Variant encoded array.
type ArrayBuilder interface {
	Builder
	Write(val any, opts ...MarshalOpts) error
	Array() ArrayBuilder
	Object() ObjectBuilder
}

// ObjectBuilder provides a mechanism to build a Variant encoded object
type ObjectBuilder interface {
	Builder
	Write(key string, val any, opts ...MarshalOpts) error
	Array(key string) (ArrayBuilder, error)
	Object(key string) (ObjectBuilder, error)
}

// Builder provides a mechanism to build something that's Variant encoded
type Builder interface {
	Build() error
}

// Options for marshaling time types into a Variant.
type MarshalOpts int

const (
	MarshalTimeNanos MarshalOpts = 1 << iota
	MarshalTimeNTZ
	MarshalAsDate
	MarshalAsTime
	MarshalAsTimestamp
	MarshalAsUUID
)

var errAlreadyBuilt = errors.New("component already built")

// VariantBuilder is a helper to build and encode a Variant
type VariantBuilder struct {
	buf     bytes.Buffer
	builder Builder
	typ     BasicType
	mdb     *metadataBuilder
	built   bool
}

func NewBuilder() *VariantBuilder {
	return &VariantBuilder{
		typ: BasicUndefined,
		mdb: newMetadataBuilder(),
	}
}

// Marshals a Variant from a provided value. This will automatically convert Go primitives
// into equivalent Variant values:
//   - Slice/Array: Converted into Variant Arrays, with the exception of []byte which is a Variant Binary Primitive
//   - map[string]any: Converted into Variant Objects
//   - Structs: Converted into Variant Objects. Keys are either the exported struct fields, or the value present
//     in the `variant` field annotation.
//   - Go primitives: Converted into Variant primitives
func Marshal(val any, opts ...MarshalOpts) (*MarshaledVariant, error) {
	b := NewBuilder()
	if err := writeCommon(val, &b.buf, b.mdb, nil); err != nil {
		return nil, err
	}
	ev, err := b.Build()
	if err != nil {
		return nil, err
	}
	return ev, nil
}

func (vb *VariantBuilder) check() error {
	if vb.built {
		return errors.New("Variant has already been built")
	}
	if vb.typ != BasicUndefined {
		return fmt.Errorf("Variant type has already been started as a %q", vb.typ)
	}
	return nil
}

// Callback to record the number of bytes written.
type doneCB func(int)

// Common functionalities in writing Variant encoded data. This will be recursed into from various places.
func writeCommon(val any, buf io.Writer, mdb *metadataBuilder, doneCB doneCB, opts ...MarshalOpts) error {
	typ := kindFromValue(val)
	switch typ {
	case BasicPrimitive:
		b, err := marshalPrimitive(val, buf, opts...)
		if err != nil {
			return fmt.Errorf("marshalPrimitive(): %v", err)
		}
		if doneCB != nil {
			doneCB(b)
		}
	case BasicObject:
		// Objects can be built from structs or maps.
		ob := newObjectBuilder(buf, mdb, doneCB)
		if reflect.ValueOf(val).Kind() == reflect.Map {
			if err := ob.fromMap(val); err != nil {
				return err
			}
		} else {
			// No need to check if this is a struct- kindFromValue() has done that already.
			if err := ob.fromStruct(val); err != nil {
				return err
			}
		}
		if err := ob.Build(); err != nil {
			return err
		}
	case BasicArray:
		ab := newArrayBuilder(buf, mdb, doneCB)
		if err := ab.fromSlice(val, opts...); err != nil {
			return err
		}
		if err := ab.Build(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown basic type: %s", typ)
	}
	return nil
}

// Sets this Variant as a primitive, and writes the provided value.
func (vb *VariantBuilder) Primitive(val any, opts ...MarshalOpts) error {
	if err := vb.check(); err != nil {
		return err
	}
	vb.typ = BasicPrimitive
	_, err := marshalPrimitive(val, &vb.buf, opts...)
	return err
}

// Sets this Variant as an Object and returns an ObjectBuilder.
func (vb *VariantBuilder) Object() (ObjectBuilder, error) {
	if err := vb.check(); err != nil {
		return nil, err
	}
	ob := newObjectBuilder(&vb.buf, vb.mdb, nil)
	vb.typ = BasicObject
	vb.builder = ob
	return ob, nil
}

// Sets this Variant as an Array and returns an ArrayBuilder.
func (vb *VariantBuilder) Array() (ArrayBuilder, error) {
	if err := vb.check(); err != nil {
		return nil, err
	}
	ab := newArrayBuilder(&vb.buf, vb.mdb, nil)
	vb.typ = BasicArray
	vb.builder = ab
	return ab, nil
}

// Builds the Variant
func (vb *VariantBuilder) Build() (*MarshaledVariant, error) {
	// Indicate that all building has completed to prevent any mutation.
	vb.built = true

	var encoded MarshaledVariant
	encoded.Metadata = vb.mdb.Build()

	// Build an object or an array if necessary
	if vb.builder != nil {
		if err := vb.builder.Build(); err != nil && err != errAlreadyBuilt {
			return nil, err
		}
	}

	encoded.Value = vb.buf.Bytes()
	return &encoded, nil
}
