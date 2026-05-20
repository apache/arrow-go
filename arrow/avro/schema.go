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

// Package avro reads Avro OCF files and presents the extracted data as records
package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/internal/utils"
	hambaAvro "github.com/hamba/avro/v2"
	avro "github.com/twmb/avro"
)

// builtinAvroTypes is the set of Type field values that mean "this SchemaNode
// is the inline definition of an Avro type." Anything else in node.Type is
// treated as a named-type reference to a previously-seen record/enum/fixed.
var builtinAvroTypes = map[string]struct{}{
	"null": {}, "boolean": {}, "int": {}, "long": {},
	"float": {}, "double": {}, "bytes": {}, "string": {},
	"record": {}, "enum": {}, "array": {}, "map": {},
	"fixed": {}, "union": {},
}

type schemaNode struct {
	name       string
	parent     *schemaNode
	node       avro.SchemaNode
	union      bool
	nullable   bool
	childrens  []*schemaNode
	arrowField arrow.Field
	namedCache map[string]avro.SchemaNode
	index      int32
}

func newSchemaNode() *schemaNode {
	return &schemaNode{index: -1, namedCache: map[string]avro.SchemaNode{}}
}

func (node *schemaNode) schemaPath() string {
	var path string
	n := node
	for n.parent != nil {
		path = "." + n.name + path
		n = n.parent
	}
	return path
}

func (node *schemaNode) newChild(n string, s avro.SchemaNode) *schemaNode {
	child := &schemaNode{
		name:       n,
		parent:     node,
		node:       s,
		namedCache: node.namedCache,
		index:      int32(len(node.childrens)),
	}
	node.childrens = append(node.childrens, child)
	return child
}
func (node *schemaNode) children() []*schemaNode { return node.childrens }

// rememberNamed adds a record/enum/fixed SchemaNode to the named-type cache
// under both its short name and (if a namespace is present) its full name,
// so later references like {"type": "Address"} or {"type": "ns.Address"}
// resolve back to the original definition.
func (node *schemaNode) rememberNamed(s avro.SchemaNode) {
	if s.Name == "" {
		return
	}
	node.namedCache[s.Name] = s
	if s.Namespace != "" {
		node.namedCache[s.Namespace+"."+s.Name] = s
	}
}

// resolveRef replaces s with its inline definition if s.Type is a named-type
// reference rather than a builtin Avro type. atField, when non-empty, names
// the field this reference appears in and is included in the panic so the
// user can locate the offending entry.
func (node *schemaNode) resolveRef(s avro.SchemaNode, atField string) avro.SchemaNode {
	if _, ok := builtinAvroTypes[s.Type]; ok {
		return s
	}
	if def, ok := node.namedCache[s.Type]; ok {
		return def
	}
	loc := node.schemaPath()
	if atField != "" {
		loc += "." + atField
	}
	panic(fmt.Errorf("unknown named type %q referenced at %s", s.Type, loc))
}

// ArrowSchemaFromAvroJSON parses an Avro schema given as JSON text and returns
// the equivalent Arrow schema.
func ArrowSchemaFromAvroJSON(schemaJSON string) (*arrow.Schema, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	return arrowSchemaFromAvroInternal(schema)
}

// ArrowSchemaFromAvro returns a new Arrow schema from a parsed Avro schema.
//
// Deprecated: Use [ArrowSchemaFromAvroJSON] instead — it does not couple
// callers to a particular Avro library through its signature.
func ArrowSchemaFromAvro(schema hambaAvro.Schema) (*arrow.Schema, error) {
	js, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: could not serialize hamba avro schema: %w", arrow.ErrInvalid, err)
	}
	return ArrowSchemaFromAvroJSON(string(js))
}

func arrowSchemaFromAvroInternal(schema *avro.Schema) (s *arrow.Schema, err error) {
	defer func() {
		if r := recover(); r != nil {
			s = nil
			err = utils.FormatRecoveredError("invalid avro schema", r)
		}
	}()
	root := schema.Root()
	n := newSchemaNode()
	n.node = root
	c := n.newChild(root.Name, root)
	arrowSchemafromAvro(c)
	var fields []arrow.Field
	for _, g := range c.children() {
		fields = append(fields, g.arrowField)
	}
	s = arrow.NewSchema(fields, nil)
	return s, nil
}

func arrowSchemafromAvro(n *schemaNode) {
	n.node = n.resolveRef(n.node, "")
	if n.node.Name != "" {
		n.rememberNamed(n.node)
	}
	switch st := n.node.Type; st {
	case "record":
		iterateFields(n)
	case "enum":
		symbols := make(map[string]string)
		for index, symbol := range n.node.Symbols {
			k := strconv.FormatInt(int64(index), 10)
			symbols[k] = symbol
		}
		dt := arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
		sl := int64(len(symbols))
		switch {
		case sl <= math.MaxUint8:
			dt.IndexType = arrow.PrimitiveTypes.Uint8
		case sl > math.MaxUint8 && sl <= math.MaxUint16:
			dt.IndexType = arrow.PrimitiveTypes.Uint16
		case sl > math.MaxUint16 && sl <= math.MaxUint32:
			dt.IndexType = arrow.PrimitiveTypes.Uint32
		}
		n.arrowField = buildArrowField(n, &dt, arrow.MetadataFrom(symbols))
	case "array":
		if n.node.Items == nil {
			panic(fmt.Errorf("avro array schema at %s has no 'items'", n.schemaPath()))
		}
		items := *n.node.Items
		c := n.newChild(n.name, items)
		if isLogicalSchemaType(items) {
			avroLogicalToArrowField(c)
		} else {
			arrowSchemafromAvro(c)
		}
		var typ *arrow.ListType
		switch c.arrowField.Nullable {
		case true:
			typ = arrow.ListOfField(c.arrowField)
		case false:
			typ = arrow.ListOfNonNullable(c.arrowField.Type)
		}
		n.arrowField = buildArrowField(n, typ, c.arrowField.Metadata)
	case "map":
		if n.node.Values == nil {
			panic(fmt.Errorf("avro map schema at %s has no 'values'", n.schemaPath()))
		}
		values := *n.node.Values
		c := n.newChild(n.name, values)
		arrowSchemafromAvro(c)
		n.arrowField = buildArrowField(n, arrow.MapOf(arrow.BinaryTypes.String, c.arrowField.Type), c.arrowField.Metadata)
	case "union":
		branch, ok := nullableBranch(n.node)
		if !ok {
			panic(fmt.Errorf("unsupported avro union at %s: only ['null', T] unions with exactly one non-null branch are supported", n.schemaPath()))
		}
		n.node = branch
		n.union = true
		n.nullable = true
		arrowSchemafromAvro(n)
	// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
	case "fixed":
		if isLogicalSchemaType(n.node) {
			avroLogicalToArrowField(n)
		} else {
			n.arrowField = buildArrowField(n, &arrow.FixedSizeBinaryType{ByteWidth: n.node.Size}, arrow.Metadata{})
		}
	case "string", "bytes", "int", "long":
		if isLogicalSchemaType(n.node) {
			avroLogicalToArrowField(n)
		} else {
			n.arrowField = buildArrowField(n, avroPrimitiveToArrowType(string(st)), arrow.Metadata{})
		}
	case "float", "double", "boolean":
		n.arrowField = buildArrowField(n, avroPrimitiveToArrowType(string(st)), arrow.Metadata{})
	case "null":
		n.nullable = true
		n.arrowField = buildArrowField(n, arrow.Null, arrow.Metadata{})
	default:
		panic(fmt.Errorf("unhandled avro type %q at %s", st, n.schemaPath()))
	}
}

// iterate record Fields
func iterateFields(n *schemaNode) {
	for _, f := range n.node.Fields {
		ft := n.resolveRef(f.Type, f.Name)
		switch ft.Type {
		// Avro "array" field type
		case "array":
			if ft.Items == nil {
				panic(fmt.Errorf("avro array field %s.%s has no 'items'", n.schemaPath(), f.Name))
			}
			items := *ft.Items
			c := n.newChild(f.Name, items)
			if isLogicalSchemaType(items) {
				avroLogicalToArrowField(c)
			} else {
				arrowSchemafromAvro(c)
			}
			switch c.arrowField.Nullable {
			case true:
				c.arrowField = arrow.Field{Name: c.name, Type: arrow.ListOfField(c.arrowField), Metadata: c.arrowField.Metadata}
			case false:
				c.arrowField = arrow.Field{Name: c.name, Type: arrow.ListOfNonNullable(c.arrowField.Type), Metadata: c.arrowField.Metadata}
			}
		// Avro "enum" field type = Arrow dictionary type
		case "enum":
			n.rememberNamed(ft)
			c := n.newChild(f.Name, ft)
			symbols := make(map[string]string)
			for index, symbol := range ft.Symbols {
				k := strconv.FormatInt(int64(index), 10)
				symbols[k] = symbol
			}
			var dt = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
			sl := len(symbols)
			switch {
			case sl <= math.MaxUint8:
				dt.IndexType = arrow.PrimitiveTypes.Uint8
			case sl > math.MaxUint8 && sl <= math.MaxUint16:
				dt.IndexType = arrow.PrimitiveTypes.Uint16
			case sl > math.MaxUint16 && sl <= math.MaxInt:
				dt.IndexType = arrow.PrimitiveTypes.Uint32
			}
			c.arrowField = buildArrowField(c, &dt, arrow.MetadataFrom(symbols))
		// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
		case "fixed":
			n.rememberNamed(ft)
			c := n.newChild(f.Name, ft)
			if isLogicalSchemaType(ft) {
				avroLogicalToArrowField(c)
			} else {
				arrowSchemafromAvro(c)
			}
		case "record":
			n.rememberNamed(ft)
			c := n.newChild(f.Name, ft)
			iterateFields(c)
		// Avro "map" field type - KVP with value of one type - keys are strings
		case "map":
			if ft.Values == nil {
				panic(fmt.Errorf("avro map field %s.%s has no 'values'", n.schemaPath(), f.Name))
			}
			values := *ft.Values
			c := n.newChild(f.Name, values)
			arrowSchemafromAvro(c)
			c.arrowField = buildArrowField(c, arrow.MapOf(arrow.BinaryTypes.String, c.arrowField.Type), c.arrowField.Metadata)
		case "union":
			branch, ok := nullableBranch(ft)
			if !ok {
				panic(fmt.Errorf("unsupported avro union at %s.%s: only ['null', T] unions with exactly one non-null branch are supported", n.schemaPath(), f.Name))
			}
			c := n.newChild(f.Name, branch)
			c.union = true
			c.nullable = true
			arrowSchemafromAvro(c)
		default:
			c := n.newChild(f.Name, ft)
			if isLogicalSchemaType(ft) {
				avroLogicalToArrowField(c)
			} else {
				arrowSchemafromAvro(c)
			}
		}
	}
	var fields []arrow.Field
	for _, child := range n.children() {
		fields = append(fields, child.arrowField)
	}

	namedSchema, ok := isNamedSchema(n.node)

	var md arrow.Metadata
	if ok && namedSchema != n.name+"_data" && n.union {
		md = arrow.NewMetadata([]string{"typeName"}, []string{namedSchema})
	}
	n.arrowField = buildArrowField(n, arrow.StructOf(fields...), md)
}

// nullableBranch returns the non-null branch of a two-element ["null", T]
// union, plus true if the union is in that nullable shape. If the union has
// more than two branches or no null branch, ok is false.
//
// Heterogeneous non-nullable unions (e.g. ["null", "int", "string"] or
// ["int", "string"]) are not supported and callers panic on them rather
// than silently picking one arm.
func nullableBranch(s avro.SchemaNode) (avro.SchemaNode, bool) {
	if s.Type != "union" || len(s.Branches) < 2 {
		return avro.SchemaNode{}, false
	}
	var nonNull *avro.SchemaNode
	for i := range s.Branches {
		b := s.Branches[i]
		if b.Type == "null" {
			continue
		}
		if nonNull != nil {
			return avro.SchemaNode{}, false
		}
		nonNull = &b
	}
	if nonNull == nil {
		return avro.SchemaNode{}, false
	}
	return *nonNull, true
}

func isLogicalSchemaType(s avro.SchemaNode) bool {
	return s.LogicalType != ""
}

func isNamedSchema(s avro.SchemaNode) (string, bool) {
	if s.Name == "" {
		return "", false
	}
	if s.Namespace != "" {
		return s.Namespace + "." + s.Name, true
	}
	return s.Name, true
}

func buildArrowField(n *schemaNode, t arrow.DataType, m arrow.Metadata) arrow.Field {
	return arrow.Field{
		Name:     n.name,
		Type:     t,
		Metadata: m,
		Nullable: n.nullable,
	}
}

// Avro primitive type.
//
// NOTE: Arrow Binary type is used as a catchall to avoid potential data loss.
func avroPrimitiveToArrowType(avroFieldType string) arrow.DataType {
	switch avroFieldType {
	// int: 32-bit signed integer
	case "int":
		return arrow.PrimitiveTypes.Int32
	// long: 64-bit signed integer
	case "long":
		return arrow.PrimitiveTypes.Int64
	// float: single precision (32-bit) IEEE 754 floating-point number
	case "float":
		return arrow.PrimitiveTypes.Float32
	// double: double precision (64-bit) IEEE 754 floating-point number
	case "double":
		return arrow.PrimitiveTypes.Float64
	// bytes: sequence of 8-bit unsigned bytes
	case "bytes":
		return arrow.BinaryTypes.Binary
	// boolean: a binary value
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	// string: unicode character sequence
	case "string":
		return arrow.BinaryTypes.String
	}
	return nil
}

func avroLogicalToArrowField(n *schemaNode) {
	var dt arrow.DataType
	// Avro logical types
	switch n.node.LogicalType {
	// The decimal logical type represents an arbitrary-precision signed decimal number of the form unscaled × 10-scale.
	// A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the two’s-complement
	// representation of the unscaled integer value in big-endian byte order. The scale is fixed, and is specified
	// using an attribute.
	//
	// The following attributes are supported:
	// scale, a JSON integer representing the scale (optional). If not specified the scale is 0.
	// precision, a JSON integer representing the (maximum) precision of decimals stored in this type (required).
	case "decimal":
		id := arrow.DECIMAL128
		if n.node.Precision > decimal128.MaxPrecision {
			id = arrow.DECIMAL256
		}
		dt, _ = arrow.NewDecimalType(id, int32(n.node.Precision), int32(n.node.Scale))

	// The uuid logical type represents a random generated universally unique identifier (UUID).
	// A uuid logical type annotates an Avro string. The string has to conform with RFC-4122
	case "uuid":
		dt = extensions.NewUUIDType()

	// The date logical type represents a date within the calendar, with no reference to a particular
	// time zone or time of day.
	// A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch,
	// 1 January 1970 (ISO calendar).
	case "date":
		dt = arrow.FixedWidthTypes.Date32

	// The time-millis logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one millisecond.
	// A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds
	// after midnight, 00:00:00.000.
	case "time-millis":
		dt = arrow.FixedWidthTypes.Time32ms

	// The time-micros logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one microsecond.
	// A time-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// after midnight, 00:00:00.000000.
	case "time-micros":
		dt = arrow.FixedWidthTypes.Time64us

	// The timestamp-millis logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one millisecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users in
	// their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds
	// from the unix epoch, 1 January 1970 00:00:00.000 UTC.
	case "timestamp-millis":
		dt = arrow.FixedWidthTypes.Timestamp_ms

	// The timestamp-micros logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one microsecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users
	// in their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
	case "timestamp-micros":
		dt = arrow.FixedWidthTypes.Timestamp_us

	// The timestamp-nanos logical type represents an instant on the global timeline with nanosecond
	// precision. twmb/avro decodes it to time.Time (UTC).
	case "timestamp-nanos":
		dt = arrow.FixedWidthTypes.Timestamp_ns

	// The local-timestamp-millis/micros/nanos logical types represent a timestamp in a local timezone.
	// Arrow models that as a TimestampType with no time zone set.
	case "local-timestamp-millis":
		dt = &arrow.TimestampType{Unit: arrow.Millisecond}
	case "local-timestamp-micros":
		dt = &arrow.TimestampType{Unit: arrow.Microsecond}
	case "local-timestamp-nanos":
		dt = &arrow.TimestampType{Unit: arrow.Nanosecond}

	// The duration logical type represents an amount of time defined by a number of months, days and milliseconds.
	// This is not equivalent to a number of milliseconds, because, depending on the moment in time from which the
	// duration is measured, the number of days in the month and number of milliseconds in a day may differ. Other
	// standard periods such as years, quarters, hours and minutes can be expressed through these basic periods.

	// A duration logical type annotates Avro fixed type of size 12, which stores three little-endian unsigned integers
	// that represent durations at different granularities of time. The first stores a number in months, the second
	// stores a number in days, and the third stores a number in milliseconds.
	case "duration":
		dt = arrow.FixedWidthTypes.MonthDayNanoInterval
	}
	n.arrowField = buildArrowField(n, dt, arrow.Metadata{})
}
