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

package arrgen

import (
	"fmt"
	"go/types"
)

// colSpec is everything the template needs to emit one Arrow column: the type
// expression for the schema, the concrete builder the appender caches, and a
// renderer for the append statement.
//
// The three are resolved together because they are three views of the same
// decision - Int64 columns get an *array.Int64Builder and an Append(int64(...))
// - and splitting them across independent switches is how they drift apart.
type colSpec struct {
	arrowType   string // Go expression for the arrow.DataType
	builderType string // Go type of the cached typed builder
	fallible    bool   // the builder's Append returns an error (dictionaries)
	needsTime   bool   // the append statement references the time package
	nilable     bool   // a nil Go value maps to null even in a non-nullable column

	// appendStmt renders the append of a known-non-nil value. bld is the cached
	// builder expression ("a.b3"), val is the value ("v.F" or "*v.F"), and recv
	// is val parenthesized where needed so a method call binds to the value and
	// not to the pointer.
	appendStmt func(bld, recv, val string) string
}

// optSupport records which tag options a column kind can honor. Options that
// would be silently ignored are rejected instead: arreflect drops a date32 tag
// on an int field on the floor, but at generate time there is a human waiting
// to be told about the typo.
type optSupport struct {
	temporal  bool
	decimal   bool
	largeView bool
	dict      bool
}

func checkOpts(o tagOpts, goType string, s optSupport) error {
	switch {
	case o.Temporal != "" && !s.temporal:
		return fmt.Errorf("%q is only valid on a time.Time field, not %s", o.Temporal, goType)
	case o.HasDecimalOpts && !s.decimal:
		return fmt.Errorf("decimal(precision,scale) is only valid on a decimal field, not %s", goType)
	case o.Large && !s.largeView:
		return fmt.Errorf("large has no effect on %s; it is only valid on string and []byte fields", goType)
	case o.View && !s.largeView:
		return fmt.Errorf("view has no effect on %s; it is only valid on string and []byte fields", goType)
	case o.Dict && !s.dict:
		return fmt.Errorf("dict is not supported on %s; it is valid on string, []byte, integer and float fields", goType)
	case o.Dict && o.Large:
		return fmt.Errorf("dict cannot be combined with large: Dictionary<Int32, LargeString> is not implemented by arrow-go")
	}
	return nil
}

// resolveColumn maps a Go field type and its parsed tag to an Arrow column,
// reporting nullable=true for a pointer field (matching arreflect, which marks
// a column nullable from the outermost pointer).
func resolveColumn(t types.Type, opts tagOpts) (spec colSpec, nullable bool, err error) {
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		nullable = true
		t = types.Unalias(p.Elem())
		if _, ok := t.(*types.Pointer); ok {
			return colSpec{}, false, fmt.Errorf("multi-level pointer type %s is not supported", t)
		}
	}
	spec, err = baseSpec(t, opts)
	return spec, nullable, err
}

func baseSpec(t types.Type, opts tagOpts) (colSpec, error) {
	if named, ok := t.(*types.Named); ok {
		return namedSpec(named, opts)
	}
	if sl, ok := t.(*types.Slice); ok {
		if b, ok := types.Unalias(sl.Elem()).(*types.Basic); ok && b.Kind() == types.Uint8 {
			return byteSliceSpec(opts)
		}
		return colSpec{}, fmt.Errorf("list column for %s is not supported; encode it with arreflect", t)
	}
	if b, ok := t.(*types.Basic); ok {
		return basicSpec(b, opts)
	}
	return colSpec{}, fmt.Errorf("type %s is not supported", t)
}

// namedSpec handles the handful of named types arreflect recognizes by identity.
// Every other named type - including a defined scalar such as `type ID int64` -
// is unsupported there too, so rejecting it here keeps the two paths aligned.
func namedSpec(named *types.Named, opts tagOpts) (colSpec, error) {
	obj := named.Obj()
	pkg := ""
	if obj.Pkg() != nil {
		pkg = obj.Pkg().Path()
	}
	switch {
	case pkg == "time" && obj.Name() == "Time":
		return timeSpec(opts)
	case pkg == "time" && obj.Name() == "Duration":
		if err := checkOpts(opts, "time.Duration", optSupport{}); err != nil {
			return colSpec{}, err
		}
		return colSpec{
			arrowType:   "&arrow.DurationType{Unit: arrow.Nanosecond}",
			builderType: "*array.DurationBuilder",
			appendStmt: func(bld, recv, _ string) string {
				return fmt.Sprintf("%s.Append(arrow.Duration(%s.Nanoseconds()))", bld, recv)
			},
		}, nil
	case isDecimalPkg(pkg, "decimal") && obj.Name() == "Decimal32":
		return decimalSpec(opts, "Decimal32", 9, "")
	case isDecimalPkg(pkg, "decimal") && obj.Name() == "Decimal64":
		return decimalSpec(opts, "Decimal64", 18, "")
	case isDecimalPkg(pkg, "decimal128") && obj.Name() == "Num":
		return decimalSpec(opts, "Decimal128", 38, "decimal128.Num")
	case isDecimalPkg(pkg, "decimal256") && obj.Name() == "Num":
		return decimalSpec(opts, "Decimal256", 76, "decimal256.Num")
	}
	if _, ok := named.Underlying().(*types.Struct); ok {
		return colSpec{}, fmt.Errorf("nested struct column for %s is not supported; encode it with arreflect", named)
	}
	return colSpec{}, fmt.Errorf("named type %s is not supported; arreflect matches scalar fields by exact type, so a defined type such as this one has no Arrow mapping in either path", named)
}

func isDecimalPkg(path, name string) bool {
	const prefix = "github.com/apache/arrow-go/v18/arrow/"
	return path == prefix+name
}

// decimalSpec maps a decimal field. structGoType is empty for decimal.Decimal32
// and decimal.Decimal64, whose Go types are defined integers, and the Go type's
// name for decimal128.Num and decimal256.Num, whose Go types are structs and so
// need an explicit decimal tag to stay in step with arreflect. See
// errUninferableStructScalar.
func decimalSpec(opts tagOpts, arrowName string, defaultPrecision int32, structGoType string) (colSpec, error) {
	goType := "decimal." + arrowName
	if err := checkOpts(opts, goType, optSupport{decimal: true}); err != nil {
		return colSpec{}, err
	}
	if structGoType != "" && !opts.HasDecimalOpts {
		return colSpec{}, errUninferableStructScalar(structGoType, "a decimal(precision,scale) tag")
	}
	precision, scale := defaultPrecision, int32(0)
	if opts.HasDecimalOpts {
		precision, scale = opts.DecimalPrecision, opts.DecimalScale
	}
	return colSpec{
		arrowType:   fmt.Sprintf("&arrow.%sType{Precision: %d, Scale: %d}", arrowName, precision, scale),
		builderType: "*array." + arrowName + "Builder",
		appendStmt:  simpleAppend(),
	}, nil
}

// timeSpec maps a time.Time field, which needs one of the four temporal tags:
// the timestamp spelling arreflect cannot infer for a struct field is rejected
// rather than generated. See errUninferableStructScalar.
func timeSpec(opts tagOpts) (colSpec, error) {
	if err := checkOpts(opts, "time.Time", optSupport{temporal: true}); err != nil {
		return colSpec{}, err
	}
	switch opts.Temporal {
	case "date32":
		return colSpec{
			arrowType:   "arrow.FixedWidthTypes.Date32",
			builderType: "*array.Date32Builder",
			appendStmt: func(bld, _, val string) string {
				return fmt.Sprintf("%s.Append(arrow.Date32FromTime(%s))", bld, val)
			},
		}, nil
	case "date64":
		return colSpec{
			arrowType:   "arrow.FixedWidthTypes.Date64",
			builderType: "*array.Date64Builder",
			appendStmt: func(bld, _, val string) string {
				return fmt.Sprintf("%s.Append(arrow.Date64FromTime(%s))", bld, val)
			},
		}, nil
	case "time32":
		return timeOfDaySpec("&arrow.Time32Type{Unit: arrow.Millisecond}", "*array.Time32Builder", "arrow.Time32", " / 1e6"), nil
	case "time64":
		return timeOfDaySpec("&arrow.Time64Type{Unit: arrow.Nanosecond}", "*array.Time64Builder", "arrow.Time64", ""), nil
	default:
		// "" and "timestamp" are the two spellings that ask for a TIMESTAMP
		// column, and neither is inferable as a struct field.
		return colSpec{}, errUninferableStructScalar("time.Time", "one of the date32, date64, time32 or time64 tags")
	}
}

// errUninferableStructScalar reports a field whose Go type is a struct that
// Arrow models as a scalar, tagged in a way arreflect cannot infer.
//
// arreflect's inferArrowType switches on reflect.Kind before it reaches the
// types it matches by identity, so a struct field of one of these types is
// resolved by inferStructType instead, which finds only unexported fields and
// yields an empty struct<> - and the value is then dropped. Only a tag that
// names the Arrow type outright survives, because arreflect applies the tag
// after the inferred type.
//
// arrgen could emit the column Arrow plainly means here, but generated code and
// arreflect would then disagree about the schema, which is the one thing this
// generator exists not to do. So the untaggable spellings are a generate-time
// error naming the field and the tag that fixes it.
func errUninferableStructScalar(goType, fix string) error {
	return fmt.Errorf("%s needs %s: arreflect infers an empty struct<> for it as a struct field "+
		"and drops the value, so a column generated here would not match arreflect.InferSchema. "+
		"Tag it, or encode the struct with arreflect instead", goType, fix)
}

// timeOfDaySpec renders arreflect's timeOfDayNanos inline. It is emitted as a
// block rather than a package-level helper so that two generated files in the
// same package cannot collide over the helper's name.
func timeOfDaySpec(arrowType, builderType, cast, divisor string) colSpec {
	return colSpec{
		arrowType:   arrowType,
		builderType: builderType,
		needsTime:   true,
		appendStmt: func(bld, recv, _ string) string {
			return fmt.Sprintf(`{
tod := %s.UTC()
%s.Append(%s(tod.Sub(time.Date(tod.Year(), tod.Month(), tod.Day(), 0, 0, 0, 0, time.UTC)).Nanoseconds()%s))
}`, recv, bld, cast, divisor)
		},
	}
}

func byteSliceSpec(opts tagOpts) (colSpec, error) {
	if err := checkOpts(opts, "[]byte", optSupport{largeView: true, dict: true}); err != nil {
		return colSpec{}, err
	}
	// A nil []byte is a null in arreflect even when the column is not nullable,
	// because its binary case tests the slice itself rather than the pointer.
	if opts.Dict {
		spec := dictSpec("arrow.BinaryTypes.Binary", "*array.BinaryDictionaryBuilder", simpleAppend())
		spec.nilable = true
		return spec, nil
	}
	switch {
	case opts.View:
		return colSpec{arrowType: "arrow.BinaryTypes.BinaryView", builderType: "*array.BinaryViewBuilder", appendStmt: simpleAppend(), nilable: true}, nil
	case opts.Large:
		return colSpec{arrowType: "arrow.BinaryTypes.LargeBinary", builderType: "*array.BinaryBuilder", appendStmt: simpleAppend(), nilable: true}, nil
	default:
		return colSpec{arrowType: "arrow.BinaryTypes.Binary", builderType: "*array.BinaryBuilder", appendStmt: simpleAppend(), nilable: true}, nil
	}
}

func stringSpec(opts tagOpts) (colSpec, error) {
	if err := checkOpts(opts, "string", optSupport{largeView: true, dict: true}); err != nil {
		return colSpec{}, err
	}
	if opts.Dict {
		return dictSpec("arrow.BinaryTypes.String", "*array.BinaryDictionaryBuilder", func(bld, _, val string) string {
			return fmt.Sprintf("%s.AppendString(%s)", bld, val)
		}), nil
	}
	switch {
	case opts.View:
		return colSpec{arrowType: "arrow.BinaryTypes.StringView", builderType: "*array.StringViewBuilder", appendStmt: simpleAppend()}, nil
	case opts.Large:
		return colSpec{arrowType: "arrow.BinaryTypes.LargeString", builderType: "*array.LargeStringBuilder", appendStmt: simpleAppend()}, nil
	default:
		return colSpec{arrowType: "arrow.BinaryTypes.String", builderType: "*array.StringBuilder", appendStmt: simpleAppend()}, nil
	}
}

// dictSpec wraps a value type in Dictionary<Int32, value>, matching
// arreflect.applyEncodingOpts. Dictionary appends can fail, so the caller
// routes the result through the appender's first-error field.
func dictSpec(valueType, builderType string, appendStmt func(bld, recv, val string) string) colSpec {
	return colSpec{
		arrowType:   fmt.Sprintf("&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: %s}", valueType),
		builderType: builderType,
		fallible:    true,
		appendStmt:  appendStmt,
	}
}

// numeric describes one Go basic kind's Arrow column.
type numeric struct {
	arrowType string // e.g. "arrow.PrimitiveTypes.Int64"
	builder   string // e.g. "Int64"
	cast      string // non-empty when the Go type is wider-named than the column ("int" -> int64)
}

var numerics = map[types.BasicKind]numeric{
	types.Int8:    {"arrow.PrimitiveTypes.Int8", "Int8", ""},
	types.Int16:   {"arrow.PrimitiveTypes.Int16", "Int16", ""},
	types.Int32:   {"arrow.PrimitiveTypes.Int32", "Int32", ""},
	types.Int64:   {"arrow.PrimitiveTypes.Int64", "Int64", ""},
	types.Int:     {"arrow.PrimitiveTypes.Int64", "Int64", "int64"},
	types.Uint8:   {"arrow.PrimitiveTypes.Uint8", "Uint8", ""},
	types.Uint16:  {"arrow.PrimitiveTypes.Uint16", "Uint16", ""},
	types.Uint32:  {"arrow.PrimitiveTypes.Uint32", "Uint32", ""},
	types.Uint64:  {"arrow.PrimitiveTypes.Uint64", "Uint64", ""},
	types.Uint:    {"arrow.PrimitiveTypes.Uint64", "Uint64", "uint64"},
	types.Float32: {"arrow.PrimitiveTypes.Float32", "Float32", ""},
	types.Float64: {"arrow.PrimitiveTypes.Float64", "Float64", ""},
}

func basicSpec(b *types.Basic, opts tagOpts) (colSpec, error) {
	switch b.Kind() {
	case types.String:
		return stringSpec(opts)
	case types.Bool:
		if err := checkOpts(opts, "bool", optSupport{}); err != nil {
			return colSpec{}, err
		}
		return colSpec{arrowType: "arrow.FixedWidthTypes.Boolean", builderType: "*array.BooleanBuilder", appendStmt: simpleAppend()}, nil
	}

	n, ok := numerics[b.Kind()]
	if !ok {
		return colSpec{}, fmt.Errorf("type %s is not supported", b)
	}
	if err := checkOpts(opts, b.Name(), optSupport{dict: true}); err != nil {
		return colSpec{}, err
	}
	appendStmt := simpleAppend()
	if n.cast != "" {
		appendStmt = castAppend(n.cast)
	}
	if opts.Dict {
		return dictSpec(n.arrowType, "*array."+n.builder+"DictionaryBuilder", appendStmt), nil
	}
	return colSpec{arrowType: n.arrowType, builderType: "*array." + n.builder + "Builder", appendStmt: appendStmt}, nil
}

func simpleAppend() func(bld, recv, val string) string {
	return func(bld, _, val string) string { return fmt.Sprintf("%s.Append(%s)", bld, val) }
}

func castAppend(cast string) func(bld, recv, val string) string {
	return func(bld, _, val string) string { return fmt.Sprintf("%s.Append(%s(%s))", bld, cast, val) }
}
