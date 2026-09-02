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
// renderer for the append statement. The three are resolved together so they
// cannot drift apart.
type colSpec struct {
	arrowType   string // Go expression for the arrow.DataType
	builderType string // Go type of the cached typed builder
	fallible    bool   // the builder's Append returns an error (dictionaries)
	needsTime   bool   // the append statement references the time package
	nilable     bool   // a nil Go value maps to null even in a non-nullable column

	// appendStmt renders the append of a known-non-nil value. bld is the cached
	// builder expression ("a.b3"), val is the value ("v.F" or "*v.F"), and recv
	// is val parenthesized so a method call binds to the value, not the pointer.
	appendStmt func(bld, recv, val string) string
}

// optSupport records which tag options a column kind can honor. An option that
// would be ignored is rejected instead, unlike arreflect.
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
// reporting nullable=true for a pointer field, as arreflect does.
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

// namedSpec handles the named types arreflect recognizes by identity. Every
// other named type, including a defined scalar such as `type ID int64`, is
// unsupported there too.
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
		return decimalSpec(opts, "Decimal32", 9)
	case isDecimalPkg(pkg, "decimal") && obj.Name() == "Decimal64":
		return decimalSpec(opts, "Decimal64", 18)
	case isDecimalPkg(pkg, "decimal128") && obj.Name() == "Num":
		return decimalSpec(opts, "Decimal128", 38)
	case isDecimalPkg(pkg, "decimal256") && obj.Name() == "Num":
		return decimalSpec(opts, "Decimal256", 76)
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

func decimalSpec(opts tagOpts, arrowName string, defaultPrecision int32) (colSpec, error) {
	goType := "decimal." + arrowName
	if err := checkOpts(opts, goType, optSupport{decimal: true}); err != nil {
		return colSpec{}, err
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

// timeSpec maps a time.Time field. An untagged time.Time becomes
// TIMESTAMP(ns, UTC), which arreflect's struct-field path does not currently
// produce; see the divergence note in README.md.
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
		return colSpec{
			arrowType:   `&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}`,
			builderType: "*array.TimestampBuilder",
			appendStmt: func(bld, recv, _ string) string {
				return fmt.Sprintf("%s.Append(arrow.Timestamp(%s.UnixNano()))", bld, recv)
			},
		}, nil
	}
}

// timeOfDaySpec renders arreflect's timeOfDayNanos inline, as a block rather
// than a package-level helper so two generated files in one package cannot
// collide over the helper's name.
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
	// A nil []byte is null in arreflect even when the column is not nullable.
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
// arreflect.applyEncodingOpts. Dictionary appends can fail.
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
