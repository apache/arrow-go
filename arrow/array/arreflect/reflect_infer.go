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

package arreflect

import (
	"fmt"
	"reflect"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

var (
	typeOfTime      = reflect.TypeOf(time.Time{})
	typeOfDuration  = reflect.TypeOf(time.Duration(0))
	typeOfDec32     = reflect.TypeOf(decimal.Decimal32(0))
	typeOfDec64     = reflect.TypeOf(decimal.Decimal64(0))
	typeOfDec128    = reflect.TypeOf(decimal128.Num{})
	typeOfDec256    = reflect.TypeOf(decimal256.Num{})
	typeOfByteSlice = reflect.TypeOf([]byte{})
	typeOfInt       = reflect.TypeOf(int(0))
	typeOfUint      = reflect.TypeOf(uint(0))
	typeOfInt8      = reflect.TypeOf(int8(0))
	typeOfInt16     = reflect.TypeOf(int16(0))
	typeOfInt32     = reflect.TypeOf(int32(0))
	typeOfInt64     = reflect.TypeOf(int64(0))
	typeOfUint8     = reflect.TypeOf(uint8(0))
	typeOfUint16    = reflect.TypeOf(uint16(0))
	typeOfUint32    = reflect.TypeOf(uint32(0))
	typeOfUint64    = reflect.TypeOf(uint64(0))
	typeOfFloat32   = reflect.TypeOf(float32(0))
	typeOfFloat64   = reflect.TypeOf(float64(0))
	typeOfBool      = reflect.TypeOf(false)
	typeOfString    = reflect.TypeOf("")
)

const (
	dec32DefaultPrecision  int32 = 9
	dec64DefaultPrecision  int32 = 18
	dec128DefaultPrecision int32 = 38
	dec256DefaultPrecision int32 = 76
)

type listElemTyper interface{ Elem() arrow.DataType }

func inferPrimitiveArrowType(t reflect.Type) (arrow.DataType, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t {
	case typeOfInt8:
		return arrow.PrimitiveTypes.Int8, nil
	case typeOfInt16:
		return arrow.PrimitiveTypes.Int16, nil
	case typeOfInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case typeOfInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case typeOfInt:
		return arrow.PrimitiveTypes.Int64, nil
	case typeOfUint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case typeOfUint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case typeOfUint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case typeOfUint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case typeOfUint:
		return arrow.PrimitiveTypes.Uint64, nil
	case typeOfFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case typeOfFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case typeOfBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case typeOfString:
		return arrow.BinaryTypes.String, nil
	case typeOfByteSlice:
		return arrow.BinaryTypes.Binary, nil
	case typeOfTime:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, nil
	case typeOfDuration:
		return &arrow.DurationType{Unit: arrow.Nanosecond}, nil
	case typeOfDec128:
		return &arrow.Decimal128Type{Precision: dec128DefaultPrecision, Scale: 0}, nil
	case typeOfDec32:
		return &arrow.Decimal32Type{Precision: dec32DefaultPrecision, Scale: 0}, nil
	case typeOfDec64:
		return &arrow.Decimal64Type{Precision: dec64DefaultPrecision, Scale: 0}, nil
	case typeOfDec256:
		return &arrow.Decimal256Type{Precision: dec256DefaultPrecision, Scale: 0}, nil
	default:
		return nil, fmt.Errorf("unsupported Go type for Arrow inference %v: %w", t, ErrUnsupportedType)
	}
}

func inferArrowType(t reflect.Type) (arrow.DataType, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t == typeOfByteSlice {
		return arrow.BinaryTypes.Binary, nil
	}

	switch t.Kind() {
	case reflect.Slice:
		elemDT, err := inferArrowType(t.Elem())
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elemDT), nil

	case reflect.Array:
		elemDT, err := inferArrowType(t.Elem())
		if err != nil {
			return nil, err
		}
		return arrow.FixedSizeListOf(int32(t.Len()), elemDT), nil

	case reflect.Map:
		keyDT, err := inferArrowType(t.Key())
		if err != nil {
			return nil, err
		}
		valDT, err := inferArrowType(t.Elem())
		if err != nil {
			return nil, err
		}
		return arrow.MapOf(keyDT, valDT), nil

	case reflect.Struct:
		return inferStructType(t)

	default:
		return inferPrimitiveArrowType(t)
	}
}

func applyDecimalOpts(dt arrow.DataType, origType reflect.Type, opts tagOpts) arrow.DataType {
	if !opts.HasDecimalOpts {
		return dt
	}
	prec, scale := opts.DecimalPrecision, opts.DecimalScale
	switch origType {
	case typeOfDec128:
		return &arrow.Decimal128Type{Precision: prec, Scale: scale}
	case typeOfDec256:
		return &arrow.Decimal256Type{Precision: prec, Scale: scale}
	case typeOfDec32:
		return &arrow.Decimal32Type{Precision: prec, Scale: scale}
	case typeOfDec64:
		return &arrow.Decimal64Type{Precision: prec, Scale: scale}
	}
	return dt
}

func applyTemporalOpts(dt arrow.DataType, origType reflect.Type, opts tagOpts) arrow.DataType {
	if origType != typeOfTime || opts.Temporal == "" || opts.Temporal == "timestamp" {
		return dt
	}
	switch opts.Temporal {
	case "date32":
		return arrow.FixedWidthTypes.Date32
	case "date64":
		return arrow.FixedWidthTypes.Date64
	case "time32":
		return &arrow.Time32Type{Unit: arrow.Millisecond}
	case "time64":
		return &arrow.Time64Type{Unit: arrow.Nanosecond}
	}
	return dt
}

func applyLargeOpts(dt arrow.DataType) arrow.DataType {
	switch dt.ID() {
	case arrow.STRING:
		return arrow.BinaryTypes.LargeString
	case arrow.BINARY:
		return arrow.BinaryTypes.LargeBinary
	case arrow.LIST:
		return arrow.LargeListOf(applyLargeOpts(dt.(*arrow.ListType).Elem()))
	case arrow.LIST_VIEW:
		return arrow.LargeListViewOf(applyLargeOpts(dt.(*arrow.ListViewType).Elem()))
	case arrow.LARGE_LIST:
		return arrow.LargeListOf(applyLargeOpts(dt.(*arrow.LargeListType).Elem()))
	case arrow.LARGE_LIST_VIEW:
		return arrow.LargeListViewOf(applyLargeOpts(dt.(*arrow.LargeListViewType).Elem()))
	case arrow.FIXED_SIZE_LIST:
		fsl := dt.(*arrow.FixedSizeListType)
		return arrow.FixedSizeListOf(fsl.Len(), applyLargeOpts(fsl.Elem()))
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return arrow.MapOf(applyLargeOpts(mt.KeyType()), applyLargeOpts(mt.ItemField().Type))
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		fields := make([]arrow.Field, st.NumFields())
		for i := 0; i < st.NumFields(); i++ {
			f := st.Field(i)
			f.Type = applyLargeOpts(f.Type)
			fields[i] = f
		}
		return arrow.StructOf(fields...)
	default:
		return dt
	}
}

func hasLargeableType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.BINARY, arrow.LIST, arrow.LIST_VIEW:
		return true
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		for i := 0; i < st.NumFields(); i++ {
			if hasLargeableType(st.Field(i).Type) {
				return true
			}
		}
		return false
	case arrow.FIXED_SIZE_LIST:
		return hasLargeableType(dt.(*arrow.FixedSizeListType).Elem())
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return hasLargeableType(mt.KeyType()) || hasLargeableType(mt.ItemField().Type)
	default:
		return false
	}
}

func applyViewOpts(dt arrow.DataType) arrow.DataType {
	switch dt.ID() {
	case arrow.STRING:
		return arrow.BinaryTypes.StringView
	case arrow.BINARY:
		return arrow.BinaryTypes.BinaryView
	case arrow.LIST:
		return arrow.ListViewOf(applyViewOpts(dt.(*arrow.ListType).Elem()))
	case arrow.LIST_VIEW:
		return arrow.ListViewOf(applyViewOpts(dt.(*arrow.ListViewType).Elem()))
	case arrow.LARGE_LIST:
		return arrow.LargeListViewOf(applyViewOpts(dt.(*arrow.LargeListType).Elem()))
	case arrow.LARGE_LIST_VIEW:
		return arrow.LargeListViewOf(applyViewOpts(dt.(*arrow.LargeListViewType).Elem()))
	case arrow.FIXED_SIZE_LIST:
		fsl := dt.(*arrow.FixedSizeListType)
		return arrow.FixedSizeListOf(fsl.Len(), applyViewOpts(fsl.Elem()))
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return arrow.MapOf(applyViewOpts(mt.KeyType()), applyViewOpts(mt.ItemField().Type))
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		fields := make([]arrow.Field, st.NumFields())
		for i := 0; i < st.NumFields(); i++ {
			f := st.Field(i)
			f.Type = applyViewOpts(f.Type)
			fields[i] = f
		}
		return arrow.StructOf(fields...)
	default:
		return dt
	}
}

func hasViewableType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.BINARY, arrow.LIST:
		return true
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		for i := 0; i < st.NumFields(); i++ {
			if hasViewableType(st.Field(i).Type) {
				return true
			}
		}
		return false
	case arrow.FIXED_SIZE_LIST:
		return hasViewableType(dt.(*arrow.FixedSizeListType).Elem())
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return hasViewableType(mt.KeyType()) || hasViewableType(mt.ItemField().Type)
	default:
		return false
	}
}

func applyEncodingOpts(dt arrow.DataType, fm fieldMeta) (arrow.DataType, error) {
	switch {
	case fm.Opts.Dict:
		if err := validateDictValueType(dt); err != nil {
			return nil, fmt.Errorf("arreflect: dict tag on field %q: %w", fm.Name, err)
		}
		return &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: dt}, nil
	case fm.Opts.REE:
		return nil, fmt.Errorf("arreflect: ree tag on struct field %q is not supported; use ree at top-level via FromSlice", fm.Name)
	}
	return dt, nil
}

func inferStructType(t reflect.Type) (*arrow.StructType, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("arreflect: expected struct, got %v", t)
	}

	fields := cachedStructFields(t)
	arrowFields := make([]arrow.Field, 0, len(fields))

	for _, fm := range fields {
		if err := validateOptions(fm.Opts); err != nil {
			return nil, fmt.Errorf("struct field %q: %w", fm.Name, err)
		}
		origType := fm.Type
		for origType.Kind() == reflect.Ptr {
			origType = origType.Elem()
		}

		dt, err := inferArrowType(fm.Type)
		if err != nil {
			return nil, fmt.Errorf("struct field %q: %w", fm.Name, err)
		}

		dt = applyDecimalOpts(dt, origType, fm.Opts)
		dt = applyTemporalOpts(dt, origType, fm.Opts)
		if fm.Opts.Large {
			dt = applyLargeOpts(dt)
		}
		if fm.Opts.View {
			dt = applyViewOpts(dt)
		}
		dt, err = applyEncodingOpts(dt, fm)
		if err != nil {
			return nil, err
		}

		arrowFields = append(arrowFields, arrow.Field{
			Name:     fm.Name,
			Type:     dt,
			Nullable: fm.Nullable,
		})
	}

	return arrow.StructOf(arrowFields...), nil
}

// InferSchema infers an *arrow.Schema from a Go struct type T.
// T must be a struct type; returns an error otherwise.
// For column-level Arrow type inspection, use [InferType].
// Field names come from arrow struct tags or Go field names.
// Pointer fields are marked Nullable=true.
func InferSchema[T any]() (*arrow.Schema, error) {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("arreflect: InferSchema requires a struct type T, got %v", t)
	}
	st, err := inferStructType(t)
	if err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, st.NumFields())
	for i := 0; i < st.NumFields(); i++ {
		fields[i] = st.Field(i)
	}
	return arrow.NewSchema(fields, nil), nil
}

// InferType infers the Arrow DataType for a Go type T.
// For struct types, [InferSchema] is preferred when the result will be used with
// arrow.Record or array.NewRecord; InferType returns an arrow.DataType that would
// require an additional cast to *arrow.StructType.
func InferType[T any]() (arrow.DataType, error) {
	t := reflect.TypeFor[T]()
	return inferArrowType(t)
}

// InferGoType returns the Go reflect.Type corresponding to the given Arrow DataType.
// For STRUCT types it constructs an anonymous struct type at runtime using
// [reflect.StructOf]; field names are exported (capitalised) with the original
// Arrow field name preserved in an arrow struct tag. Nullable Arrow fields
// (field.Nullable == true) become pointer types (*T).
// For DICTIONARY and RUN_END_ENCODED types it returns the Go type of the
// value/encoded type respectively (dictionaries are resolved transparently).
func InferGoType(dt arrow.DataType) (reflect.Type, error) {
	switch dt.ID() {
	case arrow.INT8:
		return typeOfInt8, nil
	case arrow.INT16:
		return typeOfInt16, nil
	case arrow.INT32:
		return typeOfInt32, nil
	case arrow.INT64:
		return typeOfInt64, nil
	case arrow.UINT8:
		return typeOfUint8, nil
	case arrow.UINT16:
		return typeOfUint16, nil
	case arrow.UINT32:
		return typeOfUint32, nil
	case arrow.UINT64:
		return typeOfUint64, nil
	case arrow.FLOAT32:
		return typeOfFloat32, nil
	case arrow.FLOAT64:
		return typeOfFloat64, nil
	case arrow.BOOL:
		return typeOfBool, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return typeOfString, nil
	case arrow.BINARY, arrow.LARGE_BINARY:
		return typeOfByteSlice, nil
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64:
		return typeOfTime, nil
	case arrow.DURATION:
		return typeOfDuration, nil
	case arrow.DECIMAL128:
		return typeOfDec128, nil
	case arrow.DECIMAL256:
		return typeOfDec256, nil
	case arrow.DECIMAL32:
		return typeOfDec32, nil
	case arrow.DECIMAL64:
		return typeOfDec64, nil

	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		ll, ok := dt.(listElemTyper)
		if !ok {
			return nil, fmt.Errorf("unsupported Arrow type for Go inference: %v: %w", dt, ErrUnsupportedType)
		}
		elemDT := ll.Elem()
		elemType, err := InferGoType(elemDT)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil

	case arrow.FIXED_SIZE_LIST:
		fsl := dt.(*arrow.FixedSizeListType)
		elemType, err := InferGoType(fsl.Elem())
		if err != nil {
			return nil, err
		}
		return reflect.ArrayOf(int(fsl.Len()), elemType), nil

	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		keyType, err := InferGoType(mt.KeyType())
		if err != nil {
			return nil, err
		}
		if !keyType.Comparable() {
			return nil, fmt.Errorf("arreflect: InferGoType: MAP key type %v is not comparable in Go: %w", mt.KeyType(), ErrUnsupportedType)
		}
		valType, err := InferGoType(mt.ItemField().Type)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valType), nil

	case arrow.STRUCT:
		return inferGoStructType(dt.(*arrow.StructType))

	case arrow.DICTIONARY:
		return InferGoType(dt.(*arrow.DictionaryType).ValueType)

	case arrow.RUN_END_ENCODED:
		return InferGoType(dt.(*arrow.RunEndEncodedType).Encoded())

	default:
		return nil, fmt.Errorf("unsupported Arrow type for Go inference: %v: %w", dt, ErrUnsupportedType)
	}
}

func exportedFieldName(name string, index int) (string, error) {
	if len(name) == 0 {
		return fmt.Sprintf("Field%d", index), nil
	}
	runes := []rune(name)
	// If the first rune is not a letter (e.g. '_', digit), prefix with "X"
	// to produce a valid exported Go identifier while preserving the original
	// name in the struct tag.
	if !unicode.IsLetter(runes[0]) {
		runes = append([]rune{'X'}, runes...)
	} else {
		runes[0] = unicode.ToUpper(runes[0])
	}
	for j, r := range runes {
		if j == 0 {
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", fmt.Errorf("arreflect: InferGoType: field name %q produces invalid Go identifier: %w", name, ErrUnsupportedType)
		}
	}
	return string(runes), nil
}

func inferGoStructType(st *arrow.StructType) (reflect.Type, error) {
	fields := make([]reflect.StructField, st.NumFields())
	seen := make(map[string]string, st.NumFields())
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		ft, err := InferGoType(f.Type)
		if err != nil {
			return nil, err
		}
		if f.Nullable {
			ft = reflect.PointerTo(ft)
		}
		exportedName, err := exportedFieldName(f.Name, i)
		if err != nil {
			return nil, err
		}
		if origName, dup := seen[exportedName]; dup {
			return nil, fmt.Errorf("arreflect: InferGoType: field names %q and %q both export as %q: %w", origName, f.Name, exportedName, ErrUnsupportedType)
		}
		seen[exportedName] = f.Name
		fields[i] = reflect.StructField{
			Name: exportedName,
			Type: ft,
			Tag:  reflect.StructTag(fmt.Sprintf(`arrow:%q`, f.Name)),
		}
	}
	return reflect.StructOf(fields), nil
}
