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
)

const (
	dec32DefaultPrecision  int32 = 9
	dec64DefaultPrecision  int32 = 18
	dec128DefaultPrecision int32 = 38
	dec256DefaultPrecision int32 = 76
)

func inferPrimitiveArrowType(t reflect.Type) (arrow.DataType, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t {
	case reflect.TypeOf(int8(0)):
		return arrow.PrimitiveTypes.Int8, nil
	case reflect.TypeOf(int16(0)):
		return arrow.PrimitiveTypes.Int16, nil
	case reflect.TypeOf(int32(0)):
		return arrow.PrimitiveTypes.Int32, nil
	case reflect.TypeOf(int64(0)):
		return arrow.PrimitiveTypes.Int64, nil
	case typeOfInt:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.TypeOf(uint8(0)):
		return arrow.PrimitiveTypes.Uint8, nil
	case reflect.TypeOf(uint16(0)):
		return arrow.PrimitiveTypes.Uint16, nil
	case reflect.TypeOf(uint32(0)):
		return arrow.PrimitiveTypes.Uint32, nil
	case reflect.TypeOf(uint64(0)):
		return arrow.PrimitiveTypes.Uint64, nil
	case typeOfUint:
		return arrow.PrimitiveTypes.Uint64, nil
	case reflect.TypeOf(float32(0)):
		return arrow.PrimitiveTypes.Float32, nil
	case reflect.TypeOf(float64(0)):
		return arrow.PrimitiveTypes.Float64, nil
	case reflect.TypeOf(false):
		return arrow.FixedWidthTypes.Boolean, nil
	case reflect.TypeOf(""):
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

func applyEncodingOpts(dt arrow.DataType, fm fieldMeta) (arrow.DataType, error) {
	switch {
	case fm.Opts.Dict:
		return &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: dt}, nil
	case fm.Opts.ListView:
		lt, ok := dt.(*arrow.ListType)
		if !ok {
			return nil, fmt.Errorf("arreflect: listview tag on field %q requires a slice type, got %v", fm.Name, dt)
		}
		return arrow.ListViewOf(lt.Elem()), nil
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
		return reflect.TypeOf(int8(0)), nil
	case arrow.INT16:
		return reflect.TypeOf(int16(0)), nil
	case arrow.INT32:
		return reflect.TypeOf(int32(0)), nil
	case arrow.INT64:
		return reflect.TypeOf(int64(0)), nil
	case arrow.UINT8:
		return reflect.TypeOf(uint8(0)), nil
	case arrow.UINT16:
		return reflect.TypeOf(uint16(0)), nil
	case arrow.UINT32:
		return reflect.TypeOf(uint32(0)), nil
	case arrow.UINT64:
		return reflect.TypeOf(uint64(0)), nil
	case arrow.FLOAT32:
		return reflect.TypeOf(float32(0)), nil
	case arrow.FLOAT64:
		return reflect.TypeOf(float64(0)), nil
	case arrow.BOOL:
		return reflect.TypeOf(false), nil
	case arrow.STRING, arrow.LARGE_STRING:
		return reflect.TypeOf(""), nil
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
		var elemDT arrow.DataType
		switch t := dt.(type) {
		case *arrow.ListType:
			elemDT = t.Elem()
		case *arrow.LargeListType:
			elemDT = t.Elem()
		case *arrow.ListViewType:
			elemDT = t.Elem()
		case *arrow.LargeListViewType:
			elemDT = t.Elem()
		default:
			return nil, fmt.Errorf("unsupported Arrow type for Go inference: %v: %w", dt, ErrUnsupportedType)
		}
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
		valType, err := InferGoType(mt.ItemField().Type)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valType), nil

	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		fields := make([]reflect.StructField, st.NumFields())
		for i := 0; i < st.NumFields(); i++ {
			f := st.Field(i)
			ft, err := InferGoType(f.Type)
			if err != nil {
				return nil, err
			}
			if f.Nullable {
				ft = reflect.PointerTo(ft)
			}
			var exportedName string
			if len(f.Name) == 0 {
				exportedName = fmt.Sprintf("Field%d", i)
			} else {
				runes := []rune(f.Name)
				exportedName = string(unicode.ToUpper(runes[0])) + string(runes[1:])
			}
			fields[i] = reflect.StructField{
				Name: exportedName,
				Type: ft,
				Tag:  reflect.StructTag(fmt.Sprintf(`arrow:%q`, f.Name)),
			}
		}
		return reflect.StructOf(fields), nil

	case arrow.DICTIONARY:
		return InferGoType(dt.(*arrow.DictionaryType).ValueType)

	case arrow.RUN_END_ENCODED:
		return InferGoType(dt.(*arrow.RunEndEncodedType).Encoded())

	default:
		return nil, fmt.Errorf("unsupported Arrow type for Go inference: %v: %w", dt, ErrUnsupportedType)
	}
}
