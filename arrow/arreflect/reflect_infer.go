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
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}, nil
	case typeOfDec32:
		return &arrow.Decimal32Type{Precision: 9, Scale: 0}, nil
	case typeOfDec64:
		return &arrow.Decimal64Type{Precision: 18, Scale: 0}, nil
	case typeOfDec256:
		return &arrow.Decimal256Type{Precision: 76, Scale: 0}, nil
	default:
		return nil, fmt.Errorf("arreflect: unsupported Go type for Arrow inference: %v", t)
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
			return nil, fmt.Errorf("arreflect: struct field %q: %w", fm.Name, err)
		}

		if fm.Opts.HasDecimalOpts {
			switch origType {
			case typeOfDec32:
				dt = &arrow.Decimal32Type{Precision: fm.Opts.DecimalPrecision, Scale: fm.Opts.DecimalScale}
			case typeOfDec64:
				dt = &arrow.Decimal64Type{Precision: fm.Opts.DecimalPrecision, Scale: fm.Opts.DecimalScale}
			case typeOfDec128:
				dt = &arrow.Decimal128Type{
					Precision: fm.Opts.DecimalPrecision,
					Scale:     fm.Opts.DecimalScale,
				}
			case typeOfDec256:
				dt = &arrow.Decimal256Type{
					Precision: fm.Opts.DecimalPrecision,
					Scale:     fm.Opts.DecimalScale,
				}
			}
		}

		if origType == typeOfTime && fm.Opts.Temporal != "" {
			switch fm.Opts.Temporal {
			case "date32":
				dt = arrow.FixedWidthTypes.Date32
			case "date64":
				dt = arrow.FixedWidthTypes.Date64
			case "time32":
				dt = &arrow.Time32Type{Unit: arrow.Millisecond}
			case "time64":
				dt = &arrow.Time64Type{Unit: arrow.Nanosecond}
			}
		}

		switch {
		case fm.Opts.Dict:
			dt = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: dt}
		case fm.Opts.ListView:
			lt, ok := dt.(*arrow.ListType)
			if !ok {
				return nil, fmt.Errorf("arreflect: listview tag on field %q requires a slice type, got %v", fm.Name, dt)
			}
			dt = arrow.ListViewOf(lt.Elem())
		case fm.Opts.REE:
			return nil, fmt.Errorf("arreflect: ree tag on struct field %q is not supported; use ree at top-level via FromSlice", fm.Name)
		}

		arrowFields = append(arrowFields, arrow.Field{
			Name:     fm.Name,
			Type:     dt,
			Nullable: fm.Nullable,
		})
	}

	return arrow.StructOf(arrowFields...), nil
}

func SchemaOf[T any]() (*arrow.Schema, error) {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("arreflect: SchemaOf requires a struct type T, got %v", t)
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

func TypeOf[T any]() (arrow.DataType, error) {
	t := reflect.TypeFor[T]()
	return inferArrowType(t)
}
