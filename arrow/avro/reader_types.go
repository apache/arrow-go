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

package avro

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	avro "github.com/twmb/avro"
)

type dataLoader struct {
	fields []*fieldPos
}

var (
	ErrNullStructData = errors.New("null struct data")
)

func newDataLoader() *dataLoader { return &dataLoader{} }

// drawTree keeps the top-level fields of the tree of field builders produced
// by mapFieldBuilders(). The appendFunc of a list, map, or struct field appends
// the items, keys and values, or fields under it.
func (d *dataLoader) drawTree(field *fieldPos) { d.fields = field.childrens }

// loadDatum appends each top-level field of a decoded Avro record.
func (d *dataLoader) loadDatum(data any) error {
	record, ok := data.(map[string]any)
	if !ok {
		return fmt.Errorf("avro: unsupported value of type %T for a record", data)
	}
	for _, f := range d.fields {
		if err := appendValue(f, record[f.fieldName]); err != nil {
			return err
		}
	}
	return nil
}

// appendValue invokes the field's appendFunc and propagates any error, while
// treating ErrNullStructData as a non-error signal. A struct builder returns
// ErrNullStructData when it receives nil to indicate that its sub-fields have
// already been null-filled and can be skipped; that is expected here and must
// not abort the load.
func appendValue(f *fieldPos, data any) error {
	if err := f.appendFunc(data); err != nil && err != ErrNullStructData {
		return err
	}
	return nil
}

type fieldPos struct {
	fieldName  string
	appendFunc func(val interface{}) error
	childrens  []*fieldPos
}

func newFieldPos() *fieldPos { return &fieldPos{} }

func (f *fieldPos) newChild(childName string) *fieldPos {
	child := &fieldPos{fieldName: childName}
	f.childrens = append(f.childrens, child)
	return child
}

// Avro data is loaded to Arrow arrays using the following type mapping:
//
//	Avro					Go    			Arrow
//	null					nil				Null
//	boolean					bool			Boolean
//	bytes					[]byte			Binary
//	float					float32			Float32
//	double					float64			Float64
//	long					int64			Int64
//	int						int32  			Int32
//	string					string			String
//	array					[]interface{}	List
//	enum					string			Dictionary
//	fixed					[]byte			FixedSizeBinary
//	map						map[string]any	Map
//	record					map[string]any	Struct
//
// mapFieldBuilders builds a tree of field builders matching the Arrow schema
func mapFieldBuilders(b array.Builder, field arrow.Field, parent *fieldPos) {
	f := parent.newChild(field.Name)
	switch bt := b.(type) {
	case *array.BinaryBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendBinaryData(bt, data)
		}
	case *array.BinaryDictionaryBuilder:
		// has metadata for Avro enum symbols
		f.appendFunc = func(data interface{}) error {
			return appendBinaryDictData(bt, data)
		}
		// add Avro enum symbols to builder
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for _, v := range field.Metadata.Values() {
			sb.Append(v)
		}
		sa := sb.NewStringArray()
		bt.InsertStringDictValues(sa)
	case *array.BooleanBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendBoolData(bt, data)
		}
	case *array.Date32Builder:
		f.appendFunc = func(data interface{}) error {
			return appendDate32Data(bt, data)
		}
	case *array.Decimal128Builder:
		f.appendFunc = func(data interface{}) error {
			typ, ok := field.Type.(arrow.DecimalType)
			if !ok {
				return nil
			}
			return appendDecimal128Data(bt, data, typ)
		}
	case *array.Decimal256Builder:
		f.appendFunc = func(data interface{}) error {
			typ, ok := field.Type.(arrow.DecimalType)
			if !ok {
				return nil
			}
			return appendDecimal256Data(bt, data, typ)
		}
	case *extensions.UUIDBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendUUIDData(bt, data, field.Name)
		}
	case *array.FixedSizeBinaryBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendFixedSizeBinaryData(bt, data)
		}
	case *array.Float32Builder:
		f.appendFunc = func(data interface{}) error {
			return appendFloat32Data(bt, data)
		}
	case *array.Float64Builder:
		f.appendFunc = func(data interface{}) error {
			return appendFloat64Data(bt, data)
		}
	case *array.Int32Builder:
		f.appendFunc = func(data interface{}) error {
			return appendInt32Data(bt, data)
		}
	case *array.Int64Builder:
		f.appendFunc = func(data interface{}) error {
			return appendInt64Data(bt, data)
		}
	case *array.LargeListBuilder:
		vb := bt.ValueBuilder()
		mapFieldBuilders(vb, field.Type.(*arrow.LargeListType).ElemField(), f)
		item := f.childrens[0]
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case []interface{}:
				if len(dt) == 0 {
					bt.AppendEmptyValue()
				} else {
					bt.Append(true)
				}
				for _, e := range dt {
					if err := appendValue(item, e); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("avro: unsupported value of type %T for LargeList column", data)
			}
			return nil
		}
	case *array.ListBuilder:
		vb := bt.ValueBuilder()
		mapFieldBuilders(vb, field.Type.(*arrow.ListType).ElemField(), f)
		item := f.childrens[0]
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case []interface{}:
				if len(dt) == 0 {
					bt.AppendEmptyValue()
				} else {
					bt.Append(true)
				}
				for _, e := range dt {
					if err := appendValue(item, e); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("avro: unsupported value of type %T for List column", data)
			}
			return nil
		}
	case *array.MapBuilder:
		kb := bt.KeyBuilder()
		ib := bt.ItemBuilder()
		mapFieldBuilders(kb, field.Type.(*arrow.MapType).KeyField(), f)
		mapFieldBuilders(ib, field.Type.(*arrow.MapType).ItemField(), f)
		key, value := f.childrens[0], f.childrens[1]
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case map[string]any:
				bt.Append(true)
				for k, v := range dt {
					if err := appendValue(key, k); err != nil {
						return err
					}
					if err := appendValue(value, v); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("avro: unsupported value of type %T for Map column", data)
			}
			return nil
		}
	case *array.MonthDayNanoIntervalBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendDurationData(bt, data)
		}
	case *array.StringBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendStringData(bt, data)
		}
	case *array.StructBuilder:
		// create children
		for i, p := range field.Type.(*arrow.StructType).Fields() {
			mapFieldBuilders(bt.FieldBuilder(i), p, f)
		}
		f.appendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				// AppendNull appends a null to every field under the struct.
				bt.AppendNull()
				return ErrNullStructData
			case map[string]any:
				bt.Append(true)
				for _, c := range f.childrens {
					if err := appendValue(c, dt[c.fieldName]); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("avro: unsupported value of type %T for Struct column", data)
			}
			return nil
		}
	case *array.Time32Builder:
		f.appendFunc = func(data interface{}) error {
			return appendTime32Data(bt, data)
		}
	case *array.Time64Builder:
		f.appendFunc = func(data interface{}) error {
			return appendTime64Data(bt, data)
		}
	case *array.TimestampBuilder:
		f.appendFunc = func(data interface{}) error {
			return appendTimestampData(bt, data)
		}
	default:
		f.appendFunc = func(interface{}) error {
			return fmt.Errorf("avro: unsupported Arrow type %s for column %q", field.Type, field.Name)
		}
	}
}

// appendUUIDData accepts the two shapes a UUID may arrive as: a [16]byte
// (fixed(16)+uuid) or a hex-dash string (string+uuid). Other byte lengths
// are rejected rather than re-interpreted.
func appendUUIDData(b *extensions.UUIDBuilder, data any, fieldName string) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		return b.AppendValueFromString(dt)
	case [16]byte:
		b.AppendBytes(dt)
	case []byte:
		if len(dt) != 16 {
			return fmt.Errorf("avro: %d-byte value cannot be a UUID for column %q", len(dt), fieldName)
		}
		b.AppendBytes([16]byte(dt))
	default:
		return fmt.Errorf("avro: unsupported value of type %T for UUID column %q", data, fieldName)
	}
	return nil
}

func appendBinaryData(b *array.BinaryBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Binary column", data)
	}
	return nil
}

func appendBinaryDictData(b *array.BinaryDictionaryBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		if err := b.AppendString(dt); err != nil {
			return fmt.Errorf("avro: enum symbol %q is not in the dictionary (schema/data mismatch?): %w", dt, err)
		}
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Dictionary column", data)
	}
	return nil
}

func appendBoolData(b *array.BooleanBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case bool:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Boolean column", data)
	}
	return nil
}

func appendDate32Data(b *array.Date32Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case time.Time:
		b.Append(arrow.Date32FromTime(dt))
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Date32 column", data)
	}
	return nil
}

func appendDecimal128Data(b *array.Decimal128Builder, data interface{}, typ arrow.DecimalType) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case *big.Rat:
		v := bigRatToBigInt(dt, typ)
		if v.IsInt64() {
			b.Append(decimal128.FromI64(v.Int64()))
		} else {
			b.Append(decimal128.FromBigInt(v))
		}
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Decimal128 column", data)
	}
	return nil
}

func appendDecimal256Data(b *array.Decimal256Builder, data interface{}, typ arrow.DecimalType) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case *big.Rat:
		b.Append(decimal.Decimal256FromBigInt(bigRatToBigInt(dt, typ)))
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Decimal256 column", data)
	}
	return nil
}

func bigRatToBigInt(dt *big.Rat, typ arrow.DecimalType) *big.Int {
	scale := big.NewInt(int64(typ.GetScale()))
	scaledNum := new(big.Int).Set(dt.Num())
	scaleFactor := new(big.Int).Exp(big.NewInt(10), scale, nil)
	scaledNum.Mul(scaledNum, scaleFactor)
	scaledNum.Quo(scaledNum, dt.Denom())
	return scaledNum
}

// Avro duration logical type annotates Avro fixed type of size 12, which stores three little-endian
// unsigned integers that represent durations at different granularities of time. The first stores
// a number in months, the second stores a number in days, and the third stores a number in milliseconds.
func appendDurationData(b *array.MonthDayNanoIntervalBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case avro.Duration:
		b.Append(arrow.MonthDayNanoInterval{
			Months:      int32(dt.Months),
			Days:        int32(dt.Days),
			Nanoseconds: int64(dt.Milliseconds) * int64(time.Millisecond),
		})
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Duration column", data)
	}
	return nil
}

func appendFixedSizeBinaryData(b *array.FixedSizeBinaryBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		b.Append(dt)
	default:
		// fixed(N) may arrive as a Go [N]byte; accept any byte-array via reflection.
		v := reflect.ValueOf(data)
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			buf := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(buf), v)
			b.Append(buf)
			return nil
		}
		return fmt.Errorf("avro: unsupported value of type %T for FixedSizeBinary column", data)
	}
	return nil
}

func appendFloat32Data(b *array.Float32Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float32:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Float32 column", data)
	}
	return nil
}

func appendFloat64Data(b *array.Float64Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float64:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Float64 column", data)
	}
	return nil
}

func appendInt32Data(b *array.Int32Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int32:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Int32 column", data)
	}
	return nil
}

func appendInt64Data(b *array.Int64Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int64:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Int64 column", data)
	}
	return nil
}

func appendStringData(b *array.StringBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		b.Append(dt)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for String column", data)
	}
	return nil
}

func appendTime32Data(b *array.Time32Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case time.Duration:
		b.Append(arrow.Time32(dt.Milliseconds()))
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Time32 column", data)
	}
	return nil
}

func appendTime64Data(b *array.Time64Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case time.Duration:
		b.Append(arrow.Time64(dt.Microseconds()))
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Time64 column", data)
	}
	return nil
}

func appendTimestampData(b *array.TimestampBuilder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case time.Time:
		v, err := arrow.TimestampFromTime(dt, b.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			return err
		}
		b.Append(v)
	default:
		return fmt.Errorf("avro: unsupported value of type %T for Timestamp column", data)
	}
	return nil
}
