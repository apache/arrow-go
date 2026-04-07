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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func buildArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	if vals.Kind() != reflect.Slice {
		return nil, fmt.Errorf("arreflect: expected slice, got %v", vals.Kind())
	}

	elemType := vals.Type().Elem()
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if opts.Dict {
		return buildDictionaryArray(vals, mem)
	}
	if opts.REE {
		return buildRunEndEncodedArray(vals, mem)
	}
	if opts.ListView {
		return buildListViewArray(vals, mem)
	}

	switch elemType.Kind() {
	case reflect.Slice:
		if elemType == typeOfByteSlice {
			return buildPrimitiveArray(vals, mem)
		}
		return buildListArray(vals, mem)

	case reflect.Array:
		return buildFixedSizeListArray(vals, mem)

	case reflect.Map:
		return buildMapArray(vals, mem)

	case reflect.Struct:
		switch elemType {
		case typeOfTime:
			return buildTemporalArray(vals, opts, mem)
		case typeOfDuration:
			return buildTemporalArray(vals, opts, mem)
		case typeOfDec128:
			return buildDecimalArray(vals, opts, mem)
		case typeOfDec256:
			return buildDecimalArray(vals, opts, mem)
		default:
			return buildStructArray(vals, mem)
		}

	default:
		if elemType == typeOfDec32 || elemType == typeOfDec64 {
			return buildDecimalArray(vals, opts, mem)
		}
		return buildPrimitiveArray(vals, mem)
	}
}

func buildPrimitiveArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	dt, err := inferArrowType(elemType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.Reserve(vals.Len())

	for i := 0; i < vals.Len(); i++ {
		v := vals.Index(i)
		if isPtr {
			if v.IsNil() {
				b.AppendNull()
				continue
			}
			v = v.Elem()
		}
		if err := appendPrimitiveValue(b, v, dt); err != nil {
			return nil, err
		}
	}

	return b.NewArray(), nil
}

func appendPrimitiveValue(b array.Builder, v reflect.Value, dt arrow.DataType) error {
	switch dt.ID() {
	case arrow.INT8:
		b.(*array.Int8Builder).Append(int8(v.Int()))
	case arrow.INT16:
		b.(*array.Int16Builder).Append(int16(v.Int()))
	case arrow.INT32:
		b.(*array.Int32Builder).Append(int32(v.Int()))
	case arrow.INT64:
		b.(*array.Int64Builder).Append(int64(v.Int()))
	case arrow.UINT8:
		b.(*array.Uint8Builder).Append(uint8(v.Uint()))
	case arrow.UINT16:
		b.(*array.Uint16Builder).Append(uint16(v.Uint()))
	case arrow.UINT32:
		b.(*array.Uint32Builder).Append(uint32(v.Uint()))
	case arrow.UINT64:
		b.(*array.Uint64Builder).Append(uint64(v.Uint()))
	case arrow.FLOAT32:
		b.(*array.Float32Builder).Append(float32(v.Float()))
	case arrow.FLOAT64:
		b.(*array.Float64Builder).Append(float64(v.Float()))
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(v.Bool())
	case arrow.STRING:
		b.(*array.StringBuilder).Append(v.String())
	case arrow.BINARY:
		b.(*array.BinaryBuilder).Append(v.Bytes())
	case arrow.DURATION:
		d, _ := reflect.TypeAssert[time.Duration](v)
		b.(*array.DurationBuilder).Append(arrow.Duration(d.Nanoseconds()))
	case arrow.DECIMAL128:
		n, _ := reflect.TypeAssert[decimal128.Num](v)
		b.(*array.Decimal128Builder).Append(n)
	case arrow.DECIMAL256:
		n, _ := reflect.TypeAssert[decimal256.Num](v)
		b.(*array.Decimal256Builder).Append(n)
	case arrow.DECIMAL32:
		b.(*array.Decimal32Builder).Append(decimal.Decimal32(v.Int()))
	case arrow.DECIMAL64:
		b.(*array.Decimal64Builder).Append(decimal.Decimal64(v.Int()))
	default:
		return fmt.Errorf("arreflect: unsupported Arrow type %v", dt)
	}
	return nil
}

func timeOfDayNanos(t time.Time) int64 {
	t = t.UTC()
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return t.Sub(midnight).Nanoseconds()
}

func derefSliceElem(vals reflect.Value) (elemType reflect.Type, isPtr bool) {
	elemType = vals.Type().Elem()
	isPtr = elemType.Kind() == reflect.Ptr
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	return
}

func iterSlice(vals reflect.Value, isPtr bool, appendNull func(), appendVal func(reflect.Value)) {
	for i := 0; i < vals.Len(); i++ {
		v := vals.Index(i)
		if isPtr {
			if v.IsNil() {
				appendNull()
				continue
			}
			v = v.Elem()
		}
		appendVal(v)
	}
}

func listBuildPreamble(vals reflect.Value) (elemDT arrow.DataType, isOuterPtr bool, err error) {
	outerSliceType, isOuterPtr := derefSliceElem(vals)
	innerElemType := outerSliceType.Elem()
	for innerElemType.Kind() == reflect.Ptr {
		innerElemType = innerElemType.Elem()
	}
	elemDT, err = inferArrowType(innerElemType)
	return
}

func buildTemporalArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	switch elemType {
	case typeOfTime:
		switch opts.Temporal {
		case "date32":
			b := array.NewDate32Builder(mem)
			defer b.Release()
			b.Reserve(vals.Len())
			iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
				t, _ := reflect.TypeAssert[time.Time](v)
				b.Append(arrow.Date32FromTime(t))
			})
			return b.NewArray(), nil
		case "date64":
			b := array.NewDate64Builder(mem)
			defer b.Release()
			b.Reserve(vals.Len())
			iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
				t, _ := reflect.TypeAssert[time.Time](v)
				b.Append(arrow.Date64FromTime(t))
			})
			return b.NewArray(), nil
		case "time32":
			dt := &arrow.Time32Type{Unit: arrow.Millisecond}
			b := array.NewTime32Builder(mem, dt)
			defer b.Release()
			b.Reserve(vals.Len())
			iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
				t, _ := reflect.TypeAssert[time.Time](v)
				b.Append(arrow.Time32(timeOfDayNanos(t) / int64(dt.Unit.Multiplier())))
			})
			return b.NewArray(), nil
		case "time64":
			dt := &arrow.Time64Type{Unit: arrow.Nanosecond}
			b := array.NewTime64Builder(mem, dt)
			defer b.Release()
			b.Reserve(vals.Len())
			iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
				t, _ := reflect.TypeAssert[time.Time](v)
				b.Append(arrow.Time64(timeOfDayNanos(t) / int64(dt.Unit.Multiplier())))
			})
			return b.NewArray(), nil
		default:
			dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
			tb := array.NewTimestampBuilder(mem, dt)
			defer tb.Release()
			tb.Reserve(vals.Len())
			iterSlice(vals, isPtr, tb.AppendNull, func(v reflect.Value) {
				t, _ := reflect.TypeAssert[time.Time](v)
				tb.Append(arrow.Timestamp(t.UnixNano()))
			})
			return tb.NewArray(), nil
		}

	case typeOfDuration:
		dt := &arrow.DurationType{Unit: arrow.Nanosecond}
		db := array.NewDurationBuilder(mem, dt)
		defer db.Release()
		db.Reserve(vals.Len())
		iterSlice(vals, isPtr, db.AppendNull, func(v reflect.Value) {
			d, _ := reflect.TypeAssert[time.Duration](v)
			db.Append(arrow.Duration(d.Nanoseconds()))
		})
		return db.NewArray(), nil

	default:
		return nil, fmt.Errorf("arreflect: unsupported temporal type %v", elemType)
	}
}

func buildDecimalArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	switch elemType {
	case typeOfDec128:
		precision, scale := dec128DefaultPrecision, int32(0)
		if opts.HasDecimalOpts {
			precision = opts.DecimalPrecision
			scale = opts.DecimalScale
		}
		dt := &arrow.Decimal128Type{Precision: precision, Scale: scale}
		b := array.NewDecimal128Builder(mem, dt)
		defer b.Release()
		b.Reserve(vals.Len())
		iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
			n, _ := reflect.TypeAssert[decimal128.Num](v)
			b.Append(n)
		})
		return b.NewArray(), nil

	case typeOfDec256:
		precision, scale := dec256DefaultPrecision, int32(0)
		if opts.HasDecimalOpts {
			precision = opts.DecimalPrecision
			scale = opts.DecimalScale
		}
		dt := &arrow.Decimal256Type{Precision: precision, Scale: scale}
		b := array.NewDecimal256Builder(mem, dt)
		defer b.Release()
		b.Reserve(vals.Len())
		iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
			n, _ := reflect.TypeAssert[decimal256.Num](v)
			b.Append(n)
		})
		return b.NewArray(), nil

	case typeOfDec32:
		precision, scale := dec32DefaultPrecision, int32(0)
		if opts.HasDecimalOpts {
			precision = opts.DecimalPrecision
			scale = opts.DecimalScale
		}
		dt := &arrow.Decimal32Type{Precision: precision, Scale: scale}
		b := array.NewDecimal32Builder(mem, dt)
		defer b.Release()
		b.Reserve(vals.Len())
		iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
			b.Append(decimal.Decimal32(v.Int()))
		})
		return b.NewArray(), nil

	case typeOfDec64:
		precision, scale := dec64DefaultPrecision, int32(0)
		if opts.HasDecimalOpts {
			precision = opts.DecimalPrecision
			scale = opts.DecimalScale
		}
		dt := &arrow.Decimal64Type{Precision: precision, Scale: scale}
		b := array.NewDecimal64Builder(mem, dt)
		defer b.Release()
		b.Reserve(vals.Len())
		iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) {
			b.Append(decimal.Decimal64(v.Int()))
		})
		return b.NewArray(), nil

	default:
		return nil, fmt.Errorf("arreflect: unsupported decimal type %v", elemType)
	}
}

func buildStructArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	elemType := vals.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	st, err := inferStructType(elemType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	fields := cachedStructFields(elemType)
	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()
	sb.Reserve(vals.Len())

	for i := 0; i < vals.Len(); i++ {
		v := vals.Index(i)
		if isPtr {
			if v.IsNil() {
				sb.AppendNull()
				continue
			}
			v = v.Elem()
		}
		sb.Append(true)
		for fi, fm := range fields {
			fv := v.FieldByIndex(fm.Index)
			fb := sb.FieldBuilder(fi)
			if err := appendValue(fb, fv, fm.Opts); err != nil {
				return nil, fmt.Errorf("arreflect: struct field %q: %w", fm.Name, err)
			}
		}
	}

	return sb.NewArray(), nil
}

func appendValue(b array.Builder, v reflect.Value, opts tagOpts) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			b.AppendNull()
			return nil
		}
		v = v.Elem()
	}

	switch tb := b.(type) {
	case *array.Int8Builder:
		tb.Append(int8(v.Int()))
	case *array.Int16Builder:
		tb.Append(int16(v.Int()))
	case *array.Int32Builder:
		tb.Append(int32(v.Int()))
	case *array.Int64Builder:
		tb.Append(int64(v.Int()))
	case *array.Uint8Builder:
		tb.Append(uint8(v.Uint()))
	case *array.Uint16Builder:
		tb.Append(uint16(v.Uint()))
	case *array.Uint32Builder:
		tb.Append(uint32(v.Uint()))
	case *array.Uint64Builder:
		tb.Append(uint64(v.Uint()))
	case *array.Float32Builder:
		tb.Append(float32(v.Float()))
	case *array.Float64Builder:
		tb.Append(float64(v.Float()))
	case *array.BooleanBuilder:
		tb.Append(v.Bool())
	case *array.StringBuilder:
		tb.Append(v.String())
	case *array.BinaryBuilder:
		if v.IsNil() {
			tb.AppendNull()
		} else {
			tb.Append(v.Bytes())
		}
	case *array.TimestampBuilder:
		t, _ := reflect.TypeAssert[time.Time](v)
		tb.Append(arrow.Timestamp(t.UnixNano()))
	case *array.Date32Builder:
		t, _ := reflect.TypeAssert[time.Time](v)
		tb.Append(arrow.Date32FromTime(t))
	case *array.Date64Builder:
		t, _ := reflect.TypeAssert[time.Time](v)
		tb.Append(arrow.Date64FromTime(t))
	case *array.Time32Builder:
		unit := tb.Type().(*arrow.Time32Type).Unit
		t, _ := reflect.TypeAssert[time.Time](v)
		tb.Append(arrow.Time32(timeOfDayNanos(t) / int64(unit.Multiplier())))
	case *array.Time64Builder:
		unit := tb.Type().(*arrow.Time64Type).Unit
		t, _ := reflect.TypeAssert[time.Time](v)
		tb.Append(arrow.Time64(timeOfDayNanos(t) / int64(unit.Multiplier())))
	case *array.DurationBuilder:
		d, _ := reflect.TypeAssert[time.Duration](v)
		tb.Append(arrow.Duration(d.Nanoseconds()))
	case *array.Decimal128Builder:
		n, _ := reflect.TypeAssert[decimal128.Num](v)
		tb.Append(n)
	case *array.Decimal256Builder:
		n, _ := reflect.TypeAssert[decimal256.Num](v)
		tb.Append(n)
	case *array.Decimal32Builder:
		tb.Append(decimal.Decimal32(v.Int()))
	case *array.Decimal64Builder:
		tb.Append(decimal.Decimal64(v.Int()))
	case *array.ListBuilder:
		if v.Kind() == reflect.Slice && v.IsNil() {
			tb.AppendNull()
		} else {
			tb.Append(true)
			vb := tb.ValueBuilder()
			for i := 0; i < v.Len(); i++ {
				if err := appendValue(vb, v.Index(i), tagOpts{}); err != nil {
					return err
				}
			}
		}
	case *array.FixedSizeListBuilder:
		expectedLen := int(tb.Type().(*arrow.FixedSizeListType).Len())
		if v.Len() != expectedLen {
			return fmt.Errorf("arreflect: fixed-size list length mismatch: got %d, want %d", v.Len(), expectedLen)
		}
		tb.Append(true)
		vb := tb.ValueBuilder()
		for i := 0; i < v.Len(); i++ {
			if err := appendValue(vb, v.Index(i), tagOpts{}); err != nil {
				return err
			}
		}
	case *array.MapBuilder:
		if v.IsNil() {
			tb.AppendNull()
		} else {
			tb.Append(true)
			kb := tb.KeyBuilder()
			ib := tb.ItemBuilder()
			for _, key := range v.MapKeys() {
				if err := appendValue(kb, key, tagOpts{}); err != nil {
					return err
				}
				if err := appendValue(ib, v.MapIndex(key), tagOpts{}); err != nil {
					return err
				}
			}
		}
	case *array.StructBuilder:
		elemType := v.Type()
		fields := cachedStructFields(elemType)
		tb.Append(true)
		for fi, fm := range fields {
			fv := v.FieldByIndex(fm.Index)
			fb := tb.FieldBuilder(fi)
			if err := appendValue(fb, fv, fm.Opts); err != nil {
				return fmt.Errorf("arreflect: struct field %q: %w", fm.Name, err)
			}
		}
	case *array.ListViewBuilder:
		if v.Kind() == reflect.Slice && v.IsNil() {
			tb.AppendNull()
		} else {
			tb.AppendWithSize(true, v.Len())
			vb := tb.ValueBuilder()
			for i := 0; i < v.Len(); i++ {
				if err := appendValue(vb, v.Index(i), tagOpts{}); err != nil {
					return err
				}
			}
		}
	case *array.RunEndEncodedBuilder:
		if v.Kind() == reflect.Ptr && v.IsNil() {
			tb.AppendNull()
		} else {
			if err := appendValue(tb.ValueBuilder(), v, tagOpts{}); err != nil {
				return err
			}
			tb.Append(1)
		}
	default:
		if db, ok := b.(array.DictionaryBuilder); ok {
			return appendToDictBuilder(db, v)
		}
		return fmt.Errorf("arreflect: unsupported builder type %T", b)
	}
	return nil
}

func appendToDictBuilder(db array.DictionaryBuilder, v reflect.Value) error {
	switch bdb := db.(type) {
	case *array.BinaryDictionaryBuilder:
		switch v.Kind() {
		case reflect.String:
			return bdb.AppendString(v.String())
		case reflect.Slice:
			if v.IsNil() {
				bdb.AppendNull()
				return nil
			}
			return bdb.Append(v.Bytes())
		default:
			return fmt.Errorf("arreflect: unsupported value kind %v for BinaryDictionaryBuilder", v.Kind())
		}
	case *array.Int8DictionaryBuilder:
		return bdb.Append(int8(v.Int()))
	case *array.Int16DictionaryBuilder:
		return bdb.Append(int16(v.Int()))
	case *array.Int32DictionaryBuilder:
		return bdb.Append(int32(v.Int()))
	case *array.Int64DictionaryBuilder:
		return bdb.Append(int64(v.Int()))
	case *array.Uint8DictionaryBuilder:
		return bdb.Append(uint8(v.Uint()))
	case *array.Uint16DictionaryBuilder:
		return bdb.Append(uint16(v.Uint()))
	case *array.Uint32DictionaryBuilder:
		return bdb.Append(uint32(v.Uint()))
	case *array.Uint64DictionaryBuilder:
		return bdb.Append(uint64(v.Uint()))
	case *array.Float32DictionaryBuilder:
		return bdb.Append(float32(v.Float()))
	case *array.Float64DictionaryBuilder:
		return bdb.Append(float64(v.Float()))
	}
	return fmt.Errorf("arreflect: unsupported builder type %T", db)
}

func buildListArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	elemDT, isOuterPtr, err := listBuildPreamble(vals)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	lb := array.NewListBuilder(mem, elemDT)
	defer lb.Release()

	vb := lb.ValueBuilder()

	for i := 0; i < vals.Len(); i++ {
		outer := vals.Index(i)
		if isOuterPtr {
			if outer.IsNil() {
				lb.AppendNull()
				continue
			}
			outer = outer.Elem()
		}
		if outer.IsNil() {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for j := 0; j < outer.Len(); j++ {
			if err := appendValue(vb, outer.Index(j), tagOpts{}); err != nil {
				return nil, fmt.Errorf("arreflect: list element [%d][%d]: %w", i, j, err)
			}
		}
	}

	return lb.NewArray(), nil
}

func buildMapArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	mapType := vals.Type().Elem()
	isPtr := mapType.Kind() == reflect.Ptr
	for mapType.Kind() == reflect.Ptr {
		mapType = mapType.Elem()
	}

	keyType := mapType.Key()
	valType := mapType.Elem()

	for keyType.Kind() == reflect.Ptr {
		keyType = keyType.Elem()
	}
	for valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}

	keyDT, err := inferArrowType(keyType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: map key type: %w", err)
	}
	valDT, err := inferArrowType(valType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: map value type: %w", err)
	}

	mb := array.NewMapBuilder(mem, keyDT, valDT, false)
	defer mb.Release()

	kb := mb.KeyBuilder()
	ib := mb.ItemBuilder()

	for i := 0; i < vals.Len(); i++ {
		m := vals.Index(i)
		if isPtr {
			if m.IsNil() {
				mb.AppendNull()
				continue
			}
			m = m.Elem()
		}
		if m.IsNil() {
			mb.AppendNull()
			continue
		}
		mb.Append(true)
		for _, key := range m.MapKeys() {
			if err := appendValue(kb, key, tagOpts{}); err != nil {
				return nil, fmt.Errorf("arreflect: map key: %w", err)
			}
			if err := appendValue(ib, m.MapIndex(key), tagOpts{}); err != nil {
				return nil, fmt.Errorf("arreflect: map value: %w", err)
			}
		}
	}

	return mb.NewArray(), nil
}

func buildFixedSizeListArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	elemType := vals.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Array {
		return nil, fmt.Errorf("arreflect: expected array element, got %v", elemType.Kind())
	}

	n := int32(elemType.Len())
	innerElemType := elemType.Elem()
	for innerElemType.Kind() == reflect.Ptr {
		innerElemType = innerElemType.Elem()
	}

	innerDT, err := inferArrowType(innerElemType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	fb := array.NewFixedSizeListBuilder(mem, n, innerDT)
	defer fb.Release()

	vb := fb.ValueBuilder()

	for i := 0; i < vals.Len(); i++ {
		elem := vals.Index(i)
		if isPtr {
			if elem.IsNil() {
				fb.AppendNull()
				continue
			}
			elem = elem.Elem()
		}
		fb.Append(true)
		for j := 0; j < int(n); j++ {
			if err := appendValue(vb, elem.Index(j), tagOpts{}); err != nil {
				return nil, fmt.Errorf("arreflect: fixed-size list element [%d][%d]: %w", i, j, err)
			}
		}
	}

	return fb.NewArray(), nil
}

func buildDictionaryArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	n := vals.Len()
	elemType, isPtr := derefSliceElem(vals)

	valDT, err := inferArrowType(elemType)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	dt := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: valDT,
	}
	db := array.NewDictionaryBuilder(mem, dt)
	defer db.Release()

	for i := 0; i < n; i++ {
		elem := vals.Index(i)
		if isPtr {
			if elem.IsNil() {
				db.AppendNull()
				continue
			}
			elem = elem.Elem()
		}
		if err := appendToDictBuilder(db, elem); err != nil {
			return nil, fmt.Errorf("arreflect: dictionary element [%d]: %w", i, err)
		}
	}
	return db.NewArray(), nil
}

func buildRunEndEncodedArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	if vals.Len() == 0 {
		runEndsArr, err := buildPrimitiveArray(reflect.MakeSlice(reflect.TypeOf([]int32{}), 0, 0), mem)
		if err != nil {
			return nil, err
		}
		defer runEndsArr.Release()
		valuesArr, err := buildPrimitiveArray(reflect.MakeSlice(vals.Type(), 0, 0), mem)
		if err != nil {
			return nil, err
		}
		defer valuesArr.Release()
		return array.NewRunEndEncodedArray(runEndsArr, valuesArr, 0, 0), nil
	}

	type run struct {
		end int32
		val reflect.Value
	}

	// For comparable element types use reflect.Value.Equal (fast, avoids boxing).
	// For non-comparable types (e.g. slices, maps) fall back to reflect.DeepEqual,
	// which handles structural equality but cannot compress runs of function values.
	elemType := vals.Type().Elem()
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	comparable := elemType.Comparable()

	equal := func(a, b reflect.Value) bool {
		if comparable {
			da, db := a, b
			for da.Kind() == reflect.Ptr {
				if da.IsNil() || db.IsNil() {
					return da.IsNil() && db.IsNil()
				}
				da, db = da.Elem(), db.Elem()
			}
			return da.Equal(db)
		}
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}

	var runs []run
	current := vals.Index(0)
	for i := 1; i < vals.Len(); i++ {
		next := vals.Index(i)
		if !equal(current, next) {
			runs = append(runs, run{end: int32(i), val: current})
			current = next
		}
	}
	runs = append(runs, run{end: int32(vals.Len()), val: current})

	runEnds := make([]int32, len(runs))
	for i, r := range runs {
		runEnds[i] = r.end
	}
	runEndsSlice := reflect.ValueOf(runEnds)
	runEndsArr, err := buildPrimitiveArray(runEndsSlice, mem)
	if err != nil {
		return nil, fmt.Errorf("arreflect: run-end encoded run ends: %w", err)
	}
	defer runEndsArr.Release()

	runValues := reflect.MakeSlice(vals.Type(), len(runs), len(runs))
	for i, r := range runs {
		runValues.Index(i).Set(r.val)
	}
	valuesArr, err := buildArray(runValues, tagOpts{}, mem)
	if err != nil {
		return nil, fmt.Errorf("arreflect: run-end encoded values: %w", err)
	}
	defer valuesArr.Release()

	return array.NewRunEndEncodedArray(runEndsArr, valuesArr, vals.Len(), 0), nil
}

func buildListViewArray(vals reflect.Value, mem memory.Allocator) (arrow.Array, error) {
	elemDT, isOuterPtr, err := listBuildPreamble(vals)
	if err != nil {
		return nil, fmt.Errorf("arreflect: %w", err)
	}

	lvb := array.NewListViewBuilder(mem, elemDT)
	defer lvb.Release()

	vb := lvb.ValueBuilder()

	for i := 0; i < vals.Len(); i++ {
		outer := vals.Index(i)
		if isOuterPtr {
			if outer.IsNil() {
				lvb.AppendNull()
				continue
			}
			outer = outer.Elem()
		}
		if outer.IsNil() {
			lvb.AppendNull()
			continue
		}
		lvb.AppendWithSize(true, outer.Len())
		for j := 0; j < outer.Len(); j++ {
			if err := appendValue(vb, outer.Index(j), tagOpts{}); err != nil {
				return nil, fmt.Errorf("arreflect: list-view element [%d][%d]: %w", i, j, err)
			}
		}
	}

	return lvb.NewArray(), nil
}
