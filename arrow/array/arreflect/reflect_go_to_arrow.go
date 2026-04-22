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

	if opts.Large {
		dt, err := inferArrowType(elemType)
		if err != nil {
			return nil, err
		}
		if !hasLargeableType(dt) {
			return nil, fmt.Errorf("arreflect: large option has no effect on type %s: %w", dt, ErrUnsupportedType)
		}
	}

	if opts.View {
		dt, err := inferArrowType(elemType)
		if err != nil {
			return nil, err
		}
		if !hasViewableType(dt) {
			return nil, fmt.Errorf("arreflect: view option has no effect on type %s: %w", dt, ErrUnsupportedType)
		}
	}

	if opts.Dict {
		return buildDictionaryArray(vals, opts, mem)
	}
	if opts.REE {
		return buildRunEndEncodedArray(vals, opts, mem)
	}
	if opts.View {
		if elemType.Kind() != reflect.Slice || elemType == typeOfByteSlice {
			return buildPrimitiveArray(vals, opts, mem)
		}
		return buildListViewArray(vals, opts, mem)
	}

	switch elemType {
	case typeOfDec32, typeOfDec64, typeOfDec128, typeOfDec256:
		return buildDecimalArray(vals, opts, mem)
	}

	switch elemType.Kind() {
	case reflect.Slice:
		if elemType == typeOfByteSlice {
			return buildPrimitiveArray(vals, opts, mem)
		}
		return buildListArray(vals, opts, mem)

	case reflect.Array:
		return buildFixedSizeListArray(vals, opts, mem)

	case reflect.Map:
		return buildMapArray(vals, opts, mem)

	case reflect.Struct:
		switch elemType {
		case typeOfTime:
			return buildTemporalArray(vals, opts, mem)
		default:
			return buildStructArray(vals, opts, mem)
		}

	default:
		return buildPrimitiveArray(vals, opts, mem)
	}
}

func buildPrimitiveArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	dt, err := inferArrowType(elemType)
	if err != nil {
		return nil, err
	}
	if opts.Large {
		dt = applyLargeOpts(dt)
	}
	if opts.View {
		dt = applyViewOpts(dt)
	}

	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.Reserve(vals.Len())

	if err := iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) error {
		return appendValue(b, v)
	}); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

func timeOfDayNanos(t time.Time) int64 {
	t = t.UTC()
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return t.Sub(midnight).Nanoseconds()
}

func asTime(v reflect.Value) (time.Time, error) {
	t, ok := reflect.TypeAssert[time.Time](v)
	if !ok {
		return time.Time{}, fmt.Errorf("expected time.Time, got %s: %w", v.Type(), ErrTypeMismatch)
	}
	return t, nil
}

func asDuration(v reflect.Value) (time.Duration, error) {
	d, ok := reflect.TypeAssert[time.Duration](v)
	if !ok {
		return 0, fmt.Errorf("expected time.Duration, got %s: %w", v.Type(), ErrTypeMismatch)
	}
	return d, nil
}

func derefSliceElem(vals reflect.Value) (elemType reflect.Type, isPtr bool) {
	elemType = vals.Type().Elem()
	isPtr = elemType.Kind() == reflect.Ptr
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	return
}

func iterSlice(vals reflect.Value, isPtr bool, appendNull func(), appendVal func(reflect.Value) error) error {
	for i := 0; i < vals.Len(); i++ {
		v := vals.Index(i)
		if isPtr {
			wasNull := false
			for v.Kind() == reflect.Ptr {
				if v.IsNil() {
					appendNull()
					wasNull = true
					break
				}
				v = v.Elem()
			}
			if wasNull {
				continue
			}
		}
		if err := appendVal(v); err != nil {
			return err
		}
	}
	return nil
}

func inferListElemDT(vals reflect.Value) (elemDT arrow.DataType, err error) {
	outerSliceType, _ := derefSliceElem(vals)
	innerElemType := outerSliceType.Elem()
	for innerElemType.Kind() == reflect.Ptr {
		innerElemType = innerElemType.Elem()
	}
	elemDT, err = inferArrowType(innerElemType)
	return
}

func temporalBuilder(opts tagOpts, mem memory.Allocator) array.Builder {
	switch opts.Temporal {
	case "date32":
		return array.NewDate32Builder(mem)
	case "date64":
		return array.NewDate64Builder(mem)
	case "time32":
		return array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Millisecond})
	case "time64":
		return array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Nanosecond})
	default:
		return array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	}
}

func buildTemporalArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)
	if elemType != typeOfTime {
		return nil, fmt.Errorf("unsupported temporal type %v: %w", elemType, ErrUnsupportedType)
	}
	b := temporalBuilder(opts, mem)
	defer b.Release()
	b.Reserve(vals.Len())
	if err := iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) error {
		return appendTemporalValue(b, v)
	}); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

func decimalPrecisionScale(opts tagOpts, defaultPrec int32) (precision, scale int32) {
	if opts.HasDecimalOpts {
		return opts.DecimalPrecision, opts.DecimalScale
	}
	return defaultPrec, 0
}

func buildDecimalArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	var b array.Builder
	switch elemType {
	case typeOfDec128:
		p, s := decimalPrecisionScale(opts, dec128DefaultPrecision)
		b = array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: p, Scale: s})
	case typeOfDec256:
		p, s := decimalPrecisionScale(opts, dec256DefaultPrecision)
		b = array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: p, Scale: s})
	case typeOfDec32:
		p, s := decimalPrecisionScale(opts, dec32DefaultPrecision)
		b = array.NewDecimal32Builder(mem, &arrow.Decimal32Type{Precision: p, Scale: s})
	case typeOfDec64:
		p, s := decimalPrecisionScale(opts, dec64DefaultPrecision)
		b = array.NewDecimal64Builder(mem, &arrow.Decimal64Type{Precision: p, Scale: s})
	default:
		return nil, fmt.Errorf("unsupported decimal type %v: %w", elemType, ErrUnsupportedType)
	}
	defer b.Release()
	b.Reserve(vals.Len())
	if err := iterSlice(vals, isPtr, b.AppendNull, func(v reflect.Value) error {
		return appendDecimalValue(b, v)
	}); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

func appendStructFields(sb *array.StructBuilder, v reflect.Value, fields []fieldMeta) error {
	sb.Append(true)
	for fi, fm := range fields {
		fv, ok := fieldByIndexSafe(v, fm.Index)
		if !ok {
			sb.FieldBuilder(fi).AppendNull()
			continue
		}
		if err := appendValue(sb.FieldBuilder(fi), fv); err != nil {
			return fmt.Errorf("struct field %q: %w", fm.Name, err)
		}
	}
	return nil
}

func buildStructArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	st, err := inferStructType(elemType)
	if err != nil {
		return nil, err
	}
	if opts.Large {
		// applyLargeOpts is idempotent, so per-field "large" tags already applied
		// by inferStructType are safe to walk again here.
		st = applyLargeOpts(st).(*arrow.StructType)
	}
	if opts.View {
		st = applyViewOpts(st).(*arrow.StructType)
	}

	fields := cachedStructFields(elemType)
	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()
	sb.Reserve(vals.Len())

	if err := iterSlice(vals, isPtr, sb.AppendNull, func(v reflect.Value) error {
		return appendStructFields(sb, v, fields)
	}); err != nil {
		return nil, err
	}

	return sb.NewArray(), nil
}

func appendTemporalValue(b array.Builder, v reflect.Value) error {
	switch tb := b.(type) {
	case *array.TimestampBuilder:
		unit := tb.Type().(*arrow.TimestampType).Unit
		t, err := asTime(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Timestamp(t.UnixNano() / int64(unit.Multiplier())))
	case *array.Date32Builder:
		t, err := asTime(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Date32FromTime(t))
	case *array.Date64Builder:
		t, err := asTime(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Date64FromTime(t))
	case *array.Time32Builder:
		unit := tb.Type().(*arrow.Time32Type).Unit
		t, err := asTime(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Time32(timeOfDayNanos(t) / int64(unit.Multiplier())))
	case *array.Time64Builder:
		unit := tb.Type().(*arrow.Time64Type).Unit
		t, err := asTime(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Time64(timeOfDayNanos(t) / int64(unit.Multiplier())))
	case *array.DurationBuilder:
		unit := tb.Type().(*arrow.DurationType).Unit
		d, err := asDuration(v)
		if err != nil {
			return err
		}
		tb.Append(arrow.Duration(d.Nanoseconds() / int64(unit.Multiplier())))
	default:
		return fmt.Errorf("unexpected temporal builder %T: %w", b, ErrUnsupportedType)
	}
	return nil
}

func appendDecimalValue(b array.Builder, v reflect.Value) error {
	switch tb := b.(type) {
	case *array.Decimal128Builder:
		n, ok := reflect.TypeAssert[decimal128.Num](v)
		if !ok {
			return fmt.Errorf("expected decimal128.Num, got %s: %w", v.Type(), ErrTypeMismatch)
		}
		tb.Append(n)
	case *array.Decimal256Builder:
		n, ok := reflect.TypeAssert[decimal256.Num](v)
		if !ok {
			return fmt.Errorf("expected decimal256.Num, got %s: %w", v.Type(), ErrTypeMismatch)
		}
		tb.Append(n)
	case *array.Decimal32Builder:
		tb.Append(decimal.Decimal32(v.Int()))
	case *array.Decimal64Builder:
		tb.Append(decimal.Decimal64(v.Int()))
	default:
		return fmt.Errorf("unexpected decimal builder %T: %w", b, ErrUnsupportedType)
	}
	return nil
}

func appendValue(b array.Builder, v reflect.Value) error {
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
	case array.StringLikeBuilder:
		tb.Append(v.String())
	case array.BinaryLikeBuilder:
		if v.IsNil() {
			tb.AppendNull()
		} else {
			tb.Append(v.Bytes())
		}
	case *array.TimestampBuilder, *array.Date32Builder, *array.Date64Builder,
		*array.Time32Builder, *array.Time64Builder, *array.DurationBuilder:
		return appendTemporalValue(b, v)
	case *array.Decimal128Builder, *array.Decimal256Builder, *array.Decimal32Builder, *array.Decimal64Builder:
		return appendDecimalValue(b, v)
	case *array.ListBuilder, *array.LargeListBuilder, *array.ListViewBuilder, *array.LargeListViewBuilder:
		return appendListElement(b, v)
	case *array.FixedSizeListBuilder:
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return fmt.Errorf("arreflect: cannot set fixed-size list from %s: %w", v.Type(), ErrTypeMismatch)
		}
		if v.Kind() == reflect.Slice && v.IsNil() {
			tb.AppendNull()
			return nil
		}
		expectedLen := int(tb.Type().(*arrow.FixedSizeListType).Len())
		if v.Len() != expectedLen {
			return fmt.Errorf("arreflect: fixed-size list length mismatch: got %d, want %d", v.Len(), expectedLen)
		}
		tb.Append(true)
		vb := tb.ValueBuilder()
		for i := 0; i < v.Len(); i++ {
			if err := appendValue(vb, v.Index(i)); err != nil {
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
				if err := appendValue(kb, key); err != nil {
					return err
				}
				if err := appendValue(ib, v.MapIndex(key)); err != nil {
					return err
				}
			}
		}
	case *array.StructBuilder:
		fields := cachedStructFields(v.Type())
		return appendStructFields(tb, v, fields)
	default:
		if db, ok := b.(array.DictionaryBuilder); ok {
			return appendToDictBuilder(db, v)
		}
		return fmt.Errorf("unsupported builder type %T: %w", b, ErrUnsupportedType)
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
			return fmt.Errorf("unsupported value kind %v for BinaryDictionaryBuilder: %w", v.Kind(), ErrUnsupportedType)
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
	return fmt.Errorf("unsupported builder type %T: %w", db, ErrUnsupportedType)
}

type listBuilderLike interface {
	array.Builder
	ValueBuilder() array.Builder
}

func appendListElement(b array.Builder, v reflect.Value) error {
	if v.Kind() == reflect.Slice && v.IsNil() {
		b.AppendNull()
		return nil
	}

	var vb array.Builder
	switch lb := b.(type) {
	case *array.ListBuilder:
		lb.Append(true)
		vb = lb.ValueBuilder()
	case *array.LargeListBuilder:
		lb.Append(true)
		vb = lb.ValueBuilder()
	case *array.ListViewBuilder:
		lb.AppendWithSize(true, v.Len())
		vb = lb.ValueBuilder()
	case *array.LargeListViewBuilder:
		lb.AppendWithSize(true, v.Len())
		vb = lb.ValueBuilder()
	default:
		return fmt.Errorf("unexpected list builder type %T: %w", b, ErrUnsupportedType)
	}
	for i := 0; i < v.Len(); i++ {
		if err := appendValue(vb, v.Index(i)); err != nil {
			return err
		}
	}
	return nil
}

func buildListLikeArray(vals reflect.Value, mem memory.Allocator, opts tagOpts, isView bool) (arrow.Array, error) {
	elemDT, err := inferListElemDT(vals)
	if err != nil {
		return nil, err
	}
	if opts.Large {
		elemDT = applyLargeOpts(elemDT)
	}
	if opts.View {
		elemDT = applyViewOpts(elemDT)
	}

	label := "list element"
	if isView {
		label = "list-view element"
	}

	var bldr listBuilderLike
	var beginRow func(int)
	switch {
	case isView && opts.Large:
		b := array.NewLargeListViewBuilder(mem, elemDT)
		bldr = b
		beginRow = func(n int) { b.AppendWithSize(true, n) }
	case isView:
		b := array.NewListViewBuilder(mem, elemDT)
		bldr = b
		beginRow = func(n int) { b.AppendWithSize(true, n) }
	case opts.Large:
		b := array.NewLargeListBuilder(mem, elemDT)
		bldr = b
		beginRow = func(_ int) { b.Append(true) }
	default:
		b := array.NewListBuilder(mem, elemDT)
		bldr = b
		beginRow = func(_ int) { b.Append(true) }
	}
	defer bldr.Release()

	vb := bldr.ValueBuilder()
	for i := 0; i < vals.Len(); i++ {
		outer := vals.Index(i)
		for outer.Kind() == reflect.Ptr {
			if outer.IsNil() {
				bldr.AppendNull()
				break
			}
			outer = outer.Elem()
		}
		if outer.Kind() == reflect.Ptr {
			continue
		}
		if outer.Kind() == reflect.Slice && outer.IsNil() {
			bldr.AppendNull()
			continue
		}
		if outer.Kind() != reflect.Slice && outer.Kind() != reflect.Array {
			return nil, fmt.Errorf("arreflect: %s [%d]: expected slice, got %s: %w", label, i, outer.Type(), ErrTypeMismatch)
		}
		beginRow(outer.Len())
		for j := 0; j < outer.Len(); j++ {
			if err := appendValue(vb, outer.Index(j)); err != nil {
				return nil, fmt.Errorf("%s [%d][%d]: %w", label, i, j, err)
			}
		}
	}
	return bldr.NewArray(), nil
}

func buildListArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	return buildListLikeArray(vals, mem, opts, false)
}

func buildListViewArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	return buildListLikeArray(vals, mem, opts, true)
}

func buildMapArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	mapType, isPtr := derefSliceElem(vals)

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
		return nil, fmt.Errorf("map key type: %w", err)
	}
	valDT, err := inferArrowType(valType)
	if err != nil {
		return nil, fmt.Errorf("map value type: %w", err)
	}
	if opts.Large {
		keyDT = applyLargeOpts(keyDT)
		valDT = applyLargeOpts(valDT)
	}
	if opts.View {
		keyDT = applyViewOpts(keyDT)
		valDT = applyViewOpts(valDT)
	}

	mb := array.NewMapBuilder(mem, keyDT, valDT, false)
	defer mb.Release()

	kb := mb.KeyBuilder()
	ib := mb.ItemBuilder()

	if err := iterSlice(vals, isPtr, mb.AppendNull, func(m reflect.Value) error {
		if m.IsNil() {
			mb.AppendNull()
			return nil
		}
		mb.Append(true)
		for _, key := range m.MapKeys() {
			if err := appendValue(kb, key); err != nil {
				return fmt.Errorf("map key: %w", err)
			}
			if err := appendValue(ib, m.MapIndex(key)); err != nil {
				return fmt.Errorf("map value: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return mb.NewArray(), nil
}

func buildFixedSizeListArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

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
		return nil, err
	}
	if opts.Large {
		innerDT = applyLargeOpts(innerDT)
	}
	if opts.View {
		innerDT = applyViewOpts(innerDT)
	}

	fb := array.NewFixedSizeListBuilder(mem, n, innerDT)
	defer fb.Release()

	vb := fb.ValueBuilder()

	idx := 0
	appendNullIdx := func() { fb.AppendNull(); idx++ }
	if err := iterSlice(vals, isPtr, appendNullIdx, func(elem reflect.Value) error {
		fb.Append(true)
		for j := 0; j < int(n); j++ {
			if err := appendValue(vb, elem.Index(j)); err != nil {
				return fmt.Errorf("fixed-size list element [%d][%d]: %w", idx, j, err)
			}
		}
		idx++
		return nil
	}); err != nil {
		return nil, err
	}

	return fb.NewArray(), nil
}

func validateDictValueType(dt arrow.DataType) error {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.BINARY:
		return nil
	default:
		return fmt.Errorf("arreflect: dictionary encoding not supported for %s: %w", dt, ErrUnsupportedType)
	}
}

func buildDictionaryArray(vals reflect.Value, _ tagOpts, mem memory.Allocator) (arrow.Array, error) {
	elemType, isPtr := derefSliceElem(vals)

	valDT, err := inferArrowType(elemType)
	if err != nil {
		return nil, err
	}
	// large is intentionally NOT applied here: Dictionary<Int32, LargeString> is
	// unimplemented in the Arrow library (NewDictionaryBuilder panics).

	if err := validateDictValueType(valDT); err != nil {
		return nil, err
	}

	dt := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: valDT,
	}
	db := array.NewDictionaryBuilder(mem, dt)
	defer db.Release()

	if err := iterSlice(vals, isPtr, db.AppendNull, func(elem reflect.Value) error {
		return appendToDictBuilder(db, elem)
	}); err != nil {
		return nil, err
	}
	return db.NewArray(), nil
}

func buildRunEndEncodedArray(vals reflect.Value, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	valOpts := opts
	valOpts.REE = false
	valOpts.View = false
	if vals.Len() == 0 {
		runEndsArr, err := buildPrimitiveArray(reflect.MakeSlice(reflect.TypeOf([]int32{}), 0, 0), tagOpts{}, mem)
		if err != nil {
			return nil, err
		}
		defer runEndsArr.Release()
		valuesArr, err := buildArray(reflect.MakeSlice(vals.Type(), 0, 0), valOpts, mem)
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
	runEndsArr, err := buildPrimitiveArray(runEndsSlice, tagOpts{}, mem)
	if err != nil {
		return nil, fmt.Errorf("run-end encoded run ends: %w", err)
	}
	defer runEndsArr.Release()

	runValues := reflect.MakeSlice(vals.Type(), len(runs), len(runs))
	for i, r := range runs {
		runValues.Index(i).Set(r.val)
	}
	valuesArr, err := buildArray(runValues, valOpts, mem)
	if err != nil {
		return nil, fmt.Errorf("run-end encoded values: %w", err)
	}
	defer valuesArr.Release()

	return array.NewRunEndEncodedArray(runEndsArr, valuesArr, vals.Len(), 0), nil
}
