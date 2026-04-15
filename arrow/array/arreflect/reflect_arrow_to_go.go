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
)

func assertArray[T any](arr arrow.Array) (*T, error) {
	a, ok := any(arr).(*T)
	if !ok {
		var zero T
		return nil, fmt.Errorf("expected *%T, got %T: %w", zero, arr, ErrTypeMismatch)
	}
	return a, nil
}

func isIntKind(k reflect.Kind) bool {
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 ||
		k == reflect.Int32 || k == reflect.Int64
}

func isUintKind(k reflect.Kind) bool {
	return k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 ||
		k == reflect.Uint32 || k == reflect.Uint64 || k == reflect.Uintptr
}

func isFloatKind(k reflect.Kind) bool { return k == reflect.Float32 || k == reflect.Float64 }

func setValue(v reflect.Value, arr arrow.Array, i int) error {
	if arr.IsNull(i) {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	switch arr.DataType().ID() {
	case arrow.BOOL:
		a, err := assertArray[array.Boolean](arr)
		if err != nil {
			return err
		}
		if v.Kind() != reflect.Bool {
			return fmt.Errorf("cannot set bool into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetBool(a.Value(i))

	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return setPrimitiveValue(v, arr, i)

	case arrow.STRING, arrow.LARGE_STRING:
		type stringer interface{ Value(int) string }
		a, ok := arr.(stringer)
		if !ok {
			return fmt.Errorf("expected string array, got %T: %w", arr, ErrTypeMismatch)
		}
		if v.Kind() != reflect.String {
			return fmt.Errorf("cannot set string into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetString(a.Value(i))

	case arrow.BINARY, arrow.LARGE_BINARY:
		type byter interface{ Value(int) []byte }
		a, ok := arr.(byter)
		if !ok {
			return fmt.Errorf("expected binary array, got %T: %w", arr, ErrTypeMismatch)
		}
		if v.Kind() != reflect.Slice || v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("cannot set []byte into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetBytes(a.Value(i))

	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64, arrow.DURATION:
		return setTemporalValue(v, arr, i)

	case arrow.DECIMAL128, arrow.DECIMAL256, arrow.DECIMAL32, arrow.DECIMAL64:
		return setDecimalValue(v, arr, i)

	case arrow.STRUCT:
		a, err := assertArray[array.Struct](arr)
		if err != nil {
			return err
		}
		return setStructValue(v, a, i)

	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		a, ok := arr.(array.ListLike)
		if !ok {
			return fmt.Errorf("expected ListLike, got %T: %w", arr, ErrTypeMismatch)
		}
		return setListValue(v, a, i)

	case arrow.MAP:
		a, err := assertArray[array.Map](arr)
		if err != nil {
			return err
		}
		return setMapValue(v, a, i)

	case arrow.FIXED_SIZE_LIST:
		a, err := assertArray[array.FixedSizeList](arr)
		if err != nil {
			return err
		}
		return setFixedSizeListValue(v, a, i)

	case arrow.DICTIONARY:
		a, err := assertArray[array.Dictionary](arr)
		if err != nil {
			return err
		}
		return setDictionaryValue(v, a, i)

	case arrow.RUN_END_ENCODED:
		a, err := assertArray[array.RunEndEncoded](arr)
		if err != nil {
			return err
		}
		return setRunEndEncodedValue(v, a, i)

	default:
		return fmt.Errorf("unsupported Arrow type %v for reflection: %w", arr.DataType(), ErrUnsupportedType)
	}
	return nil
}

func setPrimitiveValue(v reflect.Value, arr arrow.Array, i int) error {
	switch arr.DataType().ID() {
	case arrow.INT8:
		if !isIntKind(v.Kind()) {
			return fmt.Errorf("cannot set int8 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetInt(int64(arr.(*array.Int8).Value(i)))
	case arrow.INT16:
		if !isIntKind(v.Kind()) {
			return fmt.Errorf("cannot set int16 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetInt(int64(arr.(*array.Int16).Value(i)))
	case arrow.INT32:
		if !isIntKind(v.Kind()) {
			return fmt.Errorf("cannot set int32 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetInt(int64(arr.(*array.Int32).Value(i)))
	case arrow.INT64:
		if !isIntKind(v.Kind()) {
			return fmt.Errorf("cannot set int64 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetInt(arr.(*array.Int64).Value(i))
	case arrow.UINT8:
		if !isUintKind(v.Kind()) {
			return fmt.Errorf("cannot set uint8 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetUint(uint64(arr.(*array.Uint8).Value(i)))
	case arrow.UINT16:
		if !isUintKind(v.Kind()) {
			return fmt.Errorf("cannot set uint16 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetUint(uint64(arr.(*array.Uint16).Value(i)))
	case arrow.UINT32:
		if !isUintKind(v.Kind()) {
			return fmt.Errorf("cannot set uint32 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetUint(uint64(arr.(*array.Uint32).Value(i)))
	case arrow.UINT64:
		if !isUintKind(v.Kind()) {
			return fmt.Errorf("cannot set uint64 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetUint(arr.(*array.Uint64).Value(i))
	case arrow.FLOAT32:
		if !isFloatKind(v.Kind()) {
			return fmt.Errorf("cannot set float32 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetFloat(float64(arr.(*array.Float32).Value(i)))
	case arrow.FLOAT64:
		if !isFloatKind(v.Kind()) {
			return fmt.Errorf("cannot set float64 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.SetFloat(arr.(*array.Float64).Value(i))
	default:
		return fmt.Errorf("unsupported primitive type %v: %w", arr.DataType(), ErrUnsupportedType)
	}
	return nil
}

func setTime(v reflect.Value, t time.Time) error {
	if v.Type() != typeOfTime {
		return fmt.Errorf("cannot set time.Time into %s: %w", v.Type(), ErrTypeMismatch)
	}
	v.Set(reflect.ValueOf(t))
	return nil
}

func setTemporalValue(v reflect.Value, arr arrow.Array, i int) error {
	switch arr.DataType().ID() {
	case arrow.TIMESTAMP:
		a, err := assertArray[array.Timestamp](arr)
		if err != nil {
			return err
		}
		unit := arr.DataType().(*arrow.TimestampType).Unit
		return setTime(v, a.Value(i).ToTime(unit))

	case arrow.DATE32:
		a, err := assertArray[array.Date32](arr)
		if err != nil {
			return err
		}
		return setTime(v, a.Value(i).ToTime())

	case arrow.DATE64:
		a, err := assertArray[array.Date64](arr)
		if err != nil {
			return err
		}
		return setTime(v, a.Value(i).ToTime())

	case arrow.TIME32:
		a, err := assertArray[array.Time32](arr)
		if err != nil {
			return err
		}
		unit := arr.DataType().(*arrow.Time32Type).Unit
		return setTime(v, a.Value(i).ToTime(unit))

	case arrow.TIME64:
		a, err := assertArray[array.Time64](arr)
		if err != nil {
			return err
		}
		unit := arr.DataType().(*arrow.Time64Type).Unit
		return setTime(v, a.Value(i).ToTime(unit))

	case arrow.DURATION:
		a, err := assertArray[array.Duration](arr)
		if err != nil {
			return err
		}
		if v.Type() != typeOfDuration {
			return fmt.Errorf("cannot set time.Duration into %s: %w", v.Type(), ErrTypeMismatch)
		}
		unit := arr.DataType().(*arrow.DurationType).Unit
		dur := time.Duration(a.Value(i)) * unit.Multiplier()
		v.Set(reflect.ValueOf(dur))

	default:
		return fmt.Errorf("unsupported temporal type %v: %w", arr.DataType(), ErrUnsupportedType)
	}
	return nil
}

func setDecimalValue(v reflect.Value, arr arrow.Array, i int) error {
	switch arr.DataType().ID() {
	case arrow.DECIMAL128:
		a, err := assertArray[array.Decimal128](arr)
		if err != nil {
			return err
		}
		if v.Type() != typeOfDec128 {
			return fmt.Errorf("cannot set decimal128.Num into %s: %w", v.Type(), ErrTypeMismatch)
		}
		num := a.Value(i)
		v.Set(reflect.ValueOf(num))

	case arrow.DECIMAL256:
		a, err := assertArray[array.Decimal256](arr)
		if err != nil {
			return err
		}
		if v.Type() != typeOfDec256 {
			return fmt.Errorf("cannot set decimal256.Num into %s: %w", v.Type(), ErrTypeMismatch)
		}
		num := a.Value(i)
		v.Set(reflect.ValueOf(num))

	case arrow.DECIMAL32:
		a, err := assertArray[array.Decimal32](arr)
		if err != nil {
			return err
		}
		if v.Type() != typeOfDec32 {
			return fmt.Errorf("cannot set decimal.Decimal32 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.Set(reflect.ValueOf(a.Value(i)))

	case arrow.DECIMAL64:
		a, err := assertArray[array.Decimal64](arr)
		if err != nil {
			return err
		}
		if v.Type() != typeOfDec64 {
			return fmt.Errorf("cannot set decimal.Decimal64 into %s: %w", v.Type(), ErrTypeMismatch)
		}
		v.Set(reflect.ValueOf(a.Value(i)))

	default:
		return fmt.Errorf("unsupported decimal type %v: %w", arr.DataType(), ErrUnsupportedType)
	}
	return nil
}

func setStructValue(v reflect.Value, sa *array.Struct, i int) error {
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("cannot set struct into %s: %w", v.Type(), ErrTypeMismatch)
	}

	fields := cachedStructFields(v.Type())
	st := sa.DataType().(*arrow.StructType)

	for _, fm := range fields {
		arrowIdx, found := st.FieldIdx(fm.Name)
		if !found {
			continue
		}
		if err := setValue(v.FieldByIndex(fm.Index), sa.Field(arrowIdx), i); err != nil {
			return fmt.Errorf("arreflect: field %q: %w", fm.Name, err)
		}
	}
	return nil
}

func setListValue(v reflect.Value, arr array.ListLike, i int) error {
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("cannot set list into %s: %w", v.Type(), ErrTypeMismatch)
	}

	start, end := arr.ValueOffsets(i)
	child := arr.ListValues()
	length := int(end - start)

	result := reflect.MakeSlice(v.Type(), length, length)
	for j := 0; j < length; j++ {
		if err := setValue(result.Index(j), child, int(start)+j); err != nil {
			return fmt.Errorf("arreflect: list element %d: %w", j, err)
		}
	}
	v.Set(result)
	return nil
}

func setMapValue(v reflect.Value, arr *array.Map, i int) error {
	if v.Kind() != reflect.Map {
		return fmt.Errorf("cannot set map into %s: %w", v.Type(), ErrTypeMismatch)
	}

	start, end := arr.ValueOffsets(i)
	keys := arr.Keys()
	items := arr.Items()
	keyType := v.Type().Key()
	elemType := v.Type().Elem()

	result := reflect.MakeMap(v.Type())
	for j := int(start); j < int(end); j++ {
		keyVal := reflect.New(keyType).Elem()
		if err := setValue(keyVal, keys, j); err != nil {
			return fmt.Errorf("arreflect: map key %d: %w", j-int(start), err)
		}
		elemVal := reflect.New(elemType).Elem()
		if err := setValue(elemVal, items, j); err != nil {
			return fmt.Errorf("arreflect: map value %d: %w", j-int(start), err)
		}
		result.SetMapIndex(keyVal, elemVal)
	}
	v.Set(result)
	return nil
}

func fillFixedSizeList(dst reflect.Value, child arrow.Array, start, n int) error {
	for k := 0; k < n; k++ {
		if err := setValue(dst.Index(k), child, start+k); err != nil {
			return fmt.Errorf("arreflect: fixed-size list element %d: %w", k, err)
		}
	}
	return nil
}

func setFixedSizeListValue(v reflect.Value, arr *array.FixedSizeList, i int) error {
	n := int(arr.DataType().(*arrow.FixedSizeListType).Len())
	child := arr.ListValues()
	start, _ := arr.ValueOffsets(i)

	switch v.Kind() {
	case reflect.Array:
		if v.Len() != n {
			return fmt.Errorf("fixed-size list length %d does not match Go array length %d: %w", n, v.Len(), ErrTypeMismatch)
		}
		return fillFixedSizeList(v, child, int(start), n)
	case reflect.Slice:
		result := reflect.MakeSlice(v.Type(), n, n)
		if err := fillFixedSizeList(result, child, int(start), n); err != nil {
			return err
		}
		v.Set(result)
	default:
		return fmt.Errorf("cannot set fixed-size list into %s: %w", v.Type(), ErrTypeMismatch)
	}
	return nil
}

func setDictionaryValue(v reflect.Value, arr *array.Dictionary, i int) error {
	return setValue(v, arr.Dictionary(), arr.GetValueIndex(i))
}

func setRunEndEncodedValue(v reflect.Value, arr *array.RunEndEncoded, i int) error {
	return setValue(v, arr.Values(), arr.GetPhysicalIndex(i))
}
