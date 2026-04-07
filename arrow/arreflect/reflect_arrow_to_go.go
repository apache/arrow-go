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
		a, ok := arr.(*array.Boolean)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Boolean, got %T", arr)
		}
		if v.Kind() != reflect.Bool {
			return fmt.Errorf("arrow/reflect: cannot set bool into %s", v.Type())
		}
		v.SetBool(a.Value(i))

	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return setPrimitiveValue(v, arr, i)

	case arrow.STRING:
		a, ok := arr.(*array.String)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *String, got %T", arr)
		}
		if v.Kind() != reflect.String {
			return fmt.Errorf("arrow/reflect: cannot set string into %s", v.Type())
		}
		v.SetString(a.Value(i))

	case arrow.LARGE_STRING:
		a, ok := arr.(*array.LargeString)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *LargeString, got %T", arr)
		}
		if v.Kind() != reflect.String {
			return fmt.Errorf("arrow/reflect: cannot set string into %s", v.Type())
		}
		v.SetString(a.Value(i))

	case arrow.BINARY:
		a, ok := arr.(*array.Binary)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Binary, got %T", arr)
		}
		if v.Kind() != reflect.Slice || v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("arrow/reflect: cannot set []byte into %s", v.Type())
		}
		v.SetBytes(a.Value(i))

	case arrow.LARGE_BINARY:
		a, ok := arr.(*array.LargeBinary)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *LargeBinary, got %T", arr)
		}
		if v.Kind() != reflect.Slice || v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("arrow/reflect: cannot set []byte into %s", v.Type())
		}
		v.SetBytes(a.Value(i))

	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64, arrow.DURATION:
		return setTemporalValue(v, arr, i)

	case arrow.DECIMAL128, arrow.DECIMAL256, arrow.DECIMAL32, arrow.DECIMAL64:
		return setDecimalValue(v, arr, i)

	case arrow.STRUCT:
		a, ok := arr.(*array.Struct)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Struct, got %T", arr)
		}
		return setStructValue(v, a, i)

	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		a, ok := arr.(array.ListLike)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected ListLike, got %T", arr)
		}
		return setListValue(v, a, i)

	case arrow.MAP:
		a, ok := arr.(*array.Map)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Map, got %T", arr)
		}
		return setMapValue(v, a, i)

	case arrow.FIXED_SIZE_LIST:
		a, ok := arr.(*array.FixedSizeList)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *FixedSizeList, got %T", arr)
		}
		return setFixedSizeListValue(v, a, i)

	case arrow.DICTIONARY:
		a, ok := arr.(*array.Dictionary)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Dictionary, got %T", arr)
		}
		return setDictionaryValue(v, a, i)

	case arrow.RUN_END_ENCODED:
		a, ok := arr.(*array.RunEndEncoded)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *RunEndEncoded, got %T", arr)
		}
		return setRunEndEncodedValue(v, a, i)

	default:
		return fmt.Errorf("arrow/reflect: unsupported Arrow type %v for reflection", arr.DataType())
	}
	return nil
}

func setPrimitiveValue(v reflect.Value, arr arrow.Array, i int) error {
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	switch arr.DataType().ID() {
	case arrow.INT8:
		if v.Kind() != reflect.Int && v.Kind() != reflect.Int8 && v.Kind() != reflect.Int16 &&
			v.Kind() != reflect.Int32 && v.Kind() != reflect.Int64 {
			return fmt.Errorf("arrow/reflect: cannot set int8 into %s", v.Type())
		}
		v.SetInt(int64(arr.(*array.Int8).Value(i)))
	case arrow.INT16:
		if v.Kind() != reflect.Int && v.Kind() != reflect.Int8 && v.Kind() != reflect.Int16 &&
			v.Kind() != reflect.Int32 && v.Kind() != reflect.Int64 {
			return fmt.Errorf("arrow/reflect: cannot set int16 into %s", v.Type())
		}
		v.SetInt(int64(arr.(*array.Int16).Value(i)))
	case arrow.INT32:
		if v.Kind() != reflect.Int && v.Kind() != reflect.Int8 && v.Kind() != reflect.Int16 &&
			v.Kind() != reflect.Int32 && v.Kind() != reflect.Int64 {
			return fmt.Errorf("arrow/reflect: cannot set int32 into %s", v.Type())
		}
		v.SetInt(int64(arr.(*array.Int32).Value(i)))
	case arrow.INT64:
		if v.Kind() != reflect.Int && v.Kind() != reflect.Int8 && v.Kind() != reflect.Int16 &&
			v.Kind() != reflect.Int32 && v.Kind() != reflect.Int64 {
			return fmt.Errorf("arrow/reflect: cannot set int64 into %s", v.Type())
		}
		v.SetInt(arr.(*array.Int64).Value(i))
	case arrow.UINT8:
		if v.Kind() != reflect.Uint && v.Kind() != reflect.Uint8 && v.Kind() != reflect.Uint16 &&
			v.Kind() != reflect.Uint32 && v.Kind() != reflect.Uint64 && v.Kind() != reflect.Uintptr {
			return fmt.Errorf("arrow/reflect: cannot set uint8 into %s", v.Type())
		}
		v.SetUint(uint64(arr.(*array.Uint8).Value(i)))
	case arrow.UINT16:
		if v.Kind() != reflect.Uint && v.Kind() != reflect.Uint8 && v.Kind() != reflect.Uint16 &&
			v.Kind() != reflect.Uint32 && v.Kind() != reflect.Uint64 && v.Kind() != reflect.Uintptr {
			return fmt.Errorf("arrow/reflect: cannot set uint16 into %s", v.Type())
		}
		v.SetUint(uint64(arr.(*array.Uint16).Value(i)))
	case arrow.UINT32:
		if v.Kind() != reflect.Uint && v.Kind() != reflect.Uint8 && v.Kind() != reflect.Uint16 &&
			v.Kind() != reflect.Uint32 && v.Kind() != reflect.Uint64 && v.Kind() != reflect.Uintptr {
			return fmt.Errorf("arrow/reflect: cannot set uint32 into %s", v.Type())
		}
		v.SetUint(uint64(arr.(*array.Uint32).Value(i)))
	case arrow.UINT64:
		if v.Kind() != reflect.Uint && v.Kind() != reflect.Uint8 && v.Kind() != reflect.Uint16 &&
			v.Kind() != reflect.Uint32 && v.Kind() != reflect.Uint64 && v.Kind() != reflect.Uintptr {
			return fmt.Errorf("arrow/reflect: cannot set uint64 into %s", v.Type())
		}
		v.SetUint(arr.(*array.Uint64).Value(i))
	case arrow.FLOAT32:
		if v.Kind() != reflect.Float32 && v.Kind() != reflect.Float64 {
			return fmt.Errorf("arrow/reflect: cannot set float32 into %s", v.Type())
		}
		v.SetFloat(float64(arr.(*array.Float32).Value(i)))
	case arrow.FLOAT64:
		if v.Kind() != reflect.Float32 && v.Kind() != reflect.Float64 {
			return fmt.Errorf("arrow/reflect: cannot set float64 into %s", v.Type())
		}
		v.SetFloat(arr.(*array.Float64).Value(i))
	default:
		return fmt.Errorf("arrow/reflect: unsupported primitive type %v", arr.DataType())
	}
	return nil
}

func setTemporalValue(v reflect.Value, arr arrow.Array, i int) error {
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	switch arr.DataType().ID() {
	case arrow.TIMESTAMP:
		a, ok := arr.(*array.Timestamp)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Timestamp, got %T", arr)
		}
		if v.Type() != typeOfTime {
			return fmt.Errorf("arrow/reflect: cannot set time.Time into %s", v.Type())
		}
		unit := arr.DataType().(*arrow.TimestampType).Unit
		t := a.Value(i).ToTime(unit)
		v.Set(reflect.ValueOf(t))

	case arrow.DATE32:
		a, ok := arr.(*array.Date32)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Date32, got %T", arr)
		}
		if v.Type() != typeOfTime {
			return fmt.Errorf("arrow/reflect: cannot set time.Time into %s", v.Type())
		}
		t := a.Value(i).ToTime()
		v.Set(reflect.ValueOf(t))

	case arrow.DATE64:
		a, ok := arr.(*array.Date64)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Date64, got %T", arr)
		}
		if v.Type() != typeOfTime {
			return fmt.Errorf("arrow/reflect: cannot set time.Time into %s", v.Type())
		}
		t := a.Value(i).ToTime()
		v.Set(reflect.ValueOf(t))

	case arrow.TIME32:
		a, ok := arr.(*array.Time32)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Time32, got %T", arr)
		}
		if v.Type() != typeOfTime {
			return fmt.Errorf("arrow/reflect: cannot set time.Time into %s", v.Type())
		}
		unit := arr.DataType().(*arrow.Time32Type).Unit
		t := a.Value(i).ToTime(unit)
		v.Set(reflect.ValueOf(t))

	case arrow.TIME64:
		a, ok := arr.(*array.Time64)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Time64, got %T", arr)
		}
		if v.Type() != typeOfTime {
			return fmt.Errorf("arrow/reflect: cannot set time.Time into %s", v.Type())
		}
		unit := arr.DataType().(*arrow.Time64Type).Unit
		t := a.Value(i).ToTime(unit)
		v.Set(reflect.ValueOf(t))

	case arrow.DURATION:
		a, ok := arr.(*array.Duration)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Duration, got %T", arr)
		}
		if v.Type() != typeOfDuration {
			return fmt.Errorf("arrow/reflect: cannot set time.Duration into %s", v.Type())
		}
		unit := arr.DataType().(*arrow.DurationType).Unit
		dur := time.Duration(a.Value(i)) * unit.Multiplier()
		v.Set(reflect.ValueOf(dur))

	default:
		return fmt.Errorf("arrow/reflect: unsupported temporal type %v", arr.DataType())
	}
	return nil
}

func setDecimalValue(v reflect.Value, arr arrow.Array, i int) error {
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	switch arr.DataType().ID() {
	case arrow.DECIMAL128:
		a, ok := arr.(*array.Decimal128)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Decimal128, got %T", arr)
		}
		if v.Type() != typeOfDec128 {
			return fmt.Errorf("arrow/reflect: cannot set decimal128.Num into %s", v.Type())
		}
		num := a.Value(i)
		v.Set(reflect.ValueOf(num))

	case arrow.DECIMAL256:
		a, ok := arr.(*array.Decimal256)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Decimal256, got %T", arr)
		}
		if v.Type() != typeOfDec256 {
			return fmt.Errorf("arrow/reflect: cannot set decimal256.Num into %s", v.Type())
		}
		num := a.Value(i)
		v.Set(reflect.ValueOf(num))

	case arrow.DECIMAL32:
		a, ok := arr.(*array.Decimal32)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Decimal32, got %T", arr)
		}
		if v.Type() != typeOfDec32 {
			return fmt.Errorf("arrow/reflect: cannot set decimal.Decimal32 into %s", v.Type())
		}
		v.Set(reflect.ValueOf(a.Value(i)))

	case arrow.DECIMAL64:
		a, ok := arr.(*array.Decimal64)
		if !ok {
			return fmt.Errorf("arrow/reflect: expected *Decimal64, got %T", arr)
		}
		if v.Type() != typeOfDec64 {
			return fmt.Errorf("arrow/reflect: cannot set decimal.Decimal64 into %s", v.Type())
		}
		v.Set(reflect.ValueOf(a.Value(i)))

	default:
		return fmt.Errorf("arrow/reflect: unsupported decimal type %v", arr.DataType())
	}
	return nil
}

func setStructValue(v reflect.Value, sa *array.Struct, i int) error {
	if sa.IsNull(i) {
		if v.Kind() == reflect.Ptr {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return fmt.Errorf("arrow/reflect: cannot set struct into %s", v.Type())
	}

	fields := cachedStructFields(v.Type())
	st := sa.DataType().(*arrow.StructType)

	for _, fm := range fields {
		arrowIdx, found := st.FieldIdx(fm.Name)
		if !found {
			continue
		}
		if err := setValue(v.FieldByIndex(fm.Index), sa.Field(arrowIdx), i); err != nil {
			return fmt.Errorf("arrow/reflect: field %q: %w", fm.Name, err)
		}
	}
	return nil
}

func setListValue(v reflect.Value, arr array.ListLike, i int) error {
	if arr.IsNull(i) {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	if v.Kind() != reflect.Slice {
		return fmt.Errorf("arrow/reflect: cannot set list into %s", v.Type())
	}

	start, end := arr.ValueOffsets(i)
	child := arr.ListValues()
	length := int(end - start)

	result := reflect.MakeSlice(v.Type(), length, length)
	for j := 0; j < length; j++ {
		if err := setValue(result.Index(j), child, int(start)+j); err != nil {
			return fmt.Errorf("arrow/reflect: list element %d: %w", j, err)
		}
	}
	v.Set(result)
	return nil
}

func setMapValue(v reflect.Value, arr *array.Map, i int) error {
	if arr.IsNull(i) {
		if v.Kind() == reflect.Ptr {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	if v.Kind() != reflect.Map {
		return fmt.Errorf("arrow/reflect: cannot set map into %s", v.Type())
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
			return fmt.Errorf("arrow/reflect: map key %d: %w", j-int(start), err)
		}
		elemVal := reflect.New(elemType).Elem()
		if err := setValue(elemVal, items, j); err != nil {
			return fmt.Errorf("arrow/reflect: map value %d: %w", j-int(start), err)
		}
		result.SetMapIndex(keyVal, elemVal)
	}
	v.Set(result)
	return nil
}

func setFixedSizeListValue(v reflect.Value, arr *array.FixedSizeList, i int) error {
	if arr.IsNull(i) {
		if v.Kind() == reflect.Ptr {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}

	n := int(arr.DataType().(*arrow.FixedSizeListType).Len())
	child := arr.ListValues()
	start, _ := arr.ValueOffsets(i)

	switch v.Kind() {
	case reflect.Array:
		if v.Len() != n {
			return fmt.Errorf("arrow/reflect: fixed-size list length %d does not match Go array length %d", n, v.Len())
		}
		for k := 0; k < n; k++ {
			if err := setValue(v.Index(k), child, int(start)+k); err != nil {
				return fmt.Errorf("arrow/reflect: fixed-size list element %d: %w", k, err)
			}
		}
	case reflect.Slice:
		result := reflect.MakeSlice(v.Type(), n, n)
		for k := 0; k < n; k++ {
			if err := setValue(result.Index(k), child, int(start)+k); err != nil {
				return fmt.Errorf("arrow/reflect: fixed-size list element %d: %w", k, err)
			}
		}
		v.Set(result)
	default:
		return fmt.Errorf("arrow/reflect: cannot set fixed-size list into %s", v.Type())
	}
	return nil
}

func setDictionaryValue(v reflect.Value, arr *array.Dictionary, i int) error {
	if arr.IsNull(i) {
		if v.Kind() == reflect.Ptr {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}
	return setValue(v, arr.Dictionary(), arr.GetValueIndex(i))
}

func setRunEndEncodedValue(v reflect.Value, arr *array.RunEndEncoded, i int) error {
	if arr.IsNull(i) {
		if v.Kind() == reflect.Ptr {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}
	return setValue(v, arr.Values(), arr.GetPhysicalIndex(i))
}
