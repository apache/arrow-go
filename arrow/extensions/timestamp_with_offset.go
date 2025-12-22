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

package extensions

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
)

// TimestampWithOffsetType represents a timestamp column that stores a timezone offset per row instead of
// applying the same timezone offset to the entire column.
type TimestampWithOffsetType struct {
	arrow.ExtensionBase
}

func isOffsetTypeOk(offsetType arrow.DataType) bool {
	switch offsetType := offsetType.(type) {
	case *arrow.Int16Type:
		return true
	case *arrow.DictionaryType:
		return arrow.IsInteger(offsetType.IndexType.ID()) && arrow.TypeEqual(offsetType.ValueType, arrow.PrimitiveTypes.Int16)
	case *arrow.RunEndEncodedType:
		return offsetType.ValidRunEndsType(offsetType.RunEnds()) && 
			arrow.TypeEqual(offsetType.Encoded(), arrow.PrimitiveTypes.Int16)
			// FIXME: Technically this should be non-nullable, but a Arrow IPC does not deserialize
			// ValueNullable properly, so enforcing this here would always fail when reading from an IPC
			// stream
			// !offsetType.ValueNullable
	default:
		return false
	}
}

// Whether the storageType is compatible with TimestampWithOffset.
//
// Returns (time_unit, offset_type, ok). If ok is false, time_unit and offset_type are garbage.
func isDataTypeCompatible(storageType arrow.DataType) (arrow.TimeUnit, arrow.DataType, bool) {
	timeUnit := arrow.Second
	offsetType := arrow.PrimitiveTypes.Int16
	switch t := storageType.(type) {
	case *arrow.StructType:
		if t.NumFields() != 2 {
			return timeUnit, offsetType, false
		}

		maybeTimestamp := t.Field(0)
		maybeOffset := t.Field(1)

		timestampOk := false
		switch timestampType := maybeTimestamp.Type.(type) {
		case *arrow.TimestampType:
			if timestampType.TimeZone == "UTC" {
				timestampOk = true
				timeUnit = timestampType.TimeUnit()
			}
		default:
		}

		offsetOk := isOffsetTypeOk(maybeOffset.Type)

		ok := maybeTimestamp.Name == "timestamp" &&
			timestampOk &&
			!maybeTimestamp.Nullable &&
			maybeOffset.Name == "offset_minutes" &&
			offsetOk &&
			!maybeOffset.Nullable

		return timeUnit, maybeOffset.Type, ok
	default:
		return timeUnit, offsetType, false
	}
}

// NewTimestampWithOffsetType creates a new TimestampWithOffsetType with the underlying storage type set correctly to
// Struct(timestamp=Timestamp(T, "UTC"), offset_minutes=O), where T is any TimeUnit and O is a valid offset type.
//
// The error will be populated if the data type is not a valid encoding of the offsets field.
func NewTimestampWithOffsetType(unit arrow.TimeUnit, offsetType arrow.DataType) (*TimestampWithOffsetType, error) {
	if !isOffsetTypeOk(offsetType) {
		return nil, errors.New(fmt.Sprintf("Invalid offset type %s", offsetType))
	}

	return &TimestampWithOffsetType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: arrow.StructOf(
				arrow.Field{
					Name: "timestamp",
					Type: &arrow.TimestampType{
						Unit:     unit,
						TimeZone: "UTC",
					},
					Nullable: false,
				},
				arrow.Field{
					Name:     "offset_minutes",
					Type:     offsetType,
					Nullable: false,
				},
			),
		},
	}, nil
}


// NewTimestampWithOffsetTypePrimitiveEncoded creates a new TimestampWithOffsetType with the underlying storage type set correctly to
// Struct(timestamp=Timestamp(T, "UTC"), offset_minutes=Int16), where T is any TimeUnit.
func NewTimestampWithOffsetTypePrimitiveEncoded(unit arrow.TimeUnit) *TimestampWithOffsetType {
	v, _ := NewTimestampWithOffsetType(unit, arrow.PrimitiveTypes.Int16)
	// SAFETY: This should never error as Int16 is always a valid offset type

	return v
}

// NewTimestampWithOffsetType creates a new TimestampWithOffsetType with the underlying storage type set correctly to
// Struct(timestamp=Timestamp(T, "UTC"), offset_minutes=Dictionary(I, Int16)), where T is any TimeUnit and I is a
// valid Dictionary index type.
//
// The error will be populated if the index is not a valid dictionary-encoding index type.
func NewTimestampWithOffsetTypeDictionaryEncoded(unit arrow.TimeUnit, index arrow.DataType) (*TimestampWithOffsetType, error) {
	offsetType := arrow.DictionaryType{
		IndexType: index,
		ValueType: arrow.PrimitiveTypes.Int16,
		Ordered:   false,
	}
	return NewTimestampWithOffsetType(unit, &offsetType)
}

// NewTimestampWithOffsetType creates a new TimestampWithOffsetType with the underlying storage type set correctly to
// Struct(timestamp=Timestamp(T, "UTC"), offset_minutes=RunEndEncoded(E, Int16)), where T is any TimeUnit and E is a
// valid run-ends type.
//
// The error will be populated if runEnds is not a valid run-end encoding run-ends type.
func NewTimestampWithOffsetTypeRunEndEncoded(unit arrow.TimeUnit, runEnds arrow.DataType) (*TimestampWithOffsetType, error) {
	offsetType := arrow.RunEndEncodedOf(runEnds, arrow.PrimitiveTypes.Int16)
	if !offsetType.ValidRunEndsType(runEnds) {
		return nil, errors.New(fmt.Sprintf("Invalid run-ends type %s", runEnds))
	}

	return NewTimestampWithOffsetType(unit, offsetType)
}


func (b *TimestampWithOffsetType) ArrayType() reflect.Type {
	return reflect.TypeOf(TimestampWithOffsetArray{})
}

func (b *TimestampWithOffsetType) ExtensionName() string { return "arrow.timestamp_with_offset" }

func (b *TimestampWithOffsetType) String() string {
	return fmt.Sprintf("extension<%s>", b.ExtensionName())
}

func (e *TimestampWithOffsetType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}

func (b *TimestampWithOffsetType) Serialize() string { return "" }

func (b *TimestampWithOffsetType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	timeUnit, offsetType, ok := isDataTypeCompatible(storageType)
	if !ok {
		return nil, fmt.Errorf("invalid storage type for TimestampWithOffsetType: %s", storageType.Name())
	}

	v, _ := NewTimestampWithOffsetType(timeUnit, offsetType)
	// SAFETY: the offsetType has already been checked by isDataTypeCompatible, so we can ignore the error

	return v, nil
}

func (b *TimestampWithOffsetType) ExtensionEquals(other arrow.ExtensionType) bool {
	return b.ExtensionName() == other.ExtensionName()
}

func (b *TimestampWithOffsetType) TimeUnit() arrow.TimeUnit {
	return b.ExtensionBase.Storage.(*arrow.StructType).Fields()[0].Type.(*arrow.TimestampType).TimeUnit()
}

func (b *TimestampWithOffsetType) NewBuilder(mem memory.Allocator) array.Builder {
	v, _ := NewTimestampWithOffsetBuilder(mem, b.TimeUnit(), arrow.PrimitiveTypes.Int16)
	// SAFETY: This will never error as Int16 is always a valid type for the offset field

	return v
}

// TimestampWithOffsetArray is a simple array of struct
type TimestampWithOffsetArray struct {
	array.ExtensionArrayBase
}

func (a *TimestampWithOffsetArray) String() string {
	var o strings.Builder
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(array.NullValueStr)
		default:
			fmt.Fprintf(&o, "\"%s\"", a.Value(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func timeFromFieldValues(utcTimestamp arrow.Timestamp, offsetMinutes int16, unit arrow.TimeUnit) time.Time {
	hours := offsetMinutes / 60
	minutes := offsetMinutes % 60
	if minutes < 0 {
		minutes = -minutes
	}

	loc := time.FixedZone(fmt.Sprintf("UTC%+03d:%02d", hours, minutes), int(offsetMinutes)*60)
	return utcTimestamp.ToTime(unit).In(loc)
}

func fieldValuesFromTime(t time.Time, unit arrow.TimeUnit) (arrow.Timestamp, int16) {
	// naive "bitwise" conversion to UTC, keeping the underlying date the same
	utc := t.UTC()
	naiveUtc := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	offsetMinutes := int16(naiveUtc.Sub(t).Minutes())
	// SAFETY: unit MUST have been validated to a valid arrow.TimeUnit value before
	// this function. Otherwise, ignoring this error is not safe.
	timestamp, _ := arrow.TimestampFromTime(utc, unit)
	return timestamp, offsetMinutes
}

// Get the raw arrow values at the given index
//
// SAFETY: the value at i must not be nil
func (a *TimestampWithOffsetArray) rawValueUnsafe(i int) (arrow.Timestamp, int16, arrow.TimeUnit) {
	structs := a.Storage().(*array.Struct)

	timestampField := structs.Field(0)
	timestamps := timestampField.(*array.Timestamp)

	timeUnit := timestampField.DataType().(*arrow.TimestampType).Unit
	utcTimestamp := timestamps.Value(i)

	var offsetMinutes int16

	switch offsets := structs.Field(1).(type) {
	case *array.Int16:
		offsetMinutes = offsets.Value(i)
	case *array.Dictionary:
		offsetMinutes = offsets.Dictionary().(*array.Int16).Value(offsets.GetValueIndex(i))
	case *array.RunEndEncoded:
		offsetMinutes = offsets.Values().(*array.Int16).Value(offsets.GetPhysicalIndex(i))
	}

	return utcTimestamp, offsetMinutes, timeUnit
}

func (a *TimestampWithOffsetArray) Value(i int) time.Time {
	if a.IsNull(i) {
		return time.Unix(0, 0)
	}
	utcTimestamp, offsetMinutes, timeUnit := a.rawValueUnsafe(i)
	return timeFromFieldValues(utcTimestamp, offsetMinutes, timeUnit)
}

// Iterates over the array and calls the callback with the timestamp at each position. If it's null,
// the timestamp will be nil.
//
// This will iterate using the fastest method given the underlying storage array
func (a* TimestampWithOffsetArray) iterValues(callback func(i int, utcTimestamp *time.Time)) {
	structs := a.Storage().(*array.Struct)
	offsets := structs.Field(1)
	if reeOffsets, isRee := offsets.(*array.RunEndEncoded); isRee {
		timestampField := structs.Field(0)
		timeUnit := timestampField.DataType().(*arrow.TimestampType).Unit
		timestamps := timestampField.(*array.Timestamp)

		offsetValues := reeOffsets.Values().(*array.Int16)
		offsetPhysicalIdx := 0

		var getRunEnd (func(int) int)
		switch arr := reeOffsets.RunEndsArr().(type) {
		case *array.Int16:
			getRunEnd = func(idx int) int { return int(arr.Value(idx)) }
		case *array.Int32:
			getRunEnd = func(idx int) int { return int(arr.Value(idx)) }
		case *array.Int64:
			getRunEnd = func(idx int) int { return int(arr.Value(idx)) }
		}

		for i := 0; i < a.Len(); i++ {
			if i >= getRunEnd(offsetPhysicalIdx) {
				offsetPhysicalIdx += 1
			}

			timestamp := (*time.Time)(nil)
			if a.IsValid(i) {
				utcTimestamp := timestamps.Value(i)
				offsetMinutes := offsetValues.Value(offsetPhysicalIdx)
				v := timeFromFieldValues(utcTimestamp, offsetMinutes, timeUnit)
				timestamp = &v
			} 

			callback(i, timestamp)
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			timestamp := (*time.Time)(nil)
			if a.IsValid(i) {
				utcTimestamp, offsetMinutes, timeUnit := a.rawValueUnsafe(i)
				v := timeFromFieldValues(utcTimestamp, offsetMinutes, timeUnit)
				timestamp = &v
			} 

			callback(i, timestamp)
		}
	}
}


func (a *TimestampWithOffsetArray) Values() []time.Time {
	values := make([]time.Time, a.Len())
	a.iterValues(func(i int, timestamp *time.Time) {
		if timestamp == nil {
			values[i] = time.Unix(0, 0)
		} else {
			values[i] = *timestamp
		}
	})
	return values
}

func (a *TimestampWithOffsetArray) ValueStr(i int) string {
	switch {
	case a.IsNull(i):
		return array.NullValueStr
	default:
		return a.Value(i).String()
	}
}

func (a *TimestampWithOffsetArray) MarshalJSON() ([]byte, error) {
	values := make([]interface{}, a.Len())
	a.iterValues(func(i int, timestamp *time.Time) {
		values[i] = timestamp
	})
	return json.Marshal(values)
}

func (a *TimestampWithOffsetArray) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.Value(i)
}

// TimestampWithOffsetBuilder is a convenience builder for the TimestampWithOffset extension type,
// allowing arrays to be built with boolean values rather than the underlying storage type.
type TimestampWithOffsetBuilder struct {
	*array.ExtensionBuilder

	// The layout used to parse any timestamps from strings. Defaults to time.RFC3339
	Layout     string
	unit       arrow.TimeUnit
	offsetType arrow.DataType
	// lastOffset is only used to determine when to start new runs with run-end encoded offsets
	lastOffset int16
}

// NewTimestampWithOffsetBuilder creates a new TimestampWithOffsetBuilder, exposing a convenient and efficient interface
// for writing time.Time values to the underlying storage array.
func NewTimestampWithOffsetBuilder(mem memory.Allocator, unit arrow.TimeUnit, offsetType arrow.DataType) (*TimestampWithOffsetBuilder, error) {
	dataType, err := NewTimestampWithOffsetType(unit, offsetType)
	if err != nil {
		return nil, err
	}

	return &TimestampWithOffsetBuilder{
		unit:             unit,
		offsetType: 	  offsetType,
		lastOffset:       math.MaxInt16,
		Layout:           time.RFC3339,
		ExtensionBuilder: array.NewExtensionBuilder(mem, dataType),
	}, nil
}

func (b *TimestampWithOffsetBuilder) Append(v time.Time) {
	timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
	offsetMinutes16 := int16(offsetMinutes)
	structBuilder := b.ExtensionBuilder.Builder.(*array.StructBuilder)

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.TimestampBuilder).Append(timestamp)

	switch offsets := structBuilder.FieldBuilder(1).(type) {
	case *array.Int16Builder:
		offsets.Append(offsetMinutes16)
	case *array.Int16DictionaryBuilder:
		offsets.Append(offsetMinutes16)
	case *array.RunEndEncodedBuilder:
		if offsetMinutes != b.lastOffset {
			offsets.Append(1)
			offsets.ValueBuilder().(*array.Int16Builder).Append(offsetMinutes16)
		} else {
			offsets.ContinueRun(1)
		}

		b.lastOffset = offsetMinutes16
	}

}

func (b *TimestampWithOffsetBuilder) UnsafeAppend(v time.Time) {
	timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
	offsetMinutes16 := int16(offsetMinutes)
	structBuilder := b.ExtensionBuilder.Builder.(*array.StructBuilder)

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.TimestampBuilder).UnsafeAppend(timestamp)

	switch offsets := structBuilder.FieldBuilder(1).(type) {
	case *array.Int16Builder:
		offsets.UnsafeAppend(offsetMinutes16)
	case *array.Int16DictionaryBuilder:
		offsets.Append(offsetMinutes16)
	case *array.RunEndEncodedBuilder:
		if offsetMinutes != b.lastOffset {
			offsets.Append(1)
			offsets.ValueBuilder().(*array.Int16Builder).Append(offsetMinutes16)
		} else {
			offsets.ContinueRun(1)
		}

		b.lastOffset = offsetMinutes16
	}
}

// By default, this will try to parse the string using the RFC3339 layout.
//
// You can change the default layout by using builder.SetLayout()
func (b *TimestampWithOffsetBuilder) AppendValueFromString(s string) error {
	if s == array.NullValueStr {
		b.AppendNull()
		return nil
	}

	parsed, err := time.Parse(b.Layout, s)
	if err != nil {
		return err
	}

	b.Append(parsed)
	return nil
}

func (b *TimestampWithOffsetBuilder) AppendValues(values []time.Time, valids []bool) {
	structBuilder := b.ExtensionBuilder.Builder.(*array.StructBuilder)
	timestamps := structBuilder.FieldBuilder(0).(*array.TimestampBuilder)

	structBuilder.AppendValues(valids)
	// SAFETY: by this point we know all buffers have available space given the earlier
	// call to structBuilder.AppendValues which calls Reserve internally, so it's OK to
	// call UnsafeAppend on inner builders

	switch offsets := structBuilder.FieldBuilder(1).(type) {
	case *array.Int16Builder:
		for i, v := range values {
			timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
			if valids[i] {
				timestamps.UnsafeAppend(timestamp)
				offsets.UnsafeAppend(offsetMinutes)
			} else {
				timestamps.UnsafeAppendBoolToBitmap(false)
				offsets.UnsafeAppendBoolToBitmap(false)
			}
		}
	case *array.Int16DictionaryBuilder:
		for i, v := range values {
			timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
			if valids[i] {
				timestamps.UnsafeAppend(timestamp)
				offsets.Append(offsetMinutes)
			} else {
				timestamps.UnsafeAppendBoolToBitmap(false)
				offsets.UnsafeAppendBoolToBitmap(false)
			}
		}
	case *array.RunEndEncodedBuilder:
		offsetValuesBuilder := offsets.ValueBuilder().(*array.Int16Builder)
		for i, v := range values {
			timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
			if valids[i] {
				timestamps.UnsafeAppend(timestamp)
				offsetMinutes16 := int16(offsetMinutes)
				if offsetMinutes != b.lastOffset {
					offsets.Append(1)
					offsetValuesBuilder.Append(offsetMinutes16)
				} else {
					offsets.ContinueRun(1)
				}
				b.lastOffset = offsetMinutes16
			} else {
				timestamps.UnsafeAppendBoolToBitmap(false)
				offsets.UnsafeAppendBoolToBitmap(false)
			}
		}
	}
}

func (b *TimestampWithOffsetBuilder) UnmarshalOne(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("failed to decode json: %w", err)
	}

	switch raw := tok.(type) {
	case string:
		t, err := time.Parse(b.Layout, raw)
		if err != nil {
			return fmt.Errorf("failed to parse string \"%s\" as time.Time using layout \"%s\"", raw, b.Layout)
		}
		b.Append(t)
	case nil:
		b.AppendNull()
	default:
		return fmt.Errorf("expected date string")
	}

	return nil
}

func (b *TimestampWithOffsetBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ arrow.ExtensionType          = (*TimestampWithOffsetType)(nil)
	_ array.CustomExtensionBuilder = (*TimestampWithOffsetType)(nil)
	_ array.ExtensionArray         = (*TimestampWithOffsetArray)(nil)
	_ array.Builder                = (*TimestampWithOffsetBuilder)(nil)
)
