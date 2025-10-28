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
	_ "bytes"
	"fmt"
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

// Whether the storageType is compatible with TimestampWithOffset.
//
// Returns (time_unit, ok). If ok is false, time unit is garbage.
func isDataTypeCompatible(storageType arrow.DataType) (arrow.TimeUnit, bool) {
	timeUnit := arrow.Second
	switch t := storageType.(type) {
	case *arrow.StructType:
		if t.NumFields() != 2 {
			return timeUnit, false
		}

		maybeTimestamp := t.Field(0);
		maybeOffset := t.Field(1);

		timestampOk := false
		switch timestampType := maybeTimestamp.Type.(type) {
		case *arrow.TimestampType:
			if timestampType.TimeZone == "UTC" {
				timestampOk = true
				timeUnit = timestampType.TimeUnit()
			}
		default:
		}

		ok := maybeTimestamp.Name == "timestamp" &&
			timestampOk &&
			!maybeTimestamp.Nullable &&
			maybeOffset.Name == "offset_minutes" &&
			arrow.TypeEqual(maybeOffset.Type, arrow.PrimitiveTypes.Int16) &&
			!maybeOffset.Nullable

		return timeUnit, ok
	default:
		return timeUnit, false
	}
}


// NewTimestampWithOffsetType creates a new TimestampWithOffsetType with the underlying storage type set correctly to
// Struct(timestamp=Timestamp(T, "UTC"), offset_minutes=Int16), where T is any TimeUnit.
func NewTimestampWithOffsetType(unit arrow.TimeUnit) *TimestampWithOffsetType {
	return &TimestampWithOffsetType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: arrow.StructOf(
				arrow.Field{
					Name: "timestamp",
					Type: &arrow.TimestampType{
						Unit: unit,
						TimeZone: "UTC",
					},
					Nullable: false,
				},
				arrow.Field{
					Name: "offset_minutes",
					Type: arrow.PrimitiveTypes.Int16,
					Nullable: false,
				},
			),
		},
	}
}

func (b *TimestampWithOffsetType) ArrayType() reflect.Type { return reflect.TypeOf(TimestampWithOffsetArray{}) }

func (b *TimestampWithOffsetType) ExtensionName() string { return "arrow.timestamp_with_offset" }

func (b *TimestampWithOffsetType) String() string { return fmt.Sprintf("extension<%s>", b.ExtensionName()) }

func (e *TimestampWithOffsetType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}

func (b *TimestampWithOffsetType) Serialize() string { return "" }

func (b *TimestampWithOffsetType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	timeUnit, ok := isDataTypeCompatible(storageType)
	if !ok {
		return nil, fmt.Errorf("invalid storage type for TimestampWithOffsetType: %s", storageType.Name())
	}
	return NewTimestampWithOffsetType(timeUnit), nil
}

func (b *TimestampWithOffsetType) ExtensionEquals(other arrow.ExtensionType) bool {
	return b.ExtensionName() == other.ExtensionName()
}

func (b *TimestampWithOffsetType) TimeUnit() arrow.TimeUnit { 
	return b.ExtensionBase.Storage.(*arrow.StructType).Fields()[0].Type.(*arrow.TimestampType).TimeUnit()
}

func (b *TimestampWithOffsetType) NewBuilder(mem memory.Allocator) array.Builder {
	return NewTimestampWithOffsetBuilder(mem, b.TimeUnit())
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
	hours := offsetMinutes / 60;
	minutes := offsetMinutes % 60
	if minutes < 0 {
		minutes = -minutes
	}

	loc := time.FixedZone(fmt.Sprintf("UTC%+03d:%02d", hours, minutes), int(offsetMinutes) * 60)
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
	offsets := structs.Field(1).(*array.Int16)

	timeUnit := timestampField.DataType().(*arrow.TimestampType).Unit
	utcTimestamp := timestamps.Value(i)	
	offsetMinutes := offsets.Value(i)

	return utcTimestamp, offsetMinutes, timeUnit
}

func (a *TimestampWithOffsetArray) Value(i int) time.Time {
	if a.IsNull(i) {
		return time.Unix(0, 0)
	}
	utcTimestamp, offsetMinutes, timeUnit := a.rawValueUnsafe(i)
	return timeFromFieldValues(utcTimestamp, offsetMinutes, timeUnit)
}

func (a *TimestampWithOffsetArray) Values() []time.Time {
	values := make([]time.Time, a.Len())
	for i := range a.Len() {
		val := a.Value(i)
		values[i] = val
	}
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
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			utcTimestamp, offsetMinutes, timeUnit := a.rawValueUnsafe(i)
			values[i] = timeFromFieldValues(utcTimestamp, offsetMinutes, timeUnit)
		} else {
			values[i] = nil
		}
	}
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
	Layout string
	unit   arrow.TimeUnit
}

// NewTimestampWithOffsetBuilder creates a new TimestampWithOffsetBuilder, exposing a convenient and efficient interface
// for writing time.Time values to the underlying storage array.
func NewTimestampWithOffsetBuilder(mem memory.Allocator, unit arrow.TimeUnit) *TimestampWithOffsetBuilder {
	return &TimestampWithOffsetBuilder{
		unit: unit,
		Layout: time.RFC3339,
		ExtensionBuilder: array.NewExtensionBuilder(mem, NewTimestampWithOffsetType(unit)),
	}
}

func (b *TimestampWithOffsetBuilder) Append(v time.Time) {
	timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
	structBuilder := b.ExtensionBuilder.Builder.(*array.StructBuilder)

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.TimestampBuilder).Append(timestamp)
	structBuilder.FieldBuilder(1).(*array.Int16Builder).Append(int16(offsetMinutes))
}

func (b *TimestampWithOffsetBuilder) UnsafeAppend(v time.Time) {
	timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)
	structBuilder := b.ExtensionBuilder.Builder.(*array.StructBuilder)

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.TimestampBuilder).UnsafeAppend(timestamp)
	structBuilder.FieldBuilder(1).(*array.Int16Builder).UnsafeAppend(int16(offsetMinutes))
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
	timestamps := structBuilder.FieldBuilder(0).(*array.TimestampBuilder);
	offsets := structBuilder.FieldBuilder(1).(*array.Int16Builder);

	structBuilder.AppendValues(valids)

	for i, v := range values {
		timestamp, offsetMinutes := fieldValuesFromTime(v, b.unit)

		// SAFETY: by this point we know all buffers have available space given the earlier
		// call to structBuilder.AppendValues which calls Reserve internally
		if valids[i] {
			timestamps.UnsafeAppend(timestamp)
			offsets.UnsafeAppend(offsetMinutes)
		} else {
			timestamps.UnsafeAppendBoolToBitmap(false)
			offsets.UnsafeAppendBoolToBitmap(false)
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
	_ array.Builder 	       = (*TimestampWithOffsetBuilder)(nil)
)
