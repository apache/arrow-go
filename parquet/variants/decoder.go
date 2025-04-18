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

package variants

import (
	"errors"
	"fmt"
	"reflect"
)

// Decode provides a way to decode an encoded Variant into a Go type. The mapping of Variant
// types to Go types is:
//   - Null: nil
//   - Boolean: bool
//   - Int8, Int16, Int32, Int64: int64
//   - Float: float32
//   - Double: float64
//   - Time: time.Time
//   - Timestamp (all varieties): time.Time
//   - String: string
//   - Binary: []byte
//   - UUID: string
//   - Array: []any
//   - Object: map[string]any

// DecodeInto provides a way to decode an encoded Variant into a provided non-nil pointer dest if possible.
// For structs, this will attempt to match field names or fields annotated with the `variant` field annotation.
// TODO(Finish this comment)
func DecodeInto(encoded *MarshaledVariant, dest any) error {
	destVal := reflect.ValueOf(dest)
	if kind := destVal.Kind(); kind != reflect.Pointer {
		return fmt.Errorf("dest must be a pointer (got %s)", kind)
	}
	if destVal.IsNil() {
		return errors.New("dest pointer must not be nil")
	}

	md, err := decodeMetadata(encoded.Metadata)
	if err != nil {
		return fmt.Errorf("could not decode metadata: %v", err)
	}

	return unmarshalCommon(encoded.Value, md, 0, destVal)
}

func unmarshalCommon(raw []byte, md *decodedMetadata, offset int, dest reflect.Value) error {
	if err := checkBounds(raw, offset, offset); err != nil {
		return err
	}
	switch bt := BasicTypeFromHeader(raw[offset]); bt {
	case BasicPrimitive, BasicShortString:
		if err := unmarshalPrimitive(raw, offset, dest); err != nil {
			return fmt.Errorf("could not decode primitive: %v", err)
		}
	case BasicArray:
		if err := unmarshalArray(raw, md, offset, dest); err != nil {
			return fmt.Errorf("could not decode array: %v", err)
		}
	case BasicObject:
		if err := unmarshalObject(raw, md, offset, dest); err != nil {
			return fmt.Errorf("could not decode object: %v", err)
		}
	}
	return nil
}
