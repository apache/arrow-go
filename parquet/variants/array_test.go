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
	"bytes"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestArrayPrimitives(t *testing.T) {
	var buf bytes.Buffer
	ab := newArrayBuilder(&buf, nil, nil)
	toEncode := []any{true, 256, "hello", 10, []byte{'t', 'h', 'e', 'r', 'e'}}
	for _, te := range toEncode {
		if err := ab.Write(te); err != nil {
			t.Fatalf("Write(%v): %v", te, err)
		}
	}
	if err := ab.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0b00011, // Basic type = array, field_offset_minus_one = 0, is_large = false
		0x05,    // 5 elements in the array
		0x00,    // Index of "true"
		0x01,    // Index of 256
		0x04,    // Index of "hello"
		0x0A,    // Index of 10
		0x0C,    // Index of []byte{t,h,e,r,e}
		0x16,    // First index after last element
		0b100,   // Primitive true (header & value)
		0b10000, // Primitive int16
		0x00,
		0x01,    // 256 encoded
		0b10101, // Basic short string, length = 5
		'h',
		'e',
		'l',
		'l',
		'o',      // "hello" encoded
		0b1100,   // Primitive int8
		0x0A,     // 10 encoded
		0b111100, // Primitive binary
		0x05,
		0x00,
		0x00,
		0x00, // bytes of length 5
		't',
		'h',
		'e',
		'r',
		'e', // []byte{t,h,e,r,e} encoded
	}

	diffByteArrays(t, buf.Bytes(), wantBytes)

	// Decode and ensure we got what was expected.
	var got []any
	if err := unmarshalArray(buf.Bytes(), &decodedMetadata{}, 0, reflect.ValueOf(&got)); err != nil {
		t.Fatalf("unmarshalArray(): %v", err)
	}
	want := []any{true, int64(256), "hello", int64(10), []byte{'t', 'h', 'e', 'r', 'e'}}
	if diff := cmp.Diff(got, want); diff != "" {
		t.Fatalf("Incorrect returned array. Diff (-got +want):\n%s", diff)
	}
}

func TestArrayLarge(t *testing.T) {
	var buf bytes.Buffer
	ab := newArrayBuilder(&buf, nil, nil)
	// Create 256 items, which triggers "is_large" (256 cannot be encoded in one byte)
	for i := range 256 {
		if err := ab.Write(true); err != nil {
			t.Fatalf("Write(iter = %d): %v", i, err)
		}
	}
	if err := ab.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}
	wantBytes := []byte{
		0b10111, // Basic type = array, field_offset_minus_one = 1, is_large = true
		0x00,
		0x01,
		0x00,
		0x00, // 256 encoded in 4 bytes
	}
	// Create the offset section
	for i := range 256 {
		wantBytes = append(wantBytes, []byte{byte(i), 0}...)
	}
	wantBytes = append(wantBytes, []byte{0, 1}...) // 256- the first index after all elements.

	// Create 256 trues
	for range 256 {
		wantBytes = append(wantBytes, 4) // 0x04 is basic type true
	}
	diffByteArrays(t, buf.Bytes(), wantBytes)
}

func TestNestedArray(t *testing.T) {
	var buf bytes.Buffer
	ab := newArrayBuilder(&buf, nil, nil)

	// Create a nested array so that we get {true, 1, {false, 256}, 3}
	ab.Write(true)
	ab.Write(1)
	nested := ab.Array()
	nested.Write(false)
	nested.Write(256)
	nested.Build()
	ab.Write(3)

	if err := ab.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0b00011, // Basic type = array, field_offset_minus_one = 0, is_large = false
		0x04,    // 4 elements in the array
		0x00,    // Index of "true"
		0x01,    // Index of 1
		0x03,    // Index of nested array
		0x0C,    // Index of 3
		0x0E,    // First index after last element
		0b100,   // Primitive true (header & value)
		0b1100,  // Primitive int8
		0x01,    // 1 encoded
		// Beginning of nested array
		0b00011, // Nested array, basic type = array, field_offset_minus_one = 0, is_large = false
		0x02,    // 2 elements in the array
		0x00,    // Index of "false"
		0x01,    // Index of 256
		0x04,    // First index after last element
		0b1000,  // Primitive false (header & value)
		0b10000, // Primitive int16
		0x00,
		0x01, // 256 encoded
		// End of nested array
		0b1100, // Primitive int8
		0x03,   // 3 encoded
	}

	diffByteArrays(t, buf.Bytes(), wantBytes)
}

func TestFromSlice(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ab := newArrayBuilder(&buf, mdb, nil)
	if err := ab.fromSlice([]any{1, false, 2}); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if err := ab.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}
	wantBytes := []byte{
		0x03,         // Basic type = array, field offset size = 1, is_large = false,
		0x03,         // 3 items in array
		0x00,         // Index of 1
		0x02,         // Index of false
		0x03,         // Index of 2
		0x05,         // First index after last element
		0b1100, 0x01, // Int8, value = 1
		0b1000,       // false (value and header)
		0b1100, 0x02, // Int8, value = 2
	}
	diffByteArrays(t, buf.Bytes(), wantBytes)
}

func TestNestedObject(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ab := newArrayBuilder(&buf, mdb, nil)

	ab.Write(true)
	ab.Write(1)
	nested := ab.Object()
	nested.Write("a", false)
	nested.Write("b", 256)
	nested.Build()

	ab.Write(3)

	if err := ab.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0b00011, // Basic type = array, field_offset_minus_one = 0, is_large = false
		0x04,    // 4 elements in the array
		0x00,    // Index of "true"
		0x01,    // Index of 1
		0x03,    // Index of nested object
		0x0E,    // Index of 3
		0x10,    // First index after the last element
		0b100,   // Primitive true (header & value)
		0b1100,  // Primitive int8
		0x01,    // 1 encoded
		// Beginning of nested object
		0b00000010, // Basic type = object, field_offset_size & field_id_size = 1, is_large = false
		0x02,       // 2 elements
		0x00,       // Field ID of "a"
		0x01,       // FieldID of "b"
		0x00,       // Index of "a"s value (false)
		0x01,       // Index of "b"s value (256)
		0x04,       // First index after the last value
		0b1000,     // Primitive false (header & value)
		0b10000,    // Primitive int16
		0x00,
		0x01, // 256 encoded
		// End of nested object
		0b1100, // Primitive int8
		0x03,   // 3 encoded
	}

	diffByteArrays(t, buf.Bytes(), wantBytes)
}

func checkErr(t *testing.T, wantErr bool, err error) {
	t.Helper()
	if err != nil {
		if wantErr {
			return
		}
		t.Fatalf("Unexpected error: %v", err)
	} else if wantErr {
		t.Fatal("Got no error when one was expected")
	}
}

func TestGetArrayData(t *testing.T) {
	cases := []struct {
		name    string
		encoded []byte
		offset  int
		want    *arrayData
		wantErr bool
	}{
		{
			name: "Array with offset",
			encoded: []byte{
				0x00, 0x00, // Offset bytes
				0b11,                // Basic type = array, field offset size = 1, is_large = false,
				0x03,                // 3 elements in the array,
				0x00,                // Index of "true"
				0x01,                // Index of 256
				0x04,                // Index of "hello"
				0x0A,                // First index after last element
				0b100,               // Primitive true
				0b10000, 0x00, 0x01, // Primitive int16 val = 256
				0b10101, 'h', 'e', 'l', 'l', 'o', // Basic short string, length = 5
			},
			offset: 2,
			want: &arrayData{
				size:           16,
				numElements:    3,
				firstOffsetIdx: 4,
				firstDataIdx:   8,
				offsetWidth:    1,
			},
		},
		{
			name: "Array with large widths",
			encoded: []byte{
				0b00011011,             // Basic type = array, field offset size = 3, is_large = true
				0x03, 0x00, 0x00, 0x00, // 3 elements in the array
				0x00, 0x00, 0x00, // Index of "true"
				0x01, 0x00, 0x00, // Index of 256
				0x04, 0x00, 0x00, // Index of "hello"
				0x0A, 0x00, 0x00, // First index after last element
				0b100,               // Primitive true (header & value)
				0b10000, 0x00, 0x01, // Primitive int16 val = 256
				0b10101, 'h', 'e', 'l', 'l', 'o', // Basic short string, length = 5
			},
			want: &arrayData{
				size:           27,
				numElements:    3,
				firstOffsetIdx: 5,
				firstDataIdx:   17,
				offsetWidth:    3,
			},
		},
		{
			name:    "Not an array",
			encoded: []byte{0x00, 0x00}, // Primitive nulls
			wantErr: true,
		},
		{
			name:    "Elements would be out of bounds",
			encoded: []byte{0b11, 0x03, 0x00, 0x01, 0x04, 0x0A, 0b100, 0b10000, 0x00, 0x01 /* missing string */},
			wantErr: true,
		},
		{
			name:    "Offset is out of bounds",
			encoded: []byte{0b11, 0x01, 0x00, 0x01, 0b100}, // Array with one boolean
			offset:  10,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := getArrayData(c.encoded, c.offset)
			checkErr(t, c.wantErr, err)
			if diff := cmp.Diff(got, c.want, cmp.AllowUnexported(arrayData{})); diff != "" {
				t.Fatalf("Incorrect returned value. Diff (-got + want):\n%s", diff)
			}
		})
	}
}

func TestUnmarshalArray(t *testing.T) {
	cases := []struct {
		name    string
		md      *decodedMetadata
		encoded []byte
		offset  int
		// Normally the test will unmarhall into the type in want, but if this override is set,
		// it'll try to unmarshal into something of this type.
		overrideDecodeType reflect.Type
		want               any
		wantErr            bool
	}{
		{
			name:   "Array with large widths and offset",
			offset: 3,
			encoded: []byte{
				0x00, 0x00, 0x00, // 3 offset bytes
				0b00011011,             // Basic type = array, field offset size = 3, is_large = true
				0x03, 0x00, 0x00, 0x00, // 3 elements in the array
				0x00, 0x00, 0x00, // Index of "true"
				0x01, 0x00, 0x00, // Index of 256
				0x04, 0x00, 0x00, // Index of "hello"
				0x0A, 0x00, 0x00, // First index after last element
				0b100,               // Primitive true (header & value)
				0b10000, 0x00, 0x01, // Primitive int16 val = 256
				0b10101, 'h', 'e', 'l', 'l', 'o', // Basic short string, length = 5
			},
			want: []any{true, int64(256), "hello"},
		},
		{
			name: "Unmarshal into typed array",
			encoded: []byte{
				0b011,                  // Basic type = array, field offset size = 1, is_large = false
				0x03,                   // 3 elements in the array
				0x00, 0x02, 0x04, 0x06, // Offsets for 3 integers and the index after the last element.
				0b1100, 0x01, // Primitive int8 val = 1
				0b1100, 0x02, // Primitive int8 val = 2
				0b1100, 0x03, // Primitive int8 val = 3
			},
			want: []int{1, 2, 3},
		},
		{
			name: "Nested array",
			encoded: []byte{
				0b011,      // Basic type = array, field offset size = 1, is_large = false
				0x01,       // 1 element in the array
				0x00, 0x06, // Offsets for nested array and index after the last element
				0b011,      // Nested array, field offset size = 1, is_large = false
				0x01,       // 1 element in the array
				0x00, 0x02, // Offsets for 1 integer and index after the last element
				0b1100, 0x01, //primitive int8 val = 1
			},
			want: []any{[]any{int64(1)}},
		},
		{
			name:               "Invalid data",
			encoded:            []byte{0x00, 0x00},
			overrideDecodeType: reflect.TypeOf([]any{}),
			wantErr:            true,
		},
		{
			name: "Can't decode into primitive",
			encoded: []byte{
				0b011,                  // Basic type = array, field offset size = 1, is_large = false
				0x03,                   // 3 elements in the array
				0x00, 0x02, 0x04, 0x06, // Offsets for 3 integers and the index after the last element.
				0b1100, 0x01, // Primitive int8 val = 1
				0b1100, 0x02, // Primitive int8 val = 2
				0b1100, 0x03, // Primitive int8 val = 3
			},
			overrideDecodeType: reflect.TypeOf(""),
			wantErr:            true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			typ := reflect.TypeOf(c.want)
			if c.overrideDecodeType != nil {
				typ = c.overrideDecodeType
			}
			got := reflect.New(typ)
			if err := unmarshalArray(c.encoded, c.md, c.offset, got); err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
			diff(t, got.Elem().Interface(), c.want)
		})
	}
}
