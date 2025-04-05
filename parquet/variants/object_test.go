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

func TestObjectFromStruct(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ob := newObjectBuilder(&buf, mdb, nil)

	type nested struct {
		Baz string `variant:"d"`
	}

	st := struct {
		Foo        int  `variant:"b"`
		Bar        bool `variant:"a"`
		unexported int  `variant:"c"`
		Nest       nested
	}{1, true, 2, nested{Baz: "hi"}}

	if err := ob.fromStruct(&st); err != nil {
		t.Fatalf("fromStruct(): %v", err)
	}
	if err := ob.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0x02, // Basic object, all sizes = 1 byte
		0x03, // 3 items
		// Keys are inserted in the order "b", "a", "d", "Nest". "Nest" comes first
		// since N (value 78) < a (value 97).
		0x03,         // Nested's index
		0x01,         // a's index
		0x00,         // b's index
		0x03,         // Nest's value
		0x02,         // a's value
		0x00,         // b's value
		0x0B,         // end
		0b1100, 0x01, // b's value, int8 val = 1
		0b0100, // a's value
		// Nested object
		0x02,             // Basic object, all sizes = 1 byte
		0x01,             // 1 item
		0x02,             // d's index in the dictionary
		0x00,             // d's offset
		0x03,             // last item
		0b1001, 'h', 'i', // Basic short string, length 2
		// End of nested object
	}
	got := buf.Bytes()

	diffByteArrays(t, got, wantBytes)

	// Check the metadata keys as well to ensure the struct tags were picked up approrpiately.
	encodedMD := mdb.Build()
	decodedMetadata, err := decodeMetadata(encodedMD)
	if err != nil {
		t.Fatalf("Metadata decode error: %v", err)
	}
	gotKeys, wantKeys := decodedMetadata.keys, []string{"b", "a", "d", "Nest"}
	if diff := cmp.Diff(gotKeys, wantKeys); diff != "" {
		t.Fatalf("Incorrect metadata keys. Diff (-got +want):\n%s", diff)
	}
}

func TestObjectFromMap(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ob := newObjectBuilder(&buf, mdb, nil)

	toWrite := map[string]any{
		"key1": int64(1),
		"key2": false,
		"key3": int64(2),
	}
	if err := ob.fromMap(toWrite); err != nil {
		t.Fatalf("fromMap(): %v", err)
	}
	if err := ob.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}
	decodedMD, err := decodeMetadata(mdb.Build())
	if err != nil {
		t.Fatalf("decodeMetadata(): %v", err)
	}

	// Decode into a map and ensure things are correct. Can't really compare bytes here since the
	// iteration order of a map is undefined.
	got := map[string]any{}
	dest := reflect.ValueOf(&got)
	if err := unmarshalObject(buf.Bytes(), decodedMD, 0, dest); err != nil {
		t.Fatalf("unmarshalObject(): %v", err)
	}
	diff(t, got, toWrite)
}

func TestObjectPrimitive(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()

	ob := newObjectBuilder(&buf, mdb, nil)
	if err := ob.Write("b", 3); err != nil {
		t.Fatalf("Write(b): %v", err)
	}
	if err := ob.Write("a", 1); err != nil {
		t.Fatalf("Write(a): %v", err)
	}
	if err := ob.Write("c", 2); err != nil {
		t.Fatalf("Write(c): %v", err)
	}

	if err := ob.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0b00000010, // Basic type = object, field_offset_size & field_id_size = 1, is_large = false
		0x03,       // 3 elements
		0x01,       // Field ID of "a"
		0x00,       // Field ID of "b"
		0x02,       // Field ID of "c"
		0x02,       // Index of "a"s value (1)
		0x00,       // Index of "b"s value (3)
		0x04,       // Index of "c"s value (2)
		0x06,       // First index after elements
		0b1100,     // "b"s header- basic Int8
		0x03,       // "b"'s value of 3
		0b1100,     // "a"s header- basic Int8
		0x01,       // "a"s value of 1
		0b1100,     // "c"s header- basic Int8
		0x02,       // "c"s value of 2
	}

	diff(t, buf.Bytes(), wantBytes)

	// Check the metadata keys as well.
	encodedMD := mdb.Build()
	decodedMetadata, err := decodeMetadata(encodedMD)
	if err != nil {
		t.Fatalf("Metadata decode error: %v", err)
	}
	gotKeys, wantKeys := decodedMetadata.keys, []string{"b", "a", "c"}
	diff(t, gotKeys, wantKeys)
}

func TestObjectNestedArray(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ob := newObjectBuilder(&buf, mdb, nil)
	if err := ob.Write("b", 3); err != nil {
		t.Fatalf("Write(b): %v", err)
	}

	arr, err := ob.Array("a")
	if err != nil {
		t.Fatalf("Array(a): %v", err)
	}
	for _, val := range []any{true, 123} {
		if err := arr.Write(val); err != nil {
			t.Fatalf("arr.Write(%v): %v", val, err)
		}
	}
	arr.Build()
	ob.Write("c", 8)

	if err := ob.Build(); err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantBytes := []byte{
		0b00000010, // Basic type = object, field_offset_size & field_id_size = 1, is_large = false
		0x03,       // 3 elements
		0x01,       // Field ID of "a"
		0x00,       // Field ID of "b"
		0x02,       // Field ID of "c"
		0x02,       // Index of "a"s value (array {true, 123})
		0x00,       // Index of "b"s value (3)
		0x0A,       // Index of "c"s value (8)
		0x0C,       // First index after the last value
		0b1100,     // "b"s header- basic Int8
		0x03,       // "b"s value of 3
		// Beginning of array
		0b011,  // "a"s header- basic array
		0x02,   // 2 elements in "a"s array
		0x00,   // Index of first element (true)
		0x01,   // Index of second element (123)
		0x03,   // First index after the last value
		0b100,  // First element (basic true- header and value)
		0b1100, // Second element- basic Int8
		0x7B,   // 123 encoded
		// End of array
		0b1100, // "c"s header- basic Int8
		0x08,   // "c"s value of 8
	}
	diffByteArrays(t, buf.Bytes(), wantBytes)

	// Check the metadata keys as well.
	encodedMD := mdb.Build()
	decodedMetadata, err := decodeMetadata(encodedMD)
	if err != nil {
		t.Fatalf("Metadata decode error: %v", err)
	}
	gotKeys, wantKeys := decodedMetadata.keys, []string{"b", "a", "c"}
	diff(t, gotKeys, wantKeys)
}

func TestObjectNestedObjectSharingKeys(t *testing.T) {
	var buf bytes.Buffer
	mdb := newMetadataBuilder()
	ob := newObjectBuilder(&buf, mdb, nil)

	if err := ob.Write("a", true); err != nil {
		t.Fatalf("Write(a): %v", err)
	}

	nestedOb, err := ob.Object("b")
	if err != nil {
		t.Fatalf("Object(b): %v", err)
	}

	// Same key can exist in a nested object
	if err := nestedOb.Write("b", 123); err != nil {
		t.Fatalf("Nested Object Write(b): %v", err)
	}
	nestedOb.Build()

	if err := ob.Build(); err != nil {
		t.Fatalf("Object Build(): %v", err)
	}

	wantBytes := []byte{
		0b10,  // Basic type = object, field_offset_size & field_id_size = 1, is_large = false
		0x02,  // 2 elements
		0x00,  // Field ID of "a",
		0x01,  // Field ID of "b",
		0x00,  // Index of first element (true)
		0x01,  // Index of second element (nested object)
		0x08,  // First index after the last value
		0b100, // "a"s header & value (basic true)
		// Beginning of nested object
		0b10,   // "b"s header- basic object, field_offset_size & field_id_size = 1, is_large = false
		0x01,   // 1 element
		0x01,   // Field ID of "b"
		0x00,   // Index of "b"s value (123)
		0x02,   // First index after the last value
		0b1100, // "b"s header- basic Int8
		0x7B,   // 123 encoded
		// End of nested object
	}
	diffByteArrays(t, buf.Bytes(), wantBytes)

	// Check the metadata keys as well.
	encodedMD := mdb.Build()
	decodedMetadata, err := decodeMetadata(encodedMD)
	if err != nil {
		t.Fatalf("Metadata decode error: %v", err)
	}
	gotKeys, wantKeys := decodedMetadata.keys, []string{"a", "b"}
	diff(t, gotKeys, wantKeys)
}

func TestObjectData(t *testing.T) {
	cases := []struct {
		name    string
		encoded []byte
		offset  int
		want    *objectData
		wantErr bool
	}{
		{
			name: "Basic object no offset",
			encoded: []byte{
				0b00000010, // Basic type = object, field_offset_size & field_id_size = 1, is_large = false
				0x03,       // 3 elements
				0x01,       // Field ID of "a"
				0x00,       // Field ID of "b"
				0x02,       // Field ID of "c"
				0x02,       // Index of "a"s value (1)
				0x00,       // Index of "b"s value (3)
				0x04,       // Index of "c"s value (2)
				0x06,       // First index after elements
				0b1100,     // "b"s header- basic Int8
				0x03,       // "b"'s value of 3
				0b1100,     // "a"s header- basic Int8
				0x01,       // "a"s value of 1
				0b1100,     // "c"s header- basic Int8
				0x02,       // "c"s value of 2
			},
			want: &objectData{
				size:            15,
				numElements:     3,
				firstFieldIDIdx: 2,
				firstOffsetIdx:  5,
				firstDataIdx:    9,
				fieldIDWidth:    1,
				offsetWidth:     1,
			},
		},
		{
			name: "Basic object with offset",
			encoded: []byte{
				0x00, 0b00000010, 0x03, 0x01, 0x00, 0x02, 0x02, 0x00,
				0x04, 0x06, 0b1100, 0x03, 0b1100, 0x01, 0b1100, 0x02,
			},
			offset: 1,
			want: &objectData{
				size:            15,
				numElements:     3,
				firstFieldIDIdx: 3,
				firstOffsetIdx:  6,
				firstDataIdx:    10,
				fieldIDWidth:    1,
				offsetWidth:     1,
			},
		},
		{
			name: "Object with larger widths",
			encoded: []byte{
				0b01100110,             // Basic type = object, field offset size = 2, field ID size = 3, is_large = true,
				0x03, 0x00, 0x00, 0x00, // 3 elements (encoded in 4 bytes due to is_large)
				0x01, 0x00, 0x00, // Field ID of "a"
				0x00, 0x00, 0x00, // Field ID of "b"
				0x02, 0x00, 0x00, // Field ID of "c"
				0x02, 0x00, // Index of "a"s value (1)
				0x00, 0x00, // Index of "b"s value (3)
				0x04, 0x00, // Index of "c"s value (2)
				0x06, 0x00, // First index after elements
				0b1100, 0x03, // Basic Int8 value of 3
				0b1100, 0x01, // Basic Int8 value of 1
				0b1100, 0x02, // Basic Int8 value of 2
			},
			want: &objectData{
				size:            28,
				numElements:     3,
				firstFieldIDIdx: 5,
				firstOffsetIdx:  14,
				firstDataIdx:    22,
				fieldIDWidth:    3,
				offsetWidth:     2,
			},
		},
		{
			name:    "Incorrect basic type",
			encoded: []byte{0x00, 0x00, 0x00, 0x00, 0x00},
			wantErr: true,
		},
		{
			name:    "Num elements out of bounds",
			encoded: []byte{0x02},
			wantErr: true,
		},
		{
			name:    "Object out of bounds",
			encoded: []byte{0x02, 0x03, 0x01, 0x00, 0x02, 0x02, 0x00, 0x04, 0x06},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := getObjectData(c.encoded, c.offset)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatal("Got no error when one was expected")
			}
			if diff := cmp.Diff(got, c.want, cmp.AllowUnexported(objectData{})); diff != "" {
				t.Fatalf("Incorrect returned data. Diff (-got, +want)\n%s", diff)
			}
		})
	}
}

func TestUnmarshalObject(t *testing.T) {
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
			name: "Object built of primitives into map",
			md:   &decodedMetadata{keys: []string{"b", "a", "c"}},
			encoded: []byte{
				0b01111110,             // Basic type = object, field offset & field ID size = 4, is_large = true,
				0x03, 0x00, 0x00, 0x00, // 3 elements (encoded in 4 bytes due to is_large)
				0x01, 0x00, 0x00, 0x00, // Field ID of "a"
				0x00, 0x00, 0x00, 0x00, // Field ID of "b"
				0x02, 0x00, 0x00, 0x00, // Field ID of "c"
				0x02, 0x00, 0x00, 0x00, // Index of "a"s value (1)
				0x00, 0x00, 0x00, 0x00, // Index of "b"s value (3)
				0x04, 0x00, 0x00, 0x00, // Index of "c"s value (2)
				0x06, 0x00, 0x00, 0x00, // First index after elements
				0b1100, 0x03, // Basic Int8 value of 3
				0b1100, 0x01, // Basic Int8 value of 1
				0b1100, 0x02, // Basic Int8 value of 2
			},
			want: map[string]any{
				"a": int64(1),
				"b": int64(3),
				"c": int64(2),
			},
		},
		{
			name:   "Object built of primitives into map with offset",
			md:     &decodedMetadata{keys: []string{"b", "a", "c"}},
			offset: 3,
			encoded: []byte{
				0x00, 0x00, 0x00, // 3 offset bytes
				0b01111110,             // Basic type = object, field offset & field ID size = 4, is_large = true,
				0x03, 0x00, 0x00, 0x00, // 3 elements (encoded in 4 bytes due to is_large)
				0x01, 0x00, 0x00, 0x00, // Field ID of "a"
				0x00, 0x00, 0x00, 0x00, // Field ID of "b"
				0x02, 0x00, 0x00, 0x00, // Field ID of "c"
				0x02, 0x00, 0x00, 0x00, // Index of "a"s value (1)
				0x00, 0x00, 0x00, 0x00, // Index of "b"s value (3)
				0x04, 0x00, 0x00, 0x00, // Index of "c"s value (2)
				0x06, 0x00, 0x00, 0x00, // First index after elements
				0b1100, 0x03, // Basic Int8 value of 3
				0b1100, 0x01, // Basic Int8 value of 1
				0b1100, 0x02, // Basic Int8 value of 2
			},
			want: map[string]any{
				"a": int64(1),
				"b": int64(3),
				"c": int64(2),
			},
		},
		{
			name: "Object built of primitives into struct",
			md:   &decodedMetadata{keys: []string{"b", "a", "c"}},
			encoded: []byte{
				0b01111110,             // Basic type = object, field offset & field ID size = 4, is_large = true,
				0x03, 0x00, 0x00, 0x00, // 3 elements (encoded in 4 bytes due to is_large)
				0x01, 0x00, 0x00, 0x00, // Field ID of "a"
				0x00, 0x00, 0x00, 0x00, // Field ID of "b"
				0x02, 0x00, 0x00, 0x00, // Field ID of "c"
				0x02, 0x00, 0x00, 0x00, // Index of "a"s value (1)
				0x00, 0x00, 0x00, 0x00, // Index of "b"s value (3)
				0x04, 0x00, 0x00, 0x00, // Index of "c"s value (2)
				0x06, 0x00, 0x00, 0x00, // First index after elements
				0b1100, 0x03, // Basic Int8 value of 3
				0b1100, 0x01, // Basic Int8 value of 1
				0b1100, 0x02, // Basic Int8 value of 2
			},
			want: struct {
				A int `variant:"a"`
				B int `variant:"b"`
				C int `variant:"c"`
			}{1, 3, 2},
		},
		{
			name: "Complex object into map",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "array", "otherkey"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()

				builder := newObjectBuilder(&buf, mdb, nil)

				builder.Write("key1", 123)
				builder.Write("key2", "hello")
				ab, err := builder.Array("array")
				if err != nil {
					t.Fatalf("Array('array'): %v", err)
				}
				ab.Write(false)
				ab.Write("substr")
				ab.Build()

				builder.Write("otherkey", []byte{'b', 'y', 't', 'e'})
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()
			}(),
			want: map[string]any{
				"key1":     int64(123),
				"key2":     "hello",
				"array":    []any{false, "substr"},
				"otherkey": []byte{'b', 'y', 't', 'e'},
			},
		},
		{
			name: "Complex object into struct",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "array", "otherkey"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()

				builder := newObjectBuilder(&buf, mdb, nil)

				builder.Write("key1", 123)
				builder.Write("key2", "hello")
				ab, err := builder.Array("array")
				if err != nil {
					t.Fatalf("Array('array'): %v", err)
				}
				ab.Write(false)
				ab.Write("substr")
				ab.Build()

				builder.Write("otherkey", []byte{'b', 'y', 't', 'e'})
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()
			}(),
			want: struct {
				K1    int    `variant:"key1"`
				K2    string `variant:"key2"`
				Arr   []any  `variant:"array"`
				Other []byte `variant:"otherkey"`
			}{123, "hello", []any{false, "substr"}, []byte{'b', 'y', 't', 'e'}},
		},
		{
			name: "Unmarshal skips non-present fields in struct",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "key3"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				builder := newObjectBuilder(&buf, mdb, nil)
				builder.Write("key1", 123)
				builder.Write("key2", "hello")
				builder.Write("key3", false)
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()
			}(),
			want: struct {
				K1 int `variant:"key1"`
				// key2 is undefined
				K3 bool `variant:"key3"`
			}{123, false},
		},
		{
			name: "Unmarshal into typed map",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "key3"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				builder := newObjectBuilder(&buf, mdb, nil)
				builder.Write("key1", 123)
				builder.Write("key2", 234)
				builder.Write("key3", 345)
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()
			}(),
			want: map[string]int{
				"key1": 123,
				"key2": 234,
				"key3": 345,
			},
		},
		{
			name: "Malformed raw data",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "key3"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				builder := newObjectBuilder(&buf, mdb, nil)
				builder.Write("key1", 123)
				builder.Write("key2", 234)
				builder.Write("key3", 345)
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()[1:] // Lop off the first byte
			}(),
			overrideDecodeType: reflect.TypeOf(map[string]any{}),
			wantErr:            true,
		},
		{
			name: "Maps must be string keyed",
			md:   &decodedMetadata{keys: []string{"key1", "key2", "key3"}},
			encoded: func() []byte {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				builder := newObjectBuilder(&buf, mdb, nil)
				builder.Write("key1", 123)
				builder.Write("key2", 234)
				builder.Write("key3", 345)
				if err := builder.Build(); err != nil {
					t.Fatalf("Build(): %v", err)
				}
				return buf.Bytes()
			}(),
			overrideDecodeType: reflect.TypeOf(map[int]any{}),
			wantErr:            true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got reflect.Value
			typ := reflect.TypeOf(c.want)
			if c.overrideDecodeType != nil {
				typ = c.overrideDecodeType
			}
			if typ.Kind() == reflect.Map {
				underlying := reflect.MakeMap(typ)
				got = reflect.New(typ)
				got.Elem().Set(underlying)
			} else {
				got = reflect.New(typ)
			}
			if err := unmarshalObject(c.encoded, c.md, c.offset, got); err != nil {
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

func TestExtractFieldInfo(t *testing.T) {
	type testStruct struct {
		NoTags     int
		JustName   int `variant:"just_name"`
		EmptyTag   int `variant:""`
		WithOpts   int `variant:"with_opts,ntz,date,nanos,time"`
		OptsNoName int `variant:",uuid"`
		UnknownOpt int `variant:"unknown,not_defined_opt"`
	}
	cases := []struct {
		name     string
		field    int
		wantName string
		wantOpts []MarshalOpts
	}{
		{
			name:     "No tags",
			field:    0,
			wantName: "NoTags",
		},
		{
			name:     "Field tag with just name",
			field:    1,
			wantName: "just_name",
		},
		{
			name:     "Empty tag uses struct field name",
			field:    2,
			wantName: "EmptyTag",
		},
		{
			name:     "Field tag with name and options",
			field:    3,
			wantName: "with_opts",
			wantOpts: []MarshalOpts{MarshalTimeNTZ, MarshalAsDate, MarshalTimeNanos, MarshalAsTime},
		},
		{
			name:     "Just options, no name uses struct field name",
			field:    4,
			wantName: "OptsNoName",
			wantOpts: []MarshalOpts{MarshalAsUUID},
		},
		{
			name:     "Ignore unknown options",
			field:    5,
			wantName: "unknown",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			val := reflect.ValueOf(testStruct{})
			gotName, gotOpts := extractFieldInfo(val.Type().Field(c.field))
			if gotName != c.wantName {
				t.Errorf("Incorrect name. Got %q, want %q", gotName, c.wantName)
			}
			diff(t, gotOpts, c.wantOpts)
		})
	}
}
