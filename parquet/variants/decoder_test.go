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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func mustEncodeVariant(t *testing.T, val any) *MarshaledVariant {
	t.Helper()
	ev, err := Marshal(val)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	return ev
}

func TestUnmarshal(t *testing.T) {
	emptyMetadata := func() []byte {
		return []byte{0x01, 0x00, 0x00} // Basic, but valid, metadata with no keys.
	}
	cases := []struct {
		name    string
		encoded *MarshaledVariant
		want    any
		wantErr bool
	}{
		{
			name:    "Primitive",
			encoded: mustEncodeVariant(t, "hello"),
			want:    "hello",
		},
		{
			name:    "Array",
			encoded: mustEncodeVariant(t, []any{"hello", 256, true}),
			want:    []any{"hello", int64(256), true},
		},
		{
			name: "Object into map",
			encoded: mustEncodeVariant(t, struct {
				Key1 int    `variant:"key1"`
				Key2 []byte `variant:"key2"`
				Key3 string `variant:"key3"`
			}{1234, []byte{'b', 'y', 't', 'e'}, "hello"}),
			want: map[string]any{
				"key1": int64(1234),
				"key2": []byte{'b', 'y', 't', 'e'},
				"key3": "hello",
			},
		},
		{
			name: "Complex",
			encoded: mustEncodeVariant(t, []any{
				1234, struct {
					Key1 string `variant:"key1"`
					Arr  []any  `variant:"array"`
				}{"hello", []any{false, true, "hello"}},
				"fin"}),
			want: []any{
				int64(1234),
				map[string]any{
					"key1":  "hello",
					"array": []any{false, true, "hello"},
				},
				"fin",
			},
		},
		{
			name:    "Nil primitive",
			encoded: mustEncodeVariant(t, nil),
			want:    nil,
		},
		{
			name: "Missing metadata",
			encoded: &MarshaledVariant{
				Value: []byte{0x00}, // Primitive nil
			},
			wantErr: true,
		},
		{
			name: "Missing Value",
			encoded: &MarshaledVariant{
				Metadata: emptyMetadata(),
			},
			wantErr: true,
		},
		{
			name: "Malformed array",
			encoded: &MarshaledVariant{
				Metadata: emptyMetadata(),
				Value:    []byte{0x03, 0x02}, // Array, length 2, no other items.
			},
			wantErr: true,
		},
		{
			name: "Object missing key",
			encoded: func() *MarshaledVariant {
				builder := NewBuilder()
				ob, err := builder.Object()
				if err != nil {
					t.Fatalf("Object(): %v", err)
				}
				ob.Write("key", "value")
				encoded, err := builder.Build()
				if err != nil {
					t.Fatalf("Build(): %v", err)
				}
				encoded.Metadata = emptyMetadata()
				return encoded
			}(),
			wantErr: true,
		},
		{
			name: "Malformed object",
			encoded: func() *MarshaledVariant {
				builder := NewBuilder()
				ob, err := builder.Object()
				if err != nil {
					t.Fatalf("Object(): %v", err)
				}
				ob.Write("key", "value")
				encoded, err := builder.Build()
				if err != nil {
					t.Fatalf("Build(): %v", err)
				}
				encoded.Value = encoded.Value[:len(encoded.Value)-2] // Truncate
				return encoded
			}(),
			wantErr: true,
		},
		{
			name: "Malformed primitive",
			encoded: &MarshaledVariant{
				Metadata: emptyMetadata(),
				Value:    []byte{0xFD, 'a'}, // Short string, length 63
			},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got any
			err := DecodeInto(c.encoded, &got)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
			if diff := cmp.Diff(got, c.want); diff != "" {
				t.Fatalf("Incorrect data returned. Diff (-got, +want):\n%s", diff)
			}
		})
	}
}

func TestUnmarshalWithTypes(t *testing.T) {
	cases := []struct {
		name       string
		ev         *MarshaledVariant
		decodeType reflect.Type
		want       reflect.Value
		wantErr    bool
	}{
		{
			name:       "Primitive int",
			ev:         mustEncodeVariant(t, 1),
			decodeType: reflect.TypeOf(int(0)),
			want:       reflect.ValueOf(int(1)),
		},
		{
			name:       "Primitive string",
			ev:         mustEncodeVariant(t, "hello"),
			decodeType: reflect.TypeOf(""),
			want:       reflect.ValueOf("hello"),
		},
		{
			name:       "Nested array",
			ev:         mustEncodeVariant(t, []any{[]any{1}}),
			decodeType: reflect.TypeOf([]any{}),
			want:       reflect.ValueOf([]any{[]any{int64(1)}}),
		},
		{
			name: "Complex object into map",
			ev: mustEncodeVariant(t, map[string]any{
				"key1": 123,
				"key2": []any{true, false, "hello", []any{1, 2, 3}},
				"key3": map[string]any{
					"key1": "foo",
					"key4": "bar",
				},
			}),
			decodeType: reflect.TypeOf(map[string]any{}),
			want: reflect.ValueOf(map[string]any{
				"key1": int64(123),
				"key2": []any{true, false, "hello", []any{int64(1), int64(2), int64(3)}},
				"key3": map[string]any{
					"key1": "foo",
					"key4": "bar",
				},
			}),
		},
		{
			name:       "Object to map",
			ev:         mustEncodeVariant(t, map[string]int{"a": 1, "b": 2}),
			decodeType: reflect.TypeOf(map[string]any{}),
			want:       reflect.ValueOf(map[string]any{"a": int64(1), "b": int64(2)}),
		},
		// TODO: add tests to decode into struct
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decodedMD, err := decodeMetadata(c.ev.Metadata)
			if err != nil {
				t.Fatalf("decodeMetadata(): %v", err)
			}

			var decoded reflect.Value
			if c.decodeType.Kind() == reflect.Map {
				// Create a pointer to the map.
				underlying := reflect.MakeMap(c.decodeType)
				decoded = reflect.New(c.decodeType)
				decoded.Elem().Set(underlying)
			} else {
				decoded = reflect.New(c.decodeType)
			}
			if err := unmarshalCommon(c.ev.Value, decodedMD, 0, decoded); err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
			diff(t, decoded.Elem().Interface(), c.want.Interface())
		})
	}
}
