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
	"reflect"
	"testing"
)

func TestParseTag(t *testing.T) {
	tests := []struct {
		input string
		want  tagOpts
	}{
		{
			input: "custom_name",
			want:  tagOpts{Name: "custom_name"},
		},
		{
			input: "-",
			want:  tagOpts{Skip: true},
		},
		{
			input: "-,",
			want:  tagOpts{Name: "-"},
		},
		{
			input: "",
			want:  tagOpts{},
		},
		{
			input: "name,dict",
			want:  tagOpts{Name: "name", Dict: true},
		},
		{
			input: "name,listview",
			want:  tagOpts{Name: "name", ListView: true},
		},
		{
			input: "name,ree",
			want:  tagOpts{Name: "name", REE: true},
		},
		{
			input: "name,decimal(38,10)",
			want:  tagOpts{Name: "name", HasDecimalOpts: true, DecimalPrecision: 38, DecimalScale: 10},
		},
		{
			input: ",decimal(18,2)",
			want:  tagOpts{Name: "", HasDecimalOpts: true, DecimalPrecision: 18, DecimalScale: 2},
		},
		{
			input: "name,dict,ree",
			want:  tagOpts{Name: "name", Dict: true, REE: true},
		},
		{
			input: "name,unknown_option",
			want:  tagOpts{Name: "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseTag(tt.input)
			if got != tt.want {
				t.Errorf("parseTag(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetStructFields(t *testing.T) {
	t.Run("simple struct", func(t *testing.T) {
		type Simple struct {
			Name string
			Age  int32
		}
		fields := getStructFields(reflect.TypeOf(Simple{}))
		if len(fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(fields))
		}
		if fields[0].Name != "Name" {
			t.Errorf("fields[0].Name = %q, want %q", fields[0].Name, "Name")
		}
		if fields[1].Name != "Age" {
			t.Errorf("fields[1].Name = %q, want %q", fields[1].Name, "Age")
		}
	})

	t.Run("struct with arrow tags", func(t *testing.T) {
		type Tagged struct {
			UserName string  `arrow:"user_name"`
			Score    float64 `arrow:"score"`
			Internal string  `arrow:"-"`
		}
		fields := getStructFields(reflect.TypeOf(Tagged{}))
		if len(fields) != 2 {
			t.Fatalf("expected 2 fields, got %d: %v", len(fields), fields)
		}
		if fields[0].Name != "user_name" {
			t.Errorf("fields[0].Name = %q, want %q", fields[0].Name, "user_name")
		}
		if fields[1].Name != "score" {
			t.Errorf("fields[1].Name = %q, want %q", fields[1].Name, "score")
		}
	})

	t.Run("unexported fields skipped", func(t *testing.T) {
		type Mixed struct {
			Exported   string
			unexported string //nolint:unused
		}
		fields := getStructFields(reflect.TypeOf(Mixed{}))
		if len(fields) != 1 {
			t.Fatalf("expected 1 field, got %d", len(fields))
		}
		if fields[0].Name != "Exported" {
			t.Errorf("fields[0].Name = %q, want %q", fields[0].Name, "Exported")
		}
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type WithPointers struct {
			Required string
			Optional *string
		}
		fields := getStructFields(reflect.TypeOf(WithPointers{}))
		if len(fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(fields))
		}
		if fields[0].Nullable {
			t.Errorf("Required.Nullable = true, want false")
		}
		if !fields[1].Nullable {
			t.Errorf("Optional.Nullable = false, want true")
		}
	})

	t.Run("embedded struct promotion", func(t *testing.T) {
		type Inner struct {
			City string
			Zip  int32
		}
		type Outer struct {
			Name string
			Inner
		}
		fields := getStructFields(reflect.TypeOf(Outer{}))
		if len(fields) != 3 {
			t.Fatalf("expected 3 fields, got %d: %v", len(fields), fields)
		}
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.Name
		}
		wantNames := []string{"Name", "City", "Zip"}
		for i, want := range wantNames {
			if names[i] != want {
				t.Errorf("fields[%d].Name = %q, want %q", i, names[i], want)
			}
		}
	})

	t.Run("embedded struct conflict excluded", func(t *testing.T) {
		type A struct{ ID string }
		type B struct{ ID string }
		type Conflicted struct {
			A
			B
		}
		fields := getStructFields(reflect.TypeOf(Conflicted{}))
		if len(fields) != 0 {
			t.Errorf("expected 0 fields due to conflict, got %d: %v", len(fields), fields)
		}
	})

	t.Run("embedded with tag overrides promotion", func(t *testing.T) {
		type Inner struct {
			City string
			Zip  int32
		}
		type HasTag struct {
			Inner `arrow:"inner_struct"`
		}
		fields := getStructFields(reflect.TypeOf(HasTag{}))
		if len(fields) != 1 {
			t.Fatalf("expected 1 field, got %d: %v", len(fields), fields)
		}
		if fields[0].Name != "inner_struct" {
			t.Errorf("fields[0].Name = %q, want %q", fields[0].Name, "inner_struct")
		}
	})
}

func TestCachedStructFields(t *testing.T) {
	type S struct {
		X int32
		Y string
	}

	fields1 := cachedStructFields(reflect.TypeOf(S{}))
	fields2 := cachedStructFields(reflect.TypeOf(S{}))

	if len(fields1) != len(fields2) {
		t.Fatalf("cached call returned different lengths: %d vs %d", len(fields1), len(fields2))
	}

	for i := range fields1 {
		if fields1[i].Name != fields2[i].Name {
			t.Errorf("fields[%d].Name mismatch: %q vs %q", i, fields1[i].Name, fields2[i].Name)
		}
	}

	if len(fields1) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields1))
	}
	if fields1[0].Name != "X" {
		t.Errorf("fields1[0].Name = %q, want %q", fields1[0].Name, "X")
	}
	if fields1[1].Name != "Y" {
		t.Errorf("fields1[1].Name = %q, want %q", fields1[1].Name, "Y")
	}
}

// ── shared test types used across reflect test files ──────────────────────────

type testPrimitive struct {
	I8   int8
	I16  int16
	I32  int32
	I64  int64
	U8   uint8
	U16  uint16
	U32  uint32
	U64  uint64
	F32  float32
	F64  float64
	B    bool
	S    string
	Blob []byte
}

type testNested struct {
	Name    string
	Scores  []float64
	Tags    map[string]string
	Address struct {
		City string
		Zip  int32
	}
}

type testNullable struct {
	Required string
	Optional *string
	MaybeInt *int32
}

type testEmbedded struct {
	ID string
	testEmbeddedInner
}

type testEmbeddedInner struct { //nolint:unused
	City string
	Code int32
}

type testTagged struct {
	UserName string  `arrow:"user_name"`
	Score    float64 `arrow:"score"`
	Hidden   string  `arrow:"-"`
}

func TestHelpers(t *testing.T) {
	// Verify shared test types are usable
	_ = testPrimitive{I8: 1, I32: 2, S: "hi"}
	_ = testNested{Name: "n", Scores: []float64{1.0}}
	_ = testNullable{Required: "r"}
	_ = testTagged{UserName: "u", Score: 3.14}
	_ = testEmbedded{ID: "id"}
}
