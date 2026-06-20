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

package avro

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/avro/testdata"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	hambaAvro "github.com/hamba/avro/v2"
	avro "github.com/twmb/avro"
)

func TestSchemaStringEqual(t *testing.T) {
	tests := []struct {
		arrowSchema []arrow.Field
	}{
		{
			arrowSchema: []arrow.Field{
				{
					Name:     "inheritNull",
					Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
					Metadata: arrow.MetadataFrom(map[string]string{"0": "a", "1": "b"}),
				},
				{
					Name: "explicitNamespace",
					Type: &arrow.FixedSizeBinaryType{ByteWidth: 12},
				},
				{
					Name: "fullName",
					Type: arrow.StructOf(
						arrow.Field{
							Name: "inheritNamespace",
							Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String, Ordered: false},
						},
						arrow.Field{
							Name: "md5",
							Type: &arrow.FixedSizeBinaryType{ByteWidth: 16},
						},
					),
				},
				{
					Name: "id",
					Type: arrow.PrimitiveTypes.Int32,
				},
				{
					Name: "bigId",
					Type: arrow.PrimitiveTypes.Int64,
				},
				{
					Name:     "temperature",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
				},
				{
					Name:     "fraction",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
				},
				{
					Name: "is_emergency",
					Type: arrow.FixedWidthTypes.Boolean,
				},
				{
					Name:     "remote_ip",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
				},
				{
					Name:     "nullable_remote_ips",
					Type:     arrow.ListOfNonNullable(arrow.BinaryTypes.Binary),
					Nullable: true,
				},
				{
					Name: "person",
					Type: arrow.StructOf(
						arrow.Field{
							Name: "lastname",
							Type: arrow.BinaryTypes.String,
						},
						arrow.Field{
							Name: "address",
							Type: arrow.StructOf(
								arrow.Field{
									Name: "streetaddress",
									Type: arrow.BinaryTypes.String,
								},
								arrow.Field{
									Name: "city",
									Type: arrow.BinaryTypes.String,
								},
							),
						},
						arrow.Field{
							Name: "mapfield",
							Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
						},
						arrow.Field{
							Name: "arrayField",
							Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String),
						},
					),
				},
				{
					Name: "decimalField",
					Type: &arrow.Decimal128Type{Precision: 4, Scale: 2},
				},
				{
					Name: "decimal256Field",
					Type: &arrow.Decimal256Type{Precision: 60, Scale: 2},
				},
				{
					Name: "uuidField",
					Type: arrow.BinaryTypes.String,
				},
				{
					Name: "fixedUuidField",
					Type: extensions.NewUUIDType(),
				},
				{
					Name: "timemillis",
					Type: arrow.FixedWidthTypes.Time32ms,
				},
				{
					Name: "timemicros",
					Type: arrow.FixedWidthTypes.Time64us,
				},
				{
					Name: "timestampmillis",
					Type: arrow.FixedWidthTypes.Timestamp_ms,
				},
				{
					Name: "timestampmicros",
					Type: arrow.FixedWidthTypes.Timestamp_us,
				},
				{
					Name: "localtimestampmillis",
					Type: &arrow.TimestampType{Unit: arrow.Millisecond},
				},
				{
					Name: "localtimestampmicros",
					Type: &arrow.TimestampType{Unit: arrow.Microsecond},
				},
				{
					Name: "duration",
					Type: arrow.FixedWidthTypes.MonthDayNanoInterval,
				},
				{
					Name: "date",
					Type: arrow.FixedWidthTypes.Date32,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := arrow.NewSchema(test.arrowSchema, nil)

			schema, err := testdata.AllTypesAvroSchema()
			if err != nil {
				t.Fatalf("%v", err)
			}
			got, err := ArrowSchemaFromAvroJSON(schema)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if fmt.Sprintf("%+v", want.String()) != fmt.Sprintf("%+v", got.String()) {
				t.Fatalf("got=%v,\n want=%v", got.String(), want.String())
			} else {
				t.Logf("schema.String() comparison passed")
			}
		})
	}
}

// Remove together with [ArrowSchemaFromAvro] at the next major release.
func TestArrowSchemaFromAvro_Deprecated_PreservesLogicalTypesOnFixed(t *testing.T) {
	const schemaJSON = `{
		"type": "record",
		"name": "Sample",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "nullable_double", "type": ["null", "double"]},
			{"name": "uuid_string", "type": {"type": "string", "logicalType": "uuid"}},
			{"name": "ts_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "fixed_uuid", "type": {"type": "fixed", "name": "FUUID", "size": 16, "logicalType": "uuid"}},
			{"name": "fixed_decimal", "type": {"type": "fixed", "name": "FDec", "size": 16, "logicalType": "decimal", "precision": 20, "scale": 4}},
			{"name": "fixed_duration", "type": {"type": "fixed", "name": "FDur", "size": 12, "logicalType": "duration"}}
		]
	}`
	hambaSchema, err := hambaAvro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("hamba parse: %v", err)
	}

	got, err := ArrowSchemaFromAvro(hambaSchema)
	if err != nil {
		t.Fatalf("ArrowSchemaFromAvro: %v", err)
	}
	want, err := ArrowSchemaFromAvroJSON(schemaJSON)
	if err != nil {
		t.Fatalf("ArrowSchemaFromAvroJSON: %v", err)
	}
	if got.String() != want.String() {
		t.Fatalf("schema mismatch:\n got = %s\nwant = %s", got.String(), want.String())
	}
}

// OCF requires a record at the top level. Non-record roots are rejected by our
// own guard, while a record with an empty name is rejected earlier by the avro
// parser; both must surface an error rather than produce a degenerate schema.
func TestArrowSchemaFromAvroJSON_RejectsInvalidRoot(t *testing.T) {
	tests := []struct {
		name       string
		schemaJSON string
		wantErr    string // caught by our guard
		wantParse  bool   // caught by the avro parser before our guard
	}{
		{name: "string root", schemaJSON: `"string"`, wantErr: "must be a record"},
		{name: "array root", schemaJSON: `{"type":"array","items":"int"}`, wantErr: "must be a record"},
		{name: "empty record name", schemaJSON: `{"type":"record","name":"","fields":[{"name":"x","type":"int"}]}`, wantParse: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ArrowSchemaFromAvroJSON(tt.schemaJSON)
			if err == nil {
				t.Fatalf("expected error for %s", tt.schemaJSON)
			}
			if tt.wantParse {
				// The parser rejects it before our guard runs, so the error is
				// not wrapped as an invalid avro schema by arrowSchemaFromAvroInternal.
				if strings.Contains(err.Error(), "must be a record") {
					t.Fatalf("expected parser error, got our guard's error: %v", err)
				}
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error for %s: %v", tt.schemaJSON, err)
			}
		})
	}
}

// A named record referenced again by name resolves back to the same definition.
func TestArrowSchemaFromAvroJSON_ReusedNamedReference(t *testing.T) {
	const schemaJSON = `{"type":"record","name":"Root","fields":[
		{"name":"a","type":{"type":"record","name":"Foo","fields":[{"name":"x","type":"int"}]}},
		{"name":"b","type":"Foo"}]}`
	s, err := ArrowSchemaFromAvroJSON(schemaJSON)
	if err != nil {
		t.Fatalf("ArrowSchemaFromAvroJSON: %v", err)
	}
	if s.NumFields() != 2 {
		t.Fatalf("got %d fields, want 2", s.NumFields())
	}
	if a, b := s.Field(0).Type.String(), s.Field(1).Type.String(); a != b {
		t.Fatalf("reused reference resolved to a different type:\n a = %s\n b = %s", a, b)
	}
}

// Two records sharing a short name across namespaces are kept distinct by full
// name, and an unqualified reference to that short name is rejected instead of
// silently resolving to one of them (restoring hamba's erroring behavior).
func TestResolveRefAmbiguousBareName(t *testing.T) {
	n := newSchemaNode()
	n.rememberNamed(avro.SchemaNode{Name: "Foo", Namespace: "a"})
	n.rememberNamed(avro.SchemaNode{Name: "Foo", Namespace: "b"})

	got := n.resolveRef(avro.SchemaNode{Type: "a.Foo"}, "")
	if got.Namespace != "a" {
		t.Fatalf("a.Foo resolved to namespace %q, want a", got.Namespace)
	}

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic for ambiguous bare reference")
			}
			if !strings.Contains(fmt.Sprint(r), "ambiguous named type") {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()
		n.resolveRef(avro.SchemaNode{Type: "Foo"}, "field")
	}()
}
