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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/avro/testdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hamba "github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	tests := []struct {
		arrowSchema []arrow.Field
	}{
		{
			arrowSchema: []arrow.Field{
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
		tp := testdata.Generate()
		defer os.RemoveAll(filepath.Dir(tp.Avro))

		t.Run("ShouldParseSchemaWithEdits", func(t *testing.T) {
			want := arrow.NewSchema(test.arrowSchema, nil)

			schema, err := testdata.AllTypesAvroSchema()
			if err != nil {
				t.Fatal(err)
			}
			r := new(OCFReader)
			r.avroSchema = schema.String()
			r.editAvroSchema(schemaEdit{method: "delete", path: "fields.0"})
			schema, err = hamba.Parse(r.avroSchema)
			if err != nil {
				t.Fatalf("%v: could not parse modified avro schema", arrow.ErrInvalid)
			}
			got, err := ArrowSchemaFromAvro(schema)
			if err != nil {
				t.Fatalf("%v", err)
			}
			assert.Equal(t, want.String(), got.String())
			if fmt.Sprintf("%+v", want.String()) != fmt.Sprintf("%+v", got.String()) {
				t.Fatalf("got=%v,\n want=%v", got.String(), want.String())
			}
		})

		t.Run("ShouldLoadExpectedRecords", func(t *testing.T) {
			b, err := os.ReadFile(tp.Avro)
			if err != nil {
				t.Error(err)
			}
			r := bytes.NewReader(b)

			opts := []Option{WithChunk(-1)}
			ar, err := NewOCFReader(r, opts...)
			if err != nil {
				t.Error(err)
			}
			defer ar.Close()

			exists := ar.Next()

			if ar.Err() != nil {
				t.Error("failed to read next record: %w", ar.Err())
			}
			if !exists {
				t.Error("no record exists")
			}
			a, err := ar.RecordBatch().MarshalJSON()
			assert.NoError(t, err)
			var avroParsed []map[string]any
			json.Unmarshal(a, &avroParsed)

			j, err := os.ReadFile(tp.Json)
			assert.NoError(t, err)
			var jsonParsed map[string]any
			json.Unmarshal(j, &jsonParsed)

			assert.Equal(t, jsonParsed, avroParsed[0])
		})
	}
}

// TestOCFReaderBytesValues exercises avro `bytes` fields, both plain and as a
// ["null","bytes"] union: hamba hands the decoded value to the appenders as a
// bare []byte, which previously fell into appendBinaryData's fmt fallback and
// appended the formatted text (e.g. "[1 2 3]") instead of the payload.
func TestOCFReaderBytesValues(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "rec",
		"fields": [
			{"name": "plain", "type": "bytes"},
			{"name": "nullable", "type": ["null", "bytes"]}
		]
	}`
	payload := []byte{0x00, 0x01, 0xfe, 0xff}

	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf)
	assert.NoError(t, err)
	assert.NoError(t, enc.Encode(map[string]any{
		"plain":    payload,
		"nullable": map[string]any{"bytes": payload},
	}))
	assert.NoError(t, enc.Encode(map[string]any{
		"plain":    []byte{},
		"nullable": nil,
	}))
	assert.NoError(t, enc.Close())

	ar, err := NewOCFReader(bytes.NewReader(buf.Bytes()), WithChunk(-1))
	assert.NoError(t, err)
	defer ar.Close()

	assert.True(t, ar.Next())
	assert.NoError(t, ar.Err())
	rec := ar.RecordBatch()

	plain := rec.Column(0).(*array.Binary)
	assert.Equal(t, payload, plain.Value(0))
	assert.Equal(t, []byte{}, plain.Value(1))

	nullable := rec.Column(1).(*array.Binary)
	assert.Equal(t, payload, nullable.Value(0))
	assert.True(t, nullable.IsNull(1))
}

// Types outside what the hamba decoder produces must error rather than append
// a fmt-formatted rendering of the value.
func TestAppendBinaryAndStringDataUnexpectedTypes(t *testing.T) {
	bb := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer bb.Release()

	assert.NoError(t, appendBinaryData(bb, []byte{0x01}))
	assert.NoError(t, appendBinaryData(bb, nil))
	assert.NoError(t, appendBinaryData(bb, map[string]any{"bytes": []byte{0x02}}))
	assert.ErrorContains(t, appendBinaryData(bb, 42), "unexpected type int")
	assert.ErrorContains(t, appendBinaryData(bb, map[string]any{"bytes": "text"}), "unexpected type string")
	assert.Equal(t, 3, bb.Len())

	sb := array.NewStringBuilder(memory.DefaultAllocator)
	defer sb.Release()

	assert.NoError(t, appendStringData(sb, "ok"))
	assert.NoError(t, appendStringData(sb, []byte("ok")))
	assert.NoError(t, appendStringData(sb, nil))
	assert.ErrorContains(t, appendStringData(sb, 42), "unexpected type int")
	assert.Equal(t, 3, sb.Len())
}
