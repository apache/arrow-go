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
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/avro/testdata"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

func TestOCFReaderPreservesFirstError(t *testing.T) {
	reader := &OCFReader{}
	first := errors.New("first error")

	reader.setErr(first)
	reader.setErr(errors.New("later error"))
	reader.setErr(nil)

	require.ErrorIs(t, reader.Err(), first)
}

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
					Type: extensions.NewUUIDType(),
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
					Name: "timestampnanos",
					Type: arrow.FixedWidthTypes.Timestamp_ns,
				},
				{
					Name: "localtimestampnanos",
					Type: &arrow.TimestampType{Unit: arrow.Nanosecond},
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
			r.avroSchema = schema
			r.editAvroSchema(schemaEdit{method: "delete", path: "fields.0"})
			got, err := ArrowSchemaFromAvroJSON(r.avroSchema)
			if err != nil {
				t.Fatalf("%v: could not parse modified avro schema", arrow.ErrInvalid)
			}
			assert.Equal(t, want.String(), got.String())
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
				t.Errorf("failed to read next record: %v", ar.Err())
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

// TestOCFReaderValues writes one row per entry in rows to a record whose only
// field, f, has the Avro type typ, and compares the column that NewOCFReader
// reads back with want as JSON. Field-level attributes of f, such as a
// field-level logicalType, follow the type in typ.
func TestOCFReaderValues(t *testing.T) {
	const rec = `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	const listAndMap = `{"type":"record","name":"P","fields":[{"name":"l","type":{"type":"array","items":"int"}},{"name":"m","type":{"type":"map","values":"int"}}]}`
	payload := []byte{0x00, 0x01, 0xfe, 0xff}
	x := func(v int32) map[string]any { return map[string]any{"x": v} }
	lm := map[string]any{"l": []any{int32(1)}, "m": map[string]any{"a": int32(2)}}
	ts := time.UnixMilli(1_700_000_000_123).UTC()
	tsMicros := time.UnixMicro(1_700_000_000_123_456).UTC()
	const bigDecimal = `{"type":"bytes","logicalType":"big-decimal"}`
	manyDigits, _ := new(big.Rat).SetString("12345678901234567890.0000000001")
	tests := []struct {
		name string
		typ  string
		rows []any
		want string
	}{
		{"null", `"null"`, []any{nil, nil}, `[null,null]`},
		{"bytes", `"bytes"`, []any{payload, []byte{}}, `["AAH+/w==",""]`},
		{"nullable bytes", `["null","bytes"]`, []any{map[string]any{"bytes": payload}, nil}, `["AAH+/w==",null]`},
		{"nullable record", `["null",` + rec + `]`, []any{x(7), nil, x(8)}, `[{"x":7},null,{"x":8}]`},
		{"array of nullable records with a null", `{"type":"array","items":["null",` + rec + `]}`, []any{[]any{x(7), nil, x(8)}}, `[[{"x":7},null,{"x":8}]]`},
		{"null record of a record", `["null",{"type":"record","name":"P","fields":[{"name":"r","type":` + rec + `}]}]`,
			[]any{nil, map[string]any{"r": x(9)}}, `[null,{"r":{"x":9}}]`},
		{"null record of an array and a map", `["null",` + listAndMap + `]`,
			[]any{nil, lm}, `[null,{"l":[1],"m":[{"key":"a","value":2}]}]`},
		{"array of nullable records of an array and a map with a null", `{"type":"array","items":["null",` + listAndMap + `]}`,
			[]any{[]any{nil, lm}}, `[[null,{"l":[1],"m":[{"key":"a","value":2}]}]]`},
		{"map of nullable records with a null", `{"type":"map","values":["null",` + rec + `]}`,
			[]any{map[string]any{"a": nil}, map[string]any{"b": x(8)}}, `[[{"key":"a","value":null}],[{"key":"b","value":{"x":8}}]]`},
		{"nullable record in a nullable record", `["null",{"type":"record","name":"P","fields":[{"name":"r","type":["null",` + rec + `]}]}]`,
			[]any{map[string]any{"r": x(5)}, map[string]any{"r": nil}}, `[{"r":{"x":5}},{"r":null}]`},
		{"record in an array of records", `{"type":"array","items":{"type":"record","name":"P","fields":[{"name":"r","type":` + rec + `}]}}`,
			[]any{[]any{map[string]any{"r": x(9)}}}, `[[{"r":{"x":9}}]]`},
		{"record in a map of records", `{"type":"map","values":{"type":"record","name":"P","fields":[{"name":"r","type":` + rec + `}]}}`,
			[]any{map[string]any{"a": map[string]any{"r": x(9)}}}, `[[{"key":"a","value":{"r":{"x":9}}}]]`},
		{"record field named value", `{"type":"record","name":"P","fields":[{"name":"value","type":` + rec + `}]}`,
			[]any{map[string]any{"value": x(9)}}, `[{"value":{"x":9}}]`},
		{"record field named item in an array", `{"type":"array","items":{"type":"record","name":"P","fields":[{"name":"item","type":` + rec + `}]}}`,
			[]any{[]any{map[string]any{"item": x(9)}}}, `[[{"item":{"x":9}}]]`},
		{"array of maps", `{"type":"array","items":{"type":"map","values":"long"}}`,
			[]any{[]any{map[string]any{"a": int64(1)}, map[string]any{"b": int64(2)}}}, `[[[{"key":"a","value":1}],[{"key":"b","value":2}]]]`},
		{"map of maps", `{"type":"map","values":{"type":"map","values":"long"}}`,
			[]any{map[string]any{"c": map[string]any{"d": int64(3)}}}, `[[{"key":"c","value":[{"key":"d","value":3}]}]]`},
		{"array of arrays of maps", `{"type":"array","items":{"type":"array","items":{"type":"map","values":"long"}}}`,
			[]any{[]any{[]any{map[string]any{"e": int64(4)}}, []any{}}}, `[[[[{"key":"e","value":4}]],[]]]`},
		{"map of arrays", `{"type":"map","values":{"type":"array","items":"int"}}`,
			[]any{map[string]any{"a": []any{int32(1), int32(2)}}, map[string]any{"b": []any{}}}, `[[{"key":"a","value":[1,2]}],[{"key":"b","value":[]}]]`},
		{"map of arrays of records", `{"type":"map","values":{"type":"array","items":` + rec + `}}`,
			[]any{map[string]any{"a": []any{x(1), x(2)}}}, `[[{"key":"a","value":[{"x":1},{"x":2}]}]]`},
		{"map of arrays of maps", `{"type":"map","values":{"type":"array","items":{"type":"map","values":"int"}}}`,
			[]any{map[string]any{"a": []any{map[string]any{"b": int32(1)}}}}, `[[{"key":"a","value":[[{"key":"b","value":1}]]}]]`},
		{"map of arrays of arrays", `{"type":"map","values":{"type":"array","items":{"type":"array","items":"int"}}}`,
			[]any{map[string]any{"a": []any{[]any{int32(1)}, []any{}}}}, `[[{"key":"a","value":[[1],[]]}]]`},
		{"unknown logical type", `{"type":"string","logicalType":"foo"}`, []any{"s"}, `["s"]`},
		{"decimal on int", `{"type":"int","logicalType":"decimal","precision":4,"scale":2}`, []any{int32(5)}, `[5]`},
		{"uuid on bytes", `{"type":"bytes","logicalType":"uuid"}`, []any{[]byte("0123456789abcdef")}, `["MDEyMzQ1Njc4OWFiY2RlZg=="]`},
		{"uuid on fixed(4)", `{"type":"fixed","name":"F","size":4,"logicalType":"uuid"}`, []any{[4]byte{1, 2, 3, 4}}, `["AQIDBA=="]`},
		{"time-millis on long", `{"type":"long","logicalType":"time-millis"}`, []any{int64(5)}, `[5]`},
		{"timestamp-millis on int", `{"type":"int","logicalType":"timestamp-millis"}`, []any{int32(5)}, `[5]`},
		{"duration on fixed(10)", `{"type":"fixed","name":"D","size":10,"logicalType":"duration"}`, []any{[10]byte{1}}, `["AQAAAAAAAAAAAA=="]`},
		{"field-level timestamp-millis on long", `"long","logicalType":"timestamp-millis"`, []any{ts}, `["2023-11-14T22:13:20.123Z"]`},
		{"field-level decimal on bytes", `"bytes","logicalType":"decimal","precision":4,"scale":2`, []any{big.NewRat(1234, 100)}, `["12.34"]`},
		{"field-level timestamp-millis on a nullable long", `["null","long"],"logicalType":"timestamp-millis"`, []any{ts, nil}, `["2023-11-14T22:13:20.123Z",null]`},
		{"logical type in the type wins over the field", `{"type":"long","logicalType":"timestamp-micros"},"logicalType":"timestamp-millis"`,
			[]any{tsMicros}, `["2023-11-14T22:13:20.123456Z"]`},
		{"field-level scale fills the type's decimal", `{"type":"bytes","logicalType":"decimal","precision":6},"logicalType":"decimal","scale":2`,
			[]any{big.NewRat(1234, 100)}, `["12.34"]`},
		{"field-level logical type that does not fit", `"int","logicalType":"timestamp-millis"`, []any{int32(5)}, `[5]`},
		{"big-decimal", bigDecimal, []any{big.NewRat(1234, 100), big.NewRat(-15, 10), big.NewRat(42, 1), new(big.Rat), manyDigits},
			`["12.34","-1.5","42","0","12345678901234567890.0000000001"]`},
		{"nullable big-decimal", `["null",` + bigDecimal + `]`, []any{big.NewRat(3, 2), nil}, `["1.5",null]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := make([]any, len(tt.rows))
			for i, row := range tt.rows {
				rows[i] = map[string]any{"f": row}
			}
			schema := `{"type":"record","name":"rec","fields":[{"name":"f","type":` + tt.typ + `}]}`
			ar, err := NewOCFReader(bytes.NewReader(writeOCF(t, schema, rows...)), WithChunk(-1))
			require.NoError(t, err)
			defer ar.Close()

			require.True(t, ar.Next(), "%v", ar.Err())
			got, err := json.Marshal(ar.RecordBatch().Column(0))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

// A schema edit can give a field a type that the data in the file does not
// have. The reader must return an error for such a value.
func TestOCFReaderSchemaEditMismatch(t *testing.T) {
	fixture := writeOCF(t, `{"type":"record","name":"rec","fields":[{"name":"f","type":"int"},{"name":"g","type":"int"}]}`,
		map[string]any{"f": int32(1), "g": int32(2)})
	tests := []struct {
		name    string
		typ     string
		wantErr string
	}{
		{"array", `{"type":"array","items":"int"}`, "unsupported value of type int32 for List column"},
		{"map", `{"type":"map","values":"int"}`, "unsupported value of type int32 for Map column"},
		{"record", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, "unsupported value of type int32 for Struct column"},
		{"null", `"null"`, "unsupported value of type int32 for Null column"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ar, err := NewOCFReader(bytes.NewReader(fixture), WithChunk(-1),
				WithSchemaEdit("set", "fields.0.type", json.RawMessage(tt.typ)))
			require.NoError(t, err)
			defer ar.Close()

			require.False(t, ar.Next())
			require.ErrorContains(t, ar.Err(), tt.wantErr)
		})
	}
}

func TestOCFReaderCloseUnblocksFullQueues(t *testing.T) {
	const schema = `{"type":"record","name":"rec","fields":[{"name":"value","type":"long"}]}`
	var buf bytes.Buffer
	avroSchema, err := avro.Parse(schema)
	assert.NoError(t, err)
	enc, err := ocf.NewWriter(&buf, avroSchema)
	assert.NoError(t, err)
	for i := 0; i < 100; i++ {
		assert.NoError(t, enc.Encode(map[string]any{"value": int64(i)}))
	}
	assert.NoError(t, enc.Close())

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	reader, err := NewOCFReader(bytes.NewReader(buf.Bytes()), WithAllocator(mem),
		WithReadCacheSize(1), WithRecordCacheSize(1), WithChunk(1))
	assert.NoError(t, err)

	deadline := time.Now().Add(time.Second)
	for reader.OCFRecordsReadCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		reader.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked with full producer queues")
	}

	reader.Release()
	mem.AssertSize(t, 0)
}

func TestOCFReaderReuseWaitsForPreviousWorkers(t *testing.T) {
	const schema = `{"type":"record","name":"rec","fields":[{"name":"value","type":"long"}]}`
	encode := func(start int64) []byte {
		var buf bytes.Buffer
		avroSchema, err := avro.Parse(schema)
		assert.NoError(t, err)
		enc, err := ocf.NewWriter(&buf, avroSchema)
		assert.NoError(t, err)
		for i := range int64(20) {
			assert.NoError(t, enc.Encode(map[string]any{"value": start + i}))
		}
		assert.NoError(t, enc.Close())
		return buf.Bytes()
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	reader, err := NewOCFReader(bytes.NewReader(encode(0)), WithAllocator(mem),
		WithReadCacheSize(1), WithRecordCacheSize(1), WithChunk(1))
	assert.NoError(t, err)
	assert.True(t, reader.Next())

	assert.NoError(t, reader.Reuse(bytes.NewReader(encode(100))))
	var values []int64
	for reader.Next() {
		values = append(values, reader.RecordBatch().Column(0).(*array.Int64).Value(0))
	}
	assert.NoError(t, reader.Err())
	assert.Len(t, values, 20)
	for i, value := range values {
		assert.Equal(t, int64(100+i), value)
	}

	reader.Release()
	mem.AssertSize(t, 0)
}

func TestOCFReaderReuseDiscardsPartialBuilderState(t *testing.T) {
	const schema = `{"type":"record","name":"rec","fields":[{"name":"value","type":"long"}]}`
	encode := func(value int64) []byte {
		var buf bytes.Buffer
		avroSchema, err := avro.Parse(schema)
		require.NoError(t, err)
		enc, err := ocf.NewWriter(&buf, avroSchema)
		require.NoError(t, err)
		require.NoError(t, enc.Encode(map[string]any{"value": value}))
		require.NoError(t, enc.Close())
		return buf.Bytes()
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	reader, err := NewOCFReader(bytes.NewReader(encode(0)), WithAllocator(mem))
	require.NoError(t, err)
	reader.Close()

	reader.bld.Field(0).(*array.Int64Builder).Append(999)

	require.NoError(t, reader.Reuse(bytes.NewReader(encode(100))))
	require.True(t, reader.Next())
	record := reader.RecordBatch()
	require.EqualValues(t, 1, record.NumRows())
	require.EqualValues(t, 100, record.Column(0).(*array.Int64).Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())

	reader.Release()
	mem.AssertSize(t, 0)
}

// A nullable logical timestamp must decode to a value rather than a null: the
// union branch carries the logical type, so the reader has to honour it on the
// branch and not just on a bare long.
func TestOCFReaderNullableTimestamps(t *testing.T) {
	tests := []struct {
		logicalType string
		typ         *arrow.TimestampType
	}{
		{"timestamp-millis", arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType)},
		{"timestamp-micros", arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType)},
		{"timestamp-nanos", arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType)},
		{"local-timestamp-millis", &arrow.TimestampType{Unit: arrow.Millisecond}},
		{"local-timestamp-micros", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"local-timestamp-nanos", &arrow.TimestampType{Unit: arrow.Nanosecond}},
	}

	// Decoding must not be influenced by the machine's zone: a local-timestamp
	// carries no zone at all, so a non-UTC TZ must not shift the value the
	// reader produces.
	origLocal := time.Local
	time.Local = time.FixedZone("test", 3*60*60)
	t.Cleanup(func() { time.Local = origLocal })

	for _, tt := range tests {
		t.Run(tt.logicalType, func(t *testing.T) {
			schemaJSON := fmt.Sprintf(`{
				"type": "record",
				"name": "event",
				"fields": [{
					"name": "started_at",
					"type": ["null", {"type": "long", "logicalType": %q}]
				}]
			}`, tt.logicalType)
			schema, err := avro.Parse(schemaJSON)
			assert.NoError(t, err)

			// The value is given in UTC so that the encoder's own
			// normalisation is a no-op and the assertion below is about what
			// the reader decodes, not about how twmb chose to write it.
			value := time.Date(2026, 7, 13, 14, 15, 16, 123456789, time.UTC)

			var buf bytes.Buffer
			// WithSchema keeps the original JSON in the OCF header; the
			// default is the parsing canonical form, which by spec drops the
			// logical types this test is about.
			w, err := ocf.NewWriter(&buf, schema, ocf.WithSchema(schemaJSON))
			assert.NoError(t, err)
			assert.NoError(t, w.Encode(map[string]any{"started_at": value}))
			assert.NoError(t, w.Encode(map[string]any{"started_at": nil}))
			assert.NoError(t, w.Close())

			ar, err := NewOCFReader(bytes.NewReader(buf.Bytes()), WithChunk(-1))
			assert.NoError(t, err)
			defer ar.Close()

			field := ar.Schema().Field(0)
			assert.Equal(t, tt.typ, field.Type)
			assert.True(t, field.Nullable)

			assert.True(t, ar.Next())
			assert.NoError(t, ar.Err())
			values := ar.RecordBatch().Column(0).(*array.Timestamp)
			assert.False(t, values.IsNull(0))
			assert.True(t, values.IsNull(1))
			assert.Equal(t, 1, values.NullN())

			wantValue, err := arrow.TimestampFromTime(value, tt.typ.Unit)
			assert.NoError(t, err)
			assert.Equal(t, wantValue, values.Value(0))
		})
	}
}

// loadDatum must surface appender errors from nested paths (map values and
// list items), not just from top-level scalar fields.
func TestLoadDatumPropagatesNestedAppendErrors(t *testing.T) {
	newLoader := func(t *testing.T, avroSchema string) (*dataLoader, *array.RecordBuilder) {
		t.Helper()
		arrowSchema, err := ArrowSchemaFromAvroJSON(avroSchema)
		assert.NoError(t, err)
		bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		pos := newFieldPos()
		ldr := newDataLoader()
		for idx, fb := range bld.Fields() {
			mapFieldBuilders(fb, arrowSchema.Field(idx), pos)
		}
		ldr.drawTree(pos)
		return ldr, bld
	}

	t.Run("map value", func(t *testing.T) {
		ldr, bld := newLoader(t, `{"type":"record","name":"r","fields":[
			{"name":"m","type":{"type":"map","values":"bytes"}}]}`)
		defer bld.Release()
		assert.NoError(t, ldr.loadDatum(map[string]any{"m": map[string]any{"k": []byte{0x01}}}))
		assert.ErrorContains(t, ldr.loadDatum(map[string]any{"m": map[string]any{"k": 42}}), "unsupported value of type int")
	})

	t.Run("list item", func(t *testing.T) {
		ldr, bld := newLoader(t, `{"type":"record","name":"r","fields":[
			{"name":"l","type":{"type":"array","items":"bytes"}}]}`)
		defer bld.Release()
		assert.NoError(t, ldr.loadDatum(map[string]any{"l": []any{[]byte{0x01}}}))
		assert.ErrorContains(t, ldr.loadDatum(map[string]any{"l": []any{42}}), "unsupported value of type int")
	})
}

// writeOCF encodes data as an OCF file using schemaJSON verbatim in the
// header, so logical-type annotations survive (the writer would otherwise
// default to the parsing canonical form, which strips them).
func writeOCF(t *testing.T, schemaJSON string, data ...any) []byte {
	t.Helper()
	schema, err := avro.Parse(schemaJSON)
	assert.NoError(t, err)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithSchema(schemaJSON))
	assert.NoError(t, err)
	for _, d := range data {
		assert.NoError(t, w.Encode(d))
	}
	assert.NoError(t, w.Close())
	return buf.Bytes()
}

// Only ["null", T] unions map onto Arrow's nullability. Anything else has no
// faithful representation, so it must be reported rather than silently
// resolved to one of the branches.
func TestUnsupportedUnionReportsError(t *testing.T) {
	tests := []struct {
		name  string
		union string
	}{
		{"no null branch", `["int", "string"]`},
		{"two non-null branches", `["null", "int", "string"]`},
		{"named branches", `["null", {"type":"record","name":"A","fields":[]}, {"type":"record","name":"B","fields":[]}]`},
		{"null only", `["null"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ArrowSchemaFromAvroJSON(fmt.Sprintf(
				`{"type":"record","name":"r","fields":[{"name":"u","type":%s}]}`, tt.union))
			assert.ErrorContains(t, err, "unsupported avro union")
		})
	}

	t.Run("nested in array", func(t *testing.T) {
		_, err := ArrowSchemaFromAvroJSON(`{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"array","items":["int","string"]}}]}`)
		assert.ErrorContains(t, err, "unsupported avro union")
	})
}

// Reuse accepts a second file whose schema is semantically identical, and
// rejects one whose schema differs.
func TestOCFReaderReuse(t *testing.T) {
	const schemaJSON = `{"type":"record","name":"r","fields":[
		{"name":"id","type":"int"},
		{"name":"name","type":"string"}]}`
	// Same schema, but reformatted and with a doc attribute: the parsing
	// canonical form is unchanged, so this must still be reusable.
	const equivalentJSON = `{"name":"r","type":"record","doc":"reformatted","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	const otherJSON = `{"type":"record","name":"r","fields":[{"name":"id","type":"long"}]}`

	first := writeOCF(t, schemaJSON, map[string]any{"id": int32(1), "name": "one"})
	second := writeOCF(t, equivalentJSON, map[string]any{"id": int32(2), "name": "two"})
	other := writeOCF(t, otherJSON, map[string]any{"id": int64(3)})

	readIDs := func(t *testing.T, ar *OCFReader) []int32 {
		t.Helper()
		var got []int32
		for ar.Next() {
			assert.NoError(t, ar.Err())
			col := ar.RecordBatch().Column(0).(*array.Int32)
			got = append(got, col.Int32Values()...)
		}
		assert.NoError(t, ar.Err())
		return got
	}

	ar, err := NewOCFReader(bytes.NewReader(first), WithChunk(-1))
	assert.NoError(t, err)
	defer ar.Close()
	assert.Equal(t, []int32{1}, readIDs(t, ar))

	schemaBefore := ar.Schema()
	assert.NoError(t, ar.Reuse(bytes.NewReader(second), WithChunk(-1)))
	assert.Equal(t, []int32{2}, readIDs(t, ar))
	// Reuse keeps the builders, so the Arrow schema must be untouched.
	assert.Equal(t, schemaBefore, ar.Schema())

	assert.ErrorContains(t, ar.Reuse(bytes.NewReader(other)), "avro schema mismatch")
}

// A positive chunk size splits the datums into batches of that many rows, with
// a shorter final batch for the remainder.
func TestOCFReaderWithChunk(t *testing.T) {
	const schemaJSON = `{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}`
	const rows = 7

	data := make([]any, rows)
	for i := range data {
		data[i] = map[string]any{"id": int32(i)}
	}
	fixture := writeOCF(t, schemaJSON, data...)

	for _, chunk := range []int{1, 3, rows, rows + 1} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			ar, err := NewOCFReader(bytes.NewReader(fixture), WithChunk(chunk))
			assert.NoError(t, err)
			defer ar.Close()

			var lens []int
			var ids []int32
			for ar.Next() {
				assert.NoError(t, ar.Err())
				rec := ar.RecordBatch()
				lens = append(lens, int(rec.NumRows()))
				ids = append(ids, rec.Column(0).(*array.Int32).Int32Values()...)
			}
			assert.NoError(t, ar.Err())

			var want []int
			for left := rows; left > 0; left -= chunk {
				want = append(want, min(left, chunk))
			}
			assert.Equal(t, want, lens)
			assert.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6}, ids)
			assert.Equal(t, int64(rows), ar.OCFRecordsReadCount())
		})
	}
}

// A record made up only of enum columns must come back as dictionary arrays
// carrying the Avro symbols, including through a ["null", enum] union.
func TestOCFReaderEnumOnlyColumns(t *testing.T) {
	const schemaJSON = `{"type":"record","name":"r","fields":[
		{"name":"suit","type":{"type":"enum","name":"Suit","symbols":["hearts","spades"]}},
		{"name":"rank","type":["null",{"type":"enum","name":"Rank","symbols":["low","high"]}]}]}`

	fixture := writeOCF(t, schemaJSON,
		map[string]any{"suit": "spades", "rank": "high"},
		map[string]any{"suit": "hearts", "rank": nil},
	)

	ar, err := NewOCFReader(bytes.NewReader(fixture), WithChunk(-1))
	assert.NoError(t, err)
	defer ar.Close()

	suit := ar.Schema().Field(0)
	assert.Equal(t, &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint8,
		ValueType: arrow.BinaryTypes.String,
	}, suit.Type)
	assert.False(t, suit.Nullable)
	symbols, ok := suit.Metadata.GetValue("1")
	assert.True(t, ok)
	assert.Equal(t, "spades", symbols)
	assert.True(t, ar.Schema().Field(1).Nullable)

	assert.True(t, ar.Next())
	assert.NoError(t, ar.Err())
	rec := ar.RecordBatch()
	assert.Equal(t, int64(2), rec.NumRows())

	values := func(col arrow.Array) []string {
		d := col.(*array.Dictionary)
		dict := d.Dictionary().(*array.String)
		out := make([]string, d.Len())
		for i := range out {
			if d.IsNull(i) {
				out[i] = "<null>"
				continue
			}
			out[i] = dict.Value(d.GetValueIndex(i))
		}
		return out
	}
	assert.Equal(t, []string{"spades", "hearts"}, values(rec.Column(0)))
	assert.Equal(t, []string{"high", "<null>"}, values(rec.Column(1)))
}
