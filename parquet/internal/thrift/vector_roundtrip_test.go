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

package thrift_test

import (
	"bytes"
	"context"
	"testing"

	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	apachethrift "github.com/apache/thrift/lib/go/thrift"
)

// Verifies the hand-applied VECTOR repetition / logical type / vector_length
// additions to the generated parquet.go round-trip through the Thrift compact
// protocol that Parquet uses on disk.
func TestVectorThriftRoundTrip(t *testing.T) {
	// Enum surface.
	if got := format.FieldRepetitionType_VECTOR.String(); got != "VECTOR" {
		t.Fatalf("FieldRepetitionType_VECTOR.String() = %q, want VECTOR", got)
	}
	if v, err := format.FieldRepetitionTypeFromString("VECTOR"); err != nil || v != format.FieldRepetitionType_VECTOR {
		t.Fatalf("FieldRepetitionTypeFromString(VECTOR) = %v, %v", v, err)
	}

	// Canonical VECTOR uses an outer group annotated with LogicalType.VECTOR and
	// a VECTOR-repeated middle group carrying vector_length.
	outerRep := format.FieldRepetitionType_REQUIRED
	childCount := int32(1)
	outer := &format.SchemaElement{
		Name:           "embedding",
		RepetitionType: &outerRep,
		NumChildren:    &childCount,
		LogicalType:    &format.LogicalType{VECTOR: format.NewVectorType()},
	}
	vecRep := format.FieldRepetitionType_VECTOR
	vlen := int32(768)
	middle := &format.SchemaElement{
		Name:           "list",
		RepetitionType: &vecRep,
		NumChildren:    &childCount,
		VectorLength:   &vlen,
	}

	ser := thrift.NewThriftSerializer()
	for _, elem := range []*format.SchemaElement{outer, middle} {
		b, err := ser.Write(context.Background(), elem)
		if err != nil {
			t.Fatalf("serialize %q: %v", elem.Name, err)
		}
		got := format.NewSchemaElement()
		if _, err := thrift.DeserializeThrift(got, b); err != nil {
			t.Fatalf("deserialize %q: %v", elem.Name, err)
		}
		if !elem.Equals(got) {
			t.Fatalf("round-trip mismatch for %q:\n have %s\n want %s", elem.Name, got, elem)
		}
	}

	b, err := ser.Write(context.Background(), middle)
	if err != nil {
		t.Fatal(err)
	}
	got := format.NewSchemaElement()
	if _, err := thrift.DeserializeThrift(got, b); err != nil {
		t.Fatal(err)
	}
	if !got.IsSetVectorLength() || got.GetVectorLength() != 768 {
		t.Fatalf("vector_length not preserved: isset=%v val=%d", got.IsSetVectorLength(), got.GetVectorLength())
	}
	if got.GetRepetitionType() != format.FieldRepetitionType_VECTOR {
		t.Fatalf("repetition_type not preserved: %v", got.GetRepetitionType())
	}

	// Binary protocol exposes field ids directly; make sure vector_length uses
	// SchemaElement field id 11.
	buf := apachethrift.NewTMemoryBuffer()
	bin := apachethrift.NewTBinaryProtocolConf(buf, nil)
	if err := middle.Write(context.Background(), bin); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), []byte{byte(apachethrift.I32), 0x00, 0x0b}) {
		t.Fatalf("vector_length field id 11 not found in binary thrift: %x", buf.Bytes())
	}
	if bytes.Contains(buf.Bytes(), []byte{byte(apachethrift.I32), 0x00, 0x0c}) {
		t.Fatalf("unexpected vector_length field id 12 found in binary thrift: %x", buf.Bytes())
	}

	b, err = ser.Write(context.Background(), outer)
	if err != nil {
		t.Fatal(err)
	}
	got = format.NewSchemaElement()
	if _, err := thrift.DeserializeThrift(got, b); err != nil {
		t.Fatal(err)
	}
	if !got.IsSetLogicalType() || !got.GetLogicalType().IsSetVECTOR() {
		t.Fatalf("VECTOR logical type not preserved: %v", got.GetLogicalType())
	}
}
