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
	"context"
	"testing"

	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
)

// Verifies the hand-applied VECTOR repetition / vector_length additions to the
// generated parquet.go round-trip through the Thrift compact protocol that
// Parquet uses on disk.
func TestVectorThriftRoundTrip(t *testing.T) {
	// Enum surface.
	if got := format.FieldRepetitionType_VECTOR.String(); got != "VECTOR" {
		t.Fatalf("FieldRepetitionType_VECTOR.String() = %q, want VECTOR", got)
	}
	if v, err := format.FieldRepetitionTypeFromString("VECTOR"); err != nil || v != format.FieldRepetitionType_VECTOR {
		t.Fatalf("FieldRepetitionTypeFromString(VECTOR) = %v, %v", v, err)
	}

	// Uses VECTOR-repeated primitive leaf carrying vector_length.
	vecRep := format.FieldRepetitionType_VECTOR
	phys := format.Type_FLOAT
	vlen := int32(768)
	leaf := &format.SchemaElement{
		Name:           "embedding",
		Type:           &phys,
		RepetitionType: &vecRep,
		VectorLength:   &vlen,
	}

	ser := thrift.NewThriftSerializer()
	b, err := ser.Write(context.Background(), leaf)
	if err != nil {
		t.Fatalf("serialize %q: %v", leaf.Name, err)
	}
	gotLeaf := format.NewSchemaElement()
	if _, err := thrift.DeserializeThrift(gotLeaf, b); err != nil {
		t.Fatalf("deserialize %q: %v", leaf.Name, err)
	}
	if !leaf.Equals(gotLeaf) {
		t.Fatalf("round-trip mismatch for %q:\n have %s\n want %s", leaf.Name, gotLeaf, leaf)
	}

	// Specifically confirm vector_length (field 11) survived the wire.
	b, err = ser.Write(context.Background(), leaf)
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
	if got.GetType() != format.Type_FLOAT {
		t.Fatalf("physical type not preserved: %v", got.GetType())
	}
}
