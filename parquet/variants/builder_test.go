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
	"testing"
)

func TestVariantMarshal(t *testing.T) {
	emptyMetadata := []byte{0x01, 0x00, 0x00}
	cases := []struct {
		name        string
		val         any
		wantEncoded *MarshaledVariant
		wantErr     bool
	}{
		{
			name: "Primitive",
			val:  123,
			wantEncoded: func() *MarshaledVariant {
				var buf bytes.Buffer
				marshalPrimitive(123, &buf)
				return &MarshaledVariant{
					Metadata: emptyMetadata,
					Value:    buf.Bytes(),
				}
			}(),
		},
		{
			name: "Array",
			val:  []any{123, "hello", false, []any{321, "olleh", true}},
			wantEncoded: func() *MarshaledVariant {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				ab := newArrayBuilder(&buf, mdb, nil)
				ab.Write(123)
				ab.Write("hello")
				ab.Write(false)

				sub := ab.Array()
				sub.Write(321)
				sub.Write("olleh")
				sub.Write(true)
				sub.Build()

				ab.Build()
				return &MarshaledVariant{
					Metadata: emptyMetadata,
					Value:    buf.Bytes(),
				}
			}(),
		},
		{
			name: "Struct",
			val: struct {
				FieldKey   string
				TagKey     int   `variant:"tag_key"`
				Arr        []int `variant:"array"`
				unexported bool
			}{"hello", 1, []int{1, 2, 3}, false},
			wantEncoded: func() *MarshaledVariant {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				ob := newObjectBuilder(&buf, mdb, nil)
				ob.Write("FieldKey", "hello")
				ob.Write("tag_key", 1)

				ab, err := ob.Array("array")
				if err != nil {
					t.Fatalf("Array(): %v", err)
				}
				ab.Write(1)
				ab.Write(2)
				ab.Write(3)
				ab.Build()

				ob.Build()
				return &MarshaledVariant{
					Metadata: mdb.Build(),
					Value:    buf.Bytes(),
				}
			}(),
		},
		{
			name: "Struct pointer",
			val: &struct {
				Field1 string
				Field2 int
			}{"hello", 123},
			wantEncoded: func() *MarshaledVariant {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				ob := newObjectBuilder(&buf, mdb, nil)
				ob.Write("Field1", "hello")
				ob.Write("Field2", 123)
				ob.Build()
				return &MarshaledVariant{
					Metadata: mdb.Build(),
					Value:    buf.Bytes(),
				}
			}(),
		},
		{
			name: "Valid map",
			// Map iteration order is undefined so only use one key here to test. Rely on the tests in object.go to cover maps more fully.
			val: map[string]int{"solitary_key": 1},
			wantEncoded: func() *MarshaledVariant {
				var buf bytes.Buffer
				mdb := newMetadataBuilder()
				ob := newObjectBuilder(&buf, mdb, nil)
				ob.Write("solitary_key", 1)
				ob.Build()
				return &MarshaledVariant{
					Metadata: mdb.Build(),
					Value:    buf.Bytes(),
				}
			}(),
		},
		{
			name: "Nil",
			val:  nil,
			wantEncoded: &MarshaledVariant{
				Metadata: emptyMetadata,
				Value:    []byte{0x00},
			},
		},
		{
			name:    "Invalid map",
			val:     map[int]string{1: "hello"},
			wantErr: true,
		},
		{
			name:    "Invalid value",
			val:     func() {},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			encoded, err := Marshal(c.val)
			if err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}
			diff(t, encoded, c.wantEncoded)
		})
	}
}

func TestVariantBuilderPrimitive(t *testing.T) {
	vb := NewBuilder()
	vb.Primitive(1)
	ev, err := vb.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	// Check metadata
	md := newMetadataBuilder().Build()
	diffByteArrays(t, ev.Metadata, md)

	// Check value
	var buf bytes.Buffer
	marshalPrimitive(1, &buf)
	diffByteArrays(t, ev.Value, buf.Bytes())
}

func TestVariantBuilderArray(t *testing.T) {
	vb := NewBuilder()
	ab, err := vb.Array()
	if err != nil {
		t.Fatalf("Array(): %v", err)
	}

	buildArray := func(ab ArrayBuilder) {
		ab.Write(1)
		ab.Write(true)
		nested := ab.Array()
		nested.Write("hello")
		nested.Build()
	}

	buildArray(ab)

	ev, err := vb.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	// Check metadata
	md := newMetadataBuilder().Build()
	diffByteArrays(t, ev.Metadata, md)

	// Check value
	wantArray := func() []byte {
		var buf bytes.Buffer
		mdb := newMetadataBuilder()
		ab := newArrayBuilder(&buf, mdb, nil)
		buildArray(ab)
		ab.Build()
		return buf.Bytes()
	}()
	diffByteArrays(t, ev.Value, wantArray)
}

func TestVariantBuilderObject(t *testing.T) {
	vb := NewBuilder()
	ob, err := vb.Object()
	if err != nil {
		t.Fatalf("Object(): %v", err)
	}

	buildObject := func(ob ObjectBuilder) {
		ob.Write("b", 1)
		ob.Write("c", 2)
		ob.Write("a", 3)
		nested, _ := ob.Object("d")
		nested.Write("a", true)
		nested.Write("e", "nested")
		nested.Build()
	}
	buildObject(ob)
	ev, err := vb.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	wantEncoded := func() ([]byte, []byte) {
		var buf bytes.Buffer
		mdb := newMetadataBuilder()
		ob := newObjectBuilder(&buf, mdb, nil)
		buildObject(ob)
		ob.Build()
		md := mdb.Build()
		return md, buf.Bytes()
	}

	wantMetadata, wantValue := wantEncoded()

	diffByteArrays(t, ev.Metadata, wantMetadata)
	diffByteArrays(t, ev.Value, wantValue)
}

func TestCannotChangeVariantType(t *testing.T) {
	vbPrim := NewBuilder()
	if err := vbPrim.Primitive(1); err != nil {
		t.Fatalf("Write first time: %v", err)
	}

	if err := vbPrim.Primitive(2); err == nil {
		t.Fatal("Primitive already started")
	}
	if _, err := vbPrim.Array(); err == nil {
		t.Fatal("Prmitive already started")
	}
	if _, err := vbPrim.Object(); err == nil {
		t.Fatal("Primitive already started")
	}

	vbArr := NewBuilder()
	if _, err := vbArr.Array(); err != nil {
		t.Fatalf("Array first time: %v", err)
	}
	if err := vbArr.Primitive(1); err == nil {
		t.Fatal("Array already started")
	}
	if _, err := vbArr.Array(); err == nil {
		t.Fatalf("Array already started")
	}
	if _, err := vbArr.Object(); err == nil {
		t.Fatalf("Array already started")
	}

	vbObj := NewBuilder()
	if _, err := vbObj.Object(); err != nil {
		t.Fatalf("Object first time: %v", err)
	}
	if err := vbObj.Primitive(1); err == nil {
		t.Fatal("Object alrady started")
	}
	if _, err := vbObj.Array(); err == nil {
		t.Fatal("Object already started")
	}
	if _, err := vbObj.Object(); err == nil {
		t.Fatal("Object already started")
	}
}
