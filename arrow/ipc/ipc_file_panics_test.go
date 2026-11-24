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

package ipc_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
)

// Fuzz inputs that used to panic in NewFileReader (apache/arrow-go#1568).
func TestFileReaderFuzzPanics(t *testing.T) {
	dataDir := os.Getenv("ARROW_TEST_DATA")
	if dataDir == "" {
		t.Skip("ARROW_TEST_DATA is not set")
	}
	for _, name := range []string{
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5577412021190656",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5693640900083712",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5864855240835072.fuzz",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5749190446153728",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5768644156194816",
		"clusterfuzz-testcase-arrow-ipc-file-fuzz-5768644156194816",
	} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dataDir, "arrow-ipc-file", name))
			if err != nil {
				t.Fatal(err)
			}
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			r, err := ipc.NewFileReader(bytes.NewReader(data), ipc.WithAllocator(mem))
			if err == nil {
				r.Close()
				t.Fatal("expected an error, got success")
			}
		})
	}
}

// unknownTypeSchema builds a one-field schema whose type id is unknown.
func unknownTypeSchema(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	flatbuf.NullStart(b)
	typ := flatbuf.NullEnd(b)
	name := b.CreateString("f")
	flatbuf.FieldStart(b)
	flatbuf.FieldAddName(b, name)
	flatbuf.FieldAddTypeType(b, flatbuf.Type(200))
	flatbuf.FieldAddType(b, typ)
	field := flatbuf.FieldEnd(b)
	flatbuf.SchemaStartFieldsVector(b, 1)
	b.PrependUOffsetT(field)
	fields := b.EndVector(1)
	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddFields(b, fields)
	return flatbuf.SchemaEnd(b)
}

// A field with an unknown flatbuf type id must yield an error, not a panic.
func TestReadersUnknownTypeID(t *testing.T) {
	// stream: continuation marker, padded length, then a Message wrapping the schema.
	b := flatbuffers.NewBuilder(0)
	schema := unknownTypeSchema(b)
	flatbuf.MessageStart(b)
	flatbuf.MessageAddVersion(b, flatbuf.MetadataVersionV5)
	flatbuf.MessageAddHeaderType(b, flatbuf.MessageHeaderSchema)
	flatbuf.MessageAddHeader(b, schema)
	b.Finish(flatbuf.MessageEnd(b))
	msg := b.FinishedBytes()
	msg = append(msg, make([]byte, -len(msg)&7)...)
	stream := binary.LittleEndian.AppendUint32(nil, 0xFFFFFFFF)
	stream = binary.LittleEndian.AppendUint32(stream, uint32(len(msg)))
	stream = append(stream, msg...)
	if _, err := ipc.NewReader(bytes.NewReader(stream)); err == nil || !strings.Contains(err.Error(), "200") {
		t.Errorf("NewReader: expected an error mentioning 200, got %v", err)
	}

	// file: magic, footer holding the same schema, footer size, magic.
	b = flatbuffers.NewBuilder(0)
	schema = unknownTypeSchema(b)
	flatbuf.FooterStart(b)
	flatbuf.FooterAddVersion(b, flatbuf.MetadataVersionV5)
	flatbuf.FooterAddSchema(b, schema)
	b.Finish(flatbuf.FooterEnd(b))
	footer := b.FinishedBytes()
	file := append([]byte("ARROW1\x00\x00"), footer...)
	file = binary.LittleEndian.AppendUint32(file, uint32(len(footer)))
	file = append(file, "ARROW1"...)
	if _, err := ipc.NewFileReader(bytes.NewReader(file)); err == nil || !strings.Contains(err.Error(), "200") {
		t.Errorf("NewFileReader: expected an error mentioning 200, got %v", err)
	}
}
