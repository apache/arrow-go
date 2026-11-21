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

package ipc

import (
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

func TestLoaderReleasesOnError(t *testing.T) {
	tests := []struct {
		name    string
		dt      arrow.DataType
		nodes   []fieldMetadata
		buffers []bufferMetadata
		max     int
		// compressed makes the second buffer read fail after the validity buffer is loaded
		compressed bool
	}{
		{"union validity", arrow.SparseUnionOf([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}}, []arrow.UnionTypeCode{0}),
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 1}}, 10, false},
		{"primitive buffer", arrow.PrimitiveTypes.Int32,
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"binary buffer", arrow.BinaryTypes.String,
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"fixed size binary buffer", &arrow.FixedSizeBinaryType{ByteWidth: 4},
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"map buffer", arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32),
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"list buffer", arrow.ListOf(arrow.PrimitiveTypes.Int32),
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"list view buffer", arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
			[]fieldMetadata{{Len: 1, Nulls: 1}}, []bufferMetadata{{Len: 9}, {Len: 9}, {Offset: 64, Len: 8}}, 10, true},
		{"struct child", arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "b", Type: arrow.StructOf(arrow.Field{Name: "c", Type: arrow.Null})},
		),
			[]fieldMetadata{{Len: 1}, {Len: 1}, {Len: 1}}, []bufferMetadata{{Len: 4}}, 1, false},
		{"union child", arrow.SparseUnionOf([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int32},
			{Name: "b", Type: arrow.StructOf(arrow.Field{Name: "c", Type: arrow.Null})},
		}, []arrow.UnionTypeCode{0, 1}),
			[]fieldMetadata{{Len: 1}, {Len: 1}, {Len: 1}, {Len: 1}}, []bufferMetadata{{}, {Len: 1}, {}, {Len: 4}}, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			body := memory.NewResizableBuffer(mem)
			body.Resize(64)
			defer body.Release()
			b := flatbuffers.NewBuilder(0)
			record := recordToFB(b, 1, 64, tt.nodes, tt.buffers, -1, nil)
			b.Finish(record)
			ctx := arrayLoaderContext{src: ipcSource{meta: flatbuf.GetRootAsRecordBatch(b.FinishedBytes(), 0), rawBytes: body, mem: mem}, version: MetadataV4, max: tt.max}
			if tt.compressed {
				binary.LittleEndian.PutUint64(body.Bytes(), ^uint64(0))
				ctx.src.codec = &lz4Decompressor{}
			}
			require.Panics(t, func() { ctx.loadArray(tt.dt) })
		})
	}
}
