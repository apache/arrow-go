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

package encoding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
)

// The byte-stream-split encoders allocate their flush buffer lazily on the first
// FlushValues, so an encoder released without ever flushing (as columnWriter.Close
// does for a column chunk that received no values) must not dereference the nil buffer.
func TestByteStreamSplitReleaseWithoutFlush(t *testing.T) {
	tests := []struct {
		name    string
		typ     parquet.Type
		typeLen int32
	}{
		{"int32", parquet.Types.Int32, -1},
		{"int64", parquet.Types.Int64, -1},
		{"float32", parquet.Types.Float, -1},
		{"float64", parquet.Types.Double, -1},
		{"fixed_len_byte_array", parquet.Types.FixedLenByteArray, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descr := schema.NewColumn(schema.MustPrimitive(schema.NewPrimitiveNode(
				"col", parquet.Repetitions.Required, tt.typ, -1, tt.typeLen)), 0, 0)
			enc := NewEncoder(tt.typ, parquet.Encodings.ByteStreamSplit, false, descr, memory.DefaultAllocator)
			assert.NotPanics(t, enc.Release)
		})
	}
}
