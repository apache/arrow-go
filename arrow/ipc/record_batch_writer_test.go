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
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestRecordBatchWriter(t *testing.T) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int8}}, nil))
	bldr.Field(0).(*array.Int8Builder).AppendValues([]int8{1, 2, 3, 4, 5}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := NewRecordBatchWriter(&buf)

	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	r := NewMessageReader(&buf)
	defer r.Release()

	msg, err := r.Message()
	require.NoError(t, err)
	require.True(t, msg.Type() == MessageRecordBatch)
}
