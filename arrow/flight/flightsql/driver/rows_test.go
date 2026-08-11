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

package driver

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestRowsCloseReleasesRetainedRecords(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).Append(1)
	queued := builder.NewRecordBatch()
	builder.Field(0).(*array.Int64Builder).Append(2)
	pending := builder.NewRecordBatch()
	builder.Release()

	rows := newRows()
	ctx, cancel := context.WithCancel(context.Background())
	rows.ctxCancelFunc = cancel

	queued.Retain()
	pending.Retain()
	rows.recordChan <- queued

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(rows.recordChan)
		rows.sendRecord(ctx, pending)
	}()

	queued.Release()
	pending.Release()

	require.Positive(t, mem.CurrentAlloc())
	require.NoError(t, rows.Close())
	<-done
	require.Zero(t, mem.CurrentAlloc())
}
