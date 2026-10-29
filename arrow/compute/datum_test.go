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

//go:build go1.18

package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
)

// TestNewDatumWithoutOwningDoesNotLeakOwner verifies that calling Release()
// on a Datum constructed with NewDatumWithoutOwning is a no-op: it must not
// free the caller's underlying allocation. A CheckedAllocator tracks bytes
// before and after so any silent leak or over-release is caught.
//
// Each case follows the same pattern:
//   1. Caller builds a fresh owning value (refcount == 1).
//   2. NewDatumWithoutOwning borrows it without Retain.
//   3. Datum.Release() must be a no-op (refcount still 1).
//   4. Caller's Release() finally frees everything.
//
// This is the regression test for issue #1298.
func TestNewDatumWithoutOwningDoesNotLeakOwner(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	// -- Array --------------------------------------------------------
	{
		builder := array.NewInt32Builder(mem)
		builder.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil)
		arr := builder.NewArray()
		builder.Release()

		assert.Greater(t, mem.CurrentAlloc(), 0,
			"array builder should have allocated backing buffers")

		before := mem.CurrentAlloc()
		d := NewDatumWithoutOwning(arr)
		d.Release() // must be no-op

		assert.Equal(t, before, mem.CurrentAlloc(),
			"ArrayDatum.Release() must not free caller's allocation")

		arr.Release()
	}

	// -- Chunked ------------------------------------------------------
	{
		builder := array.NewInt32Builder(mem)
		builder.AppendValues([]int32{9, 10, 11, 12, 13, 14, 15, 16}, nil)
		arr := builder.NewArray()
		builder.Release()

		chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		// chunked owns arr (Retain), caller can now release arr
		arr.Release()

		before := mem.CurrentAlloc()
		d := NewDatumWithoutOwning(chunked)
		d.Release()

		assert.Equal(t, before, mem.CurrentAlloc(),
			"ChunkedDatum.Release() must not free caller's allocation")

		chunked.Release()
	}

	// -- RecordBatch --------------------------------------------------
	{
		builder := array.NewInt32Builder(mem)
		builder.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil)
		arr := builder.NewArray()
		builder.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arr.DataType()}}, nil)
		rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 8)
		// rec owns arr (Retain), caller can now release arr
		arr.Release()

		before := mem.CurrentAlloc()
		d := NewDatumWithoutOwning(rec)
		d.Release()

		assert.Equal(t, before, mem.CurrentAlloc(),
			"RecordDatum.Release() must not free caller's allocation")

		rec.Release()
	}

	// -- Table --------------------------------------------------------
	{
		builder := array.NewInt32Builder(mem)
		builder.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil)
		arr := builder.NewArray()
		builder.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arr.DataType()}}, nil)
		chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		// chunked owns arr
		arr.Release()

		col := arrow.NewColumn(schema.Field(0), chunked)
		// column owns chunked
		chunked.Release()

		tbl := array.NewTable(schema, []arrow.Column{*col}, -1)
		// table owns column
		col.Release()

		before := mem.CurrentAlloc()
		d := NewDatumWithoutOwning(tbl)
		d.Release()

		assert.Equal(t, before, mem.CurrentAlloc(),
			"TableDatum.Release() must not free caller's allocation")

		tbl.Release()
	}

	// -- Scalar (list scalar wraps an array, so it allocates) ---------
	{
		builder := array.NewInt32Builder(mem)
		builder.AppendValues([]int32{100, 200, 300}, nil)
		arr := builder.NewArray()
		builder.Release()

		sc := scalar.NewListScalar(arr)
		// list scalar wraps arr's data via array.MakeFromData
		arr.Release()

		assert.Greater(t, mem.CurrentAlloc(), 0,
			"list scalar should wrap an allocated array")

		before := mem.CurrentAlloc()
		d := NewDatumWithoutOwning(sc)
		d.Release()

		assert.Equal(t, before, mem.CurrentAlloc(),
			"ScalarDatum.Release() must not free caller's allocation")

		sc.Release()
	}
}

// TestNewDatumOwns verifies the owning NewDatum path releases its
// Retain on Datum.Release(), leaving the caller a single Release to do.
func TestNewDatumOwns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{1, 2, 3}, nil)
	arr := builder.NewArray()
	builder.Release()

	// NewDatum Retains the array data; Datum.Release() must Relinquish.
	before := mem.CurrentAlloc()
	d := NewDatum(arr)
	d.Release()
	assert.Equal(t, before, mem.CurrentAlloc(),
		"Datum.Release() must exactly match the Retain from NewDatum")

	// caller still owns arr's last reference
	arr.Release()
}
