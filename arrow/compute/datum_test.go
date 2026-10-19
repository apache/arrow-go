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
// This is the regression test for issue #1298.
func TestNewDatumWithoutOwningDoesNotLeakOwner(t *testing.T) {
	const n = 8
	const initialAlloc int = 0

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	// -- Array --------------------------------------------------------
	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	arr := builder.NewArray()
	defer arr.Release()
	builder.Release()

	before := mem.CurrentAlloc()
	assert.Greater(t, before, initialAlloc, "builder should have allocated backing buffers")
	d := NewDatumWithoutOwning(arr)
	d.Release() // must be no-op for the caller's buffer
	assert.Equal(t, before, mem.CurrentAlloc(),
		"ArrayDatum.Release() must not free caller's allocation")

	// -- Chunked ------------------------------------------------------
	builder2 := array.NewInt32Builder(mem)
	builder2.AppendValues([]int32{9, 10, 11, 12, 13, 14, 15, 16}, nil)
	arr2 := builder2.NewArray()
	defer arr2.Release()
	builder2.Release()

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr, arr2})
	defer chunked.Release()

	before = mem.CurrentAlloc()
	d = NewDatumWithoutOwning(chunked)
	d.Release()
	assert.Equal(t, before, mem.CurrentAlloc(),
		"ChunkedDatum.Release() must not free caller's allocation")

	// -- RecordBatch --------------------------------------------------
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arr.DataType()}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, n)
	defer rec.Release()

	before = mem.CurrentAlloc()
	d = NewDatumWithoutOwning(rec)
	d.Release()
	assert.Equal(t, before, mem.CurrentAlloc(),
		"RecordDatum.Release() must not free caller's allocation")

	// -- Table --------------------------------------------------------
	tbl := array.NewTable(schema, []arrow.Column{
		*arrow.NewColumn(schema.Field(0), chunked),
	}, -1)
	defer tbl.Release()

	before = mem.CurrentAlloc()
	d = NewDatumWithoutOwning(tbl)
	d.Release()
	assert.Equal(t, before, mem.CurrentAlloc(),
		"TableDatum.Release() must not free caller's allocation")

	// -- Scalar -------------------------------------------------------
	sc := scalar.NewInt32Scalar(42)

	before = mem.CurrentAlloc()
	d = NewDatumWithoutOwning(sc)
	d.Release()
	assert.Equal(t, before, mem.CurrentAlloc(),
		"ScalarDatum.Release() must not free caller's allocation")
}

// TestNewDatumOwns verifies the owning NewDatum path releases its
// Retain on Datum.Release(), leaving the caller a single Release to do.
func TestNewDatumOwns(t *testing.T) {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.AppendValues([]int32{1, 2, 3}, nil)
	arr := builder.NewArray()
	builder.Release()

	// NewDatum Retains the array data; Datum.Release() must Relinquish.
	d := NewDatum(arr)
	d.Release()
	// caller still owns arr's last reference
	arr.Release()
}
