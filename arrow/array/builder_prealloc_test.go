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

package array_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBinaryBuilder_AppendValues_PreAlloc(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
	defer builder.Release()

	// Test that AppendValues pre-allocates correctly
	data := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	builder.AppendValues(data, nil)

	arr := builder.NewBinaryArray()
	defer arr.Release()

	if arr.Len() != 3 {
		t.Fatalf("expected length 3, got %d", arr.Len())
	}

	for i, expected := range data {
		if string(arr.Value(i)) != string(expected) {
			t.Errorf("index %d: expected %q, got %q", i, expected, arr.Value(i))
		}
	}
}

func TestStringBuilder_AppendValues_PreAlloc(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	// Test that AppendValues pre-allocates correctly
	data := []string{
		"hello",
		"world",
		"test",
		"longer string value here",
	}

	builder.AppendValues(data, nil)

	arr := builder.NewStringArray()
	defer arr.Release()

	if arr.Len() != 4 {
		t.Fatalf("expected length 4, got %d", arr.Len())
	}

	for i, expected := range data {
		if arr.Value(i) != expected {
			t.Errorf("index %d: expected %q, got %q", i, expected, arr.Value(i))
		}
	}
}

func TestBinaryBuilder_ReserveData_PreAlloc(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
	defer builder.Release()

	// Reserve space for data upfront
	totalSize := 1000
	builder.Reserve(10)
	builder.ReserveData(totalSize)

	// Append data that fits within reservation
	for i := 0; i < 10; i++ {
		data := make([]byte, 100)
		for j := range data {
			data[j] = byte(i)
		}
		builder.Append(data)
	}

	arr := builder.NewBinaryArray()
	defer arr.Release()

	if arr.Len() != 10 {
		t.Fatalf("expected length 10, got %d", arr.Len())
	}
}

func TestStringBuilder_ReserveData_PreAlloc(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	// Reserve space for data upfront
	totalSize := 500
	builder.Reserve(5)
	builder.ReserveData(totalSize)

	// Append strings that fit within reservation
	strings := []string{"hello", "world", "test", "string", "values"}
	for _, s := range strings {
		builder.Append(s)
	}

	arr := builder.NewStringArray()
	defer arr.Release()

	if arr.Len() != 5 {
		t.Fatalf("expected length 5, got %d", arr.Len())
	}

	for i, expected := range strings {
		if arr.Value(i) != expected {
			t.Errorf("index %d: expected %q, got %q", i, expected, arr.Value(i))
		}
	}
}

func TestInt64Builder_AppendValues_Bulk(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	// Test bulk append
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	builder.AppendValues(data, nil)

	arr := builder.NewInt64Array()
	defer arr.Release()

	if arr.Len() != 10 {
		t.Fatalf("expected length 10, got %d", arr.Len())
	}

	for i, expected := range data {
		if arr.Value(i) != expected {
			t.Errorf("index %d: expected %d, got %d", i, expected, arr.Value(i))
		}
	}
}

func TestBufferBuilder_Capacity(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	builder := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
	defer builder.Release()

	// Test that DataCap returns capacity correctly
	initialCap := builder.DataCap()

	builder.ReserveData(1024)

	newCap := builder.DataCap()
	if newCap < 1024 {
		t.Errorf("expected capacity >= 1024 after ReserveData, got %d", newCap)
	}

	if newCap < initialCap {
		t.Errorf("capacity should not decrease: initial=%d, new=%d", initialCap, newCap)
	}
}
