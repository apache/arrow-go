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

package arreflect

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestToGo(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("int32 element 0", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendValues([]int32{10, 20, 30}, nil)
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := Get[int32](arr, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != 10 {
			t.Errorf("expected 10, got %d", got)
		}
	})

	t.Run("string element 1", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues([]string{"hello", "world"}, nil)
		arr := b.NewStringArray()
		defer arr.Release()

		got, err := Get[string](arr, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got != "world" {
			t.Errorf("expected world, got %q", got)
		}
	})

	t.Run("struct element 0", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int32
		}
		vals := []Person{{"Alice", 30}, {"Bob", 25}}
		arr, err := FromSlice(vals, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		got, err := Get[Person](arr, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.Name != "Alice" || got.Age != 30 {
			t.Errorf("expected {Alice 30}, got %+v", got)
		}
	})

	t.Run("null element to *int32 is nil", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendNull()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := Get[*int32](arr, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil pointer for null, got %v", *got)
		}
	})

	t.Run("null element to int32 is zero", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendNull()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := Get[int32](arr, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("expected 0 for null, got %d", got)
		}
	})
}

func TestToGoSlice(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("[]int32", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendValues([]int32{1, 2, 3}, nil)
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := ToSlice[int32](arr)
		if err != nil {
			t.Fatal(err)
		}
		want := []int32{1, 2, 3}
		if len(got) != len(want) {
			t.Fatalf("expected len %d, got %d", len(want), len(got))
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("index %d: expected %d, got %d", i, v, got[i])
			}
		}
	})

	t.Run("[]string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues([]string{"foo", "bar", "baz"}, nil)
		arr := b.NewStringArray()
		defer arr.Release()

		got, err := ToSlice[string](arr)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"foo", "bar", "baz"}
		if len(got) != len(want) {
			t.Fatalf("expected len %d, got %d", len(want), len(got))
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("index %d: expected %q, got %q", i, v, got[i])
			}
		}
	})

	t.Run("[]struct{Name string}", func(t *testing.T) {
		type Row struct {
			Name string
		}
		vals := []Row{{"Alice"}, {"Bob"}, {"Charlie"}}
		arr, err := FromSlice(vals, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		got, err := ToSlice[Row](arr)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(vals) {
			t.Fatalf("expected len %d, got %d", len(vals), len(got))
		}
		for i, want := range vals {
			if got[i].Name != want.Name {
				t.Errorf("index %d: expected %q, got %q", i, want.Name, got[i].Name)
			}
		}
	})

	t.Run("empty array gives empty slice", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := ToSlice[int32](arr)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(got) != 0 {
			t.Errorf("expected len 0, got %d", len(got))
		}
	})
}

func TestFromGoSlice(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("[]int32", func(t *testing.T) {
		arr, err := FromSlice([]int32{1, 2, 3}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		if arr.Len() != 3 {
			t.Fatalf("expected len 3, got %d", arr.Len())
		}
		typed := arr.(*array.Int32)
		for i, want := range []int32{1, 2, 3} {
			if typed.Value(i) != want {
				t.Errorf("index %d: expected %d, got %d", i, want, typed.Value(i))
			}
		}
	})

	t.Run("[]string", func(t *testing.T) {
		arr, err := FromSlice([]string{"a", "b"}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		if arr.Len() != 2 {
			t.Fatalf("expected len 2, got %d", arr.Len())
		}
		typed := arr.(*array.String)
		if typed.Value(0) != "a" || typed.Value(1) != "b" {
			t.Errorf("expected [a b], got [%s %s]", typed.Value(0), typed.Value(1))
		}
	})

	t.Run("[]struct{Name string; Score float64}", func(t *testing.T) {
		type Row struct {
			Name  string
			Score float64
		}
		vals := []Row{{"Alice", 9.5}, {"Bob", 8.0}}
		arr, err := FromSlice(vals, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		if arr.Len() != 2 {
			t.Fatalf("expected len 2, got %d", arr.Len())
		}
		got, err := ToSlice[Row](arr)
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range vals {
			if got[i].Name != want.Name || got[i].Score != want.Score {
				t.Errorf("index %d: expected %+v, got %+v", i, want, got[i])
			}
		}
	})

	t.Run("[]*int32 with nil produces null", func(t *testing.T) {
		v := int32(42)
		arr, err := FromSlice([]*int32{&v, nil}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		if arr.Len() != 2 {
			t.Fatalf("expected len 2, got %d", arr.Len())
		}
		if arr.IsNull(1) == false {
			t.Error("expected index 1 to be null")
		}
		typed := arr.(*array.Int32)
		if typed.Value(0) != 42 {
			t.Errorf("expected 42 at index 0, got %d", typed.Value(0))
		}
	})

	t.Run("empty []int32 gives length-0 array", func(t *testing.T) {
		arr, err := FromSlice([]int32{}, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()

		if arr.Len() != 0 {
			t.Errorf("expected len 0, got %d", arr.Len())
		}
	})
}

func TestRecordToSlice(t *testing.T) {
	mem := memory.NewGoAllocator()

	type Row struct {
		Name  string
		Score float64
	}

	buildRecord := func(rows []Row) arrow.Record {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "Name", Type: arrow.BinaryTypes.String},
			{Name: "Score", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		nameB := array.NewStringBuilder(mem)
		defer nameB.Release()
		scoreB := array.NewFloat64Builder(mem)
		defer scoreB.Release()
		for _, r := range rows {
			nameB.Append(r.Name)
			scoreB.Append(r.Score)
		}
		nameArr := nameB.NewStringArray()
		defer nameArr.Release()
		scoreArr := scoreB.NewFloat64Array()
		defer scoreArr.Release()
		return array.NewRecord(schema, []arrow.Array{nameArr, scoreArr}, int64(len(rows)))
	}

	t.Run("basic 3-row record", func(t *testing.T) {
		want := []Row{{"Alice", 9.5}, {"Bob", 8.0}, {"Carol", 7.5}}
		rec := buildRecord(want)
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("expected len %d, got %d", len(want), len(got))
		}
		for i, w := range want {
			if got[i].Name != w.Name || got[i].Score != w.Score {
				t.Errorf("index %d: expected %+v, got %+v", i, w, got[i])
			}
		}
	})

	t.Run("empty record gives empty slice", func(t *testing.T) {
		rec := buildRecord(nil)
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty slice, got len %d", len(got))
		}
	})
}

func TestRecordFromSlice(t *testing.T) {
	mem := memory.NewGoAllocator()

	type Row struct {
		Name  string
		Score float64
	}

	t.Run("struct slice produces correct schema and values", func(t *testing.T) {
		vals := []Row{{"Alice", 9.5}, {"Bob", 8.0}}
		rec, err := RecordFromSlice(vals, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer rec.Release()

		if rec.NumCols() != 2 {
			t.Fatalf("expected 2 cols, got %d", rec.NumCols())
		}
		if rec.NumRows() != 2 {
			t.Fatalf("expected 2 rows, got %d", rec.NumRows())
		}
		if rec.Schema().Field(0).Name != "Name" {
			t.Errorf("expected col 0 name 'Name', got %q", rec.Schema().Field(0).Name)
		}
		if rec.Schema().Field(1).Name != "Score" {
			t.Errorf("expected col 1 name 'Score', got %q", rec.Schema().Field(1).Name)
		}
		nameCol := rec.Column(0).(*array.String)
		if nameCol.Value(0) != "Alice" || nameCol.Value(1) != "Bob" {
			t.Errorf("unexpected name values: %q %q", nameCol.Value(0), nameCol.Value(1))
		}
		scoreCol := rec.Column(1).(*array.Float64)
		if scoreCol.Value(0) != 9.5 || scoreCol.Value(1) != 8.0 {
			t.Errorf("unexpected score values: %v %v", scoreCol.Value(0), scoreCol.Value(1))
		}
	})

	t.Run("non-struct T returns error", func(t *testing.T) {
		_, err := RecordFromSlice([]int32{1, 2, 3}, mem)
		if err == nil {
			t.Fatal("expected error for non-struct T, got nil")
		}
	})

	t.Run("round-trip RecordFromSlice then RecordToSlice", func(t *testing.T) {
		want := []Row{{"Alice", 9.5}, {"Bob", 8.0}, {"Carol", 7.5}}
		rec, err := RecordFromSlice(want, mem)
		if err != nil {
			t.Fatal(err)
		}
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("expected len %d, got %d", len(want), len(got))
		}
		for i, w := range want {
			if got[i].Name != w.Name || got[i].Score != w.Score {
				t.Errorf("index %d: expected %+v, got %+v", i, w, got[i])
			}
		}
	})
}

func TestGetAny(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewInt32Builder(mem)
	defer b.Release()
	b.Append(42)
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()

	got, err := GetAny(arr, 0)
	if err != nil {
		t.Fatalf("GetAny(0): %v", err)
	}
	if v, ok := got.(int32); !ok || v != 42 {
		t.Errorf("GetAny(0) = %v (%T), want int32(42)", got, got)
	}

	got, err = GetAny(arr, 1)
	if err != nil {
		t.Fatalf("GetAny(1): %v", err)
	}
	if v, ok := got.(int32); !ok || v != 0 {
		t.Errorf("GetAny(1) = %v, want int32(0)", got)
	}
}

func TestToAnySlice(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append("hello")
	b.Append("world")
	arr := b.NewArray()
	defer arr.Release()

	got, err := ToAnySlice(arr)
	if err != nil {
		t.Fatalf("ToAnySlice: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].(string) != "hello" || got[1].(string) != "world" {
		t.Errorf("got %v, want [hello world]", got)
	}
}
