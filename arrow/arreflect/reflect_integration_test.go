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
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type integOrderItem struct {
	Product string
	Tags    map[string]string
	Ratings [5]float32
}

type integOrder struct {
	ID    int64
	Items []integOrderItem
}

type integLargeRow struct {
	X int32
	Y float64
}

type integNullable struct {
	A *string
	B *int32
	C *float64
}

type integMixed struct {
	Required   string
	Optional   *string
	Count      int32
	MaybeCount *int32
}

type integBase struct {
	ID int64
}

type integExtended struct {
	integBase
	Name string `arrow:"name"`
	Skip string `arrow:"-"`
}

func TestReflectIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("complex nested round-trip", func(t *testing.T) {
		orders := []integOrder{
			{
				ID: 1001,
				Items: []integOrderItem{
					{Product: "widget", Tags: map[string]string{"color": "red"}, Ratings: [5]float32{4.5, 3.0, 5.0, 4.0, 3.5}},
					{Product: "gadget", Tags: map[string]string{"size": "large"}, Ratings: [5]float32{1.0, 2.0, 3.0, 4.0, 5.0}},
				},
			},
			{
				ID: 1002,
				Items: []integOrderItem{
					{Product: "thingamajig", Tags: map[string]string{"material": "steel", "finish": "matte"}, Ratings: [5]float32{5.0, 5.0, 5.0, 5.0, 5.0}},
				},
			},
			{
				ID:    1003,
				Items: nil,
			},
			{
				ID: 1004,
				Items: []integOrderItem{
					{Product: "doohickey", Tags: map[string]string{"brand": "acme"}, Ratings: [5]float32{2.5, 3.5, 4.5, 1.5, 0.5}},
					{Product: "whatchamacallit", Tags: map[string]string{"type": "premium"}, Ratings: [5]float32{3.0, 3.0, 3.0, 3.0, 3.0}},
					{Product: "thingy", Tags: map[string]string{"category": "misc"}, Ratings: [5]float32{1.0, 1.0, 1.0, 1.0, 1.0}},
				},
			},
			{
				ID: 1005,
				Items: []integOrderItem{
					{Product: "sprocket", Tags: map[string]string{"grade": "A"}, Ratings: [5]float32{4.0, 4.0, 4.0, 4.0, 4.0}},
				},
			},
		}

		arr, err := FromGoSlice(orders, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		output, err := ToGoSlice[integOrder](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}

		if len(output) != len(orders) {
			t.Fatalf("length mismatch: got %d, want %d", len(output), len(orders))
		}

		for i, want := range orders {
			got := output[i]
			if got.ID != want.ID {
				t.Errorf("[%d] ID: got %d, want %d", i, got.ID, want.ID)
			}
			if len(got.Items) != len(want.Items) {
				t.Errorf("[%d] Items length: got %d, want %d", i, len(got.Items), len(want.Items))
				continue
			}
			for j, wantItem := range want.Items {
				gotItem := got.Items[j]
				if gotItem.Product != wantItem.Product {
					t.Errorf("[%d][%d] Product: got %q, want %q", i, j, gotItem.Product, wantItem.Product)
				}
				if !reflect.DeepEqual(gotItem.Ratings, wantItem.Ratings) {
					t.Errorf("[%d][%d] Ratings: got %v, want %v", i, j, gotItem.Ratings, wantItem.Ratings)
				}
				if !reflect.DeepEqual(gotItem.Tags, wantItem.Tags) {
					t.Errorf("[%d][%d] Tags: got %v, want %v", i, j, gotItem.Tags, wantItem.Tags)
				}
			}
		}
	})

	t.Run("large array round-trip", func(t *testing.T) {
		const n = 10000
		rows := make([]integLargeRow, n)
		for i := range rows {
			rows[i] = integLargeRow{X: int32(i), Y: float64(i) * 1.5}
		}

		arr, err := FromGoSlice(rows, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		if arr.Len() != n {
			t.Fatalf("array length: got %d, want %d", arr.Len(), n)
		}

		output, err := ToGoSlice[integLargeRow](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}

		if len(output) != n {
			t.Fatalf("output length: got %d, want %d", len(output), n)
		}
		for i, want := range rows {
			if output[i].X != want.X || output[i].Y != want.Y {
				t.Errorf("[%d] got %+v, want %+v", i, output[i], want)
			}
		}
	})

	t.Run("all-null fields", func(t *testing.T) {
		rows := []integNullable{
			{A: nil, B: nil, C: nil},
			{A: nil, B: nil, C: nil},
			{A: nil, B: nil, C: nil},
		}

		arr, err := FromGoSlice(rows, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		output, err := ToGoSlice[integNullable](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}

		if len(output) != 3 {
			t.Fatalf("length: got %d, want 3", len(output))
		}
		for i, got := range output {
			if got.A != nil {
				t.Errorf("[%d] A: expected nil, got non-nil", i)
			}
			if got.B != nil {
				t.Errorf("[%d] B: expected nil, got non-nil", i)
			}
			if got.C != nil {
				t.Errorf("[%d] C: expected nil, got non-nil", i)
			}
		}
	})

	t.Run("empty int32 slice", func(t *testing.T) {
		arr, err := FromGoSlice[int32]([]int32{}, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		if arr.Len() != 0 {
			t.Errorf("array length: got %d, want 0", arr.Len())
		}

		output, err := ToGoSlice[int32](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}
		if output == nil {
			t.Error("ToGoSlice returned nil, want non-nil empty slice")
		}
		if len(output) != 0 {
			t.Errorf("output length: got %d, want 0", len(output))
		}
	})

	t.Run("empty struct slice", func(t *testing.T) {
		type simpleXY struct{ X int32 }
		arr, err := FromGoSlice[simpleXY]([]simpleXY{}, mem)
		if err != nil {
			t.Fatalf("FromGoSlice empty struct: %v", err)
		}
		defer arr.Release()

		if arr.Len() != 0 {
			t.Errorf("array length: got %d, want 0", arr.Len())
		}
		if arr.DataType().ID() != arrow.STRUCT {
			t.Errorf("expected STRUCT type for empty struct slice, got %v", arr.DataType())
		}
	})

	t.Run("mixed nullability round-trip", func(t *testing.T) {
		s1 := "hello"
		s2 := "world"
		c1 := int32(42)
		c3 := int32(99)

		rows := []integMixed{
			{Required: "first", Optional: &s1, Count: 10, MaybeCount: &c1},
			{Required: "second", Optional: nil, Count: 20, MaybeCount: nil},
			{Required: "third", Optional: &s2, Count: 30, MaybeCount: &c3},
			{Required: "fourth", Optional: nil, Count: 40, MaybeCount: nil},
		}

		arr, err := FromGoSlice(rows, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		output, err := ToGoSlice[integMixed](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}

		if len(output) != len(rows) {
			t.Fatalf("length: got %d, want %d", len(output), len(rows))
		}

		for i, want := range rows {
			got := output[i]
			if got.Required != want.Required {
				t.Errorf("[%d] Required: got %q, want %q", i, got.Required, want.Required)
			}
			if got.Count != want.Count {
				t.Errorf("[%d] Count: got %d, want %d", i, got.Count, want.Count)
			}
			if (got.Optional == nil) != (want.Optional == nil) {
				t.Errorf("[%d] Optional nil mismatch: got nil=%v, want nil=%v", i, got.Optional == nil, want.Optional == nil)
			} else if got.Optional != nil && *got.Optional != *want.Optional {
				t.Errorf("[%d] Optional value: got %q, want %q", i, *got.Optional, *want.Optional)
			}
			if (got.MaybeCount == nil) != (want.MaybeCount == nil) {
				t.Errorf("[%d] MaybeCount nil mismatch: got nil=%v, want nil=%v", i, got.MaybeCount == nil, want.MaybeCount == nil)
			} else if got.MaybeCount != nil && *got.MaybeCount != *want.MaybeCount {
				t.Errorf("[%d] MaybeCount value: got %d, want %d", i, *got.MaybeCount, *want.MaybeCount)
			}
		}
	})

	t.Run("embedded struct with tags", func(t *testing.T) {
		rows := []integExtended{
			{integBase: integBase{ID: 1}, Name: "alice"},
			{integBase: integBase{ID: 2}, Name: "bob"},
			{integBase: integBase{ID: 3}, Name: "carol"},
		}

		arr, err := FromGoSlice(rows, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		st, ok := arr.DataType().(*arrow.StructType)
		if !ok {
			t.Fatalf("expected StructType, got %T", arr.DataType())
		}

		var hasID, hasName, hasSkip bool
		for i := 0; i < st.NumFields(); i++ {
			switch st.Field(i).Name {
			case "ID":
				hasID = true
			case "name":
				hasName = true
			case "Skip":
				hasSkip = true
			}
		}
		if !hasID {
			t.Error("expected field 'ID' in schema")
		}
		if !hasName {
			t.Error("expected field 'name' in schema")
		}
		if hasSkip {
			t.Error("unexpected field 'Skip' in schema (should be skipped by arrow:\"-\" tag)")
		}

		output, err := ToGoSlice[integExtended](arr)
		if err != nil {
			t.Fatalf("ToGoSlice: %v", err)
		}

		if len(output) != len(rows) {
			t.Fatalf("length: got %d, want %d", len(output), len(rows))
		}
		for i, want := range rows {
			got := output[i]
			if got.ID != want.ID {
				t.Errorf("[%d] ID: got %d, want %d", i, got.ID, want.ID)
			}
			if got.Name != want.Name {
				t.Errorf("[%d] Name: got %q, want %q", i, got.Name, want.Name)
			}
			if got.Skip != "" {
				t.Errorf("[%d] Skip: expected empty string, got %q", i, got.Skip)
			}
		}
	})

	t.Run("schema consistency", func(t *testing.T) {
		orders := []integOrder{
			{ID: 1, Items: []integOrderItem{{Product: "a", Tags: map[string]string{"k": "v"}, Ratings: [5]float32{1, 2, 3, 4, 5}}}},
		}

		schema, err := InferArrowSchema[integOrder]()
		if err != nil {
			t.Fatalf("InferArrowSchema: %v", err)
		}

		arr, err := FromGoSlice(orders, mem)
		if err != nil {
			t.Fatalf("FromGoSlice: %v", err)
		}
		defer arr.Release()

		st, ok := arr.DataType().(*arrow.StructType)
		if !ok {
			t.Fatalf("expected StructType, got %T", arr.DataType())
		}

		if st.NumFields() != schema.NumFields() {
			t.Fatalf("field count mismatch: array has %d, schema has %d", st.NumFields(), schema.NumFields())
		}

		for i := 0; i < schema.NumFields(); i++ {
			schemaField := schema.Field(i)
			structField := st.Field(i)
			if structField.Name != schemaField.Name {
				t.Errorf("field[%d] name: array has %q, schema has %q", i, structField.Name, schemaField.Name)
			}
		}
	})

	t.Run("cache reuse without corruption", func(t *testing.T) {
		batch1 := make([]integLargeRow, 3)
		for i := range batch1 {
			batch1[i] = integLargeRow{X: int32(i + 1), Y: float64(i+1) * 2.0}
		}

		arr1, err := FromGoSlice(batch1, mem)
		if err != nil {
			t.Fatalf("FromGoSlice batch1: %v", err)
		}
		defer arr1.Release()

		batch2 := make([]integLargeRow, 5)
		for i := range batch2 {
			batch2[i] = integLargeRow{X: int32(i * 10), Y: float64(i) * 3.14}
		}

		arr2, err := FromGoSlice(batch2, mem)
		if err != nil {
			t.Fatalf("FromGoSlice batch2: %v", err)
		}
		defer arr2.Release()

		out1, err := ToGoSlice[integLargeRow](arr1)
		if err != nil {
			t.Fatalf("ToGoSlice batch1: %v", err)
		}
		out2, err := ToGoSlice[integLargeRow](arr2)
		if err != nil {
			t.Fatalf("ToGoSlice batch2: %v", err)
		}

		if len(out1) != len(batch1) {
			t.Fatalf("batch1 length: got %d, want %d", len(out1), len(batch1))
		}
		if len(out2) != len(batch2) {
			t.Fatalf("batch2 length: got %d, want %d", len(out2), len(batch2))
		}

		for i, want := range batch1 {
			if out1[i] != want {
				t.Errorf("batch1[%d]: got %+v, want %+v", i, out1[i], want)
			}
		}
		for i, want := range batch2 {
			if out2[i] != want {
				t.Errorf("batch2[%d]: got %+v, want %+v", i, out2[i], want)
			}
		}
	})

	t.Run("record batch round-trip", func(t *testing.T) {
		rows := []integLargeRow{
			{X: 10, Y: 1.1},
			{X: 20, Y: 2.2},
			{X: 30, Y: 3.3},
			{X: 40, Y: 4.4},
			{X: 50, Y: 5.5},
		}

		rec, err := RecordFromSlice(rows, mem)
		if err != nil {
			t.Fatalf("RecordFromSlice: %v", err)
		}
		defer rec.Release()

		if rec.NumRows() != int64(len(rows)) {
			t.Fatalf("NumRows: got %d, want %d", rec.NumRows(), len(rows))
		}

		output, err := RecordToSlice[integLargeRow](rec)
		if err != nil {
			t.Fatalf("RecordToSlice: %v", err)
		}

		if len(output) != len(rows) {
			t.Fatalf("output length: got %d, want %d", len(output), len(rows))
		}

		if !reflect.DeepEqual(rows, output) {
			t.Errorf("record round-trip mismatch:\n got:  %v\n want: %v", output, rows)
		}
	})
}

func BenchmarkReflectFromGoSlice(b *testing.B) {
	mem := memory.NewGoAllocator()
	rows := make([]integLargeRow, 1000)
	for i := range rows {
		rows[i] = integLargeRow{X: int32(i), Y: float64(i) * 1.5}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arr, err := FromGoSlice(rows, mem)
		if err != nil {
			b.Fatal(err)
		}
		arr.Release()
	}
}

func BenchmarkReflectToGoSlice(b *testing.B) {
	mem := memory.NewGoAllocator()
	rows := make([]integLargeRow, 1000)
	for i := range rows {
		rows[i] = integLargeRow{X: int32(i), Y: float64(i) * 1.5}
	}

	arr, err := FromGoSlice(rows, mem)
	if err != nil {
		b.Fatal(err)
	}
	defer arr.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := ToGoSlice[integLargeRow](arr)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
