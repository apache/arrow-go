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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	mem := testMem()

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

		arr, err := FromSlice(orders, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		output, err := ToSlice[integOrder](arr)
		require.NoError(t, err, "ToSlice")

		require.Len(t, output, len(orders))

		for i, want := range orders {
			got := output[i]
			assert.Equal(t, want.ID, got.ID, "[%d] ID", i)
			if assert.Len(t, got.Items, len(want.Items), "[%d] Items length", i) {
				for j, wantItem := range want.Items {
					gotItem := got.Items[j]
					assert.Equal(t, wantItem.Product, gotItem.Product, "[%d][%d] Product", i, j)
					assert.Equal(t, wantItem.Ratings, gotItem.Ratings, "[%d][%d] Ratings", i, j)
					assert.Equal(t, wantItem.Tags, gotItem.Tags, "[%d][%d] Tags", i, j)
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

		arr, err := FromSlice(rows, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		require.Equal(t, n, arr.Len())

		output, err := ToSlice[integLargeRow](arr)
		require.NoError(t, err, "ToSlice")

		require.Len(t, output, n)
		for i, want := range rows {
			assert.Equal(t, want.X, output[i].X, "[%d] X", i)
			assert.Equal(t, want.Y, output[i].Y, "[%d] Y", i)
		}
	})

	t.Run("all-null fields", func(t *testing.T) {
		rows := []integNullable{
			{A: nil, B: nil, C: nil},
			{A: nil, B: nil, C: nil},
			{A: nil, B: nil, C: nil},
		}

		arr, err := FromSlice(rows, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		output, err := ToSlice[integNullable](arr)
		require.NoError(t, err, "ToSlice")

		require.Len(t, output, 3)
		for i, got := range output {
			assert.Nil(t, got.A, "[%d] A: expected nil", i)
			assert.Nil(t, got.B, "[%d] B: expected nil", i)
			assert.Nil(t, got.C, "[%d] C: expected nil", i)
		}
	})

	t.Run("empty int32 slice", func(t *testing.T) {
		arr, err := FromSlice[int32]([]int32{}, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		assert.Equal(t, 0, arr.Len())

		output, err := ToSlice[int32](arr)
		require.NoError(t, err, "ToSlice")
		assert.NotNil(t, output, "ToSlice returned nil, want non-nil empty slice")
		assert.Len(t, output, 0)
	})

	t.Run("empty struct slice", func(t *testing.T) {
		type simpleXY struct{ X int32 }
		arr, err := FromSlice[simpleXY]([]simpleXY{}, mem)
		require.NoError(t, err, "FromSlice empty struct")
		defer arr.Release()

		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.STRUCT, arr.DataType().ID())
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

		arr, err := FromSlice(rows, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		output, err := ToSlice[integMixed](arr)
		require.NoError(t, err, "ToSlice")

		require.Len(t, output, len(rows))

		for i, want := range rows {
			got := output[i]
			assert.Equal(t, want.Required, got.Required, "[%d] Required", i)
			assert.Equal(t, want.Count, got.Count, "[%d] Count", i)
			if assert.Equal(t, want.Optional == nil, got.Optional == nil, "[%d] Optional nil mismatch", i) {
				if got.Optional != nil {
					assert.Equal(t, *want.Optional, *got.Optional, "[%d] Optional value", i)
				}
			}
			if assert.Equal(t, want.MaybeCount == nil, got.MaybeCount == nil, "[%d] MaybeCount nil mismatch", i) {
				if got.MaybeCount != nil {
					assert.Equal(t, *want.MaybeCount, *got.MaybeCount, "[%d] MaybeCount value", i)
				}
			}
		}
	})

	t.Run("embedded struct with tags", func(t *testing.T) {
		rows := []integExtended{
			{integBase: integBase{ID: 1}, Name: "alice"},
			{integBase: integBase{ID: 2}, Name: "bob"},
			{integBase: integBase{ID: 3}, Name: "carol"},
		}

		arr, err := FromSlice(rows, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		st, ok := arr.DataType().(*arrow.StructType)
		require.True(t, ok, "expected StructType, got %T", arr.DataType())

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
		assert.True(t, hasID, "expected field 'ID' in schema")
		assert.True(t, hasName, "expected field 'name' in schema")
		assert.False(t, hasSkip, "unexpected field 'Skip' in schema (should be skipped by arrow:\"-\" tag)")

		output, err := ToSlice[integExtended](arr)
		require.NoError(t, err, "ToSlice")

		require.Len(t, output, len(rows))
		for i, want := range rows {
			got := output[i]
			assert.Equal(t, want.ID, got.ID, "[%d] ID", i)
			assert.Equal(t, want.Name, got.Name, "[%d] Name", i)
			assert.Equal(t, "", got.Skip, "[%d] Skip: expected empty string", i)
		}
	})

	t.Run("schema consistency", func(t *testing.T) {
		orders := []integOrder{
			{ID: 1, Items: []integOrderItem{{Product: "a", Tags: map[string]string{"k": "v"}, Ratings: [5]float32{1, 2, 3, 4, 5}}}},
		}

		schema, err := InferSchema[integOrder]()
		require.NoError(t, err, "SchemaOf")

		arr, err := FromSlice(orders, mem)
		require.NoError(t, err, "FromSlice")
		defer arr.Release()

		st, ok := arr.DataType().(*arrow.StructType)
		require.True(t, ok, "expected StructType, got %T", arr.DataType())

		require.Equal(t, schema.NumFields(), st.NumFields())

		for i := 0; i < schema.NumFields(); i++ {
			schemaField := schema.Field(i)
			structField := st.Field(i)
			assert.Equal(t, schemaField.Name, structField.Name, "field[%d] name", i)
		}
	})

	t.Run("cache reuse without corruption", func(t *testing.T) {
		batch1 := make([]integLargeRow, 3)
		for i := range batch1 {
			batch1[i] = integLargeRow{X: int32(i + 1), Y: float64(i+1) * 2.0}
		}

		arr1, err := FromSlice(batch1, mem)
		require.NoError(t, err, "FromSlice batch1")
		defer arr1.Release()

		batch2 := make([]integLargeRow, 5)
		for i := range batch2 {
			batch2[i] = integLargeRow{X: int32(i * 10), Y: float64(i) * 3.14}
		}

		arr2, err := FromSlice(batch2, mem)
		require.NoError(t, err, "FromSlice batch2")
		defer arr2.Release()

		out1, err := ToSlice[integLargeRow](arr1)
		require.NoError(t, err, "ToSlice batch1")
		out2, err := ToSlice[integLargeRow](arr2)
		require.NoError(t, err, "ToSlice batch2")

		require.Len(t, out1, len(batch1))
		require.Len(t, out2, len(batch2))

		for i, want := range batch1 {
			assert.Equal(t, want, out1[i], "batch1[%d]", i)
		}
		for i, want := range batch2 {
			assert.Equal(t, want, out2[i], "batch2[%d]", i)
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
		require.NoError(t, err, "RecordFromSlice")
		defer rec.Release()

		require.Equal(t, int64(len(rows)), rec.NumRows())

		output, err := RecordToSlice[integLargeRow](rec)
		require.NoError(t, err, "RecordToSlice")

		require.Len(t, output, len(rows))
		assert.Equal(t, rows, output)
	})

	t.Run("listview_struct_field_roundtrip", func(t *testing.T) {
		type Row struct {
			Name string   `arrow:"name"`
			Tags []string `arrow:"tags,view"`
		}
		rows := []Row{
			{"alice", []string{"admin", "user"}},
			{"bob", []string{"guest"}},
		}
		arr, err := FromSlice(rows, nil)
		require.NoError(t, err)
		defer arr.Release()

		sa := arr.(*array.Struct)
		require.Equal(t, arrow.LIST_VIEW, sa.Field(1).DataType().ID())

		output, err := ToSlice[Row](arr)
		require.NoError(t, err)
		assert.Equal(t, rows, output)
	})

	t.Run("duration_struct_field_roundtrip", func(t *testing.T) {
		type Row struct {
			Name    string        `arrow:"name"`
			Elapsed time.Duration `arrow:"elapsed"`
		}
		rows := []Row{
			{"fast", 100 * time.Millisecond},
			{"slow", 5 * time.Second},
		}
		arr, err := FromSlice(rows, nil)
		require.NoError(t, err)
		defer arr.Release()

		sa := arr.(*array.Struct)
		assert.Equal(t, arrow.DURATION, sa.Field(1).DataType().ID())

		output, err := ToSlice[Row](arr)
		require.NoError(t, err)
		assert.Equal(t, rows, output)
	})
}

func BenchmarkReflectFromGoSlice(b *testing.B) {
	mem := testMem()
	rows := make([]integLargeRow, 1000)
	for i := range rows {
		rows[i] = integLargeRow{X: int32(i), Y: float64(i) * 1.5}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arr, err := FromSlice(rows, mem)
		if err != nil {
			b.Fatal(err)
		}
		arr.Release()
	}
}

func BenchmarkReflectToGoSlice(b *testing.B) {
	mem := testMem()
	rows := make([]integLargeRow, 1000)
	for i := range rows {
		rows[i] = integLargeRow{X: int32(i), Y: float64(i) * 1.5}
	}

	arr, err := FromSlice(rows, mem)
	if err != nil {
		b.Fatal(err)
	}
	defer arr.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := ToSlice[integLargeRow](arr)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
