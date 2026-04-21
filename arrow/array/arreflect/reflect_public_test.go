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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMem() memory.Allocator { return memory.NewGoAllocator() }

func fieldValueByTag(v reflect.Value, tag string) reflect.Value {
	for i := 0; i < v.NumField(); i++ {
		if v.Type().Field(i).Tag.Get("arrow") == tag {
			return v.Field(i)
		}
	}
	return reflect.Value{}
}

func TestToGo(t *testing.T) {
	mem := testMem()

	t.Run("int32 element 0", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendValues([]int32{10, 20, 30}, nil)
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := At[int32](arr, 0)
		require.NoError(t, err)
		assert.Equal(t, int32(10), got)
	})

	t.Run("string element 1", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues([]string{"hello", "world"}, nil)
		arr := b.NewStringArray()
		defer arr.Release()

		got, err := At[string](arr, 1)
		require.NoError(t, err)
		assert.Equal(t, "world", got)
	})

	t.Run("struct element 0", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int32
		}
		vals := []Person{{"Alice", 30}, {"Bob", 25}}
		arr, err := FromSlice(vals, mem)
		require.NoError(t, err)
		defer arr.Release()

		got, err := At[Person](arr, 0)
		require.NoError(t, err)
		assert.Equal(t, "Alice", got.Name)
		assert.Equal(t, int32(30), got.Age)
	})

	t.Run("null element to *int32 is nil", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendNull()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := At[*int32](arr, 0)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("null element to int32 is zero", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendNull()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := At[int32](arr, 0)
		require.NoError(t, err)
		assert.Equal(t, int32(0), got)
	})
}

func TestToGoSlice(t *testing.T) {
	mem := testMem()

	t.Run("[]int32", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.AppendValues([]int32{1, 2, 3}, nil)
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := ToSlice[int32](arr)
		require.NoError(t, err)
		want := []int32{1, 2, 3}
		require.Len(t, got, len(want))
		for i, v := range want {
			assert.Equal(t, v, got[i], "index %d", i)
		}
	})

	t.Run("[]string", func(t *testing.T) {
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues([]string{"foo", "bar", "baz"}, nil)
		arr := b.NewStringArray()
		defer arr.Release()

		got, err := ToSlice[string](arr)
		require.NoError(t, err)
		want := []string{"foo", "bar", "baz"}
		require.Len(t, got, len(want))
		for i, v := range want {
			assert.Equal(t, v, got[i], "index %d", i)
		}
	})

	t.Run("[]struct{Name string}", func(t *testing.T) {
		type Row struct {
			Name string
		}
		vals := []Row{{"Alice"}, {"Bob"}, {"Charlie"}}
		arr, err := FromSlice(vals, mem)
		require.NoError(t, err)
		defer arr.Release()

		got, err := ToSlice[Row](arr)
		require.NoError(t, err)
		require.Len(t, got, len(vals))
		for i, want := range vals {
			assert.Equal(t, want.Name, got[i].Name, "index %d", i)
		}
	})

	t.Run("empty array gives empty slice", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		arr := b.NewInt32Array()
		defer arr.Release()

		got, err := ToSlice[int32](arr)
		require.NoError(t, err)
		assert.NotNil(t, got, "expected non-nil empty slice, got nil")
		assert.Len(t, got, 0)
	})
}

func TestFromGoSlice(t *testing.T) {
	mem := testMem()

	t.Run("[]int32", func(t *testing.T) {
		arr, err := FromSlice([]int32{1, 2, 3}, mem)
		require.NoError(t, err)
		defer arr.Release()

		require.Equal(t, 3, arr.Len())
		typed := arr.(*array.Int32)
		for i, want := range []int32{1, 2, 3} {
			assert.Equal(t, want, typed.Value(i), "index %d", i)
		}
	})

	t.Run("[]string", func(t *testing.T) {
		arr, err := FromSlice([]string{"a", "b"}, mem)
		require.NoError(t, err)
		defer arr.Release()

		require.Equal(t, 2, arr.Len())
		typed := arr.(*array.String)
		assert.Equal(t, "a", typed.Value(0))
		assert.Equal(t, "b", typed.Value(1))
	})

	t.Run("[]struct{Name string; Score float64}", func(t *testing.T) {
		type Row struct {
			Name  string
			Score float64
		}
		vals := []Row{{"Alice", 9.5}, {"Bob", 8.0}}
		arr, err := FromSlice(vals, mem)
		require.NoError(t, err)
		defer arr.Release()

		require.Equal(t, 2, arr.Len())
		got, err := ToSlice[Row](arr)
		require.NoError(t, err)
		for i, want := range vals {
			assert.Equal(t, want.Name, got[i].Name, "index %d Name", i)
			assert.Equal(t, want.Score, got[i].Score, "index %d Score", i)
		}
	})

	t.Run("[]*int32 with nil produces null", func(t *testing.T) {
		v := int32(42)
		arr, err := FromSlice([]*int32{&v, nil}, mem)
		require.NoError(t, err)
		defer arr.Release()

		require.Equal(t, 2, arr.Len())
		assert.True(t, arr.IsNull(1), "expected index 1 to be null")
		typed := arr.(*array.Int32)
		assert.Equal(t, int32(42), typed.Value(0))
	})

	t.Run("empty []int32 gives length-0 array", func(t *testing.T) {
		arr, err := FromSlice([]int32{}, mem)
		require.NoError(t, err)
		defer arr.Release()

		assert.Equal(t, 0, arr.Len())
	})

	t.Run("empty slice with WithListView", func(t *testing.T) {
		arr, err := FromSlice([][]int32{}, mem, WithListView())
		require.NoError(t, err)
		defer arr.Release()

		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
		assert.Equal(t, arrow.INT32, arr.DataType().(*arrow.ListViewType).Elem().ID())
	})

	t.Run("empty slice with WithREE", func(t *testing.T) {
		arr, err := FromSlice([]int32{}, mem, WithREE())
		require.NoError(t, err)
		defer arr.Release()

		assert.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID())
	})

	t.Run("WithTemporal invalid value returns error", func(t *testing.T) {
		_, err := FromSlice([]time.Time{}, mem, WithTemporal("invalid"))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("WithTemporal on non-time type returns error", func(t *testing.T) {
		_, err := FromSlice([]string{}, mem, WithTemporal("date32"))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("WithTemporal timestamp on non-time type returns error", func(t *testing.T) {
		_, err := FromSlice([]string{}, mem, WithTemporal("timestamp"))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("struct field with malformed decimal tag returns error", func(t *testing.T) {
		type BadDecimal struct {
			Amount decimal128.Num `arrow:",decimal(18,two)"`
		}
		_, err := FromSlice([]BadDecimal{}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("conflicting options return error", func(t *testing.T) {
		cases := []struct {
			name string
			opts []Option
		}{
			{"WithDict+WithREE", []Option{WithDict(), WithREE()}},
			{"WithDict+WithListView", []Option{WithDict(), WithListView()}},
			{"WithREE+WithListView", []Option{WithREE(), WithListView()}},
			{"all three", []Option{WithDict(), WithREE(), WithListView()}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := FromSlice([]int32{1, 2, 3}, mem, tc.opts...)
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrUnsupportedType)
			})
		}
	})
}

func TestRecordToSlice(t *testing.T) {
	mem := testMem()

	type Row struct {
		Name  string
		Score float64
	}

	buildRecord := func(rows []Row) arrow.RecordBatch {
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
		return array.NewRecordBatch(schema, []arrow.Array{nameArr, scoreArr}, int64(len(rows)))
	}

	t.Run("basic 3-row record", func(t *testing.T) {
		want := []Row{{"Alice", 9.5}, {"Bob", 8.0}, {"Carol", 7.5}}
		rec := buildRecord(want)
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		require.NoError(t, err)
		require.Len(t, got, len(want))
		for i, w := range want {
			assert.Equal(t, w.Name, got[i].Name, "index %d Name", i)
			assert.Equal(t, w.Score, got[i].Score, "index %d Score", i)
		}
	})

	t.Run("empty record gives empty slice", func(t *testing.T) {
		rec := buildRecord(nil)
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})
}

func TestRecordFromSlice(t *testing.T) {
	mem := testMem()

	type Row struct {
		Name  string
		Score float64
	}

	t.Run("struct slice produces correct schema and values", func(t *testing.T) {
		vals := []Row{{"Alice", 9.5}, {"Bob", 8.0}}
		rec, err := RecordFromSlice(vals, mem)
		require.NoError(t, err)
		defer rec.Release()

		require.Equal(t, int64(2), rec.NumCols())
		require.Equal(t, int64(2), rec.NumRows())
		assert.Equal(t, "Name", rec.Schema().Field(0).Name)
		assert.Equal(t, "Score", rec.Schema().Field(1).Name)
		nameCol := rec.Column(0).(*array.String)
		assert.Equal(t, "Alice", nameCol.Value(0))
		assert.Equal(t, "Bob", nameCol.Value(1))
		scoreCol := rec.Column(1).(*array.Float64)
		assert.Equal(t, 9.5, scoreCol.Value(0))
		assert.Equal(t, 8.0, scoreCol.Value(1))
	})

	t.Run("non-struct T returns error", func(t *testing.T) {
		_, err := RecordFromSlice([]int32{1, 2, 3}, mem)
		require.Error(t, err)
	})

	t.Run("round-trip RecordFromSlice then RecordToSlice", func(t *testing.T) {
		want := []Row{{"Alice", 9.5}, {"Bob", 8.0}, {"Carol", 7.5}}
		rec, err := RecordFromSlice(want, mem)
		require.NoError(t, err)
		defer rec.Release()

		got, err := RecordToSlice[Row](rec)
		require.NoError(t, err)
		require.Len(t, got, len(want))
		for i, w := range want {
			assert.Equal(t, w.Name, got[i].Name, "index %d Name", i)
			assert.Equal(t, w.Score, got[i].Score, "index %d Score", i)
		}
	})
}

func TestAtAny(t *testing.T) {
	mem := testMem()
	b := array.NewInt32Builder(mem)
	defer b.Release()
	b.Append(42)
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()

	got, err := AtAny(arr, 0)
	require.NoError(t, err, "AtAny(0)")
	v, ok := got.(int32)
	assert.True(t, ok, "AtAny(0): expected int32 type, got %T", got)
	assert.Equal(t, int32(42), v, "AtAny(0) value")

	got, err = AtAny(arr, 1)
	require.NoError(t, err, "AtAny(1)")
	v, ok = got.(int32)
	assert.True(t, ok, "AtAny(1): expected int32 type, got %T", got)
	assert.Equal(t, int32(0), v, "AtAny(1) value")
}

func TestAtAnyErrors(t *testing.T) {
	arr := array.NewNull(1)
	defer arr.Release()

	_, err := AtAny(arr, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

func TestToAnySlice(t *testing.T) {
	mem := testMem()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append("hello")
	b.Append("world")
	arr := b.NewArray()
	defer arr.Release()

	got, err := ToAnySlice(arr)
	require.NoError(t, err, "ToAnySlice")
	require.Len(t, got, 2)
	assert.Equal(t, "hello", got[0].(string))
	assert.Equal(t, "world", got[1].(string))
}

func TestErrSentinels(t *testing.T) {
	mem := testMem()

	t.Run("ErrTypeMismatch via setValue wrong kind", func(t *testing.T) {
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.Append(42)
		arr := b.NewArray()
		defer arr.Release()

		var got string
		v := reflect.ValueOf(&got).Elem()
		err := setValue(v, arr, 0)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTypeMismatch)
	})

	t.Run("ErrUnsupportedType via InferGoType", func(t *testing.T) {
		_, err := InferGoType(arrow.Null)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("ErrTypeMismatch propagates through struct field context wrapper", func(t *testing.T) {
		st := arrow.StructOf(arrow.Field{Name: "name", Type: arrow.BinaryTypes.String})
		sb := array.NewStructBuilder(mem, st)
		defer sb.Release()
		sb.Append(true)
		sb.FieldBuilder(0).(*array.StringBuilder).Append("hello")
		arr := sb.NewArray()
		defer arr.Release()

		type wrongType struct {
			Name int32 `arrow:"name"`
		}
		_, err := At[wrongType](arr, 0)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTypeMismatch)
	})
}

func TestRecordAt(t *testing.T) {
	mem := testMem()
	type Row struct {
		Name  string  `arrow:"name"`
		Score float64 `arrow:"score"`
	}
	rows := []Row{{"alice", 9.5}, {"bob", 7.0}}
	rec, err := RecordFromSlice(rows, mem)
	require.NoError(t, err, "RecordFromSlice")
	defer rec.Release()

	got, err := RecordAt[Row](rec, 0)
	require.NoError(t, err, "RecordAt(0)")
	assert.Equal(t, rows[0], got)

	got, err = RecordAt[Row](rec, 1)
	require.NoError(t, err, "RecordAt(1)")
	assert.Equal(t, rows[1], got)
}

func TestRecordAtAny(t *testing.T) {
	mem := testMem()
	type Row struct {
		Name  string  `arrow:"name"`
		Score float64 `arrow:"score"`
	}
	rows := []Row{{"alice", 9.5}, {"bob", 7.0}}
	rec, err := RecordFromSlice(rows, mem)
	require.NoError(t, err, "RecordFromSlice")
	defer rec.Release()

	got, err := RecordAtAny(rec, 0)
	require.NoError(t, err, "RecordAtAny(0)")
	v := reflect.ValueOf(got)
	require.Equal(t, reflect.Struct, v.Kind())
	nameField := fieldValueByTag(v, "name")
	scoreField := fieldValueByTag(v, "score")
	require.True(t, nameField.IsValid(), "name field not found")
	require.True(t, scoreField.IsValid(), "score field not found")
	assert.Equal(t, "alice", nameField.String())
	assert.Equal(t, 9.5, scoreField.Float())
}

func TestRecordToAnySlice(t *testing.T) {
	mem := testMem()
	type Row struct {
		Name  string  `arrow:"name"`
		Score float64 `arrow:"score"`
	}
	rows := []Row{{"alice", 9.5}, {"bob", 7.0}}
	rec, err := RecordFromSlice(rows, mem)
	require.NoError(t, err, "RecordFromSlice")
	defer rec.Release()

	got, err := RecordToAnySlice(rec)
	require.NoError(t, err, "RecordToAnySlice")
	require.Len(t, got, 2)
	for i, row := range got {
		v := reflect.ValueOf(row)
		require.Equal(t, reflect.Struct, v.Kind(), "row %d", i)
		nameField := fieldValueByTag(v, "name")
		assert.Equal(t, rows[i].Name, nameField.String(), "row %d name", i)
	}
}

func TestAtAnyComposite(t *testing.T) {
	mem := testMem()

	t.Run("struct", func(t *testing.T) {
		st := arrow.StructOf(
			arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		)
		sb := array.NewStructBuilder(mem, st)
		defer sb.Release()
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(99)
		sb.FieldBuilder(1).(*array.StringBuilder).Append("alice")
		arr := sb.NewArray()
		defer arr.Release()

		got, err := AtAny(arr, 0)
		require.NoError(t, err, "AtAny")

		v := reflect.ValueOf(got)
		require.Equal(t, reflect.Struct, v.Kind())

		idField := fieldValueByTag(v, "id")
		nameField := fieldValueByTag(v, "name")
		require.True(t, idField.IsValid(), "id field not found")
		require.True(t, nameField.IsValid(), "name field not found")
		assert.Equal(t, int64(99), idField.Int())
		assert.Equal(t, "alice", nameField.String())
	})

	t.Run("list", func(t *testing.T) {
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
		defer lb.Release()
		lb.Append(true)
		lb.ValueBuilder().(*array.Int32Builder).Append(1)
		lb.ValueBuilder().(*array.Int32Builder).Append(2)
		lb.ValueBuilder().(*array.Int32Builder).Append(3)
		arr := lb.NewArray()
		defer arr.Release()

		got, err := AtAny(arr, 0)
		require.NoError(t, err, "AtAny")

		v := reflect.ValueOf(got)
		require.Equal(t, reflect.Slice, v.Kind())
		require.Equal(t, 3, v.Len())
		assert.Equal(t, int64(1), v.Index(0).Int())
		assert.Equal(t, int64(3), v.Index(2).Int())
	})

	t.Run("map", func(t *testing.T) {
		mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
		defer mb.Release()
		mb.Append(true)
		mb.KeyBuilder().(*array.StringBuilder).Append("x")
		mb.ItemBuilder().(*array.Int32Builder).Append(7)
		arr := mb.NewArray()
		defer arr.Release()

		got, err := AtAny(arr, 0)
		require.NoError(t, err, "AtAny")

		v := reflect.ValueOf(got)
		require.Equal(t, reflect.Map, v.Kind())
		key := reflect.ValueOf("x")
		val := v.MapIndex(key)
		require.True(t, val.IsValid(), "key 'x' not found in map")
		assert.Equal(t, int64(7), val.Int())
	})
}

func TestToAnySliceStructArray(t *testing.T) {
	mem := testMem()
	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	)
	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()

	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("alpha")
	sb.FieldBuilder(2).(*array.Float64Builder).Append(9.5)

	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(2)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("beta")
	sb.FieldBuilder(2).(*array.Float64Builder).Append(3.14)

	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(3)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("gamma")
	sb.FieldBuilder(2).(*array.Float64Builder).AppendNull()

	arr := sb.NewArray()
	defer arr.Release()

	got, err := ToAnySlice(arr)
	require.NoError(t, err, "ToAnySlice")
	require.Len(t, got, 3)

	type expected struct {
		id    int64
		label string
		score float64
	}
	want := []expected{
		{1, "alpha", 9.5},
		{2, "beta", 3.14},
		{3, "gamma", 0},
	}

	for i, row := range got {
		v := reflect.ValueOf(row)
		require.Equal(t, reflect.Struct, v.Kind(), "row %d", i)
		require.Equal(t, 3, v.NumField(), "row %d", i)

		id := fieldValueByTag(v, "id")
		label := fieldValueByTag(v, "label")
		score := fieldValueByTag(v, "score")
		require.True(t, id.IsValid(), "row %d: id field not found", i)
		require.True(t, label.IsValid(), "row %d: label field not found", i)
		require.True(t, score.IsValid(), "row %d: score field not found", i)
		assert.Equal(t, want[i].id, id.Int(), "row %d id", i)
		assert.Equal(t, want[i].label, label.String(), "row %d label", i)
		if score.Kind() == reflect.Ptr {
			if i == 2 {
				assert.True(t, score.IsNil(), "row 2 score: want nil")
			} else {
				if assert.False(t, score.IsNil(), "row %d score: unexpected nil", i) {
					assert.Equal(t, want[i].score, score.Elem().Float(), "row %d score", i)
				}
			}
		} else {
			assert.Equal(t, want[i].score, score.Float(), "row %d score", i)
		}
	}
}
