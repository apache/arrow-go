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
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferPrimitiveArrowType(t *testing.T) {
	cases := []struct {
		name    string
		goType  reflect.Type
		wantID  arrow.Type
		wantErr bool
	}{
		{"int8", reflect.TypeOf(int8(0)), arrow.INT8, false},
		{"int16", reflect.TypeOf(int16(0)), arrow.INT16, false},
		{"int32", reflect.TypeOf(int32(0)), arrow.INT32, false},
		{"int64", reflect.TypeOf(int64(0)), arrow.INT64, false},
		{"int", reflect.TypeOf(int(0)), arrow.INT64, false},
		{"uint8", reflect.TypeOf(uint8(0)), arrow.UINT8, false},
		{"uint16", reflect.TypeOf(uint16(0)), arrow.UINT16, false},
		{"uint32", reflect.TypeOf(uint32(0)), arrow.UINT32, false},
		{"uint64", reflect.TypeOf(uint64(0)), arrow.UINT64, false},
		{"uint", reflect.TypeOf(uint(0)), arrow.UINT64, false},
		{"float32", reflect.TypeOf(float32(0)), arrow.FLOAT32, false},
		{"float64", reflect.TypeOf(float64(0)), arrow.FLOAT64, false},
		{"bool", reflect.TypeOf(false), arrow.BOOL, false},
		{"string", reflect.TypeOf(""), arrow.STRING, false},
		{"[]byte", reflect.TypeOf([]byte{}), arrow.BINARY, false},
		{"time.Time", reflect.TypeOf(time.Time{}), arrow.TIMESTAMP, false},
		{"time.Duration", reflect.TypeOf(time.Duration(0)), arrow.DURATION, false},
		{"decimal128.Num", reflect.TypeOf(decimal128.Num{}), arrow.DECIMAL128, false},
		{"decimal256.Num", reflect.TypeOf(decimal256.Num{}), arrow.DECIMAL256, false},
		{"decimal.Decimal32", reflect.TypeOf(decimal.Decimal32(0)), arrow.DECIMAL32, false},
		{"decimal.Decimal64", reflect.TypeOf(decimal.Decimal64(0)), arrow.DECIMAL64, false},
		{"*int32 pointer transparent", reflect.TypeOf((*int32)(nil)), arrow.INT32, false},
		{"chan int unsupported", reflect.TypeOf(make(chan int)), 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := inferPrimitiveArrowType(tc.goType)
			if tc.wantErr {
				require.Error(t, err, "expected error, got nil (type: %v)", got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantID, got.ID())
		})
	}
}

func TestInferArrowType(t *testing.T) {
	t.Run("[]int32 is LIST", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf([]int32{}))
		require.NoError(t, err)
		assert.Equal(t, arrow.LIST, dt.ID())
	})

	t.Run("[3]float64 is FIXED_SIZE_LIST size 3", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf([3]float64{}))
		require.NoError(t, err)
		assert.Equal(t, arrow.FIXED_SIZE_LIST, dt.ID())
		fsl := dt.(*arrow.FixedSizeListType)
		assert.Equal(t, int32(3), fsl.Len())
	})

	t.Run("map[string]int64 is MAP", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf(map[string]int64{}))
		require.NoError(t, err)
		assert.Equal(t, arrow.MAP, dt.ID())
	})

	t.Run("struct with 2 fields is STRUCT", func(t *testing.T) {
		type S struct {
			Name string
			Age  int32
		}
		dt, err := inferArrowType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		assert.Equal(t, arrow.STRUCT, dt.ID())
		st := dt.(*arrow.StructType)
		assert.Equal(t, 2, st.NumFields())
	})

	t.Run("[]map[string]struct{Score float64} nested", func(t *testing.T) {
		type Inner struct {
			Score float64
		}
		dt, err := inferArrowType(reflect.TypeOf([]map[string]Inner{}))
		require.NoError(t, err)
		assert.Equal(t, arrow.LIST, dt.ID())
		lt := dt.(*arrow.ListType)
		assert.Equal(t, arrow.MAP, lt.Elem().ID())
		mt := lt.Elem().(*arrow.MapType)
		assert.Equal(t, arrow.STRUCT, mt.ItemField().Type.ID())
	})

	t.Run("*[]string pointer to slice is LIST", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf((*[]string)(nil)))
		require.NoError(t, err)
		assert.Equal(t, arrow.LIST, dt.ID())
	})
}

func TestInferStructType(t *testing.T) {
	t.Run("simple struct field names and types", func(t *testing.T) {
		type S struct {
			Name  string
			Score float32
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		require.Equal(t, 2, st.NumFields())
		assert.Equal(t, "Name", st.Field(0).Name)
		assert.Equal(t, arrow.STRING, st.Field(0).Type.ID())
		assert.Equal(t, "Score", st.Field(1).Name)
		assert.Equal(t, arrow.FLOAT32, st.Field(1).Type.ID())
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type S struct {
			ID    int32
			Label *string
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		assert.False(t, st.Field(0).Nullable, "ID should not be nullable")
		assert.True(t, st.Field(1).Nullable, "Label should be nullable")
	})

	t.Run("arrow:\"-\" tagged field is excluded", func(t *testing.T) {
		type S struct {
			Keep   string
			Hidden int32 `arrow:"-"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		assert.Equal(t, 1, st.NumFields())
		assert.Equal(t, "Keep", st.Field(0).Name)
	})

	t.Run("arrow custom name tag", func(t *testing.T) {
		type S struct {
			GoName int64 `arrow:"custom_name"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		assert.Equal(t, "custom_name", st.Field(0).Name)
	})

	t.Run("decimal128 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal128.Num `arrow:",decimal(18,2)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		dt := st.Field(0).Type
		require.Equal(t, arrow.DECIMAL128, dt.ID())
		d128 := dt.(*arrow.Decimal128Type)
		assert.Equal(t, int32(18), d128.Precision)
		assert.Equal(t, int32(2), d128.Scale)
	})

	t.Run("decimal256 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal256.Num `arrow:",decimal(40,5)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		dt := st.Field(0).Type
		require.Equal(t, arrow.DECIMAL256, dt.ID())
		d256 := dt.(*arrow.Decimal256Type)
		assert.Equal(t, int32(40), d256.Precision)
		assert.Equal(t, int32(5), d256.Scale)
	})

	t.Run("decimal32 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal.Decimal32 `arrow:",decimal(9,2)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		dt := st.Field(0).Type
		require.Equal(t, arrow.DECIMAL32, dt.ID())
		d32 := dt.(*arrow.Decimal32Type)
		assert.Equal(t, int32(9), d32.Precision)
		assert.Equal(t, int32(2), d32.Scale)
	})

	t.Run("non-struct returns error", func(t *testing.T) {
		_, err := inferStructType(reflect.TypeOf(42))
		assert.Error(t, err, "expected error for non-struct, got nil")
	})

	t.Run("time.Time with date32 tag maps to DATE32", func(t *testing.T) {
		type S struct {
			Ts time.Time `arrow:",date32"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		require.NoError(t, err)
		dt := st.Field(0).Type
		assert.Equal(t, arrow.DATE32, dt.ID())
	})
}

func TestInferArrowSchema(t *testing.T) {
	t.Run("simple struct mixed fields", func(t *testing.T) {
		type S struct {
			Name  string
			Age   int32
			Score float64
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		require.Equal(t, 3, schema.NumFields())
		assert.Equal(t, "Name", schema.Field(0).Name)
		assert.Equal(t, arrow.STRING, schema.Field(0).Type.ID())
		assert.Equal(t, "Age", schema.Field(1).Name)
		assert.Equal(t, arrow.INT32, schema.Field(1).Type.ID())
		assert.Equal(t, "Score", schema.Field(2).Name)
		assert.Equal(t, arrow.FLOAT64, schema.Field(2).Type.ID())
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type S struct {
			ID    int32
			Label *string
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		assert.False(t, schema.Field(0).Nullable, "ID should not be nullable")
		assert.True(t, schema.Field(1).Nullable, "Label should be nullable")
	})

	t.Run("arrow:\"-\" tag excludes field", func(t *testing.T) {
		type S struct {
			Keep   string
			Hidden int32 `arrow:"-"`
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		assert.Equal(t, 1, schema.NumFields())
		assert.Equal(t, "Keep", schema.Field(0).Name)
	})

	t.Run("arrow custom name tag", func(t *testing.T) {
		type S struct {
			GoName int64 `arrow:"custom_name"`
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		assert.Equal(t, "custom_name", schema.Field(0).Name)
	})

	t.Run("non-struct type returns error", func(t *testing.T) {
		_, err := InferSchema[int]()
		assert.Error(t, err, "expected error for non-struct, got nil")
	})
}

func TestInferArrowTypePublic(t *testing.T) {
	t.Run("int32 is INT32", func(t *testing.T) {
		dt, err := InferType[int32]()
		require.NoError(t, err)
		assert.Equal(t, arrow.INT32, dt.ID())
	})

	t.Run("[]string is LIST", func(t *testing.T) {
		dt, err := InferType[[]string]()
		require.NoError(t, err)
		assert.Equal(t, arrow.LIST, dt.ID())
	})

	t.Run("map[string]float64 is MAP", func(t *testing.T) {
		dt, err := InferType[map[string]float64]()
		require.NoError(t, err)
		assert.Equal(t, arrow.MAP, dt.ID())
	})

	t.Run("struct{X int32} is STRUCT", func(t *testing.T) {
		type S struct{ X int32 }
		dt, err := InferType[S]()
		require.NoError(t, err)
		assert.Equal(t, arrow.STRUCT, dt.ID())
	})
}

func TestInferArrowSchemaStructFieldEncoding(t *testing.T) {
	t.Run("dict-tagged string field becomes DICTIONARY", func(t *testing.T) {
		type S struct {
			Name string `arrow:"name,dict"`
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		f, ok := schema.FieldsByName("name")
		require.True(t, ok && len(f) > 0, "field 'name' not found in schema")
		assert.Equal(t, arrow.DICTIONARY, f[0].Type.ID())
	})

	t.Run("view-tagged []string field becomes LIST_VIEW", func(t *testing.T) {
		type S struct {
			Tags []string `arrow:"tags,view"`
		}
		schema, err := InferSchema[S]()
		require.NoError(t, err)
		f, ok := schema.FieldsByName("tags")
		require.True(t, ok && len(f) > 0, "field 'tags' not found in schema")
		assert.Equal(t, arrow.LIST_VIEW, f[0].Type.ID())
	})

	t.Run("ree-tagged field on struct is unsupported", func(t *testing.T) {
		type REERow struct {
			Val string `arrow:"val,ree"`
		}
		_, err := InferSchema[REERow]()
		require.Error(t, err, "expected error for ree tag on struct field, got nil")
		assert.True(t, strings.Contains(err.Error(), "ree tag on struct field"), "unexpected error message: %v", err)
	})
}

func TestInferGoType(t *testing.T) {
	primitives := []struct {
		dt   arrow.DataType
		want reflect.Type
	}{
		{arrow.PrimitiveTypes.Int32, reflect.TypeOf(int32(0))},
		{arrow.PrimitiveTypes.Float64, reflect.TypeOf(float64(0))},
		{arrow.FixedWidthTypes.Boolean, reflect.TypeOf(bool(false))},
		{arrow.BinaryTypes.String, reflect.TypeOf("")},
		{arrow.BinaryTypes.Binary, reflect.TypeOf([]byte{})},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, reflect.TypeOf(time.Time{})},
		{&arrow.DurationType{Unit: arrow.Nanosecond}, reflect.TypeOf(time.Duration(0))},
	}
	for _, tt := range primitives {
		got, err := InferGoType(tt.dt)
		if assert.NoError(t, err, "InferGoType(%v)", tt.dt) {
			assert.Equal(t, tt.want, got, "InferGoType(%v)", tt.dt)
		}
	}

	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	structType, err := InferGoType(st)
	require.NoError(t, err, "struct")
	require.Equal(t, reflect.Struct, structType.Kind())
	require.Equal(t, 2, structType.NumField())
	assert.Equal(t, reflect.Ptr, structType.Field(1).Type.Kind(), "nullable field should be pointer")
	assert.Equal(t, reflect.String, structType.Field(1).Type.Elem().Kind(), "nullable field should be *string")

	listType, err := InferGoType(arrow.ListOf(arrow.PrimitiveTypes.Int32))
	require.NoError(t, err, "list")
	require.Equal(t, reflect.Slice, listType.Kind())
	assert.Equal(t, reflect.TypeOf(int32(0)), listType.Elem(), "list elem wrong")

	fslType, err := InferGoType(arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32))
	require.NoError(t, err, "fsl")
	require.Equal(t, reflect.Array, fslType.Kind())
	assert.Equal(t, 3, fslType.Len(), "array len want 3")

	_, err = InferGoType(arrow.Null)
	require.Error(t, err, "expected error for unsupported type")
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

func TestInferGoTypeMapNonComparableKey(t *testing.T) {
	t.Run("MAP with non-comparable key returns error", func(t *testing.T) {
		dt := arrow.MapOf(arrow.ListOf(arrow.PrimitiveTypes.Int32), arrow.BinaryTypes.String)
		_, err := InferGoType(dt)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestInferGoTypeStructDuplicateExportedNames(t *testing.T) {
	t.Run("STRUCT with colliding exported names returns error", func(t *testing.T) {
		st := arrow.StructOf(
			arrow.Field{Name: "foo", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "Foo", Type: arrow.PrimitiveTypes.Int64},
		)
		_, err := InferGoType(st)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}

func TestInferGoTypeStructInvalidIdentifier(t *testing.T) {
	cases := []struct {
		name      string
		fieldName string
	}{
		{"hyphenated", "my-field"},
		{"space", "a b"},
		{"dot", "first.name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := arrow.StructOf(arrow.Field{Name: tc.fieldName, Type: arrow.PrimitiveTypes.Int32})
			_, err := InferGoType(st)
			assert.ErrorIs(t, err, ErrUnsupportedType)
		})
	}

	t.Run("non-letter prefix mapped", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			expected string
		}{
			{"_id", "X_id"},
			{"1st", "X1st"},
		} {
			st := arrow.StructOf(arrow.Field{Name: tc.name, Type: arrow.PrimitiveTypes.Int32})
			goType, err := InferGoType(st)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, goType.Field(0).Name)
		}
	})
}

func TestInferGoTypeAllPrimitives(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		want reflect.Type
	}{
		{"int8", arrow.PrimitiveTypes.Int8, reflect.TypeOf(int8(0))},
		{"int16", arrow.PrimitiveTypes.Int16, reflect.TypeOf(int16(0))},
		{"int64", arrow.PrimitiveTypes.Int64, reflect.TypeOf(int64(0))},
		{"uint8", arrow.PrimitiveTypes.Uint8, reflect.TypeOf(uint8(0))},
		{"uint16", arrow.PrimitiveTypes.Uint16, reflect.TypeOf(uint16(0))},
		{"uint32", arrow.PrimitiveTypes.Uint32, reflect.TypeOf(uint32(0))},
		{"uint64", arrow.PrimitiveTypes.Uint64, reflect.TypeOf(uint64(0))},
		{"float32", arrow.PrimitiveTypes.Float32, reflect.TypeOf(float32(0))},
		{"large_string", arrow.BinaryTypes.LargeString, reflect.TypeOf("")},
		{"large_binary", arrow.BinaryTypes.LargeBinary, reflect.TypeOf([]byte{})},
		{"date32", arrow.FixedWidthTypes.Date32, reflect.TypeOf(time.Time{})},
		{"date64", arrow.FixedWidthTypes.Date64, reflect.TypeOf(time.Time{})},
		{"time32_ms", &arrow.Time32Type{Unit: arrow.Millisecond}, reflect.TypeOf(time.Time{})},
		{"time64_ns", &arrow.Time64Type{Unit: arrow.Nanosecond}, reflect.TypeOf(time.Time{})},
		{"decimal32", &arrow.Decimal32Type{Precision: 9, Scale: 2}, reflect.TypeOf(decimal.Decimal32(0))},
		{"decimal64", &arrow.Decimal64Type{Precision: 18, Scale: 3}, reflect.TypeOf(decimal.Decimal64(0))},
		{"decimal128", &arrow.Decimal128Type{Precision: 10, Scale: 2}, reflect.TypeOf(decimal128.Num{})},
		{"decimal256", &arrow.Decimal256Type{Precision: 20, Scale: 4}, reflect.TypeOf(decimal256.Num{})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InferGoType(tc.dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestInferGoTypeCompositeTypes(t *testing.T) {
	t.Run("large_list", func(t *testing.T) {
		got, err := InferGoType(arrow.LargeListOf(arrow.PrimitiveTypes.Int64))
		require.NoError(t, err)
		assert.Equal(t, reflect.Slice, got.Kind())
		assert.Equal(t, reflect.Int64, got.Elem().Kind())
	})

	t.Run("list_view", func(t *testing.T) {
		got, err := InferGoType(arrow.ListViewOf(arrow.PrimitiveTypes.Int32))
		require.NoError(t, err)
		assert.Equal(t, reflect.Slice, got.Kind())
		assert.Equal(t, reflect.Int32, got.Elem().Kind())
	})

	t.Run("large_list_view", func(t *testing.T) {
		got, err := InferGoType(arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32))
		require.NoError(t, err)
		assert.Equal(t, reflect.Slice, got.Kind())
	})

	t.Run("list with unsupported element returns error", func(t *testing.T) {
		_, err := InferGoType(arrow.ListOf(arrow.Null))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("fixed size list with unsupported element returns error", func(t *testing.T) {
		_, err := InferGoType(arrow.FixedSizeListOf(3, arrow.Null))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("map with unsupported key returns error", func(t *testing.T) {
		_, err := InferGoType(arrow.MapOf(arrow.Null, arrow.BinaryTypes.String))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("map with unsupported value returns error", func(t *testing.T) {
		_, err := InferGoType(arrow.MapOf(arrow.BinaryTypes.String, arrow.Null))
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("map with comparable key builds map type", func(t *testing.T) {
		got, err := InferGoType(arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32))
		require.NoError(t, err)
		assert.Equal(t, reflect.Map, got.Kind())
		assert.Equal(t, reflect.String, got.Key().Kind())
		assert.Equal(t, reflect.Int32, got.Elem().Kind())
	})

	t.Run("dictionary unwraps to value type", func(t *testing.T) {
		dt := &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}
		got, err := InferGoType(dt)
		require.NoError(t, err)
		assert.Equal(t, reflect.String, got.Kind())
	})

	t.Run("run end encoded unwraps to encoded type", func(t *testing.T) {
		dt := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64)
		got, err := InferGoType(dt)
		require.NoError(t, err)
		assert.Equal(t, reflect.Int64, got.Kind())
	})
}

func TestApplyTemporalOptsAllBranches(t *testing.T) {
	timeType := reflect.TypeOf(time.Time{})
	base := arrow.FixedWidthTypes.Timestamp_ns

	t.Run("non-time type returns dt unchanged", func(t *testing.T) {
		got := applyTemporalOpts(base, reflect.TypeOf(int32(0)), tagOpts{Temporal: "date32"})
		assert.Equal(t, base, got)
	})

	t.Run("empty temporal returns dt unchanged", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: ""})
		assert.Equal(t, base, got)
	})

	t.Run("timestamp returns dt unchanged", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "timestamp"})
		assert.Equal(t, base, got)
	})

	t.Run("date32", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "date32"})
		assert.Equal(t, arrow.DATE32, got.ID())
	})

	t.Run("date64", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "date64"})
		assert.Equal(t, arrow.DATE64, got.ID())
	})

	t.Run("time32", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "time32"})
		assert.Equal(t, arrow.TIME32, got.ID())
	})

	t.Run("time64", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "time64"})
		assert.Equal(t, arrow.TIME64, got.ID())
	})

	t.Run("unknown temporal falls through", func(t *testing.T) {
		got := applyTemporalOpts(base, timeType, tagOpts{Temporal: "bogus"})
		assert.Equal(t, base, got)
	})
}

func TestApplyDecimalOptsAllBranches(t *testing.T) {
	base := arrow.BinaryTypes.String
	opts := tagOpts{HasDecimalOpts: true, DecimalPrecision: 18, DecimalScale: 4}

	t.Run("no_decimal_opts_returns_dt_unchanged", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(decimal128.Num{}), tagOpts{})
		assert.Equal(t, base, got)
	})

	t.Run("decimal128", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(decimal128.Num{}), opts)
		dt, ok := got.(*arrow.Decimal128Type)
		require.True(t, ok, "expected *arrow.Decimal128Type, got %T", got)
		assert.Equal(t, int32(18), dt.Precision)
		assert.Equal(t, int32(4), dt.Scale)
	})

	t.Run("decimal256", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(decimal256.Num{}), opts)
		dt, ok := got.(*arrow.Decimal256Type)
		require.True(t, ok, "expected *arrow.Decimal256Type, got %T", got)
		assert.Equal(t, int32(18), dt.Precision)
		assert.Equal(t, int32(4), dt.Scale)
	})

	t.Run("decimal32", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(decimal.Decimal32(0)), opts)
		dt, ok := got.(*arrow.Decimal32Type)
		require.True(t, ok, "expected *arrow.Decimal32Type, got %T", got)
		assert.Equal(t, int32(18), dt.Precision)
		assert.Equal(t, int32(4), dt.Scale)
	})

	t.Run("decimal64", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(decimal.Decimal64(0)), opts)
		dt, ok := got.(*arrow.Decimal64Type)
		require.True(t, ok, "expected *arrow.Decimal64Type, got %T", got)
		assert.Equal(t, int32(18), dt.Precision)
		assert.Equal(t, int32(4), dt.Scale)
	})

	t.Run("non_decimal_type_returns_dt_unchanged", func(t *testing.T) {
		got := applyDecimalOpts(base, reflect.TypeOf(int32(0)), opts)
		assert.Equal(t, base, got)
	})
}

func TestApplyLargeOpts(t *testing.T) {
	cases := []struct {
		name  string
		input arrow.DataType
		want  arrow.Type
	}{
		{"string→large_string", arrow.BinaryTypes.String, arrow.LARGE_STRING},
		{"binary→large_binary", arrow.BinaryTypes.Binary, arrow.LARGE_BINARY},
		{"list<string>→large_list<large_string>", arrow.ListOf(arrow.BinaryTypes.String), arrow.LARGE_LIST},
		{"list_view<binary>→large_list_view<large_binary>", arrow.ListViewOf(arrow.BinaryTypes.Binary), arrow.LARGE_LIST_VIEW},
		{"int64 unchanged", arrow.PrimitiveTypes.Int64, arrow.INT64},
		{"float32 unchanged", arrow.PrimitiveTypes.Float32, arrow.FLOAT32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyLargeOpts(tc.input)
			assert.Equal(t, tc.want, got.ID())
		})
	}

	t.Run("list<binary> elem is large_binary", func(t *testing.T) {
		got := applyLargeOpts(arrow.ListOf(arrow.BinaryTypes.Binary))
		ll, ok := got.(*arrow.LargeListType)
		require.True(t, ok)
		assert.Equal(t, arrow.LARGE_BINARY, ll.Elem().ID())
	})

	t.Run("fixed_size_list<string> recurses", func(t *testing.T) {
		got := applyLargeOpts(arrow.FixedSizeListOf(3, arrow.BinaryTypes.String))
		fsl, ok := got.(*arrow.FixedSizeListType)
		require.True(t, ok)
		assert.Equal(t, arrow.LARGE_STRING, fsl.Elem().ID())
	})

	t.Run("map<string,binary> recurses", func(t *testing.T) {
		got := applyLargeOpts(arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.Binary))
		mt, ok := got.(*arrow.MapType)
		require.True(t, ok)
		assert.Equal(t, arrow.LARGE_STRING, mt.KeyType().ID())
		assert.Equal(t, arrow.LARGE_BINARY, mt.ItemField().Type.ID())
	})

	t.Run("struct recurses into fields", func(t *testing.T) {
		st := arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		)
		got := applyLargeOpts(st)
		gst, ok := got.(*arrow.StructType)
		require.True(t, ok)
		assert.Equal(t, arrow.LARGE_STRING, gst.Field(0).Type.ID())
		assert.Equal(t, arrow.INT64, gst.Field(1).Type.ID())
	})
}

func TestInferStructTypeWithLarge(t *testing.T) {
	type Row struct {
		Name  string `arrow:",large"`
		Count int64
	}
	st, err := inferStructType(reflect.TypeOf(Row{}))
	require.NoError(t, err)
	assert.Equal(t, arrow.LARGE_STRING, st.Field(0).Type.ID(), "Name should be LARGE_STRING")
	assert.Equal(t, arrow.INT64, st.Field(1).Type.ID(), "Count should be INT64")
}

func TestApplyViewOptsViewCombinations(t *testing.T) {
	t.Run("view+large: LARGE_LIST→LARGE_LIST_VIEW", func(t *testing.T) {
		dt := applyLargeOpts(arrow.ListOf(arrow.BinaryTypes.String))
		// dt is now LARGE_LIST<LARGE_STRING>
		got := applyViewOpts(dt)
		assert.Equal(t, arrow.LARGE_LIST_VIEW, got.ID())
	})

	t.Run("view only: LIST→LIST_VIEW", func(t *testing.T) {
		dt := arrow.ListOf(arrow.BinaryTypes.String)
		got := applyViewOpts(dt)
		assert.Equal(t, arrow.LIST_VIEW, got.ID())
		lv := got.(*arrow.ListViewType)
		assert.Equal(t, arrow.STRING_VIEW, lv.Elem().ID())
	})
}

func TestInferStructTypeWithView(t *testing.T) {
	type Row struct {
		Name string   `arrow:",view"`
		Tags []string `arrow:"tags,view"`
	}
	st, err := inferStructType(reflect.TypeOf(Row{}))
	require.NoError(t, err)
	assert.Equal(t, arrow.STRING_VIEW, st.Field(0).Type.ID(), "Name should be STRING_VIEW")
	assert.Equal(t, arrow.LIST_VIEW, st.Field(1).Type.ID(), "Tags should be LIST_VIEW")
	lv := st.Field(1).Type.(*arrow.ListViewType)
	assert.Equal(t, arrow.STRING_VIEW, lv.Elem().ID())
}

func TestHasLargeableType(t *testing.T) {
	assert.True(t, hasLargeableType(arrow.BinaryTypes.String))
	assert.True(t, hasLargeableType(arrow.BinaryTypes.Binary))
	assert.True(t, hasLargeableType(arrow.ListOf(arrow.PrimitiveTypes.Int64)))
	assert.True(t, hasLargeableType(arrow.ListViewOf(arrow.PrimitiveTypes.Int64)))
	assert.False(t, hasLargeableType(arrow.PrimitiveTypes.Int64))
	assert.False(t, hasLargeableType(arrow.PrimitiveTypes.Float32))

	t.Run("struct with string field is true", func(t *testing.T) {
		st := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String})
		assert.True(t, hasLargeableType(st))
	})
	t.Run("struct with only ints is false", func(t *testing.T) {
		st := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32})
		assert.False(t, hasLargeableType(st))
	})
	t.Run("fixed_size_list<string> is true", func(t *testing.T) {
		assert.True(t, hasLargeableType(arrow.FixedSizeListOf(4, arrow.BinaryTypes.String)))
	})
	t.Run("fixed_size_list<int32> is false", func(t *testing.T) {
		assert.False(t, hasLargeableType(arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int32)))
	})
	t.Run("map with string key is true", func(t *testing.T) {
		assert.True(t, hasLargeableType(arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)))
	})
	t.Run("map with no strings is false", func(t *testing.T) {
		assert.False(t, hasLargeableType(arrow.MapOf(arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64)))
	})
}

func TestApplyViewOpts(t *testing.T) {
	cases := []struct {
		name  string
		input arrow.DataType
		want  arrow.Type
	}{
		{"string→string_view", arrow.BinaryTypes.String, arrow.STRING_VIEW},
		{"binary→binary_view", arrow.BinaryTypes.Binary, arrow.BINARY_VIEW},
		{"large_string→string_view", arrow.BinaryTypes.LargeString, arrow.STRING_VIEW},
		{"large_binary→binary_view", arrow.BinaryTypes.LargeBinary, arrow.BINARY_VIEW},
		{"list<string>→list_view<string_view>", arrow.ListOf(arrow.BinaryTypes.String), arrow.LIST_VIEW},
		{"int64 unchanged", arrow.PrimitiveTypes.Int64, arrow.INT64},
		{"float32 unchanged", arrow.PrimitiveTypes.Float32, arrow.FLOAT32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyViewOpts(tc.input)
			assert.Equal(t, tc.want, got.ID())
		})
	}

	t.Run("list<string> elem is string_view", func(t *testing.T) {
		got := applyViewOpts(arrow.ListOf(arrow.BinaryTypes.String))
		lv, ok := got.(*arrow.ListViewType)
		require.True(t, ok)
		assert.Equal(t, arrow.STRING_VIEW, lv.Elem().ID())
	})

	t.Run("fixed_size_list<string> recurses", func(t *testing.T) {
		got := applyViewOpts(arrow.FixedSizeListOf(3, arrow.BinaryTypes.String))
		fsl, ok := got.(*arrow.FixedSizeListType)
		require.True(t, ok)
		assert.Equal(t, arrow.STRING_VIEW, fsl.Elem().ID())
	})

	t.Run("map<string,binary> recurses", func(t *testing.T) {
		got := applyViewOpts(arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.Binary))
		mt, ok := got.(*arrow.MapType)
		require.True(t, ok)
		assert.Equal(t, arrow.STRING_VIEW, mt.KeyType().ID())
		assert.Equal(t, arrow.BINARY_VIEW, mt.ItemField().Type.ID())
	})

	t.Run("struct recurses into fields", func(t *testing.T) {
		st := arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		)
		got := applyViewOpts(st)
		gst, ok := got.(*arrow.StructType)
		require.True(t, ok)
		assert.Equal(t, arrow.STRING_VIEW, gst.Field(0).Type.ID())
		assert.Equal(t, arrow.INT64, gst.Field(1).Type.ID())
	})

	t.Run("list_view<string> is idempotent", func(t *testing.T) {
		got := applyViewOpts(arrow.ListViewOf(arrow.BinaryTypes.String))
		lv, ok := got.(*arrow.ListViewType)
		require.True(t, ok)
		assert.Equal(t, arrow.STRING_VIEW, lv.Elem().ID())
	})
}

func TestHasViewableType(t *testing.T) {
	assert.True(t, hasViewableType(arrow.BinaryTypes.String))
	assert.True(t, hasViewableType(arrow.BinaryTypes.Binary))
	assert.True(t, hasViewableType(arrow.ListOf(arrow.PrimitiveTypes.Int64)))
	assert.False(t, hasViewableType(arrow.PrimitiveTypes.Int64))
	assert.False(t, hasViewableType(arrow.PrimitiveTypes.Float32))

	t.Run("struct with string field is true", func(t *testing.T) {
		st := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String})
		assert.True(t, hasViewableType(st))
	})
	t.Run("struct with only ints is false", func(t *testing.T) {
		st := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32})
		assert.False(t, hasViewableType(st))
	})
	t.Run("fixed_size_list<string> is true", func(t *testing.T) {
		assert.True(t, hasViewableType(arrow.FixedSizeListOf(4, arrow.BinaryTypes.String)))
	})
	t.Run("map with string key is true", func(t *testing.T) {
		assert.True(t, hasViewableType(arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)))
	})
	t.Run("large_string is true", func(t *testing.T) {
		assert.True(t, hasViewableType(arrow.BinaryTypes.LargeString))
	})
	t.Run("large_binary is true", func(t *testing.T) {
		assert.True(t, hasViewableType(arrow.BinaryTypes.LargeBinary))
	})
}
