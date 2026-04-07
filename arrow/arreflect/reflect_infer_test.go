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
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
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
				if err == nil {
					t.Fatalf("expected error, got nil (type: %v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.ID() != tc.wantID {
				t.Errorf("got ID %v, want %v", got.ID(), tc.wantID)
			}
		})
	}
}

func TestInferArrowType(t *testing.T) {
	t.Run("[]int32 is LIST", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf([]int32{}))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.LIST {
			t.Errorf("got %v, want LIST", dt.ID())
		}
	})

	t.Run("[3]float64 is FIXED_SIZE_LIST size 3", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf([3]float64{}))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.FIXED_SIZE_LIST {
			t.Errorf("got %v, want FIXED_SIZE_LIST", dt.ID())
		}
		fsl := dt.(*arrow.FixedSizeListType)
		if fsl.Len() != 3 {
			t.Errorf("got size %d, want 3", fsl.Len())
		}
	})

	t.Run("map[string]int64 is MAP", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf(map[string]int64{}))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.MAP {
			t.Errorf("got %v, want MAP", dt.ID())
		}
	})

	t.Run("struct with 2 fields is STRUCT", func(t *testing.T) {
		type S struct {
			Name string
			Age  int32
		}
		dt, err := inferArrowType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.STRUCT {
			t.Errorf("got %v, want STRUCT", dt.ID())
		}
		st := dt.(*arrow.StructType)
		if st.NumFields() != 2 {
			t.Errorf("got %d fields, want 2", st.NumFields())
		}
	})

	t.Run("[]map[string]struct{Score float64} nested", func(t *testing.T) {
		type Inner struct {
			Score float64
		}
		dt, err := inferArrowType(reflect.TypeOf([]map[string]Inner{}))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.LIST {
			t.Errorf("got %v, want LIST", dt.ID())
		}
		lt := dt.(*arrow.ListType)
		if lt.Elem().ID() != arrow.MAP {
			t.Errorf("list elem got %v, want MAP", lt.Elem().ID())
		}
		mt := lt.Elem().(*arrow.MapType)
		if mt.ValueType().ID() != arrow.STRUCT {
			t.Errorf("map value got %v, want STRUCT", mt.ValueType().ID())
		}
	})

	t.Run("*[]string pointer to slice is LIST", func(t *testing.T) {
		dt, err := inferArrowType(reflect.TypeOf((*[]string)(nil)))
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.LIST {
			t.Errorf("got %v, want LIST", dt.ID())
		}
	})
}

func TestInferStructType(t *testing.T) {
	t.Run("simple struct field names and types", func(t *testing.T) {
		type S struct {
			Name  string
			Score float32
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		if st.NumFields() != 2 {
			t.Fatalf("got %d fields, want 2", st.NumFields())
		}
		if st.Field(0).Name != "Name" || st.Field(0).Type.ID() != arrow.STRING {
			t.Errorf("field 0: got %v/%v, want Name/STRING", st.Field(0).Name, st.Field(0).Type.ID())
		}
		if st.Field(1).Name != "Score" || st.Field(1).Type.ID() != arrow.FLOAT32 {
			t.Errorf("field 1: got %v/%v, want Score/FLOAT32", st.Field(1).Name, st.Field(1).Type.ID())
		}
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type S struct {
			ID    int32
			Label *string
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		if st.Field(0).Nullable {
			t.Errorf("ID should not be nullable")
		}
		if !st.Field(1).Nullable {
			t.Errorf("Label should be nullable")
		}
	})

	t.Run("arrow:\"-\" tagged field is excluded", func(t *testing.T) {
		type S struct {
			Keep   string
			Hidden int32 `arrow:"-"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		if st.NumFields() != 1 {
			t.Errorf("got %d fields, want 1", st.NumFields())
		}
		if st.Field(0).Name != "Keep" {
			t.Errorf("got field name %q, want Keep", st.Field(0).Name)
		}
	})

	t.Run("arrow custom name tag", func(t *testing.T) {
		type S struct {
			GoName int64 `arrow:"custom_name"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		if st.Field(0).Name != "custom_name" {
			t.Errorf("got %q, want custom_name", st.Field(0).Name)
		}
	})

	t.Run("decimal128 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal128.Num `arrow:",decimal(18,2)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		dt := st.Field(0).Type
		if dt.ID() != arrow.DECIMAL128 {
			t.Fatalf("got %v, want DECIMAL128", dt.ID())
		}
		d128 := dt.(*arrow.Decimal128Type)
		if d128.Precision != 18 || d128.Scale != 2 {
			t.Errorf("got precision=%d scale=%d, want 18,2", d128.Precision, d128.Scale)
		}
	})

	t.Run("decimal256 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal256.Num `arrow:",decimal(40,5)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		dt := st.Field(0).Type
		if dt.ID() != arrow.DECIMAL256 {
			t.Fatalf("got %v, want DECIMAL256", dt.ID())
		}
		d256 := dt.(*arrow.Decimal256Type)
		if d256.Precision != 40 || d256.Scale != 5 {
			t.Errorf("got precision=%d scale=%d, want 40,5", d256.Precision, d256.Scale)
		}
	})

	t.Run("decimal32 with precision/scale tag", func(t *testing.T) {
		type S struct {
			Amount decimal.Decimal32 `arrow:",decimal(9,2)"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		dt := st.Field(0).Type
		if dt.ID() != arrow.DECIMAL32 {
			t.Fatalf("got %v, want DECIMAL32", dt.ID())
		}
		d32 := dt.(*arrow.Decimal32Type)
		if d32.Precision != 9 || d32.Scale != 2 {
			t.Errorf("got precision=%d scale=%d, want 9,2", d32.Precision, d32.Scale)
		}
	})

	t.Run("non-struct returns error", func(t *testing.T) {
		_, err := inferStructType(reflect.TypeOf(42))
		if err == nil {
			t.Error("expected error for non-struct, got nil")
		}
	})

	t.Run("time.Time with date32 tag maps to DATE32", func(t *testing.T) {
		type S struct {
			Ts time.Time `arrow:",date32"`
		}
		st, err := inferStructType(reflect.TypeOf(S{}))
		if err != nil {
			t.Fatal(err)
		}
		dt := st.Field(0).Type
		if dt.ID() != arrow.DATE32 {
			t.Errorf("got %v, want DATE32", dt.ID())
		}
	})
}

func TestInferArrowSchema(t *testing.T) {
	t.Run("simple struct mixed fields", func(t *testing.T) {
		type S struct {
			Name  string
			Age   int32
			Score float64
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatal(err)
		}
		if schema.NumFields() != 3 {
			t.Fatalf("got %d fields, want 3", schema.NumFields())
		}
		if schema.Field(0).Name != "Name" || schema.Field(0).Type.ID() != arrow.STRING {
			t.Errorf("field 0: got %v/%v, want Name/STRING", schema.Field(0).Name, schema.Field(0).Type.ID())
		}
		if schema.Field(1).Name != "Age" || schema.Field(1).Type.ID() != arrow.INT32 {
			t.Errorf("field 1: got %v/%v, want Age/INT32", schema.Field(1).Name, schema.Field(1).Type.ID())
		}
		if schema.Field(2).Name != "Score" || schema.Field(2).Type.ID() != arrow.FLOAT64 {
			t.Errorf("field 2: got %v/%v, want Score/FLOAT64", schema.Field(2).Name, schema.Field(2).Type.ID())
		}
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type S struct {
			ID    int32
			Label *string
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatal(err)
		}
		if schema.Field(0).Nullable {
			t.Errorf("ID should not be nullable")
		}
		if !schema.Field(1).Nullable {
			t.Errorf("Label should be nullable")
		}
	})

	t.Run("arrow:\"-\" tag excludes field", func(t *testing.T) {
		type S struct {
			Keep   string
			Hidden int32 `arrow:"-"`
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatal(err)
		}
		if schema.NumFields() != 1 {
			t.Errorf("got %d fields, want 1", schema.NumFields())
		}
		if schema.Field(0).Name != "Keep" {
			t.Errorf("got field name %q, want Keep", schema.Field(0).Name)
		}
	})

	t.Run("arrow custom name tag", func(t *testing.T) {
		type S struct {
			GoName int64 `arrow:"custom_name"`
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatal(err)
		}
		if schema.Field(0).Name != "custom_name" {
			t.Errorf("got %q, want custom_name", schema.Field(0).Name)
		}
	})

	t.Run("non-struct type returns error", func(t *testing.T) {
		_, err := SchemaOf[int]()
		if err == nil {
			t.Error("expected error for non-struct, got nil")
		}
	})
}

func TestInferArrowTypePublic(t *testing.T) {
	t.Run("int32 is INT32", func(t *testing.T) {
		dt, err := TypeOf[int32]()
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.INT32 {
			t.Errorf("got %v, want INT32", dt.ID())
		}
	})

	t.Run("[]string is LIST", func(t *testing.T) {
		dt, err := TypeOf[[]string]()
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.LIST {
			t.Errorf("got %v, want LIST", dt.ID())
		}
	})

	t.Run("map[string]float64 is MAP", func(t *testing.T) {
		dt, err := TypeOf[map[string]float64]()
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.MAP {
			t.Errorf("got %v, want MAP", dt.ID())
		}
	})

	t.Run("struct{X int32} is STRUCT", func(t *testing.T) {
		type S struct{ X int32 }
		dt, err := TypeOf[S]()
		if err != nil {
			t.Fatal(err)
		}
		if dt.ID() != arrow.STRUCT {
			t.Errorf("got %v, want STRUCT", dt.ID())
		}
	})
}

func TestInferArrowSchemaStructFieldEncoding(t *testing.T) {
	t.Run("dict-tagged string field becomes DICTIONARY", func(t *testing.T) {
		type S struct {
			Name string `arrow:"name,dict"`
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, ok := schema.FieldsByName("name")
		if !ok || len(f) == 0 {
			t.Fatal("field 'name' not found in schema")
		}
		if f[0].Type.ID() != arrow.DICTIONARY {
			t.Errorf("got %v, want DICTIONARY", f[0].Type.ID())
		}
	})

	t.Run("listview-tagged []string field becomes LIST_VIEW", func(t *testing.T) {
		type S struct {
			Tags []string `arrow:"tags,listview"`
		}
		schema, err := SchemaOf[S]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, ok := schema.FieldsByName("tags")
		if !ok || len(f) == 0 {
			t.Fatal("field 'tags' not found in schema")
		}
		if f[0].Type.ID() != arrow.LIST_VIEW {
			t.Errorf("got %v, want LIST_VIEW", f[0].Type.ID())
		}
	})

	t.Run("ree-tagged field on struct is unsupported", func(t *testing.T) {
		type REERow struct {
			Val string `arrow:"val,ree"`
		}
		_, err := SchemaOf[REERow]()
		if err == nil {
			t.Fatal("expected error for ree tag on struct field, got nil")
		}
		if !strings.Contains(err.Error(), "ree tag on struct field") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestGoTypeOf(t *testing.T) {
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
		got, err := GoTypeOf(tt.dt)
		if err != nil {
			t.Errorf("GoTypeOf(%v): %v", tt.dt, err)
			continue
		}
		if got != tt.want {
			t.Errorf("GoTypeOf(%v) = %v, want %v", tt.dt, got, tt.want)
		}
	}

	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	structType, err := GoTypeOf(st)
	if err != nil {
		t.Fatalf("struct: %v", err)
	}
	if structType.Kind() != reflect.Struct {
		t.Fatalf("want struct, got %v", structType.Kind())
	}
	if structType.NumField() != 2 {
		t.Fatalf("want 2 fields, got %d", structType.NumField())
	}
	if structType.Field(1).Type.Kind() != reflect.Ptr {
		t.Errorf("nullable field should be pointer")
	}
	if structType.Field(1).Type.Elem().Kind() != reflect.String {
		t.Errorf("nullable field should be *string")
	}

	listType, err := GoTypeOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if listType.Kind() != reflect.Slice {
		t.Fatalf("want slice, got %v", listType.Kind())
	}
	if listType.Elem() != reflect.TypeOf(int32(0)) {
		t.Errorf("list elem wrong")
	}

	fslType, err := GoTypeOf(arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32))
	if err != nil {
		t.Fatalf("fsl: %v", err)
	}
	if fslType.Kind() != reflect.Array {
		t.Fatalf("want array, got %v", fslType.Kind())
	}
	if fslType.Len() != 3 {
		t.Errorf("array len want 3, got %d", fslType.Len())
	}

	_, err = GoTypeOf(arrow.Null)
	if err == nil {
		t.Error("expected error for unsupported type")
	}
	if !errors.Is(err, ErrUnsupportedType) {
		t.Errorf("want ErrUnsupportedType, got %v", err)
	}
}
