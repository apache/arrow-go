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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTag(t *testing.T) {
	tests := []struct {
		input string
		want  tagOpts
	}{
		{
			input: "custom_name",
			want:  tagOpts{Name: "custom_name"},
		},
		{
			input: "-",
			want:  tagOpts{Skip: true},
		},
		{
			input: "-,",
			want:  tagOpts{Name: "-"},
		},
		{
			input: "",
			want:  tagOpts{},
		},
		{
			input: "name,dict",
			want:  tagOpts{Name: "name", Dict: true},
		},
		{
			input: "name,listview",
			want:  tagOpts{Name: "name", ListView: true},
		},
		{
			input: "name,ree",
			want:  tagOpts{Name: "name", REE: true},
		},
		{
			input: "name,decimal(38,10)",
			want:  tagOpts{Name: "name", HasDecimalOpts: true, DecimalPrecision: 38, DecimalScale: 10},
		},
		{
			input: ",decimal(18,2)",
			want:  tagOpts{Name: "", HasDecimalOpts: true, DecimalPrecision: 18, DecimalScale: 2},
		},
		{
			input: "name,dict,ree",
			want:  tagOpts{Name: "name", Dict: true, REE: true},
		},
		{
			input: "name,unknown_option",
			want:  tagOpts{Name: "name", ParseErr: "unknown option \"unknown_option\""},
		},
		{
			input: `field,Date32`,
			want:  tagOpts{Name: "field", ParseErr: "unknown option \"Date32\""},
		},
		{
			input: "name,large",
			want:  tagOpts{Name: "name", Large: true},
		},
		{
			input: "name,large,listview",
			want:  tagOpts{Name: "name", Large: true, ListView: true},
		},
		{
			input: "name,large,dict",
			want:  tagOpts{Name: "name", Large: true, Dict: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseTag(tt.input)
			assert.Equal(t, tt.want, got, "parseTag(%q)", tt.input)
		})
	}
}

func TestGetStructFields(t *testing.T) {
	t.Run("simple struct", func(t *testing.T) {
		type Simple struct {
			Name string
			Age  int32
		}
		fields := getStructFields(reflect.TypeOf(Simple{}))
		require.Len(t, fields, 2)
		assert.Equal(t, "Name", fields[0].Name)
		assert.Equal(t, "Age", fields[1].Name)
	})

	t.Run("struct with arrow tags", func(t *testing.T) {
		type Tagged struct {
			UserName string  `arrow:"user_name"`
			Score    float64 `arrow:"score"`
			Internal string  `arrow:"-"`
		}
		fields := getStructFields(reflect.TypeOf(Tagged{}))
		require.Len(t, fields, 2)
		assert.Equal(t, "user_name", fields[0].Name)
		assert.Equal(t, "score", fields[1].Name)
	})

	t.Run("unexported fields skipped", func(t *testing.T) {
		type Mixed struct {
			Exported   string
			unexported string //nolint:unused
		}
		fields := getStructFields(reflect.TypeOf(Mixed{}))
		require.Len(t, fields, 1)
		assert.Equal(t, "Exported", fields[0].Name)
	})

	t.Run("pointer fields are nullable", func(t *testing.T) {
		type WithPointers struct {
			Required string
			Optional *string
		}
		fields := getStructFields(reflect.TypeOf(WithPointers{}))
		require.Len(t, fields, 2)
		assert.False(t, fields[0].Nullable, "Required.Nullable = true, want false")
		assert.True(t, fields[1].Nullable, "Optional.Nullable = false, want true")
	})

	t.Run("embedded struct promotion", func(t *testing.T) {
		type Inner struct {
			City string
			Zip  int32
		}
		type Outer struct {
			Name string
			Inner
		}
		fields := getStructFields(reflect.TypeOf(Outer{}))
		require.Len(t, fields, 3)
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.Name
		}
		wantNames := []string{"Name", "City", "Zip"}
		for i, want := range wantNames {
			assert.Equal(t, want, names[i], "fields[%d].Name", i)
		}
	})

	t.Run("embedded struct conflict excluded", func(t *testing.T) {
		type A struct{ ID string }
		type B struct{ ID string }
		type Conflicted struct {
			A
			B
		}
		fields := getStructFields(reflect.TypeOf(Conflicted{}))
		assert.Len(t, fields, 0, "expected 0 fields due to conflict")
	})

	t.Run("embedded with tag overrides promotion", func(t *testing.T) {
		type Inner struct {
			City string
			Zip  int32
		}
		type HasTag struct {
			Inner `arrow:"inner_struct"`
		}
		fields := getStructFields(reflect.TypeOf(HasTag{}))
		require.Len(t, fields, 1)
		assert.Equal(t, "inner_struct", fields[0].Name)
	})

	t.Run("pointer to struct is dereferenced", func(t *testing.T) {
		type Simple struct {
			X int32
			Y string
		}
		fields := getStructFields(reflect.TypeOf(&Simple{}))
		require.Len(t, fields, 2)
		assert.Equal(t, "X", fields[0].Name)
		assert.Equal(t, "Y", fields[1].Name)
	})

	t.Run("multi-level pointer to struct is dereferenced", func(t *testing.T) {
		type Simple struct {
			X int32
		}
		var pp **Simple
		fields := getStructFields(reflect.TypeOf(pp))
		require.Len(t, fields, 1)
		assert.Equal(t, "X", fields[0].Name)
	})

	t.Run("non-struct type returns nil", func(t *testing.T) {
		assert.Nil(t, getStructFields(reflect.TypeOf(int32(0))))
		assert.Nil(t, getStructFields(reflect.TypeOf("")))
		assert.Nil(t, getStructFields(reflect.TypeOf([]int32{})))
	})
}

func TestCachedStructFields(t *testing.T) {
	type S struct {
		X int32
		Y string
	}

	fields1 := cachedStructFields(reflect.TypeOf(S{}))
	fields2 := cachedStructFields(reflect.TypeOf(S{}))

	require.Len(t, fields2, len(fields1), "cached call returned different lengths")

	for i := range fields1 {
		assert.Equal(t, fields1[i].Name, fields2[i].Name, "fields[%d].Name mismatch", i)
	}

	require.Len(t, fields1, 2)
	assert.Equal(t, "X", fields1[0].Name)
	assert.Equal(t, "Y", fields1[1].Name)
}

func TestBuildEmptyTyped(t *testing.T) {
	mem := checkedMem(t)

	t.Run("unsupported_type_returns_error", func(t *testing.T) {
		_, err := buildEmptyTyped(reflect.TypeOf((chan int)(nil)), tagOpts{}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("pointer_element_type_is_dereferenced", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf((*int32)(nil)), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.INT32, arr.DataType().ID())
	})

	t.Run("multi_level_pointer_element_type", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf((**int32)(nil)), tagOpts{}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.INT32, arr.DataType().ID())
	})

	t.Run("listview_on_non_slice_type_errors", func(t *testing.T) {
		_, err := buildEmptyTyped(reflect.TypeOf(int32(0)), tagOpts{ListView: true}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("listview_on_byte_slice_errors", func(t *testing.T) {
		_, err := buildEmptyTyped(reflect.TypeOf([]byte(nil)), tagOpts{ListView: true}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("listview_with_slice_of_pointers_derefs_inner", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf([]*int32(nil)), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
	})

	t.Run("listview_happy_path", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf([]int32(nil)), tagOpts{ListView: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, arrow.LIST_VIEW, arr.DataType().ID())
	})

	t.Run("dict_with_unsupported_value_type_errors", func(t *testing.T) {
		_, err := buildEmptyTyped(reflect.TypeOf(time.Time{}), tagOpts{Dict: true}, mem)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("dict_happy_path", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf(""), tagOpts{Dict: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.DICTIONARY, arr.DataType().ID())
	})

	t.Run("ree_happy_path", func(t *testing.T) {
		arr, err := buildEmptyTyped(reflect.TypeOf(int32(0)), tagOpts{REE: true}, mem)
		require.NoError(t, err)
		defer arr.Release()
		assert.Equal(t, 0, arr.Len())
		assert.Equal(t, arrow.RUN_END_ENCODED, arr.DataType().ID())
	})
}

func TestParseDecimalOpt(t *testing.T) {
	t.Run("valid_tag_sets_precision_and_scale", func(t *testing.T) {
		got := parseTag(",decimal(18,2)")
		assert.True(t, got.HasDecimalOpts)
		assert.Equal(t, int32(18), got.DecimalPrecision)
		assert.Equal(t, int32(2), got.DecimalScale)
		assert.Empty(t, got.ParseErr)
	})

	t.Run("non_integer_precision_records_error", func(t *testing.T) {
		got := parseTag(",decimal(abc,2)")
		assert.False(t, got.HasDecimalOpts)
		assert.NotEmpty(t, got.ParseErr)
	})

	t.Run("non_integer_scale_records_error", func(t *testing.T) {
		got := parseTag(",decimal(18,two)")
		assert.False(t, got.HasDecimalOpts)
		assert.NotEmpty(t, got.ParseErr)
	})

	t.Run("missing_scale_records_error", func(t *testing.T) {
		got := parseTag(",decimal(18)")
		assert.False(t, got.HasDecimalOpts)
		assert.NotEmpty(t, got.ParseErr)
	})

	t.Run("validateOptions_surfaces_parse_error", func(t *testing.T) {
		err := validateOptions(tagOpts{ParseErr: "bad decimal tag"})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	})
}
