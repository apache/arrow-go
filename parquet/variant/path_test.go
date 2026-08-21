// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package variant_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildVar(t *testing.T, v any) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, b.Append(v))
	val, err := b.Build()
	require.NoError(t, err)

	return val
}

func TestGetByPathFieldAndIndex(t *testing.T) {
	v := buildVar(t, map[string]any{"a": map[string]any{"b": int64(5)}, "arr": []any{int64(10), int64(20)}})

	leaf, found, err := v.GetByPath(variant.VariantPath{}.Field("a").Field("b"))
	require.NoError(t, err)
	require.True(t, found)
	assert.EqualValues(t, 5, leaf.Value())

	leaf, found, err = v.GetByPath(variant.VariantPath{}.Field("arr").Index(1))
	require.NoError(t, err)
	require.True(t, found)
	assert.EqualValues(t, 20, leaf.Value())
}

func TestGetByPathAbsent(t *testing.T) {
	v := buildVar(t, map[string]any{"a": int64(1), "arr": []any{int64(10)}})

	for _, p := range []variant.VariantPath{
		variant.VariantPath{}.Field("missing"),      // absent object field
		variant.VariantPath{}.Field("arr").Index(9), // out-of-range index
		variant.VariantPath{}.Field("a").Index(0),   // index into a scalar
		variant.VariantPath{}.Index(0),              // index into an object
	} {
		_, found, err := v.GetByPath(p)
		require.NoError(t, err)
		assert.False(t, found)
	}
}

// TestGetByPathFieldOnScalarErrors: a field step into a non-object is a type error.
func TestGetByPathFieldOnScalarErrors(t *testing.T) {
	v := buildVar(t, int64(1))
	_, _, err := v.GetByPath(variant.VariantPath{}.Field("a"))
	require.ErrorIs(t, err, arrow.ErrInvalid)
}

// TestGetByPathHugeIndex: a huge index must not wrap; it is simply absent.
func TestGetByPathHugeIndex(t *testing.T) {
	v := buildVar(t, []any{int64(10), int64(20)})
	_, found, err := v.GetByPath(variant.VariantPath{}.Index(1 << 40))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestVariantPathJoinAndStepAt(t *testing.T) {
	p := variant.VariantPath{}.Field("a").Join(variant.VariantPath{}.Index(2).Field("b"))
	require.Equal(t, 3, p.Len())

	name, _ := p.StepAt(0)
	assert.Equal(t, "a", name)
	name, idx := p.StepAt(1)
	assert.Equal(t, "", name)
	assert.Equal(t, 2, idx)
	name, _ = p.StepAt(2)
	assert.Equal(t, "b", name)
}
