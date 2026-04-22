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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func checkedMem(t *testing.T) *memory.CheckedAllocator {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return mem
}

func setValueInto[T any](t *testing.T, dst *T, arr arrow.Array, i int) {
	t.Helper()
	require.NoError(t, setValue(reflect.ValueOf(dst).Elem(), arr, i))
}

func assertMultiLevelPtrNullPattern(t *testing.T, arr arrow.Array) {
	t.Helper()
	assert.Equal(t, 3, arr.Len())
	assert.False(t, arr.IsNull(0), "index 0 should not be null")
	assert.True(t, arr.IsNull(1), "index 1 should be null")
	assert.False(t, arr.IsNull(2), "index 2 should not be null")
}

func makeStringArray(t *testing.T, mem memory.Allocator, vals ...string) *array.String {
	t.Helper()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.AppendValues(vals, nil)
	a := b.NewStringArray()
	t.Cleanup(a.Release)
	return a
}

func makeInt32Array(t *testing.T, mem memory.Allocator, vals ...int32) *array.Int32 {
	t.Helper()
	b := array.NewInt32Builder(mem)
	defer b.Release()
	b.AppendValues(vals, nil)
	a := b.NewInt32Array()
	t.Cleanup(a.Release)
	return a
}

func makeStructArray(t *testing.T, arrays []arrow.Array, names []string) *array.Struct {
	t.Helper()
	sa, err := array.NewStructArray(arrays, names)
	require.NoError(t, err)
	t.Cleanup(sa.Release)
	return sa
}

func mustBuildArray(t *testing.T, vals any, opts tagOpts, mem memory.Allocator) arrow.Array {
	t.Helper()
	arr, err := buildArray(reflect.ValueOf(vals), opts, mem)
	require.NoError(t, err)
	t.Cleanup(arr.Release)
	return arr
}

func mustBuildDefault(t *testing.T, vals any, mem memory.Allocator) arrow.Array {
	t.Helper()
	return mustBuildArray(t, vals, tagOpts{}, mem)
}
