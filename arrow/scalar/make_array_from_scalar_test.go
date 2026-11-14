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

package scalar_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeArrayFromLargeListScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.AppendValues([]int32{1, 2, 3}, nil)
	values := bldr.NewArray()
	defer values.Release()
	sc := scalar.NewLargeListScalar(values)
	defer sc.Release()

	got, err := scalar.MakeArrayFromScalar(sc, 3, mem)
	require.NoError(t, err)
	defer got.Release()
	require.NoError(t, array.ValidateFull(got))

	listBldr := array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer listBldr.Release()
	for range 3 {
		listBldr.Append(true)
		listBldr.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	}
	want := listBldr.NewArray()
	defer want.Release()
	assert.Truef(t, array.Equal(want, got), "want %s, got %s", want, got)
}

func TestMakeArrayFromSparseUnionScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := arrow.SparseUnionOf([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, []arrow.UnionTypeCode{5, 7})
	staleChildID := scalar.NewSparseUnionScalar(
		[]scalar.Scalar{scalar.NewInt32Scalar(10), scalar.NewStringScalar("hi")}, 7, dt)
	staleChildID.ChildID = 0

	tests := []struct {
		name string
		sc   scalar.Scalar
		// nonNull is the index of the child that holds the string "hi" in every
		// slot, or -1 when every child is null.
		nonNull int
	}{
		{name: "valid with other children set", sc: scalar.NewSparseUnionScalar(
			[]scalar.Scalar{scalar.NewInt32Scalar(10), scalar.NewStringScalar("hi")}, 7, dt), nonNull: 1},
		{name: "child id disagrees with the type code", sc: staleChildID, nonNull: 1},
		{name: "null with a later type code", sc: scalar.NewSparseUnionScalarFromValue(
			scalar.MakeNullScalar(arrow.BinaryTypes.String), 1, dt), nonNull: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const length = 3
			got, err := scalar.MakeArrayFromScalar(tc.sc, length, mem)
			require.NoError(t, err)
			defer got.Release()
			require.NoError(t, array.ValidateFull(got))
			require.Equal(t, length, got.Len())

			union := got.(*array.SparseUnion)
			for i := range length {
				assert.Equal(t, arrow.UnionTypeCode(7), union.TypeCode(i), "slot %d", i)
			}
			for child := range dt.NumFields() {
				field := union.Field(child)
				require.Equal(t, length, field.Len(), "child %d", child)
				if child != tc.nonNull {
					assert.Equal(t, length, field.NullN(), "child %d", child)
					continue
				}
				assert.Zero(t, field.NullN(), "child %d", child)
				for i := range length {
					assert.Equal(t, "hi", field.(*array.String).Value(i), "slot %d", i)
				}
			}
		})
	}
}

func TestMakeArrayFromDenseUnionScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := arrow.DenseUnionOf([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, []arrow.UnionTypeCode{5, 7})

	tests := []struct {
		name  string
		sc    scalar.Scalar
		valid bool
	}{
		{name: "valid", sc: scalar.NewDenseUnionScalar(scalar.NewStringScalar("hi"), 7, dt), valid: true},
		{name: "null with a later type code", sc: scalar.NewDenseUnionScalar(
			scalar.MakeNullScalar(arrow.BinaryTypes.String), 7, dt)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const length = 3
			got, err := scalar.MakeArrayFromScalar(tc.sc, length, mem)
			require.NoError(t, err)
			defer got.Release()
			require.NoError(t, array.ValidateFull(got))
			require.Equal(t, length, got.Len())

			union := got.(*array.DenseUnion)
			for i := range length {
				assert.Equal(t, arrow.UnionTypeCode(7), union.TypeCode(i), "slot %d", i)
				assert.Zero(t, union.ValueOffset(i), "slot %d", i)
			}
			assert.Zero(t, union.Field(0).Len())
			str := union.Field(1)
			require.Equal(t, 1, str.Len())
			if tc.valid {
				assert.Equal(t, "hi", str.(*array.String).Value(0))
			} else {
				assert.True(t, str.IsNull(0))
			}
		})
	}
}

func TestMakeArrayFromExtensionScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := extensions.NewUUIDType()
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	storage := scalar.NewFixedSizeBinaryScalar(memory.NewBufferBytes(id[:]), dt.StorageType())

	const length = 3
	got, err := scalar.MakeArrayFromScalar(scalar.NewExtensionScalar(storage, dt), length, mem)
	require.NoError(t, err)
	defer got.Release()
	require.NoError(t, array.ValidateFull(got))
	require.Equal(t, length, got.Len())

	uuids := got.(*extensions.UUIDArray)
	for i := range length {
		require.True(t, uuids.IsValid(i), "slot %d", i)
		assert.Equal(t, uuid.UUID(id), uuids.Value(i), "slot %d", i)
	}
}

func TestMakeArrayFromInvalidNestedScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	sparse := arrow.SparseUnionOf(fields, []arrow.UnionTypeCode{5, 7})
	dense := arrow.DenseUnionOf(fields, []arrow.UnionTypeCode{5, 7})
	uuidType := extensions.NewUUIDType()

	tests := []struct {
		name    string
		sc      scalar.Scalar
		wantErr string
	}{
		{name: "sparse union with a missing child", sc: scalar.NewSparseUnionScalar(
			[]scalar.Scalar{scalar.NewInt32Scalar(10)}, 5, sparse), wantErr: "sparse union scalar value had"},
		{name: "dense union with an unknown type code", sc: scalar.NewDenseUnionScalar(
			scalar.NewInt32Scalar(10), 6, dense), wantErr: "invalid type code 6"},
		{name: "extension without a storage value", sc: scalar.NewExtensionScalar(nil, uuidType),
			wantErr: "doesn't have a storage value"},
		{name: "extension with a non extension type", sc: scalar.NewExtensionScalar(
			scalar.NewInt32Scalar(10), arrow.PrimitiveTypes.Int32), wantErr: "non extension type"},
		{name: "extension with the wrong storage type", sc: scalar.NewExtensionScalar(scalar.NewInt32Scalar(10), uuidType),
			wantErr: "has storage value of type int32"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scalar.MakeArrayFromScalar(tc.sc, 3, mem)
			if got != nil {
				got.Release()
			}
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}
