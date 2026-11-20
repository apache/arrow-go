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

package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestFiniteVectorSchema(t *testing.T) {
	logical, err := NewFiniteVectorLogicalType(3)
	require.NoError(t, err)
	_, err = NewFiniteVectorLogicalType(0)
	require.Error(t, err)

	element, err := NewPrimitiveNodeLogical("element", parquet.Repetitions.Required,
		nil, parquet.Types.Float, -1, -1)
	require.NoError(t, err)
	list, err := NewGroupNode("list", parquet.Repetitions.Repeated, FieldList{element}, -1)
	require.NoError(t, err)

	for _, annotation := range []LogicalType{logical, &logical} {
		_, err = NewGroupNodeLogical("v", parquet.Repetitions.Repeated, FieldList{list}, annotation, -1)
		require.Error(t, err)
		require.Nil(t, list.Parent(), "a rejected group must not become the child's parent")
	}
	_, err = NewGroupNodeLogical("v", parquet.Repetitions.Optional, FieldList{list}, (*FiniteVectorLogicalType)(nil), -1)
	require.Error(t, err)

	vector, err := NewGroupNodeLogical("v", parquet.Repetitions.Optional, FieldList{list}, &logical, -1)
	require.NoError(t, err)
	require.Equal(t, logical, vector.LogicalType())
	root, err := NewGroupNode("schema", parquet.Repetitions.Required, FieldList{vector}, -1)
	require.NoError(t, err)

	encoded := ToThrift(root)
	require.Equal(t, int32(3), encoded[1].LogicalType.FINITE_VECTOR.NumElements)
	require.Equal(t, ConvertedTypes.List, ConvertedType(encoded[1].GetConvertedType()))
	decoded, err := FromParquet(encoded)
	require.NoError(t, err)
	require.True(t, root.Equals(decoded))

	encoded[1].LogicalType.FINITE_VECTOR.NumElements = 0
	_, err = FromParquet(encoded)
	require.ErrorContains(t, err, "must have a positive size")
}
