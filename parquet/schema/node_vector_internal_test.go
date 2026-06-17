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
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPrimitiveNodeVector(t *testing.T) {
	leaf, err := NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, 768, -1)
	require.NoError(t, err)

	assert.Equal(t, parquet.Repetitions.Vector, leaf.RepetitionType())
	assert.EqualValues(t, 768, leaf.VectorLength())
	assert.True(t, leaf.LogicalType().IsNone())

	// toThrift emits repetition VECTOR and vector_length on the primitive leaf.
	se := leaf.toThrift()
	assert.Equal(t, format.FieldRepetitionType_VECTOR, se.GetRepetitionType())
	require.True(t, se.IsSetVectorLength())
	assert.EqualValues(t, 768, se.GetVectorLength())

	// Round-trip the vector leaf node through thrift.
	leaf2, err := PrimitiveNodeFromThrift(se)
	require.NoError(t, err)
	assert.True(t, leaf.Equals(leaf2))
	assert.EqualValues(t, 768, leaf2.VectorLength())
}

func TestVectorLengthDefaultIsNegativeOne(t *testing.T) {
	prim := MustPrimitive(NewPrimitiveNode("x", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1))
	assert.EqualValues(t, -1, prim.VectorLength())

	grp := MustGroup(NewGroupNode("g", parquet.Repetitions.Optional, FieldList{prim}, -1))
	assert.EqualValues(t, -1, grp.VectorLength())
}

func TestVectorValidationErrors(t *testing.T) {
	elem := MustPrimitive(NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))

	t.Run("non-positive vector_length", func(t *testing.T) {
		_, err := NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, 0, -1)
		assert.Error(t, err)
		_, err = NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, -3, -1)
		assert.Error(t, err)
	})

	t.Run("VECTOR repetition on a primitive without vector_length", func(t *testing.T) {
		_, err := NewPrimitiveNode("p", parquet.Repetitions.Vector, parquet.Types.Float, -1, -1)
		assert.Error(t, err)
	})

	t.Run("VECTOR repetition on a group", func(t *testing.T) {
		_, err := NewGroupNode("g", parquet.Repetitions.Vector, FieldList{elem}, -1)
		assert.Error(t, err)
	})

	t.Run("VECTOR primitive with variable-width BYTE_ARRAY", func(t *testing.T) {
		_, err := NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.ByteArray, -1, 8, -1)
		assert.Error(t, err)
	})

	t.Run("vector_length set on non-VECTOR group thrift", func(t *testing.T) {
		rep := format.FieldRepetitionType_OPTIONAL
		vlen := int32(8)
		se := &format.SchemaElement{Name: "g", RepetitionType: &rep, VectorLength: &vlen}
		_, err := GroupNodeFromThrift(se, FieldList{})
		assert.Error(t, err)
	})

	t.Run("vector_length set on non-VECTOR primitive thrift", func(t *testing.T) {
		rep := format.FieldRepetitionType_OPTIONAL
		typ := format.Type_FLOAT
		vlen := int32(8)
		se := &format.SchemaElement{Name: "p", RepetitionType: &rep, Type: &typ, VectorLength: &vlen}
		_, err := PrimitiveNodeFromThrift(se)
		assert.Error(t, err)
	})

	t.Run("VECTOR group thrift", func(t *testing.T) {
		rep := format.FieldRepetitionType_VECTOR
		vlen := int32(8)
		se := &format.SchemaElement{Name: "list", RepetitionType: &rep, VectorLength: &vlen}
		_, err := GroupNodeFromThrift(se, FieldList{elem})
		assert.Error(t, err)
	})

	t.Run("VECTOR primitive thrift without vector_length", func(t *testing.T) {
		rep := format.FieldRepetitionType_VECTOR
		typ := format.Type_FLOAT
		se := &format.SchemaElement{Name: "embedding", RepetitionType: &rep, Type: &typ}
		_, err := PrimitiveNodeFromThrift(se)
		assert.Error(t, err)
	})
}

// Full reduced Option B vector leaf round-trips through thrift:
//
//	vector float embedding [768];
func TestVectorLeafRoundTrip(t *testing.T) {
	leaf := MustPrimitive(NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, 768, -1))

	leafSE := leaf.toThrift()
	assert.Equal(t, format.FieldRepetitionType_VECTOR, leafSE.GetRepetitionType())
	assert.True(t, leafSE.IsSetVectorLength())
	assert.EqualValues(t, 768, leafSE.GetVectorLength())

	leaf2, err := PrimitiveNodeFromThrift(leafSE)
	require.NoError(t, err)

	assert.True(t, leaf.Equals(leaf2))
	assert.EqualValues(t, 768, leaf2.VectorLength())
}
