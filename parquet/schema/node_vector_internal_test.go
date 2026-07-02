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

func vectorGroupNode(t *testing.T, vectorLen int32) (*GroupNode, *PrimitiveNode) {
	t.Helper()
	elem := MustPrimitive(NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
	vec := MustGroup(NewGroupNodeVector("list", FieldList{elem}, vectorLen, -1))
	return vec, elem
}

func TestNewGroupNodeVector(t *testing.T) {
	vec, _ := vectorGroupNode(t, 768)

	assert.Equal(t, parquet.Repetitions.Vector, vec.RepetitionType())
	assert.EqualValues(t, 768, vec.VectorLength())
	assert.True(t, vec.LogicalType().IsNone())

	// toThrift emits repetition VECTOR and vector_length on the middle group.
	se := vec.toThrift()
	assert.Equal(t, format.FieldRepetitionType_VECTOR, se.GetRepetitionType())
	require.True(t, se.IsSetVectorLength())
	assert.EqualValues(t, 768, se.GetVectorLength())

	// Round-trip the vector group node through thrift.
	vec2, err := GroupNodeFromThrift(se, FieldList{vec.Field(0)})
	require.NoError(t, err)
	assert.True(t, vec.Equals(vec2))
	assert.EqualValues(t, 768, vec2.VectorLength())
}

func TestVectorLogicalTypeRoundTrip(t *testing.T) {
	vec, _ := vectorGroupNode(t, 3)
	outer := MustGroup(NewGroupNodeLogical("embedding", parquet.Repetitions.Optional, FieldList{vec}, VectorLogicalType{}, -1))

	se := outer.toThrift()
	require.True(t, se.IsSetLogicalType())
	require.True(t, se.GetLogicalType().IsSetVECTOR())

	roundtripped, err := GroupNodeFromThrift(se, FieldList{vec})
	require.NoError(t, err)
	assert.True(t, roundtripped.LogicalType().Equals(VectorLogicalType{}))
	assert.True(t, outer.Equals(roundtripped))
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
		_, err := NewGroupNodeVector("list", FieldList{elem}, 0, -1)
		assert.Error(t, err)
		_, err = NewGroupNodeVector("list", FieldList{elem}, -3, -1)
		assert.Error(t, err)
	})

	t.Run("VECTOR repetition on a primitive", func(t *testing.T) {
		_, err := NewPrimitiveNode("p", parquet.Repetitions.Vector, parquet.Types.Float, -1, -1)
		assert.Error(t, err)
	})

	t.Run("VECTOR repetition on a group without vector_length", func(t *testing.T) {
		_, err := NewGroupNode("g", parquet.Repetitions.Vector, FieldList{elem}, -1)
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

	t.Run("VECTOR group with logical type", func(t *testing.T) {
		_, err := newGroupNodeLogical("list", parquet.Repetitions.Vector, FieldList{elem}, NewListLogicalType(), -1, 8)
		assert.Error(t, err)
	})

	t.Run("VECTOR group thrift without vector_length", func(t *testing.T) {
		rep := format.FieldRepetitionType_VECTOR
		se := &format.SchemaElement{Name: "list", RepetitionType: &rep}
		_, err := GroupNodeFromThrift(se, FieldList{elem})
		assert.Error(t, err)
	})

	t.Run("VECTOR group thrift with logical type", func(t *testing.T) {
		rep := format.FieldRepetitionType_VECTOR
		vlen := int32(8)
		se := &format.SchemaElement{Name: "list", RepetitionType: &rep, VectorLength: &vlen, LogicalType: &format.LogicalType{LIST: format.NewListType()}}
		_, err := GroupNodeFromThrift(se, FieldList{elem})
		assert.Error(t, err)
	})

	t.Run("VECTOR primitive thrift", func(t *testing.T) {
		rep := format.FieldRepetitionType_VECTOR
		typ := format.Type_FLOAT
		vlen := int32(8)
		se := &format.SchemaElement{Name: "embedding", RepetitionType: &rep, Type: &typ, VectorLength: &vlen}
		_, err := PrimitiveNodeFromThrift(se)
		assert.Error(t, err)
	})
}
