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

package schema_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vectorRoot builds a root group containing the canonical 3-level vector form:
//
//	required group embedding (VECTOR) {
//	  vector group list [listSize] {
//	    required float element;
//	  }
//	}
func vectorRoot(t *testing.T, listSize int32) *schema.GroupNode {
	t.Helper()
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
	list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, listSize, -1))
	embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
	return schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
}

// VECTOR repetition must not increase the leaf's max definition or repetition
// level; the fixed multiplicity is carried by vector_length on the middle group.
func TestVectorBuildTreeLevels(t *testing.T) {
	sc := schema.NewSchema(vectorRoot(t, 128))
	require.Equal(t, 1, sc.NumColumns())
	col := sc.Column(0)
	assert.Equal(t, "element", col.Name())
	assert.Equal(t, "embedding.list.element", col.Path())
	assert.Equal(t, parquet.Types.Float, col.PhysicalType())
	assert.EqualValues(t, 0, col.MaxDefinitionLevel(), "max definition level")
	assert.EqualValues(t, 0, col.MaxRepetitionLevel(), "max repetition level")
	assert.True(t, col.InVectorColumn())
	assert.EqualValues(t, 128, col.EffectiveVectorLength())
}

func TestVectorRejectsNullableOuterLevel(t *testing.T) {
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
	list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 8, -1))
	embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Optional, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
	_, err := schema.NewSchemaChecked(root)
	assert.ErrorContains(t, err, "nullable VECTOR columns")
	assert.Panics(t, func() { schema.NewSchema(root) })
	_, err = schema.FromParquet(schema.ToThrift(root))
	assert.ErrorContains(t, err, "nullable VECTOR columns")
}

func TestVectorRejectsNullableElement(t *testing.T) {
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Optional, parquet.Types.Float, -1, -1))
	list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 8, -1))
	embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
	_, err := schema.NewSchemaChecked(root)
	assert.ErrorContains(t, err, "nullable VECTOR elements")
	assert.Panics(t, func() { schema.NewSchema(root) })
	_, err = schema.FromParquet(schema.ToThrift(root))
	assert.ErrorContains(t, err, "nullable VECTOR elements")
}

// The flattened schema must carry VECTOR logical type on the outer group and
// VECTOR repetition/vector_length on the middle group, and reconstruct
// losslessly via FromParquet.
func TestVectorSchemaThriftRoundTrip(t *testing.T) {
	root := vectorRoot(t, 128)
	sc := schema.NewSchema(root)

	elems := schema.ToThrift(root)

	var sawVectorLogical, sawVectorRep bool
	for _, e := range elems {
		if e.IsSetLogicalType() && e.GetLogicalType().IsSetVECTOR() {
			sawVectorLogical = true
			assert.Equal(t, format.FieldRepetitionType_REQUIRED, e.GetRepetitionType())
		}
		if e.GetRepetitionType() == format.FieldRepetitionType_VECTOR {
			sawVectorRep = true
			require.True(t, e.IsSetVectorLength(), "VECTOR group must carry vector_length")
			assert.EqualValues(t, 128, e.GetVectorLength())
			assert.False(t, e.IsSetLogicalType(), "VECTOR-repeated middle group must not carry a logical type")
			assert.False(t, e.IsSetType(), "VECTOR-repeated middle group must be a group")
		}
	}
	assert.True(t, sawVectorLogical, "expected a VECTOR logical outer group")
	assert.True(t, sawVectorRep, "expected a VECTOR-repeated middle group")

	recon, err := schema.FromParquet(elems)
	require.NoError(t, err)
	reconSchema := schema.NewSchema(recon.(*schema.GroupNode))

	assert.True(t, sc.Equals(reconSchema), "schema must round-trip through ToThrift/FromParquet")
	assert.EqualValues(t, 128, reconSchema.Column(0).EffectiveVectorLength())
	assert.True(t, reconSchema.Column(0).InVectorColumn())
}

func TestVectorRejectsRepeatedAncestors(t *testing.T) {
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
	list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 8, -1))
	embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
	rep := schema.MustGroup(schema.NewGroupNode("rep", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{rep}, -1))
	_, err := schema.NewSchemaChecked(root)
	assert.Error(t, err)
	assert.Panics(t, func() { schema.NewSchema(root) })
	_, err = schema.FromParquet(schema.ToThrift(root))
	assert.Error(t, err)
}

func TestVectorRejectsMalformedCanonicalShape(t *testing.T) {
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))

	t.Run("bare VECTOR-repeated group", func(t *testing.T) {
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 8, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{list}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "single child of a group annotated")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "single child of a group annotated")
	})

	t.Run("logical VECTOR without VECTOR child", func(t *testing.T) {
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{element}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "VECTOR-repeated group child")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "VECTOR-repeated group child")
	})

	t.Run("nested VECTOR logical group", func(t *testing.T) {
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
		wrapper := schema.MustGroup(schema.NewGroupNode("wrapper", parquet.Repetitions.Required, schema.FieldList{embedding}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{wrapper}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "top-level fields")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "top-level fields")
	})

	t.Run("middle group name", func(t *testing.T) {
		items := schema.MustGroup(schema.NewGroupNodeVector("items", schema.FieldList{element}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{items}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "must be named list")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "must be named list")
	})

	t.Run("element name", func(t *testing.T) {
		item := schema.MustPrimitive(schema.NewPrimitiveNode("item", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{item}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "must be named element")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "must be named element")
	})

	t.Run("non-primitive element", func(t *testing.T) {
		x := schema.MustPrimitive(schema.NewPrimitiveNode("x", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
		groupElement := schema.MustGroup(schema.NewGroupNode("element", parquet.Repetitions.Required, schema.FieldList{x}, -1))
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{groupElement}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "elements must be primitive")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "elements must be primitive")
	})

	t.Run("variable-width element", func(t *testing.T) {
		binary := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1))
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{binary}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "fixed-width primitive")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "fixed-width primitive")
	})

	t.Run("repeated descendant", func(t *testing.T) {
		repeated := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Repeated, parquet.Types.Float, -1, -1))
		list := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{repeated}, 8, -1))
		embedding := schema.MustGroup(schema.NewGroupNodeLogical("embedding", parquet.Repetitions.Required, schema.FieldList{list}, schema.VectorLogicalType{}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{embedding}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.ErrorContains(t, err, "repeated fields inside VECTOR")
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.ErrorContains(t, err, "repeated fields inside VECTOR")
	})
}

func TestVectorEffectiveLength(t *testing.T) {
	sc := schema.NewSchema(vectorRoot(t, 128))
	col := sc.Column(0)
	assert.True(t, col.InVectorColumn())
	assert.EqualValues(t, 128, col.EffectiveVectorLength())

	// A non-VECTOR column reports -1 / false.
	plain := schema.MustPrimitive(schema.NewPrimitiveNode("x", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1))
	psc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{plain}, -1)))
	assert.False(t, psc.Column(0).InVectorColumn())
	assert.EqualValues(t, -1, psc.Column(0).EffectiveVectorLength())

	// Survives the thrift round-trip.
	recon, err := schema.FromParquet(schema.ToThrift(vectorRoot(t, 128)))
	require.NoError(t, err)
	rsc := schema.NewSchema(recon.(*schema.GroupNode))
	assert.EqualValues(t, 128, rsc.Column(0).EffectiveVectorLength())
	assert.True(t, rsc.Column(0).InVectorColumn())
}
