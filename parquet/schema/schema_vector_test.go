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

// vectorRoot builds a root group containing the reduced Option B vector leaf:
//
//	vector float embedding [listSize];
func vectorRoot(t *testing.T, listSize int32) *schema.GroupNode {
	t.Helper()
	leaf := schema.MustPrimitive(schema.NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, listSize, -1))
	return schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{leaf}, -1))
}

// VECTOR repetition must not increase the leaf's max definition or repetition
// level; the fixed multiplicity is carried by vector_length on the leaf.
func TestVectorBuildTreeLevels(t *testing.T) {
	sc := schema.NewSchema(vectorRoot(t, 128))
	require.Equal(t, 1, sc.NumColumns())
	col := sc.Column(0)
	assert.Equal(t, "embedding", col.Name())
	assert.Equal(t, "embedding", col.Path())
	assert.Equal(t, parquet.Types.Float, col.PhysicalType())
	assert.EqualValues(t, 0, col.MaxDefinitionLevel(), "max definition level")
	assert.EqualValues(t, 0, col.MaxRepetitionLevel(), "max repetition level")
	assert.True(t, col.InVectorColumn())
	assert.EqualValues(t, 128, col.EffectiveVectorLength())
}

// The flattened schema must carry VECTOR repetition and vector_length directly
// on the primitive leaf, and reconstruct losslessly via FromParquet.
func TestVectorSchemaThriftRoundTrip(t *testing.T) {
	root := vectorRoot(t, 128)
	sc := schema.NewSchema(root)

	elems := schema.ToThrift(root)

	var sawVectorRep bool
	for _, e := range elems {
		if e.GetRepetitionType() == format.FieldRepetitionType_VECTOR {
			sawVectorRep = true
			require.True(t, e.IsSetVectorLength(), "VECTOR leaf must carry vector_length")
			assert.EqualValues(t, 128, e.GetVectorLength())
			assert.False(t, e.IsSetLogicalType(), "VECTOR leaf must not carry a VECTOR logical type")
			assert.True(t, e.IsSetType(), "VECTOR leaf must be primitive")
		}
	}
	assert.True(t, sawVectorRep, "expected a VECTOR-repeated primitive leaf")

	recon, err := schema.FromParquet(elems)
	require.NoError(t, err)
	reconSchema := schema.NewSchema(recon.(*schema.GroupNode))

	assert.True(t, sc.Equals(reconSchema), "schema must round-trip through ToThrift/FromParquet")
	assert.EqualValues(t, 128, reconSchema.Column(0).EffectiveVectorLength())
	assert.True(t, reconSchema.Column(0).InVectorColumn())
}

func TestVectorRejectsNullableOrRepeatedAncestors(t *testing.T) {
	leaf := schema.MustPrimitive(schema.NewPrimitiveNodeLogicalVector("embedding", nil, parquet.Types.Float, -1, 8, -1))

	t.Run("optional ancestor", func(t *testing.T) {
		opt := schema.MustGroup(schema.NewGroupNode("opt", parquet.Repetitions.Optional, schema.FieldList{leaf}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{opt}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.Error(t, err)
		assert.Panics(t, func() { schema.NewSchema(root) })
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.Error(t, err)
	})

	t.Run("repeated ancestor", func(t *testing.T) {
		rep := schema.MustGroup(schema.NewGroupNode("rep", parquet.Repetitions.Repeated, schema.FieldList{leaf}, -1))
		root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{rep}, -1))
		_, err := schema.NewSchemaChecked(root)
		assert.Error(t, err)
		assert.Panics(t, func() { schema.NewSchema(root) })
		_, err = schema.FromParquet(schema.ToThrift(root))
		assert.Error(t, err)
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
