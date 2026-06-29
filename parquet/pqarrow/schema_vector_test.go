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

package pqarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toParquetVector(t *testing.T, field arrow.Field, enableVector bool) *schema.Schema {
	t.Helper()
	asc := arrow.NewSchema([]arrow.Field{field}, nil)
	var opts []pqarrow.WriterOption
	if enableVector {
		opts = append(opts, pqarrow.WithVectorEncoding())
	}
	ps, err := pqarrow.ToParquet(asc, nil, pqarrow.NewArrowWriterProperties(opts...))
	require.NoError(t, err)
	return ps
}

func isVectorField(n schema.Node) bool {
	g, ok := n.(*schema.GroupNode)
	return ok && g.LogicalType().Equals(schema.VectorLogicalType{})
}

func treeHasVector(n schema.Node) bool {
	if n.RepetitionType() == parquet.Repetitions.Vector {
		return true
	}
	g, ok := n.(*schema.GroupNode)
	if !ok {
		return false
	}
	for i := 0; i < g.NumFields(); i++ {
		if treeHasVector(g.Field(i)) {
			return true
		}
	}
	return false
}

func fslField(name string, elem arrow.DataType, size int32, valNullable, elemNullable bool) arrow.Field {
	listType := arrow.FixedSizeListOfField(size, arrow.Field{Name: "element", Type: elem, Nullable: elemNullable})
	return arrow.Field{Name: name, Type: listType, Nullable: valNullable}
}

func TestToParquetFixedSizeListVector(t *testing.T) {
	cases := []struct {
		name         string
		field        arrow.Field
		enableVector bool
		wantVector   bool
		wantLen      int32
		wantPhysical parquet.Type
	}{
		{"float32 dense enabled", fslField("emb", arrow.PrimitiveTypes.Float32, 128, false, false), true, true, 128, parquet.Types.Float},
		{"int32 dense enabled", fslField("v", arrow.PrimitiveTypes.Int32, 3, false, false), true, true, 3, parquet.Types.Int32},
		{"float64 dense enabled", fslField("v", arrow.PrimitiveTypes.Float64, 8, false, false), true, true, 8, parquet.Types.Double},
		{"flag disabled -> LIST", fslField("emb", arrow.PrimitiveTypes.Float32, 128, false, false), false, false, 0, 0},
		{"nullable value -> LIST", fslField("emb", arrow.PrimitiveTypes.Float32, 128, true, false), true, false, 0, 0},
		{"nullable element -> LIST", fslField("emb", arrow.PrimitiveTypes.Float32, 128, false, true), true, false, 0, 0},
		{"string element -> LIST", fslField("emb", arrow.BinaryTypes.String, 4, false, false), true, false, 0, 0},
		{"dictionary element -> LIST", fslField("emb", &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Int32}, 4, false, false), true, false, 0, 0},
		{"extension element -> LIST", fslField("emb", extensions.NewUUIDType(), 4, false, false), true, false, 0, 0},
		{"fixed-size-list element -> LIST", fslField("outer", arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32), 4, false, false), true, false, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps := toParquetVector(t, tc.field, tc.enableVector)
			top := ps.Root().Field(0)

			if !tc.wantVector {
				assert.False(t, treeHasVector(top), "expected no VECTOR node in the schema")
				return
			}

			require.True(t, isVectorField(top), "top-level field should be a VECTOR logical group")
			outer := top.(*schema.GroupNode)
			assert.Equal(t, tc.field.Name, outer.Name())
			require.Equal(t, 1, outer.NumFields())
			vec := outer.Field(0).(*schema.GroupNode)
			assert.Equal(t, parquet.Repetitions.Vector, vec.RepetitionType())
			assert.Equal(t, tc.wantLen, vec.VectorLength())
			require.Equal(t, 1, vec.NumFields())
			elem := vec.Field(0).(*schema.PrimitiveNode)
			assert.Equal(t, tc.wantPhysical, elem.PhysicalType())
			assert.Equal(t, "element", elem.Name())

			// The leaf column carries the effective vector length and no inner levels.
			require.Equal(t, 1, ps.NumColumns())
			col := ps.Column(0)
			assert.True(t, col.InVectorColumn())
			assert.Equal(t, tc.wantLen, col.EffectiveVectorLength())
			assert.EqualValues(t, 0, col.MaxDefinitionLevel())
			assert.EqualValues(t, 0, col.MaxRepetitionLevel())
		})
	}
}

// A FixedSizeList nested inside a (top-level) struct must NOT become a VECTOR in
// Only top-level FixedSizeList columns are eligible, so nested ones use the
// standard LIST encoding.
func TestToParquetNestedFixedSizeListStaysList(t *testing.T) {
	inner := fslField("emb", arrow.PrimitiveTypes.Float32, 16, false, false)
	cases := []arrow.Field{
		{Name: "s", Type: arrow.StructOf(inner)},
		{Name: "l", Type: arrow.ListOfField(inner)},
	}
	for _, field := range cases {
		ps := toParquetVector(t, field, true /* enableVector */)
		assert.False(t, treeHasVector(ps.Root().Field(0)), "nested FixedSizeList must not be encoded as VECTOR")
	}
}

// The read-side schema manifest reconstructs a VECTOR logical group into an
// Arrow FixedSizeList (with IsVector set) without needing a stored Arrow schema.
func TestVectorSchemaManifestReconstruction(t *testing.T) {
	field := fslField("emb", arrow.PrimitiveTypes.Float32, 128, false, false)
	ps := toParquetVector(t, field, true)

	manifest, err := pqarrow.NewSchemaManifest(ps, nil, &pqarrow.ArrowReadProperties{})
	require.NoError(t, err)
	require.Len(t, manifest.Fields, 1)

	f := manifest.Fields[0]
	assert.True(t, f.IsVector, "reconstructed field should be marked as VECTOR")
	require.NotNil(t, f.Field)
	fsl, ok := f.Field.Type.(*arrow.FixedSizeListType)
	require.True(t, ok, "reconstructed type should be FixedSizeList, got %s", f.Field.Type)
	assert.EqualValues(t, 128, fsl.Len())
	assert.Equal(t, arrow.FLOAT32, fsl.Elem().ID())
	assert.False(t, f.Field.Nullable)
	assert.EqualValues(t, 0, f.LevelInfo.DefLevel)
	assert.EqualValues(t, 0, f.LevelInfo.RepLevel)

	require.Len(t, f.Children, 1)
	leaf := f.Children[0]
	assert.True(t, leaf.IsLeaf())
	assert.True(t, leaf.IsVector)
	assert.Equal(t, "element", leaf.Field.Name)
	assert.Equal(t, arrow.FLOAT32, leaf.Field.Type.ID())
	assert.False(t, leaf.Field.Nullable)
}

func TestVectorSchemaManifestRejectsRepeatedVectorAncestor(t *testing.T) {
	element := schema.MustPrimitive(schema.NewPrimitiveNode("element", parquet.Repetitions.Required, parquet.Types.Float, -1, -1))
	vec := schema.MustGroup(schema.NewGroupNodeVector("list", schema.FieldList{element}, 128, -1))
	field := schema.MustGroup(schema.NewGroupNodeLogical("emb", parquet.Repetitions.Required, schema.FieldList{vec}, schema.VectorLogicalType{}, -1))
	repeated := schema.MustGroup(schema.NewGroupNode("items", parquet.Repetitions.Repeated, schema.FieldList{field}, -1))
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{repeated}, -1))
	require.Panics(t, func() { schema.NewSchema(root) })
}

// Without the vector flag the same FixedSizeList is a LIST on disk and the
// manifest field is not marked as VECTOR.
func TestVectorSchemaManifestListFallback(t *testing.T) {
	field := fslField("emb", arrow.PrimitiveTypes.Float32, 128, false, false)
	ps := toParquetVector(t, field, false)

	manifest, err := pqarrow.NewSchemaManifest(ps, nil, &pqarrow.ArrowReadProperties{})
	require.NoError(t, err)
	require.Len(t, manifest.Fields, 1)
	assert.False(t, manifest.Fields[0].IsVector)
	_, isFSL := manifest.Fields[0].Field.Type.(*arrow.FixedSizeListType)
	assert.False(t, isFSL, "LIST-encoded column should not reconstruct as FixedSizeList without a stored Arrow schema")
}
