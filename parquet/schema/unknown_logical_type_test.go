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
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A logical type added after this version is skipped by the thrift decoder, which leaves
// an empty union behind. The format tells readers to ignore the annotation (and the column
// order) of such a column and to use its physical type only.
func TestUnknownThriftLogicalType(t *testing.T) {
	unknown := &format.LogicalType{}

	assert.IsType(t, NoLogicalType{}, getLogicalType(unknown))
	assert.IsType(t, NoLogicalType{}, getLogicalType(nil))

	t.Run("primitive", func(t *testing.T) {
		for _, converted := range []*format.ConvertedType{nil, format.ConvertedTypePtr(format.ConvertedType_UTF8)} {
			node, err := PrimitiveNodeFromThrift(&format.SchemaElement{
				Name:           "col",
				Type:           format.TypePtr(format.Type_BYTE_ARRAY),
				RepetitionType: format.FieldRepetitionTypePtr(format.FieldRepetitionType_OPTIONAL),
				ConvertedType:  converted,
				LogicalType:    unknown,
			})
			require.NoError(t, err)
			assert.Equal(t, parquet.Types.ByteArray, node.PhysicalType())
			assert.True(t, node.LogicalType().IsNone())
			assert.Equal(t, ConvertedTypes.None, node.ConvertedType())

			root, err := NewGroupNode("schema", parquet.Repetitions.Required, FieldList{node}, -1)
			require.NoError(t, err)
			assert.Equal(t, SortUNKNOWN, NewSchema(root).Column(0).SortOrder())
		}
	})

	t.Run("group", func(t *testing.T) {
		for _, converted := range []*format.ConvertedType{nil, format.ConvertedTypePtr(format.ConvertedType_LIST)} {
			node, err := GroupNodeFromThrift(&format.SchemaElement{
				Name:           "col",
				NumChildren:    thrift.Int32Ptr(0),
				RepetitionType: format.FieldRepetitionTypePtr(format.FieldRepetitionType_OPTIONAL),
				ConvertedType:  converted,
				LogicalType:    unknown,
			}, FieldList{})
			require.NoError(t, err)
			assert.True(t, node.LogicalType().IsNone())
			assert.Equal(t, ConvertedTypes.None, node.ConvertedType())
		}
	})
}

// The same section of the format covers a known logical type on a physical type it does not
// apply to, such as TIMESTAMP on FIXED_LEN_BYTE_ARRAY(12), which newer writers may produce.
func TestInapplicableThriftLogicalType(t *testing.T) {
	elems := []*format.SchemaElement{
		{
			Name:           "schema",
			NumChildren:    thrift.Int32Ptr(1),
			RepetitionType: format.FieldRepetitionTypePtr(format.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "ts",
			Type:           format.TypePtr(format.Type_FIXED_LEN_BYTE_ARRAY),
			TypeLength:     thrift.Int32Ptr(12),
			RepetitionType: format.FieldRepetitionTypePtr(format.FieldRepetitionType_OPTIONAL),
			LogicalType:    NewTimestampLogicalType(true, TimeUnitNanos).toThrift(),
		},
	}

	root, err := FromParquet(elems)
	require.NoError(t, err)

	col := NewSchema(root.(*GroupNode)).Column(0)
	assert.Equal(t, parquet.Types.FixedLenByteArray, col.PhysicalType())
	assert.Equal(t, 12, col.TypeLength())
	assert.True(t, col.LogicalType().IsNone())
	assert.Equal(t, ConvertedTypes.None, col.ConvertedType())
	assert.Equal(t, SortUNKNOWN, col.SortOrder())
}
