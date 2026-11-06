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

package schemas

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestUnifySchemas(t *testing.T) {
	schema1Metadata := arrow.MetadataFrom(map[string]string{"source": "schema1"})
	schema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
		},
		&schema1Metadata,
	)

	t.Run("UnifySchemas with default promotion (no type conversion)", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Float64}, // Conflicting type
			},
			nil,
		)

		_, err := UnifySchemas(false, schema1, schema2)
		assert.Error(t, err) // Type conversion is not allowed
	})

	t.Run("UnifySchemas with permissive promotion (int32 to float64)", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Float64}, // Upgrade int32 to float64
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64}, // Promoted to float64
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with permissive promotion (int16 to int32)", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "structField", Type: arrow.StructOf(
					arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)}, // Upgrade int16 to int32
				)},
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}, // Promoted to int32
			), Nullable: true},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with unsupported type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.BinaryTypes.Binary}, // Unsupported upgrade
			},
			nil,
		)

		_, err := UnifySchemas(true, schema1, schema2)
		assert.Error(t, err) // Unsupported type upgrade should return an error
	})

	t.Run("UnifySchemas with identical schemas", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
				{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
				{Name: "structField", Type: arrow.StructOf(
					arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
				), Nullable: true},
			},
			&schema1Metadata,
		)

		unifiedSchema, err := UnifySchemas(false, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)
		assert.Equal(t, schema1.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with additional field in second schema", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
				{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
				{Name: "structField", Type: arrow.StructOf(
					arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
				), Nullable: true},
				{Name: "f5", Type: arrow.FixedWidthTypes.Boolean}, // New field
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "f5", Type: arrow.FixedWidthTypes.Boolean}, // Added field
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with conflicting nullable property", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, // Nullable conflict
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, // Resolved as nullable
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with LIST to LARGE_LIST upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "structField", Type: arrow.StructOf(
					arrow.Field{Name: "f4", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int16)}, // Upgrade LIST to LARGE_LIST
				)},
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int16), Nullable: true}, // Promoted to LARGE_LIST
			), Nullable: true},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with metadata merge", func(t *testing.T) {
		schema2Metadata := arrow.MetadataFrom(map[string]string{"source": "schema2"})
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			},
			&schema2Metadata,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedMetadata := arrow.MetadataFrom(map[string]string{"source": "schema1"})
		assert.Equal(t, expectedMetadata, unifiedSchema.Metadata())
	})

	t.Run("UnifySchemas with STRING to LARGE_STRING upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f5", Type: arrow.BinaryTypes.LargeString}, // Upgrade STRING to LARGE_STRING
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "f5", Type: arrow.BinaryTypes.LargeString}, // Added field
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with integer promotion to incompatible float type", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Float32}, // Upgrade int32 to float32
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.Error(t, err) // Float32 cannot safely store all int32 values
		assert.Nil(t, unifiedSchema)
	})
	t.Run("UnifySchemas with MAP type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "mapField", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64)}, // New MAP field
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "mapField", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64)}, // Added MAP field
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with DICTIONARY type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "dictField", Type: &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}}, // New DICTIONARY field
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "dictField", Type: &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int32,
				ValueType: arrow.BinaryTypes.String,
			}}, // Added DICTIONARY field
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with RUN_END_ENCODING type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "runEndField", Type: arrow.RunEndEncodedOf(
					arrow.PrimitiveTypes.Int32,
					arrow.BinaryTypes.String,
				)}, // New RUN_END_ENCODING field
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "runEndField", Type: arrow.RunEndEncodedOf(
				arrow.PrimitiveTypes.Int32,
				arrow.BinaryTypes.String,
			)}, // Added RUN_END_ENCODING field
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})
	t.Run("UnifySchemas with RUN_END_ENCODING type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "runEndField", Type: arrow.RunEndEncodedOf(
					arrow.PrimitiveTypes.Int64,   // Upgrade run_ends from Int32 to Int64
					arrow.PrimitiveTypes.Float64, // Upgrade values from Int32 to Float64
				)},
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "runEndField", Type: arrow.RunEndEncodedOf(
				arrow.PrimitiveTypes.Int64,   // Upgraded run_ends
				arrow.PrimitiveTypes.Float64, // Upgraded values
			)},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with DICTIONARY type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "dictField", Type: &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int64,    // Upgrade index from Int32 to Int64
					ValueType: arrow.BinaryTypes.LargeString, // Upgrade value from String to LargeString
				}},
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "dictField", Type: &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int64,    // Upgraded index
				ValueType: arrow.BinaryTypes.LargeString, // Upgraded value
			}},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})

	t.Run("UnifySchemas with MAP type upgrade", func(t *testing.T) {
		schema2 := arrow.NewSchema(
			[]arrow.Field{
				{Name: "mapField", Type: arrow.MapOf(
					arrow.PrimitiveTypes.Int64,   // Upgrade key from Int32 to Int64
					arrow.PrimitiveTypes.Float64, // Upgrade value from Int32 to Float64
				)},
			},
			nil,
		)

		unifiedSchema, err := UnifySchemas(true, schema1, schema2)
		assert.NoError(t, err)
		assert.NotNil(t, unifiedSchema)

		expectedFields := []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "structField", Type: arrow.StructOf(
				arrow.Field{Name: "f3", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "f4", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16), Nullable: true},
			), Nullable: true},
			{Name: "mapField", Type: arrow.MapOf(
				arrow.PrimitiveTypes.Int64,   // Upgraded key
				arrow.PrimitiveTypes.Float64, // Upgraded value
			)},
		}
		expectedSchema := arrow.NewSchema(expectedFields, &schema1Metadata)
		assert.Equal(t, expectedSchema.Fields(), unifiedSchema.Fields())
	})
}
