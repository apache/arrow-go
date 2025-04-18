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

package pqarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariantExtensionType(t *testing.T) {
	variant1, err := newVariantType(arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)
	variant2, err := newVariantType(arrow.StructOf(
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)

	assert.True(t, arrow.TypeEqual(variant1, variant2))

	// can be provided in either order
	variantFieldsFlipped, err := newVariantType(arrow.StructOf(
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false}))
	require.NoError(t, err)

	assert.Equal(t, "metadata", variantFieldsFlipped.Metadata().Name)
	assert.Equal(t, "value", variantFieldsFlipped.Value().Name)

	invalidTypes := []arrow.DataType{
		arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary}),
		arrow.StructOf(arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary}),
		arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}),
		arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary},
			arrow.Field{Name: "extra", Type: arrow.BinaryTypes.Binary}),
		arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: true},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: false}),
		arrow.StructOf(arrow.Field{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true}),
	}

	for _, tt := range invalidTypes {
		_, err := newVariantType(tt)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "invalid storage type for unshredded variant: "+tt.String())
	}
}
