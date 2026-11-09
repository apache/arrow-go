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

//go:build go1.18

package exprs_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	substraitext "github.com/substrait-io/substrait-go/v8/extensions"
)

type registryPair struct {
	arrowRegistry     *exprs.ExtensionIDRegistry
	substraitRegistry expr.ExtensionRegistry
}

func (r registryPair) GetArrowRegistry() *exprs.ExtensionIDRegistry {
	return r.arrowRegistry
}

func (r registryPair) GetSubstraitRegistry() expr.ExtensionRegistry {
	return r.substraitRegistry
}

func TestExecuteScalarFunctionWithoutAnchorRegistry(t *testing.T) {
	function, err := exprs.NewScalarCall(exprs.NewDefaultExtensionSet(), "add", nil,
		expr.NewPrimitiveLiteral(int32(1), false),
		expr.NewPrimitiveLiteral(int32(10), false))
	require.NoError(t, err)

	extensionSet := registryPair{arrowRegistry: exprs.DefaultExtensionIDRegistry}
	ctx := exprs.WithExtensionIDSet(context.Background(), extensionSet)
	schema := arrow.NewSchema(nil, nil)
	input := array.NewRecordBatch(schema, nil, 1)
	defer input.Release()

	result, err := exprs.ExecuteScalarExpression(ctx, schema, function, compute.NewDatumWithoutOwning(input))
	require.NoError(t, err)
	defer result.Release()
	assert.Equal(t, int32(11), result.(*compute.ScalarDatum).Value.(*scalar.Int32).Value)
}

func TestExtensionTypeRoundTripWithoutAnchorAPI(t *testing.T) {
	opaqueType := extensions.NewOpaqueType(arrow.BinaryTypes.Binary, "example", "test")
	registry := exprs.NewExtensionIDRegistry()
	require.NoError(t, registry.RegisterType(substraitext.ID{
		URN: "extension:test:types", Name: "opaque",
	}, opaqueType))

	for _, test := range []struct {
		dataType arrow.DataType
		registry *exprs.ExtensionIDRegistry
	}{
		{arrow.PrimitiveTypes.Uint32, exprs.DefaultExtensionIDRegistry},
		{opaqueType, registry},
	} {
		t.Run(test.dataType.String(), func(t *testing.T) {
			extensionSet := registryPair{
				arrowRegistry:     test.registry,
				substraitRegistry: expr.NewEmptyExtensionRegistry(substraitext.GetDefaultCollectionWithNoError()),
			}
			substraitType, err := exprs.ToSubstraitType(test.dataType, true, extensionSet)
			require.NoError(t, err)

			result, nullable, err := exprs.FromSubstraitType(substraitType, extensionSet)
			require.NoError(t, err)
			assert.True(t, arrow.TypeEqual(test.dataType, result))
			assert.True(t, nullable)
		})
	}
}
