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
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	"github.com/substrait-io/substrait-go/v8/extensions"
	"github.com/substrait-io/substrait-go/v8/types"
)

// No embedded Arrow extension set or anchor methods are needed.
type customExtensionSet struct {
	arrowRegistry     *exprs.ExtensionIDRegistry
	substraitRegistry expr.ExtensionRegistry
}

func (e *customExtensionSet) GetArrowRegistry() *exprs.ExtensionIDRegistry {
	return e.arrowRegistry
}

func (e *customExtensionSet) GetSubstraitRegistry() expr.ExtensionRegistry {
	return e.substraitRegistry
}

func TestCustomExtensionSet(t *testing.T) {
	ext := &customExtensionSet{
		arrowRegistry:     exprs.NewExtensionIDRegistry(),
		substraitRegistry: expr.NewEmptyExtensionRegistry(extensions.GetDefaultCollectionWithNoError()),
	}

	t.Run("type conversion", func(t *testing.T) {
		id := extensions.ID{URN: "extension:test:types", Name: "unsigned"}
		require.NoError(t, ext.arrowRegistry.RegisterType(id, arrow.PrimitiveTypes.Uint32))
		st, err := exprs.ToSubstraitType(arrow.PrimitiveTypes.Uint32, true, ext)
		require.NoError(t, err)
		decodedID, ok := ext.substraitRegistry.DecodeTypeVariation(st.GetTypeVariationReference())
		require.True(t, ok)
		assert.Equal(t, id, decodedID)
		dt, nullable, err := exprs.FromSubstraitType(st, ext)
		require.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Uint32, dt)
		assert.True(t, nullable)

		anchor := ext.substraitRegistry.GetTypeAnchor(id)
		dt, _, err = exprs.FromSubstraitType(&types.UserDefinedType{TypeReference: anchor}, ext)
		require.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Uint32, dt)
	})

	t.Run("function execution", func(t *testing.T) {
		id := extensions.ID{URN: exprs.SubstraitArithmeticFuncsURI, Name: "add"}
		called := false
		require.NoError(t, ext.arrowRegistry.AddSubstraitScalarToArrow(id,
			func(_ *expr.ScalarFunction, args []compute.Datum) (string, []compute.Datum, compute.FunctionOptions, error) {
				called = true
				return "add", args, nil, nil
			}))
		fn, err := expr.NewScalarFunc(ext.substraitRegistry, id, nil,
			expr.NewPrimitiveLiteral(int32(1), false), expr.NewPrimitiveLiteral(int32(10), false))
		require.NoError(t, err)
		schema := arrow.NewSchema(nil, nil)
		input := array.NewRecordBatch(schema, nil, 1)
		defer input.Release()
		ctx := exprs.WithExtensionIDSet(context.Background(), ext)
		result, err := exprs.ExecuteScalarExpression(ctx, schema, fn, compute.NewDatumWithoutOwning(input))
		require.NoError(t, err)
		defer result.Release()
		assert.True(t, called)
		assert.True(t, scalar.Equals(scalar.NewInt32Scalar(11), result.(*compute.ScalarDatum).Value))
	})
}
