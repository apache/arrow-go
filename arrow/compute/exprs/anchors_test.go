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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	"github.com/substrait-io/substrait-go/v8/extensions"
)

// These assignments protect constructor signatures used by downstream wrappers.
var (
	_ func() exprs.ExtensionIDSet                                                   = exprs.NewDefaultExtensionSet
	_ func(expr.ExtensionRegistry) exprs.ExtensionIDSet                             = exprs.NewExtensionSetDefault
	_ func(expr.ExtensionRegistry, *exprs.ExtensionIDRegistry) exprs.ExtensionIDSet = exprs.NewExtensionSet
)

type trackingExtensionSet struct {
	exprs.ExtensionIDSet
	encodedVariations int
	decodedTypes      int
}

func (e *trackingExtensionSet) EncodeTypeVariation(dt arrow.DataType) (extensions.ID, uint32, bool) {
	e.encodedVariations++
	return e.ExtensionIDSet.EncodeTypeVariation(dt)
}

func (e *trackingExtensionSet) DecodeTypeArrow(anchor uint32) (extensions.ID, arrow.DataType, bool) {
	e.decodedTypes++
	return e.ExtensionIDSet.DecodeTypeArrow(anchor)
}

func TestCustomExtensionSetTypeConversion(t *testing.T) {
	ext := &trackingExtensionSet{ExtensionIDSet: exprs.NewDefaultExtensionSet()}
	st, err := exprs.ToSubstraitType(arrow.PrimitiveTypes.Uint32, true, ext)
	require.NoError(t, err)
	assert.Equal(t, 1, ext.encodedVariations)

	dt, nullable, err := exprs.FromSubstraitType(st, ext)
	require.NoError(t, err)
	assert.Equal(t, arrow.PrimitiveTypes.Uint32, dt)
	assert.True(t, nullable)
	assert.Equal(t, 1, ext.decodedTypes)
}

func TestDeprecatedAnchorMethods(t *testing.T) {
	ext := exprs.NewDefaultExtensionSet()
	for _, encode := range []func(arrow.DataType) (extensions.ID, uint32, bool){ext.EncodeType, ext.EncodeTypeVariation} {
		id, anchor, ok := encode(arrow.PrimitiveTypes.Uint32)
		require.True(t, ok)
		decodedID, dt, ok := ext.DecodeTypeArrow(anchor)
		require.True(t, ok)
		assert.Equal(t, id, decodedID)
		assert.Equal(t, arrow.PrimitiveTypes.Uint32, dt)
	}

	_, _, ok := ext.DecodeTypeArrow(^uint32(0))
	assert.False(t, ok)

	registry := ext.GetSubstraitRegistry()
	id := extensions.ID{URN: exprs.SubstraitArithmeticFuncsURI, Name: "add:i32_i32"}
	encoder, ok := ext.(interface {
		EncodeFunction(extensions.ID) uint32
		GetFuncAnchor(extensions.ID) uint32
	})
	require.True(t, ok)
	anchor := encoder.EncodeFunction(id)
	assert.Equal(t, registry.GetFuncAnchor(id), anchor)
	assert.Equal(t, encoder.GetFuncAnchor(id), anchor)
	decodedID, convert, ok := ext.DecodeFunction(anchor)
	require.True(t, ok)
	assert.Equal(t, "add", decodedID.Name)
	assert.NotNil(t, convert)

	_, _, ok = ext.DecodeFunction(^uint32(0))
	assert.False(t, ok)
}
