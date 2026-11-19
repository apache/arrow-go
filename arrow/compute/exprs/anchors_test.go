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

package exprs

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/extensions"
)

func TestAnchorHelpers(t *testing.T) {
	ext := NewDefaultExtensionSet()
	for _, encode := range []func(ExtensionIDSet, arrow.DataType) (extensions.ID, uint32, bool){encodeType, encodeTypeVariation} {
		id, anchor, ok := encode(ext, arrow.PrimitiveTypes.Uint32)
		require.True(t, ok)
		decodedID, dt, ok := decodeTypeArrow(ext, anchor)
		require.True(t, ok)
		assert.Equal(t, id, decodedID)
		assert.Equal(t, arrow.PrimitiveTypes.Uint32, dt)
	}

	_, _, ok := decodeTypeArrow(ext, ^uint32(0))
	assert.False(t, ok)

	registry := ext.GetSubstraitRegistry()
	id := extensions.ID{URN: SubstraitArithmeticFuncsURI, Name: "add:i32_i32"}
	anchor := registry.GetFuncAnchor(id)
	decodedID, convert, ok := decodeFunction(ext, anchor)
	require.True(t, ok)
	assert.Equal(t, "add", decodedID.Name)
	assert.NotNil(t, convert)

	_, _, ok = decodeFunction(ext, ^uint32(0))
	assert.False(t, ok)
}
