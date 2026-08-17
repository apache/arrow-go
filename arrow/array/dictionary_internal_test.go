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

package array

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestCheckIndexBoundsAllowsSignedIndexAtTypeLimit(t *testing.T) {
	tests := []struct {
		name       string
		indexType  arrow.DataType
		indexBytes []byte
		upperLimit uint64
	}{
		{"int8", arrow.PrimitiveTypes.Int8,
			arrow.Int8Traits.CastToBytes([]int8{math.MaxInt8}), uint64(math.MaxInt8) + 1},
		{"int16", arrow.PrimitiveTypes.Int16,
			arrow.Int16Traits.CastToBytes([]int16{math.MaxInt16}), uint64(math.MaxInt16) + 1},
		{"int32", arrow.PrimitiveTypes.Int32,
			arrow.Int32Traits.CastToBytes([]int32{math.MaxInt32}), uint64(math.MaxInt32) + 1},
		{"int64", arrow.PrimitiveTypes.Int64,
			arrow.Int64Traits.CastToBytes([]int64{math.MaxInt64}), uint64(math.MaxInt64) + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := memory.NewBufferBytes(tt.indexBytes)
			indices := NewData(tt.indexType, 1, []*memory.Buffer{nil, values}, nil, 0, 0)
			values.Release()
			defer indices.Release()

			require.NoError(t, checkIndexBounds(indices, tt.upperLimit))
		})
	}
}
