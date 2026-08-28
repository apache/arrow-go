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

package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDictionaryEncodeNumericTypes(t *testing.T) {
	tests := []struct {
		name    string
		typ     arrow.DataType
		input   string
		masked  string
		encoded string
	}{
		{
			name:    "int32",
			typ:     arrow.PrimitiveTypes.Int32,
			input:   "[-3, 1, -3, null, 2]",
			masked:  "[-3, 1, 2]",
			encoded: "[-3, 1, null, 2]",
		},
		{
			name:    "int64",
			typ:     arrow.PrimitiveTypes.Int64,
			input:   "[-3, 1, -3, null, 2]",
			masked:  "[-3, 1, 2]",
			encoded: "[-3, 1, null, 2]",
		},
		{
			name:    "float32",
			typ:     arrow.PrimitiveTypes.Float32,
			input:   "[-3.5, 1.25, -3.5, null, 2.75]",
			masked:  "[-3.5, 1.25, 2.75]",
			encoded: "[-3.5, 1.25, null, 2.75]",
		},
		{
			name:    "float64",
			typ:     arrow.PrimitiveTypes.Float64,
			input:   "[-3.5, 1.25, -3.5, null, 2.75]",
			masked:  "[-3.5, 1.25, 2.75]",
			encoded: "[-3.5, 1.25, null, 2.75]",
		},
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input, _, err := array.FromJSON(mem, tc.typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			defer input.Release()

			maskedExpected, _, err := array.FromJSON(mem, tc.typ, strings.NewReader(tc.masked))
			require.NoError(t, err)
			defer maskedExpected.Release()

			encodedExpected, _, err := array.FromJSON(mem, tc.typ, strings.NewReader(tc.encoded))
			require.NoError(t, err)
			defer encodedExpected.Release()

			for _, mode := range []struct {
				name          string
				nullEncoding  compute.NullEncodingBehavior
				expectedDict  arrow.Array
				expectedIndex []int32
				nullCount     int
			}{
				{
					name:          "mask nulls",
					nullEncoding:  compute.NullEncodingMask,
					expectedDict:  maskedExpected,
					expectedIndex: []int32{0, 1, 0, 0, 2},
					nullCount:     1,
				},
				{
					name:          "encode nulls",
					nullEncoding:  compute.NullEncodingEncode,
					expectedDict:  encodedExpected,
					expectedIndex: []int32{0, 1, 0, 2, 3},
					nullCount:     0,
				},
			} {
				t.Run(mode.name, func(t *testing.T) {
					result, err := compute.DictionaryEncodeArray(ctx, compute.DictionaryEncodeOptions{
						NullEncoding: mode.nullEncoding,
					}, input)
					require.NoError(t, err)
					defer result.Release()

					encoded := result.(*array.Dictionary)
					require.True(t, array.Equal(mode.expectedDict, encoded.Dictionary()))
					require.Equal(t, mode.expectedIndex, encoded.Indices().(*array.Int32).Int32Values())
					require.Equal(t, mode.nullCount, encoded.NullN())
				})
			}
		})
	}
}
