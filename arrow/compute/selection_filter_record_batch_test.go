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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterRecordBatchSerialPaths(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)
	batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[
		{"a": null, "b": "yo"},
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`))
	require.NoError(t, err)
	defer batch.Release()

	filter, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[true, null, false, true]`))
	require.NoError(t, err)
	defer filter.Release()

	oneColumnSchema := arrow.NewSchema(fields[:1], nil)
	oneColumnBatch := array.NewRecordBatch(oneColumnSchema, []arrow.Array{batch.Column(0)}, batch.NumRows())
	defer oneColumnBatch.Release()

	tests := []struct {
		name          string
		batch         arrow.RecordBatch
		numParallel   int
		nullSelection compute.NullSelectionBehavior
		expected      string
	}{
		{
			name:          "one column",
			batch:         oneColumnBatch,
			numParallel:   2,
			nullSelection: compute.SelectionEmitNulls,
			expected:      `[{"a": null}, {"a": null}, {"a": 4}]`,
		},
		{
			name:          "one parallel worker",
			batch:         batch,
			numParallel:   1,
			nullSelection: compute.SelectionEmitNulls,
			expected: `[
				{"a": null, "b": "yo"},
				{"a": null, "b": null},
				{"a": 4, "b": "eh"}
			]`,
		},
		{
			name:          "zero parallel workers",
			batch:         batch,
			numParallel:   0,
			nullSelection: compute.SelectionDropNulls,
			expected: `[
				{"a": null, "b": "yo"},
				{"a": 4, "b": "eh"}
			]`,
		},
		{
			name:          "parallel workers",
			batch:         batch,
			numParallel:   2,
			nullSelection: compute.SelectionDropNulls,
			expected: `[
				{"a": null, "b": "yo"},
				{"a": 4, "b": "eh"}
			]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCtx := compute.DefaultExecCtx()
			execCtx.NumParallel = tt.numParallel
			ctx := compute.SetExecCtx(context.Background(), execCtx)

			actual, err := compute.FilterRecordBatch(ctx, tt.batch, filter, &compute.FilterOptions{NullSelection: tt.nullSelection})
			require.NoError(t, err)
			defer actual.Release()

			expected, _, err := array.RecordFromJSON(mem, tt.batch.Schema(), strings.NewReader(tt.expected))
			require.NoError(t, err)
			defer expected.Release()
			assert.Truef(t, array.RecordEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)
		})
	}

	shortFilter, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[true]`))
	require.NoError(t, err)
	defer shortFilter.Release()
	_, err = compute.FilterRecordBatch(context.Background(), batch, shortFilter, compute.DefaultFilterOptions())
	require.ErrorIs(t, err, arrow.ErrInvalid)
}
