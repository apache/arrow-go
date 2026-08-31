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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serialTakeContext(mem memory.Allocator, numParallel int) context.Context {
	execCtx := compute.DefaultExecCtx()
	execCtx.NumParallel = numParallel
	return compute.SetExecCtx(compute.WithAllocator(context.Background(), mem), execCtx)
}

func TestTakeRecordBatchSerialExecution(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	values, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[
		{"a": 0, "b": "zero"},
		{"a": 1, "b": "one"},
		{"a": null, "b": "null"},
		{"a": 3, "b": "three"}
	]`))
	require.NoError(t, err)
	defer values.Release()
	indices, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[3, 1, null, 0]`))
	require.NoError(t, err)
	defer indices.Release()
	expected, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[
		{"a": 3, "b": "three"},
		{"a": 1, "b": "one"},
		{"a": null, "b": null},
		{"a": 0, "b": "zero"}
	]`))
	require.NoError(t, err)
	defer expected.Release()

	for _, numParallel := range []int{1, 0, -1} {
		numParallel := numParallel
		t.Run(fmt.Sprintf("parallelism-%d", numParallel), func(t *testing.T) {
			result, err := compute.Take(serialTakeContext(mem, numParallel), *compute.DefaultTakeOptions(),
				&compute.RecordDatum{Value: values}, &compute.ArrayDatum{Value: indices.Data()})
			require.NoError(t, err)
			defer result.Release()

			assert.True(t, array.RecordEqual(expected, result.(*compute.RecordDatum).Value))
		})
	}
}

func TestTakeTableSerialExecution(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	values, err := array.TableFromJSON(mem, schema, []string{
		`[{"a": 0, "b": "zero"}, {"a": 1, "b": "one"}]`,
		`[{"a": 2, "b": "two"}, {"a": 3, "b": "three"}]`,
	})
	require.NoError(t, err)
	defer values.Release()
	indices, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[3, 1, 0]`))
	require.NoError(t, err)
	defer indices.Release()
	expected, err := array.TableFromJSON(mem, schema, []string{
		`[{"a": 3, "b": "three"}, {"a": 1, "b": "one"}, {"a": 0, "b": "zero"}]`,
	})
	require.NoError(t, err)
	defer expected.Release()

	for _, numParallel := range []int{1, 0, -1} {
		numParallel := numParallel
		t.Run(fmt.Sprintf("parallelism-%d", numParallel), func(t *testing.T) {
			result, err := compute.Take(serialTakeContext(mem, numParallel), *compute.DefaultTakeOptions(),
				&compute.TableDatum{Value: values}, &compute.ArrayDatum{Value: indices.Data()})
			require.NoError(t, err)
			defer result.Release()

			assert.True(t, array.TableEqual(expected, result.(*compute.TableDatum).Value))
		})
	}
}

func TestTakeArraySerialExecution(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[10, 20, 30, 40]`))
	require.NoError(t, err)
	defer values.Release()
	indices, err := array.ChunkedFromJSON(mem, arrow.PrimitiveTypes.Int32, []string{`[3, 1]`, `[null, 0]`})
	require.NoError(t, err)
	defer indices.Release()
	expected, err := array.ChunkedFromJSON(mem, arrow.PrimitiveTypes.Int32, []string{`[40, 20]`, `[null, 10]`})
	require.NoError(t, err)
	defer expected.Release()

	for _, numParallel := range []int{1, 0, -1} {
		numParallel := numParallel
		t.Run(fmt.Sprintf("parallelism-%d", numParallel), func(t *testing.T) {
			result, err := compute.Take(serialTakeContext(mem, numParallel), *compute.DefaultTakeOptions(),
				&compute.ArrayDatum{Value: values.Data()}, &compute.ChunkedDatum{Value: indices})
			require.NoError(t, err)
			defer result.Release()

			assert.True(t, array.ChunkedEqual(expected, result.(*compute.ChunkedDatum).Value))
		})
	}
}
