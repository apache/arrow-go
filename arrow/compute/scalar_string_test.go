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

package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func checkStartsWith(t *testing.T, ctx context.Context, pattern string, input, expected compute.Datum) {
	t.Helper()
	got, err := compute.StartsWith(ctx, compute.MatchSubstringOptions{Pattern: pattern}, input)
	require.NoError(t, err)
	defer got.Release()
	assertDatumsEqual(t, expected, got, nil, nil)
}

func TestStartsWith(t *testing.T) {
	newCtx := func(t *testing.T) (context.Context, memory.Allocator) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		return compute.WithAllocator(context.Background(), mem), mem
	}
	fromJSON := func(t *testing.T, mem memory.Allocator, dt arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	tests := []struct {
		name, pattern, input, expected string
	}{
		{"mixed", "ab", `["", "a", "ab", "abc", "ba", "xabc", null]`, `[false, false, true, true, false, false, null]`},
		{"empty pattern", "", `["", "a", null]`, `[true, true, null]`},
		{"all null", "ab", `[null, null]`, `[null, null]`},
		{"empty", "ab", `[]`, `[]`},
	}

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
	} {
		for _, tt := range tests {
			t.Run(dt.String()+"/"+tt.name, func(t *testing.T) {
				ctx, mem := newCtx(t)

				strType := arrow.BinaryTypes.String
				if arrow.IsLargeBinaryLike(dt.ID()) {
					strType = arrow.BinaryTypes.LargeString
				}
				strs := fromJSON(t, mem, strType, tt.input)
				defer strs.Release()
				input, err := compute.CastArray(ctx, strs, compute.SafeCastOptions(dt))
				require.NoError(t, err)
				defer input.Release()
				expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, tt.expected)
				defer expected.Release()

				checkStartsWith(t, ctx, tt.pattern, compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))

				for i := 0; i < input.Len(); i++ {
					in, err := scalar.GetScalar(input, i)
					require.NoError(t, err)
					want, err := scalar.GetScalar(expected, i)
					require.NoError(t, err)
					inDatum := compute.NewDatumWithoutOwning(in)
					checkStartsWith(t, ctx, tt.pattern, inDatum, compute.NewDatumWithoutOwning(want))
					inDatum.Release()
				}
			})
		}
	}

	t.Run("non-utf8 binary", func(t *testing.T) {
		ctx, mem := newCtx(t)
		input := fromJSON(t, mem, arrow.BinaryTypes.Binary, `["//5h", "/w==", "Yf8=", ""]`)
		defer input.Release()
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, true, false, false]`)
		defer expected.Release()

		checkStartsWith(t, ctx, "\xff", compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	t.Run("sliced", func(t *testing.T) {
		ctx, mem := newCtx(t)
		full := fromJSON(t, mem, arrow.BinaryTypes.String,
			`["ab", null, "x", "abc", null, "ab", "b", "abd", "a", "ab"]`)
		defer full.Release()
		input := array.NewSlice(full, 3, 9)
		defer input.Release()
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, true, false, true, false]`)
		defer expected.Release()

		checkStartsWith(t, ctx, "ab", compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	t.Run("chunked", func(t *testing.T) {
		ctx, mem := newCtx(t)
		var chunks []arrow.Array
		for _, json := range []string{`["ab", null]`, `[]`, `["xab", "abc"]`} {
			c := fromJSON(t, mem, arrow.BinaryTypes.String, json)
			defer c.Release()
			chunks = append(chunks, c)
		}
		input := arrow.NewChunked(arrow.BinaryTypes.String, chunks)
		defer input.Release()
		want := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, false, true]`)
		defer want.Release()
		expected := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{want})
		defer expected.Release()

		checkStartsWith(t, ctx, "ab", compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	t.Run("dictionary", func(t *testing.T) {
		ctx, mem := newCtx(t)
		dict := fromJSON(t, mem, arrow.BinaryTypes.String, `["ab", null, "xab"]`)
		defer dict.Release()
		indices := fromJSON(t, mem, arrow.PrimitiveTypes.Int8, `[0, 1, 2, null, 0]`)
		defer indices.Release()
		input := array.NewDictionaryArray(&arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, indices, dict)
		defer input.Release()
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, false, null, true]`)
		defer expected.Release()

		checkStartsWith(t, ctx, "ab", compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	t.Run("options", func(t *testing.T) {
		fn, ok := compute.GetFunctionRegistry().GetFunction("starts_with")
		require.True(t, ok)
		assert.NoError(t, fn.Validate())

		ctx, mem := newCtx(t)
		arr := fromJSON(t, mem, arrow.BinaryTypes.String, `["ab", "b"]`)
		defer arr.Release()
		input := compute.NewDatumWithoutOwning(arr)
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, false]`)
		defer expected.Release()

		got, err := compute.CallFunction(ctx, "starts_with", compute.MatchSubstringOptions{Pattern: "a"}, input)
		require.NoError(t, err)
		defer got.Release()
		assertDatumsEqual(t, compute.NewDatumWithoutOwning(expected), got, nil, nil)

		_, err = compute.CallFunction(ctx, "starts_with", nil, input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.CallFunction(ctx, "starts_with", (*compute.MatchSubstringOptions)(nil), input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.CallFunction(ctx, "starts_with", &compute.ArithmeticOptions{}, input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.StartsWith(ctx, compute.MatchSubstringOptions{Pattern: "a", IgnoreCase: true}, input)
		assert.ErrorIs(t, err, arrow.ErrNotImplemented)
	})
}
