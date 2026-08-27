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
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cumulativeInput(t *testing.T, mem memory.Allocator, typ arrow.DataType, values string) arrow.Array {
	arr, _, err := array.FromJSON(mem, typ, strings.NewReader(values))
	require.NoError(t, err)
	return arr
}

func TestCumulativeSum(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	defer input.Release()
	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6, 10]`)
	defer expected.Release()

	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)

}

func TestCumulativeSumValueOptions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2, 3]`)
	defer input.Release()
	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6]`)
	defer expected.Release()

	result, err := compute.CallFunction(
		ctx,
		"cumulative_sum",
		compute.CumulativeOptions{},
		&compute.ArrayDatum{Value: input.Data()},
	)
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
}

func TestCumulativeSumTypedNilStart(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2, 3]`)
	defer input.Release()
	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6]`)
	defer expected.Release()

	var start *scalar.Int32
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{Start: start},
		&compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
}

func TestCumulativeSumAdditionalInputs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	tests := []struct {
		name string
		typ  arrow.DataType
		in   string
		want string
	}{
		{name: "empty", typ: arrow.PrimitiveTypes.Int32, in: `[]`, want: `[]`},
		{name: "all null", typ: arrow.PrimitiveTypes.Int32, in: `[null, null]`, want: `[null, null]`},
		{name: "uint8", typ: arrow.PrimitiveTypes.Uint8, in: `[1, 2, 3]`, want: `[1, 3, 6]`},
		{name: "float32", typ: arrow.PrimitiveTypes.Float32, in: `[1.5, 2.5]`, want: `[1.5, 4]`},
		{name: "float64", typ: arrow.PrimitiveTypes.Float64, in: `[1.5, 2.5]`, want: `[1.5, 4]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, tc.in)
			defer input.Release()
			expected := cumulativeInput(t, mem, tc.typ, tc.want)
			defer expected.Release()

			result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
			require.NoError(t, err)
			defer result.Release()
			assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
		})
	}

	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[3]`)
	defer expected.Release()
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, compute.NewDatum(int32(3)))
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)

	fullInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[0, 1, 2, 3]`)
	defer fullInput.Release()
	slicedInput := array.NewSlice(fullInput, 1, 3)
	defer slicedInput.Release()
	slicedExpected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3]`)
	defer slicedExpected.Release()
	result, err = compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: slicedInput.Data()})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: slicedExpected.Data()}, result, nil, nil)

	t.Run("sliced nulls", func(t *testing.T) {
		fullInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[9, null, 2, 3, 99]`)
		defer fullInput.Release()
		slicedInput := array.NewSlice(fullInput, 1, 4)
		defer slicedInput.Release()

		tests := []struct {
			name string
			opts compute.CumulativeOptions
			want string
		}{
			{name: "propagate nulls", want: `[null, null, null]`},
			{name: "skip nulls", opts: compute.CumulativeOptions{SkipNulls: true}, want: `[null, 2, 5]`},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, tc.want)
				defer expected.Release()

				result, err := compute.CumulativeSum(ctx, tc.opts, &compute.ArrayDatum{Value: slicedInput.Data()})
				require.NoError(t, err)
				defer result.Release()
				assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
			})
		}
	})
}

func TestCumulativeSumNullScalarInput(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	types := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}
	functions := []struct {
		name string
		run  func(context.Context, compute.CumulativeOptions, compute.Datum) (compute.Datum, error)
	}{
		{name: "unchecked", run: compute.CumulativeSum},
		{name: "checked", run: compute.CumulativeSumChecked},
	}
	options := []struct {
		name  string
		start bool
		skip  bool
	}{
		{name: "no_start_no_skip"},
		{name: "no_start_skip", skip: true},
		{name: "start_no_skip", start: true},
		{name: "start_skip", start: true, skip: true},
	}

	for _, typ := range types {
		start, err := scalar.ParseScalar(typ, "10")
		require.NoError(t, err)
		if releasable, ok := start.(scalar.Releasable); ok {
			defer releasable.Release()
		}

		for _, fn := range functions {
			for _, tc := range options {
				t.Run(typ.Name()+"/"+fn.name+"/"+tc.name, func(t *testing.T) {
					input := compute.NewDatum(scalar.MakeNullScalar(typ))
					defer input.Release()

					opts := compute.CumulativeOptions{SkipNulls: tc.skip}
					if tc.start {
						opts.Start = start
					}

					result, err := fn.run(ctx, opts, input)
					require.NoError(t, err)
					defer result.Release()

					expected := cumulativeInput(t, mem, typ, `[null]`)
					defer expected.Release()
					assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
				})
			}
		}
	}
}

func TestCumulativeSumNullsAndStart(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, 2, null, 3]`)
	defer input.Release()

	t.Run("propagate nulls", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, null, null, null]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

	t.Run("skip nulls", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, 3, null, 6]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{SkipNulls: true}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

	t.Run("start value", func(t *testing.T) {
		expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[11, null, 13, null, 16]`)
		defer expected.Release()
		result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
			Start:     scalar.NewInt64Scalar(10),
			SkipNulls: true,
		}, &compute.ArrayDatum{Value: input.Data()})
		require.NoError(t, err)
		defer result.Release()
		assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
	})

}

func TestCumulativeSumRejectsTypedNullStart(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1]`)
	defer input.Release()

	for _, tc := range []struct {
		name string
		run  func(context.Context, compute.CumulativeOptions, compute.Datum) (compute.Datum, error)
	}{
		{name: "unchecked", run: compute.CumulativeSum},
		{name: "checked", run: compute.CumulativeSumChecked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.run(ctx, compute.CumulativeOptions{
				Start: scalar.MakeNullScalar(arrow.PrimitiveTypes.Int32),
			}, &compute.ArrayDatum{Value: input.Data()})
			if result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestCumulativeSumStartSafeCast(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	tests := []struct {
		name  string
		typ   arrow.DataType
		start scalar.Scalar
	}{
		{name: "signed integer overflow", typ: arrow.PrimitiveTypes.Int8, start: scalar.NewInt64Scalar(128)},
		{name: "signed integer underflow", typ: arrow.PrimitiveTypes.Int8, start: scalar.NewInt64Scalar(-129)},
		{name: "unsigned integer underflow", typ: arrow.PrimitiveTypes.Uint8, start: scalar.NewInt64Scalar(-1)},
		{name: "float truncation", typ: arrow.PrimitiveTypes.Int32, start: scalar.NewFloat64Scalar(1.5)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, `[0]`)
			defer input.Release()

			result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
				Start: tc.start,
			}, &compute.ArrayDatum{Value: input.Data()})
			if result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[0]`)
	defer input.Release()
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
		Start: scalar.NewInt64Scalar(127),
	}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.Equal(t, int8(127), actual.(*array.Int8).Value(0))
}

func TestCumulativeSumStartSafeCastDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	dictValues := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int64, `[10]`)
	defer dictValues.Release()
	dictIndices := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[0]`)
	defer dictIndices.Release()
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.PrimitiveTypes.Int64,
	}
	dict := array.NewDictionaryArray(dictType, dictIndices, dictValues)
	defer dict.Release()

	start, err := scalar.GetScalar(dict, 0)
	require.NoError(t, err)
	if releasable, ok := start.(scalar.Releasable); ok {
		defer releasable.Release()
	}

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1]`)
	defer input.Release()
	expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[11]`)
	defer expected.Release()

	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{Start: start},
		&compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer result.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
}

func TestCumulativeSumStartScalarConversions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1]`)
	defer input.Release()

	tests := []struct {
		name  string
		start scalar.Scalar
		want  string
	}{
		{name: "string", start: scalar.NewStringScalar("10"), want: `[11]`},
		{name: "boolean", start: scalar.NewBooleanScalar(true), want: `[2]`},
	}
	binaryBuffer := memory.NewBufferBytes([]byte("10"))
	tests = append(tests, struct {
		name  string
		start scalar.Scalar
		want  string
	}{name: "binary", start: scalar.NewBinaryScalar(binaryBuffer, arrow.BinaryTypes.Binary), want: `[11]`})
	binaryBuffer.Release()
	for _, tc := range tests {
		if releasable, ok := tc.start.(scalar.Releasable); ok {
			defer releasable.Release()
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, tc.want)
			defer expected.Release()

			result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
				Start: tc.start,
			}, &compute.ArrayDatum{Value: input.Data()})
			require.NoError(t, err)
			defer result.Release()
			assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
		})
	}

	invalidStart := scalar.NewStringScalar("not a number")
	defer invalidStart.Release()
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
		Start: invalidStart,
	}, &compute.ArrayDatum{Value: input.Data()})
	if result != nil {
		result.Release()
	}
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestCumulativeSumDoesNotTakeStartOwnership(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	data := mem.Allocate(2)
	copy(data, "10")
	buffer := memory.NewBufferWithAllocator(data, mem)
	start := scalar.NewBinaryScalar(buffer, arrow.BinaryTypes.Binary)
	buffer.Release()

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1]`)
	defer input.Release()

	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{Start: start},
		&compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	result.Release()

	assert.Equal(t, "10", string(start.Data()))
	start.Release()
}

func TestCumulativeSumDecimalStarts(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	decimalType := &arrow.Decimal128Type{Precision: 10, Scale: 0}
	decimalStart, err := scalar.ParseScalar(decimalType, "10")
	require.NoError(t, err)

	intInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1]`)
	defer intInput.Release()
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{
		Start: decimalStart,
	}, &compute.ArrayDatum{Value: intInput.Data()})
	require.NoError(t, err)
	defer result.Release()
	actual := result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.Equal(t, int32(11), actual.(*array.Int32).Value(0))

	floatInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Float64, `[1.5]`)
	defer floatInput.Release()
	result, err = compute.CumulativeSum(ctx, compute.CumulativeOptions{
		Start: decimalStart,
	}, &compute.ArrayDatum{Value: floatInput.Data()})
	require.NoError(t, err)
	defer result.Release()
	actual = result.(*compute.ArrayDatum).MakeArray()
	defer actual.Release()
	assert.Equal(t, 11.5, actual.(*array.Float64).Value(0))

	fractionalStart, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 10, Scale: 1}, "1.5")
	require.NoError(t, err)
	result, err = compute.CumulativeSum(ctx, compute.CumulativeOptions{
		Start: fractionalStart,
	}, &compute.ArrayDatum{Value: intInput.Data()})
	if result != nil {
		result.Release()
	}
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestCumulativeOptionsSerialization(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	binaryBuffer := memory.NewBufferBytes([]byte("10"))
	binaryStart := scalar.NewBinaryScalar(binaryBuffer, arrow.BinaryTypes.Binary)
	binaryBuffer.Release()

	tests := []struct {
		name  string
		start scalar.Scalar
	}{
		{name: "nil", start: nil},
		{name: "int32", start: scalar.NewInt32Scalar(10)},
		{name: "string", start: scalar.NewStringScalar("10")},
		{name: "binary", start: binaryStart},
		{name: "typed null", start: scalar.MakeNullScalar(arrow.PrimitiveTypes.Int32)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := compute.NewCall("cumulative_sum", []compute.Expression{compute.NewFieldRef("values")},
				&compute.CumulativeOptions{Start: tc.start, SkipNulls: true})

			serialized, err := compute.SerializeExpr(expr, mem)
			require.NoError(t, err)
			roundTripped, err := compute.DeserializeExpr(mem, serialized)
			serialized.Release()
			require.NoError(t, err)

			assert.True(t, expr.Equals(roundTripped))
			assert.NotEmpty(t, roundTripped.String())
			roundTripped.Release()
			expr.Release()
			if releasable, ok := tc.start.(scalar.Releasable); ok {
				releasable.Release()
			}
		})
	}
}

func TestCumulativeOptionsDictionarySerialization(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictValues := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int64, `[10]`)
	defer dictValues.Release()
	dictIndices := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[0]`)
	defer dictIndices.Release()
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.PrimitiveTypes.Int64,
	}
	dict := array.NewDictionaryArray(dictType, dictIndices, dictValues)
	defer dict.Release()

	start, err := scalar.GetScalar(dict, 0)
	require.NoError(t, err)
	defer start.(scalar.Releasable).Release()
	expr := compute.NewCall("cumulative_sum", []compute.Expression{compute.NewFieldRef("values")},
		&compute.CumulativeOptions{Start: start})
	defer expr.Release()

	serialized, err := compute.SerializeExpr(expr, mem)
	require.NoError(t, err)
	defer serialized.Release()

	roundTripped, err := compute.DeserializeExpr(mem, serialized)
	require.NoError(t, err)
	defer roundTripped.Release()

	assert.True(t, expr.Equals(roundTripped))
}

func TestCumulativeSumChunked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	first := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2]`)
	second := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[3, 4]`)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	defer input.Release()
	defer first.Release()
	defer second.Release()

	expectedArray := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6, 10]`)
	defer expectedArray.Release()
	expected := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{expectedArray})
	defer expected.Release()

	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ChunkedDatum{Value: input})
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, compute.KindChunked, result.Kind())
	assertDatumsEqual(t, &compute.ChunkedDatum{Value: expected}, result, nil, nil)

}

func TestCumulativeSumEmptyChunkedInput(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, nil)
	defer input.Release()

	result, err := compute.CumulativeSum(
		context.Background(),
		compute.CumulativeOptions{},
		&compute.ChunkedDatum{Value: input},
	)
	require.NoError(t, err)
	defer result.Release()

	chunked, ok := result.(*compute.ChunkedDatum)
	require.True(t, ok)
	assert.Empty(t, chunked.Value.Chunks())
	assert.Equal(t, int64(0), result.Len())
}

func TestCumulativeSumChunkedOutputIgnoresChunkSizeAndEmptyChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	emptyBefore := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[]`)
	values := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	emptyAfter := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[]`)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{emptyBefore, values, emptyAfter})
	defer input.Release()
	defer emptyBefore.Release()
	defer values.Release()
	defer emptyAfter.Release()

	execCtx := compute.DefaultExecCtx()
	execCtx.ChunkSize = 2
	ctx := compute.SetExecCtx(context.Background(), execCtx)
	ctx = compute.WithAllocator(ctx, mem)

	expectedArray := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, 3, 6, 10]`)
	defer expectedArray.Release()
	expected := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{expectedArray})
	defer expected.Release()
	result, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ChunkedDatum{Value: input})
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, compute.KindChunked, result.Kind())
	assertDatumsEqual(t, &compute.ChunkedDatum{Value: expected}, result, nil, nil)
}

func TestCumulativeSumStateAcrossChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	first := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null]`)
	second := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[2, 3]`)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	defer input.Release()
	defer first.Release()
	defer second.Release()

	for _, tc := range []struct {
		name     string
		opts     compute.CumulativeOptions
		expected string
	}{
		{name: "propagate nulls", expected: `[1, null, null, null]`},
		{name: "skip nulls", opts: compute.CumulativeOptions{SkipNulls: true}, expected: `[1, null, 3, 6]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedArray := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, tc.expected)
			defer expectedArray.Release()
			expected := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{expectedArray})
			defer expected.Release()

			result, err := compute.CumulativeSum(ctx, tc.opts, &compute.ChunkedDatum{Value: input})
			require.NoError(t, err)
			defer result.Release()
			require.Equal(t, compute.KindChunked, result.Kind())
			assertDatumsEqual(t, &compute.ChunkedDatum{Value: expected}, result, nil, nil)
		})
	}
}

func TestCumulativeSumCheckedChunkedOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	first := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[127]`)
	second := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[1]`)
	input := arrow.NewChunked(arrow.PrimitiveTypes.Int8, []arrow.Array{first, second})
	defer input.Release()
	defer first.Release()
	defer second.Release()

	ctx := compute.WithAllocator(context.Background(), mem)
	result, err := compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{}, &compute.ChunkedDatum{Value: input})
	assert.Nil(t, result)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestCumulativeSumIgnoresExecutorChunkSize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, `[1, null, 2, 3]`)
	defer input.Release()

	execCtx := compute.DefaultExecCtx()
	execCtx.ChunkSize = 1
	ctx := compute.WithAllocator(context.Background(), mem)
	ctx = compute.SetExecCtx(ctx, execCtx)

	tests := []struct {
		name     string
		opts     compute.CumulativeOptions
		expected string
	}{
		{name: "propagate nulls", expected: `[1, null, null, null]`},
		{name: "skip nulls", opts: compute.CumulativeOptions{SkipNulls: true}, expected: `[1, null, 3, 6]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expectedArray := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int32, tc.expected)
			defer expectedArray.Release()

			result, err := compute.CumulativeSum(ctx, tc.opts, &compute.ArrayDatum{Value: input.Data()})
			require.NoError(t, err)
			defer result.Release()
			require.Equal(t, compute.KindArray, result.Kind())
			assertDatumsEqual(t, &compute.ArrayDatum{Value: expectedArray.Data()}, result, nil, nil)
		})
	}

	overflowInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[127, 1]`)
	defer overflowInput.Release()
	result, err := compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: overflowInput.Data()})
	if result != nil {
		result.Release()
	}
	assert.ErrorIs(t, err, arrow.ErrInvalid)

	startOverflowInput := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[1]`)
	defer startOverflowInput.Release()
	result, err = compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{
		Start: scalar.NewInt8Scalar(127),
	}, &compute.ArrayDatum{Value: startOverflowInput.Data()})
	if result != nil {
		result.Release()
	}
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestCumulativeSumChecked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)
	input := cumulativeInput(t, mem, arrow.PrimitiveTypes.Int8, `[127, 1]`)
	defer input.Release()

	unchecked, err := compute.CumulativeSum(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	require.NoError(t, err)
	defer unchecked.Release()
	uncheckedArray := unchecked.(*compute.ArrayDatum).MakeArray()
	defer uncheckedArray.Release()
	assert.Equal(t, int8(-128), uncheckedArray.(*array.Int8).Value(1))

	_, err = compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{}, &compute.ArrayDatum{Value: input.Data()})
	assert.ErrorIs(t, err, arrow.ErrInvalid)

}

func TestCumulativeSumCheckedIntegerOverflow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	tests := []struct {
		name   string
		typ    arrow.DataType
		values string
	}{
		{name: "int8 positive", typ: arrow.PrimitiveTypes.Int8, values: `[127, 1]`},
		{name: "int8 negative", typ: arrow.PrimitiveTypes.Int8, values: `[-128, -1]`},
		{name: "int16 positive", typ: arrow.PrimitiveTypes.Int16, values: `[32767, 1]`},
		{name: "int16 negative", typ: arrow.PrimitiveTypes.Int16, values: `[-32768, -1]`},
		{name: "int32 positive", typ: arrow.PrimitiveTypes.Int32, values: `[2147483647, 1]`},
		{name: "int32 negative", typ: arrow.PrimitiveTypes.Int32, values: `[-2147483648, -1]`},
		{name: "int64 positive", typ: arrow.PrimitiveTypes.Int64, values: `[9223372036854775807, 1]`},
		{name: "int64 negative", typ: arrow.PrimitiveTypes.Int64, values: `[-9223372036854775808, -1]`},
		{name: "uint8", typ: arrow.PrimitiveTypes.Uint8, values: `[255, 1]`},
		{name: "uint16", typ: arrow.PrimitiveTypes.Uint16, values: `[65535, 1]`},
		{name: "uint32", typ: arrow.PrimitiveTypes.Uint32, values: `[4294967295, 1]`},
		{name: "uint64", typ: arrow.PrimitiveTypes.Uint64, values: `[18446744073709551615, 1]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, tc.values)
			defer input.Release()

			result, err := compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{},
				&compute.ArrayDatum{Value: input.Data()})
			if result != nil {
				result.Release()
			}
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestCumulativeSumCheckedIntegerBoundaries(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	tests := []struct {
		name     string
		typ      arrow.DataType
		input    string
		expected string
	}{
		{name: "int8 positive", typ: arrow.PrimitiveTypes.Int8, input: `[126, 1]`, expected: `[126, 127]`},
		{name: "int8 negative", typ: arrow.PrimitiveTypes.Int8, input: `[-127, -1]`, expected: `[-127, -128]`},
		{name: "int64 negative", typ: arrow.PrimitiveTypes.Int64, input: `[-9223372036854775807, -1]`, expected: `[-9223372036854775807, -9223372036854775808]`},
		{name: "uint8", typ: arrow.PrimitiveTypes.Uint8, input: `[254, 1]`, expected: `[254, 255]`},
		{name: "uint64", typ: arrow.PrimitiveTypes.Uint64, input: `[18446744073709551614, 1]`, expected: `[18446744073709551614, 18446744073709551615]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := cumulativeInput(t, mem, tc.typ, tc.input)
			defer input.Release()
			expected := cumulativeInput(t, mem, tc.typ, tc.expected)
			defer expected.Release()

			result, err := compute.CumulativeSumChecked(ctx, compute.CumulativeOptions{},
				&compute.ArrayDatum{Value: input.Data()})
			require.NoError(t, err)
			defer result.Release()
			assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, result, nil, nil)
		})
	}
}
