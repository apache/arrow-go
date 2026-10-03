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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The expected values in this file were produced by the C++ implementation
// through pyarrow for the same fixtures and options.

func aggArray(t *testing.T, mem memory.Allocator, dt arrow.DataType, jsonVals string) arrow.Array {
	t.Helper()
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(jsonVals))
	require.NoError(t, err)
	return arr
}

// callAgg calls the named aggregate function and returns the scalar it
// produced. Aggregate results are scalars for every kernel in this file.
func callAgg(t *testing.T, ctx context.Context, fname string, opts compute.FunctionOptions, d compute.Datum) scalar.Scalar {
	t.Helper()
	res, err := compute.CallFunction(ctx, fname, opts, d)
	require.NoError(t, err)
	defer res.Release()

	sd, ok := res.(*compute.ScalarDatum)
	require.Truef(t, ok, "expected a scalar datum, got %T", res)
	return sd.Value
}

func aggOf(t *testing.T, ctx context.Context, fname string, opts compute.FunctionOptions, mem memory.Allocator, dt arrow.DataType, jsonVals string) scalar.Scalar {
	t.Helper()
	arr := aggArray(t, mem, dt, jsonVals)
	defer arr.Release()
	return callAgg(t, ctx, fname, opts, compute.NewDatumWithoutOwning(arr))
}

func assertNullResult(t *testing.T, sc scalar.Scalar, dt arrow.DataType) {
	t.Helper()
	assert.Falsef(t, sc.IsValid(), "expected a null result, got %s", sc)
	assert.Truef(t, arrow.TypeEqual(dt, sc.DataType()), "expected type %s, got %s", dt, sc.DataType())
}

func assertInt64Result(t *testing.T, sc scalar.Scalar, want int64) {
	t.Helper()
	v, ok := sc.(*scalar.Int64)
	require.Truef(t, ok, "expected an int64 scalar, got %T", sc)
	require.True(t, v.IsValid(), "expected a valid result")
	assert.Equal(t, want, v.Value)
}

func assertUint64Result(t *testing.T, sc scalar.Scalar, want uint64) {
	t.Helper()
	v, ok := sc.(*scalar.Uint64)
	require.Truef(t, ok, "expected a uint64 scalar, got %T", sc)
	require.True(t, v.IsValid(), "expected a valid result")
	assert.Equal(t, want, v.Value)
}

func assertFloat64Result(t *testing.T, sc scalar.Scalar, want float64) {
	t.Helper()
	v, ok := sc.(*scalar.Float64)
	require.Truef(t, ok, "expected a float64 scalar, got %T", sc)
	require.True(t, v.IsValid(), "expected a valid result")
	assert.Equal(t, want, v.Value)
}

func aggTestContext(t *testing.T) (context.Context, *memory.CheckedAllocator) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return compute.WithAllocator(context.Background(), mem), mem
}

func skipNulls(skip bool, minCount uint32) *compute.ScalarAggregateOptions {
	return &compute.ScalarAggregateOptions{SkipNulls: skip, MinCount: minCount}
}

func TestScalarAggregateRegistered(t *testing.T) {
	reg := compute.GetFunctionRegistry()
	for _, name := range []string{"count", "sum"} {
		fn, ok := reg.GetFunction(name)
		require.Truef(t, ok, "function %s is not registered", name)
		assert.Equal(t, compute.FuncScalarAgg, fn.Kind())
		assert.Equal(t, compute.Unary(), fn.Arity())
		assert.NoError(t, fn.Validate())
		assert.NotNil(t, fn.DefaultOptions())
		assert.Greater(t, fn.NumKernels(), 0)
	}

	// exact dispatch: sum has no kernel for a type it does not accept
	sumFn, _ := reg.GetFunction("sum")
	_, err := sumFn.DispatchBest(arrow.BinaryTypes.String)
	assert.ErrorIs(t, err, arrow.ErrNotImplemented)
	_, err = sumFn.DispatchBest(arrow.FixedWidthTypes.Float16)
	assert.ErrorIs(t, err, arrow.ErrNotImplemented)
	k, err := sumFn.DispatchBest(arrow.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.IsType(t, (*exec.ScalarAggKernel)(nil), k)

	// count takes any input type
	countFn, _ := reg.GetFunction("count")
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32,
		arrow.Null, arrow.FixedWidthTypes.Boolean} {
		_, err := countFn.DispatchBest(dt)
		assert.NoErrorf(t, err, "count should accept %s", dt)
	}
}

func TestScalarAggregateFunctionAddKernel(t *testing.T) {
	fn := compute.NewScalarAggregateFunction("test_add_kernel", compute.Unary(), compute.EmptyFuncDoc)

	in := []exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Int64)}
	out := exec.NewOutputType(arrow.PrimitiveTypes.Int64)
	init := func(*exec.KernelCtx, exec.KernelInitArgs) (exec.KernelState, error) { return struct{}{}, nil }
	consume := func(*exec.KernelCtx, *exec.ExecSpan) error { return nil }
	merge := func(*exec.KernelCtx, exec.KernelState, exec.KernelState) error { return nil }
	finalize := func(*exec.KernelCtx) (*exec.AggregateResult, error) { return nil, nil }

	// the arity has to match
	assert.ErrorIs(t, fn.AddNewKernel(nil, out, init, consume, merge, finalize), arrow.ErrInvalid)

	// every one of the four lifecycle functions is required
	assert.ErrorIs(t, fn.AddNewKernel(in, out, nil, consume, merge, finalize), arrow.ErrInvalid)
	assert.ErrorIs(t, fn.AddNewKernel(in, out, init, nil, merge, finalize), arrow.ErrInvalid)
	assert.ErrorIs(t, fn.AddNewKernel(in, out, init, consume, nil, finalize), arrow.ErrInvalid)
	assert.ErrorIs(t, fn.AddNewKernel(in, out, init, consume, merge, nil), arrow.ErrInvalid)
	assert.Zero(t, fn.NumKernels())

	require.NoError(t, fn.AddNewKernel(in, out, init, consume, merge, finalize))
	assert.Equal(t, 1, fn.NumKernels())
	assert.False(t, fn.Kernels()[0].IsOrdered())
}

func TestScalarAggregateOptionsForms(t *testing.T) {
	ctx, mem := aggTestContext(t)

	const vals = `[1, null, 3, -2]`
	// nil options select the defaults: skip nulls, min_count 1
	assertInt64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Int64, vals), 2)
	assertInt64Result(t, aggOf(t, ctx, "sum", compute.DefaultScalarAggregateOptions(), mem, arrow.PrimitiveTypes.Int64, vals), 2)
	assert.Equal(t, &compute.ScalarAggregateOptions{SkipNulls: true, MinCount: 1}, compute.DefaultScalarAggregateOptions())
	assert.Equal(t, &compute.CountOptions{Mode: compute.CountOnlyValid}, compute.DefaultCountOptions())

	// an explicitly supplied zero value stays {false, 0} and is not
	// silently turned into the defaults
	assertNullResult(t, aggOf(t, ctx, "sum", &compute.ScalarAggregateOptions{}, mem, arrow.PrimitiveTypes.Int64, vals),
		arrow.PrimitiveTypes.Int64)

	// options are accepted by pointer and by value
	arr := aggArray(t, mem, arrow.PrimitiveTypes.Int64, vals)
	defer arr.Release()
	d := compute.NewDatumWithoutOwning(arr)
	assertInt64Result(t, callAgg(t, ctx, "sum", *skipNulls(true, 1), d), 2)
	assertInt64Result(t, callAgg(t, ctx, "count", compute.CountOptions{Mode: compute.CountAllRows}, d), 4)

	// and the option types are registered for deserialization
	assert.Equal(t, "ScalarAggregateOptions", compute.ScalarAggregateOptions{}.TypeName())
	assert.Equal(t, "CountOptions", compute.CountOptions{}.TypeName())

	_, err := compute.CallFunction(ctx, "sum", &compute.CountOptions{}, d)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	_, err = compute.CallFunction(ctx, "count", &compute.CountOptions{Mode: compute.CountMode(17)}, d)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestScalarAggregateSumNumericTypes(t *testing.T) {
	ctx, mem := aggTestContext(t)

	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64} {
		t.Run(dt.String(), func(t *testing.T) {
			sc := aggOf(t, ctx, "sum", nil, mem, dt, `[1, null, 3, -2]`)
			assertInt64Result(t, sc, 2)
			assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, dt, `[1, null, 3, -2]`), 3)
		})
	}

	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64} {
		t.Run(dt.String(), func(t *testing.T) {
			assertUint64Result(t, aggOf(t, ctx, "sum", nil, mem, dt, `[1, null, 3, 7]`), 11)
			assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, dt, `[1, null, 3, 7]`), 3)
		})
	}

	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		t.Run(dt.String(), func(t *testing.T) {
			assertFloat64Result(t, aggOf(t, ctx, "sum", nil, mem, dt, `[1.5, null, -2.5, 4.0]`), 3.0)
			assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, dt, `[1.5, null, -2.5, 4.0]`), 3)
		})
	}

	t.Run("bool", func(t *testing.T) {
		const vals = `[true, null, false, true]`
		assertUint64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.FixedWidthTypes.Boolean, vals), 2)
		assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, arrow.FixedWidthTypes.Boolean, vals), 3)
	})
}

func TestScalarAggregateSumNullType(t *testing.T) {
	ctx, _ := aggTestContext(t)

	nulls := array.NewNull(3)
	defer nulls.Release()
	d := compute.NewDatumWithoutOwning(nulls)

	assertNullResult(t, callAgg(t, ctx, "sum", nil, d), arrow.PrimitiveTypes.Int64)
	assertInt64Result(t, callAgg(t, ctx, "sum", skipNulls(true, 0), d), 0)
	assertNullResult(t, callAgg(t, ctx, "sum", skipNulls(false, 0), d), arrow.PrimitiveTypes.Int64)

	assertInt64Result(t, callAgg(t, ctx, "count", nil, d), 0)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, d), 3)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, d), 3)

	// an empty null array never saw a value, so even skip_nulls=false sums
	// to zero when min_count allows it
	empty := array.NewNull(0)
	defer empty.Release()
	assertInt64Result(t, callAgg(t, ctx, "sum", skipNulls(false, 0), compute.NewDatumWithoutOwning(empty)), 0)
}

func TestScalarAggregateSumNoNullsAllNullEmpty(t *testing.T) {
	ctx, mem := aggTestContext(t)
	i64 := arrow.PrimitiveTypes.Int64

	t.Run("no nulls", func(t *testing.T) {
		const vals = `[4, 5, 6]`
		assertInt64Result(t, aggOf(t, ctx, "sum", nil, mem, i64, vals), 15)
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(true, 0), mem, i64, vals), 15)
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(false, 1), mem, i64, vals), 15)
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(false, 0), mem, i64, vals), 15)
		assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, i64, vals), 3)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, mem, i64, vals), 0)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, mem, i64, vals), 3)
	})

	t.Run("all null", func(t *testing.T) {
		const vals = `[null, null]`
		assertNullResult(t, aggOf(t, ctx, "sum", nil, mem, i64, vals), i64)
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(true, 0), mem, i64, vals), 0)
		assertNullResult(t, aggOf(t, ctx, "sum", skipNulls(false, 1), mem, i64, vals), i64)
		assertNullResult(t, aggOf(t, ctx, "sum", skipNulls(false, 0), mem, i64, vals), i64)
		assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, i64, vals), 0)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, mem, i64, vals), 2)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, mem, i64, vals), 2)
	})

	t.Run("empty", func(t *testing.T) {
		const vals = `[]`
		assertNullResult(t, aggOf(t, ctx, "sum", nil, mem, i64, vals), i64)
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(true, 0), mem, i64, vals), 0)
		assertNullResult(t, aggOf(t, ctx, "sum", skipNulls(false, 1), mem, i64, vals), i64)
		// nothing was consumed, so no null was observed either
		assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(false, 0), mem, i64, vals), 0)
		assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, i64, vals), 0)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, mem, i64, vals), 0)
		assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, mem, i64, vals), 0)
	})
}

func TestScalarAggregateSkipNullsAndMinCount(t *testing.T) {
	ctx, mem := aggTestContext(t)
	i64 := arrow.PrimitiveTypes.Int64
	const vals = `[1, null, 3, -2]`

	assertNullResult(t, aggOf(t, ctx, "sum", skipNulls(false, 1), mem, i64, vals), i64)
	assertInt64Result(t, aggOf(t, ctx, "sum", skipNulls(true, 3), mem, i64, vals), 2)
	assertNullResult(t, aggOf(t, ctx, "sum", skipNulls(true, 4), mem, i64, vals), i64)

	// count ignores ScalarAggregateOptions entirely
	assertInt64Result(t, aggOf(t, ctx, "count", nil, mem, i64, vals), 3)
	assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, mem, i64, vals), 1)
	assertInt64Result(t, aggOf(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, mem, i64, vals), 4)
}

func TestScalarAggregateSumWraps(t *testing.T) {
	ctx, mem := aggTestContext(t)

	assertInt64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Int64,
		fmt.Sprintf(`[%d, 1]`, int64(math.MaxInt64))), math.MinInt64)
	assertUint64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Uint64,
		fmt.Sprintf(`[%d, 2]`, uint64(math.MaxUint64))), 1)
	assertUint64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Uint64,
		fmt.Sprintf(`[%d, 1]`, uint64(math.MaxUint64))), 0)
	// a narrow integer widens into its accumulator instead of wrapping
	assertInt64Result(t, aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Int8, `[127, 1]`), 128)
}

func TestScalarAggregateSumFloatNaN(t *testing.T) {
	ctx, mem := aggTestContext(t)

	sc := aggOf(t, ctx, "sum", nil, mem, arrow.PrimitiveTypes.Float64, `[null, 1.0]`)
	assertFloat64Result(t, sc, 1.0)

	arr := aggArray(t, mem, arrow.PrimitiveTypes.Float64, `[1.0, 2.0]`)
	defer arr.Release()
	nanArr := array.NewFloat64Builder(mem)
	defer nanArr.Release()
	nanArr.AppendValues([]float64{math.NaN(), 1.0}, nil)
	withNaN := nanArr.NewArray()
	defer withNaN.Release()

	res := callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(withNaN))
	v, ok := res.(*scalar.Float64)
	require.True(t, ok)
	assert.True(t, math.IsNaN(v.Value))
}

func TestScalarAggregateCountTypes(t *testing.T) {
	ctx, mem := aggTestContext(t)

	strs := aggArray(t, mem, arrow.BinaryTypes.String, `["a", null, "c"]`)
	defer strs.Release()
	d := compute.NewDatumWithoutOwning(strs)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, d), 2)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, d), 1)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, d), 3)
}

// The logical null count of a run-end encoded or dictionary array is not the
// number of unset bits in its own validity bitmap, so count rejects those
// inputs rather than returning a wrong answer.
func TestScalarAggregateCountLogicalNulls(t *testing.T) {
	ctx, mem := aggTestContext(t)

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()
	require.NoError(t, bldr.AppendValueFromString("a"))
	bldr.AppendNull()
	dict := bldr.NewArray()
	defer dict.Release()

	d := compute.NewDatumWithoutOwning(dict)
	_, err := compute.CallFunction(ctx, "count", nil, d)
	assert.ErrorIs(t, err, arrow.ErrNotImplemented)

	// counting every row does not need a null count at all
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, d), 2)
}

func TestScalarAggregateScalarInput(t *testing.T) {
	ctx, _ := aggTestContext(t)

	five := compute.NewDatum(scalar.NewInt64Scalar(5))
	defer five.Release()
	assertInt64Result(t, callAgg(t, ctx, "sum", nil, five), 5)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, five), 1)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, five), 0)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, five), 1)

	nullScalar := compute.NewDatum(scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64))
	defer nullScalar.Release()
	assertNullResult(t, callAgg(t, ctx, "sum", nil, nullScalar), arrow.PrimitiveTypes.Int64)
	assertInt64Result(t, callAgg(t, ctx, "sum", skipNulls(true, 0), nullScalar), 0)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, nullScalar), 0)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, nullScalar), 1)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, nullScalar), 1)

	boolTrue := compute.NewDatum(scalar.NewBooleanScalar(true))
	defer boolTrue.Release()
	assertUint64Result(t, callAgg(t, ctx, "sum", nil, boolTrue), 1)

	// a narrow input is still summed into its accumulator type
	three := compute.NewDatum(scalar.NewInt8Scalar(3))
	defer three.Release()
	assertInt64Result(t, callAgg(t, ctx, "sum", nil, three), 3)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, three), 1)
}

func TestScalarAggregateChunked(t *testing.T) {
	ctx, mem := aggTestContext(t)
	i64 := arrow.PrimitiveTypes.Int64

	chunks := make([]arrow.Array, 0, 3)
	for _, vals := range []string{`[1, 2]`, `[null, 4]`, `[5]`} {
		chunks = append(chunks, aggArray(t, mem, i64, vals))
	}
	chunked := arrow.NewChunked(i64, chunks)
	for _, c := range chunks {
		c.Release()
	}
	defer chunked.Release()

	d := compute.NewDatumWithoutOwning(chunked)
	assertInt64Result(t, callAgg(t, ctx, "sum", nil, d), 12)
	assertNullResult(t, callAgg(t, ctx, "sum", skipNulls(false, 1), d), i64)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, d), 4)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, d), 1)
	assertInt64Result(t, callAgg(t, ctx, "count", &compute.CountOptions{Mode: compute.CountAllRows}, d), 5)

	emptyChunked := arrow.NewChunked(i64, nil)
	defer emptyChunked.Release()
	ed := compute.NewDatumWithoutOwning(emptyChunked)
	assertNullResult(t, callAgg(t, ctx, "sum", nil, ed), i64)
	assertInt64Result(t, callAgg(t, ctx, "sum", skipNulls(true, 0), ed), 0)
	assertInt64Result(t, callAgg(t, ctx, "count", nil, ed), 0)
}

// xorshiftDoubles reproduces the fixture the C++ results were taken from.
func xorshiftDoubles(n int) []float64 {
	s := uint64(0x9E3779B97F4A7C15)
	out := make([]float64, n)
	for i := range out {
		s ^= s >> 12
		s ^= s << 25
		s ^= s >> 27
		r := s * 0x2545F4914F6CDD1D
		u := float64(r>>11) / float64(uint64(1)<<53)
		out[i] = (u - 0.5) * 1000.0
	}
	return out
}

// TestScalarAggregatePairwiseSum pins the float sum to the pairwise summation
// the C++ implementation uses. A naive left to right sum of the same fixture
// gives 75247.35643756675, so this test distinguishes the two.
func TestScalarAggregatePairwiseSum(t *testing.T) {
	ctx, mem := aggTestContext(t)

	vals := xorshiftDoubles(100001)
	valid := make([]bool, len(vals))
	for i := range valid {
		valid[i] = i%7 != 6
	}

	f64Bldr := array.NewFloat64Builder(mem)
	defer f64Bldr.Release()
	f64Bldr.AppendValues(vals, nil)
	f64 := f64Bldr.NewArray()
	defer f64.Release()

	f64Bldr.AppendValues(vals, valid)
	f64n := f64Bldr.NewArray()
	defer f64n.Release()

	f32Bldr := array.NewFloat32Builder(mem)
	defer f32Bldr.Release()
	f32vals := make([]float32, len(vals))
	i32vals := make([]int32, len(vals))
	for i, v := range vals {
		f32vals[i] = float32(v)
		i32vals[i] = int32(v)
	}
	f32Bldr.AppendValues(f32vals, nil)
	f32 := f32Bldr.NewArray()
	defer f32.Release()

	i32Bldr := array.NewInt32Builder(mem)
	defer i32Bldr.Release()
	i32Bldr.AppendValues(i32vals, nil)
	i32 := i32Bldr.NewArray()
	defer i32.Release()

	assertFloat64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(f64)), 75247.35643756694)

	// the pinned value is the pairwise one: a naive left-to-right sum of the
	// same fixture lands on a different double, so the assertions above can
	// only pass with the C++ summation order
	var naive float64
	for _, v := range vals {
		naive += v
	}
	assert.NotEqual(t, 75247.35643756694, naive)
	assertFloat64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(f64n)), 94384.73372326966)
	assertFloat64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(f32)), 75247.3534175111)
	assertInt64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(i32)), 75006)

	// slicing shifts the offset without changing the summation order
	sliced := array.NewSlice(f64, 5, int64(f64.Len()))
	defer sliced.Release()
	assertFloat64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(sliced)), 75650.35881616312)

	slicedNulls := array.NewSlice(f64n, 5, int64(f64n.Len()))
	defer slicedNulls.Release()
	assertFloat64Result(t, callAgg(t, ctx, "sum", nil, compute.NewDatumWithoutOwning(slicedNulls)), 94787.73610186583)
}

// TestScalarAggregateChunkSizeInvariance runs every aggregate over the same
// input at chunk sizes 1 to 65 and compares against the single-span result.
// A kernel which cached a null count back into the span the executor re-slices
// would fail here without failing any of the tests above.
func TestScalarAggregateChunkSizeInvariance(t *testing.T) {
	ctx, mem := aggTestContext(t)

	cases := []struct {
		dt   arrow.DataType
		vals string
	}{
		{arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, null, 7, 8, null, 10, 11, 12, null, 14, 15, 16, 17, null, 19, 20]`},
		{arrow.PrimitiveTypes.Uint32, `[1, null, 3, 7, 5, null, 7, 8, null, 10, 11, 12, null, 14, 15, 16, 17, null, 19, 20]`},
		{arrow.PrimitiveTypes.Float64, `[1.5, null, -2.5, 4.0, 5.5, null, 7.25, 8.125, null, 10.0, 11, 12, null, 14, 15, 16, 17, null, 19, 20]`},
		{arrow.FixedWidthTypes.Boolean, `[true, null, false, true, true, null, false, true, null, true, false, true, null, true, true, false, true, null, false, true]`},
		{arrow.BinaryTypes.String, `["a", null, "c", "d", "e", null, "g", "h", null, "j", "k", "l", null, "n", "o", "p", "q", null, "s", "t"]`},
	}

	optSets := []compute.FunctionOptions{nil, skipNulls(true, 0), skipNulls(false, 1), skipNulls(true, 21)}
	countOpts := []compute.FunctionOptions{
		&compute.CountOptions{Mode: compute.CountOnlyValid},
		&compute.CountOptions{Mode: compute.CountOnlyNull},
		&compute.CountOptions{Mode: compute.CountAllRows},
	}

	for _, tc := range cases {
		arr := aggArray(t, mem, tc.dt, tc.vals)
		d := compute.NewDatumWithoutOwning(arr)

		type invocation struct {
			fn   string
			opts compute.FunctionOptions
		}
		invocations := make([]invocation, 0, len(optSets)+len(countOpts))
		for _, o := range countOpts {
			invocations = append(invocations, invocation{"count", o})
		}
		if tc.dt.ID() != arrow.STRING {
			for _, o := range optSets {
				invocations = append(invocations, invocation{"sum", o})
			}
		}

		for _, inv := range invocations {
			want := callAgg(t, ctx, inv.fn, inv.opts, d)
			for chunkSize := int64(1); chunkSize <= 65; chunkSize++ {
				ectx := compute.DefaultExecCtx()
				ectx.ChunkSize = chunkSize
				got := callAgg(t, compute.SetExecCtx(ctx, ectx), inv.fn, inv.opts, d)
				assert.Truef(t, scalar.Equals(want, got),
					"%s over %s at chunk size %d: expected %s, got %s", inv.fn, tc.dt, chunkSize, want, got)
			}
		}
		arr.Release()
	}
}

// TestScalarAggregateMerge consumes the two halves of an input into separate
// states and merges them, checking that the merged state finalizes to the
// same value the executor produces for the whole input.
func TestScalarAggregateMerge(t *testing.T) {
	ctx, mem := aggTestContext(t)

	cases := []struct {
		fn   string
		dt   arrow.DataType
		vals string
		opts any
	}{
		{"sum", arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, 6]`, skipNulls(true, 1)},
		{"sum", arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, 6]`, skipNulls(false, 1)},
		{"sum", arrow.PrimitiveTypes.Uint16, `[1, null, 3, 7, 5, 6]`, skipNulls(true, 1)},
		{"sum", arrow.PrimitiveTypes.Float64, `[1.5, null, -2.5, 4.0, 5.5, 6.25]`, skipNulls(true, 1)},
		{"sum", arrow.FixedWidthTypes.Boolean, `[true, null, false, true, true, false]`, skipNulls(true, 1)},
		{"count", arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, 6]`, &compute.CountOptions{Mode: compute.CountOnlyValid}},
		{"count", arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, 6]`, &compute.CountOptions{Mode: compute.CountOnlyNull}},
		{"count", arrow.PrimitiveTypes.Int64, `[1, null, 3, -2, 5, 6]`, &compute.CountOptions{Mode: compute.CountAllRows}},
	}

	reg := compute.GetFunctionRegistry()
	for _, tc := range cases {
		t.Run(tc.fn+"/"+tc.dt.String(), func(t *testing.T) {
			arr := aggArray(t, mem, tc.dt, tc.vals)
			defer arr.Release()

			fn, ok := reg.GetFunction(tc.fn)
			require.True(t, ok)
			k, err := fn.DispatchBest(tc.dt)
			require.NoError(t, err)
			aggKernel := k.(*exec.ScalarAggKernel)

			kctx := &exec.KernelCtx{Ctx: ctx, Kernel: aggKernel}
			initArgs := exec.KernelInitArgs{Kernel: aggKernel, Inputs: []arrow.DataType{tc.dt}, Options: tc.opts}

			states := make([]exec.KernelState, 0, 2)
			for _, half := range [][2]int64{{0, 3}, {3, 6}} {
				state, err := aggKernel.GetInitFn()(kctx, initArgs)
				require.NoError(t, err)

				slice := array.NewSlice(arr, half[0], half[1])
				span := exec.ExecSpan{Len: int64(slice.Len()), Values: make([]exec.ExecValue, 1)}
				span.Values[0].Array.SetMembers(slice.Data())

				stateCtx := &exec.KernelCtx{Ctx: ctx, Kernel: aggKernel, State: state}
				require.NoError(t, aggKernel.Consume(stateCtx, &span))
				slice.Release()
				states = append(states, state)
			}

			merged, err := exec.MergeAll(aggKernel, kctx, states)
			require.NoError(t, err)
			kctx.State = merged
			res, err := aggKernel.Finalize(kctx)
			require.NoError(t, err)
			defer res.Release()

			var opts compute.FunctionOptions
			if o, ok := tc.opts.(compute.FunctionOptions); ok {
				opts = o
			}
			want := callAgg(t, ctx, tc.fn, opts, compute.NewDatumWithoutOwning(arr))
			assert.Truef(t, scalar.Equals(want, res.Scalar()), "expected %s, got %s", want, res.Scalar())
		})
	}
}

func TestScalarAggregateWrappers(t *testing.T) {
	ctx, mem := aggTestContext(t)

	arr := aggArray(t, mem, arrow.PrimitiveTypes.Int64, `[1, null, 3, -2]`)
	defer arr.Release()
	d := compute.NewDatumWithoutOwning(arr)

	res, err := compute.Sum(ctx, *compute.DefaultScalarAggregateOptions(), d)
	require.NoError(t, err)
	assertInt64Result(t, res.(*compute.ScalarDatum).Value, 2)
	res.Release()

	res, err = compute.Count(ctx, *compute.DefaultCountOptions(), d)
	require.NoError(t, err)
	assertInt64Result(t, res.(*compute.ScalarDatum).Value, 3)
	res.Release()

	res, err = compute.Count(ctx, compute.CountOptions{Mode: compute.CountAllRows}, d)
	require.NoError(t, err)
	assertInt64Result(t, res.(*compute.ScalarDatum).Value, 4)
	res.Release()
}

// ----------------------------------------------------------------------
// framework tests which the primitive count and sum kernels cannot expose

var errTestAgg = errors.New("test aggregate failure")

type testAggBehaviour struct {
	consumeErr  error
	finalizeErr error
	initErr     error
	cleanupErr  error
	arrayResult bool
	cleanups    *int
}

type testAggState struct {
	behaviour *testAggBehaviour
	mem       memory.Allocator
	// buf is owned by the state and released by the cleanup function; the
	// result finalize returns must stay valid after that
	buf   *memory.Buffer
	total int64
}

// newTestAggFunction registers an aggregate function whose result is backed
// by allocated memory, so that the ownership rule of Finalize is exercised.
func newTestAggFunction(t *testing.T, name string, behaviour *testAggBehaviour, mem memory.Allocator) *compute.ScalarAggregateFunction {
	t.Helper()

	outType := arrow.DataType(arrow.BinaryTypes.String)
	if behaviour.arrayResult {
		outType = arrow.PrimitiveTypes.Int64
	}

	fn := compute.NewScalarAggregateFunction(name, compute.Unary(), compute.EmptyFuncDoc)
	kernel := exec.NewScalarAggKernel(
		[]exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Int64)},
		exec.NewOutputType(outType),
		func(*exec.KernelCtx, exec.KernelInitArgs) (exec.KernelState, error) {
			if behaviour.initErr != nil {
				return nil, behaviour.initErr
			}
			buf := memory.NewResizableBuffer(mem)
			buf.Resize(8)
			return &testAggState{behaviour: behaviour, mem: mem, buf: buf}, nil
		},
		func(ctx *exec.KernelCtx, span *exec.ExecSpan) error {
			st := ctx.State.(*testAggState)
			if st.behaviour.consumeErr != nil {
				return st.behaviour.consumeErr
			}
			st.total += span.Len
			return nil
		},
		func(_ *exec.KernelCtx, src, dst exec.KernelState) error {
			dst.(*testAggState).total += src.(*testAggState).total
			return nil
		},
		func(ctx *exec.KernelCtx) (*exec.AggregateResult, error) {
			st := ctx.State.(*testAggState)
			if st.behaviour.finalizeErr != nil {
				return nil, st.behaviour.finalizeErr
			}
			if st.behaviour.arrayResult {
				bldr := array.NewInt64Builder(st.mem)
				defer bldr.Release()
				bldr.Append(st.total)
				arr := bldr.NewArray()
				defer arr.Release()
				arr.Data().Retain()
				return exec.NewArrayResult(arr.Data()), nil
			}
			// a buffer-backed scalar which must outlive the state's own
			// buffer: the result takes its own reference
			out := memory.NewResizableBuffer(st.mem)
			defer out.Release()
			text := fmt.Sprintf("total=%d", st.total)
			out.Resize(len(text))
			copy(out.Bytes(), text)
			return exec.NewScalarResult(scalar.NewStringScalarFromBuffer(out)), nil
		})
	kernel.CleanupFn = func(_ *exec.KernelCtx, state exec.KernelState) error {
		if state != nil {
			state.(*testAggState).buf.Release()
		}
		*behaviour.cleanups++
		return behaviour.cleanupErr
	}
	require.NoError(t, fn.AddKernel(kernel))
	return fn
}

func testAggContext(t *testing.T, ctx context.Context, fn compute.Function) context.Context {
	t.Helper()
	reg := compute.NewChildRegistry(compute.GetFunctionRegistry())
	require.True(t, reg.AddFunction(fn, false))
	ectx := compute.DefaultExecCtx()
	ectx.Registry = reg
	return compute.SetExecCtx(ctx, ectx)
}

// TestScalarAggregateResultOwnership checks that the value finalize returned
// outlives the cleanup of the aggregate state which produced it, both for a
// buffer-backed scalar and for an array result.
func TestScalarAggregateResultOwnership(t *testing.T) {
	for _, arrayResult := range []bool{false, true} {
		name := "scalar result"
		if arrayResult {
			name = "array result"
		}
		t.Run(name, func(t *testing.T) {
			ctx, mem := aggTestContext(t)

			var cleanups int
			behaviour := &testAggBehaviour{arrayResult: arrayResult, cleanups: &cleanups}
			fn := newTestAggFunction(t, "test_agg_ownership", behaviour, mem)
			ctx = testAggContext(t, ctx, fn)

			arr := aggArray(t, mem, arrow.PrimitiveTypes.Int64, `[1, 2, 3, 4, 5]`)
			defer arr.Release()

			res, err := compute.CallFunction(ctx, "test_agg_ownership", nil, compute.NewDatumWithoutOwning(arr))
			require.NoError(t, err)
			assert.Equal(t, 1, cleanups, "the aggregate state is cleaned up exactly once")

			// the state's buffer is gone by now; the result is not
			if arrayResult {
				ad := res.(*compute.ArrayDatum)
				out := ad.MakeArray()
				assert.Equal(t, []int64{5}, out.(*array.Int64).Int64Values())
				out.Release()
			} else {
				assert.Equal(t, "total=5", res.(*compute.ScalarDatum).Value.(*scalar.String).String())
			}
			res.Release()
		})
	}
}

// TestScalarAggregateStateCleanup checks the promise that cleanup runs
// exactly once whatever happens.
func TestScalarAggregateStateCleanup(t *testing.T) {
	newCall := func(t *testing.T, behaviour *testAggBehaviour, vals string) (context.Context, compute.Datum, func()) {
		ctx, mem := aggTestContext(t)
		fn := newTestAggFunction(t, "test_agg_cleanup", behaviour, mem)
		ctx = testAggContext(t, ctx, fn)
		arr := aggArray(t, mem, arrow.PrimitiveTypes.Int64, vals)
		return ctx, compute.NewDatumWithoutOwning(arr), arr.Release
	}

	t.Run("success", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		res, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		require.NoError(t, err)
		res.Release()
		assert.Equal(t, 1, cleanups)
	})

	t.Run("empty input", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{cleanups: &cleanups}, `[]`)
		defer done()
		res, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		require.NoError(t, err)
		res.Release()
		assert.Equal(t, 1, cleanups)
	})

	t.Run("consume error", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{consumeErr: errTestAgg, cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		_, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		assert.ErrorIs(t, err, errTestAgg)
		assert.Equal(t, 1, cleanups)
	})

	t.Run("finalize error", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{finalizeErr: errTestAgg, cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		_, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		assert.ErrorIs(t, err, errTestAgg)
		assert.Equal(t, 1, cleanups)
	})

	t.Run("cleanup error is reported", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{cleanupErr: errTestAgg, cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		_, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		assert.ErrorIs(t, err, errTestAgg)
		assert.Equal(t, 1, cleanups)
	})

	t.Run("init error", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{initErr: errTestAgg, cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		_, err := compute.CallFunction(ctx, "test_agg_cleanup", nil, d)
		assert.ErrorIs(t, err, errTestAgg)
		// no state was produced, so there is nothing to clean up
		assert.Zero(t, cleanups)
	})

	t.Run("cancellation", func(t *testing.T) {
		var cleanups int
		ctx, d, done := newCall(t, &testAggBehaviour{cleanups: &cleanups}, `[1, 2, 3]`)
		defer done()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := compute.CallFunction(cancelled, "test_agg_cleanup", nil, d)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, cleanups)
	})
}

// TestScalarAggregateScalarSpanLength drives the executor with a batch of
// scalars whose logical length is greater than one, which the public
// CallFunction path cannot produce. A kernel which ignored the span length
// for scalar input would pass every other test in this file.
func TestScalarAggregateScalarSpanLength(t *testing.T) {
	ctx, _ := aggTestContext(t)

	reg := compute.GetFunctionRegistry()
	const spanLen = 5

	run := func(t *testing.T, fname string, opts compute.FunctionOptions, sc scalar.Scalar) scalar.Scalar {
		t.Helper()
		fn, ok := reg.GetFunction(fname)
		require.True(t, ok)
		k, err := fn.DispatchBest(sc.DataType())
		require.NoError(t, err)

		kctx := &exec.KernelCtx{Ctx: ctx, Kernel: k}
		initArgs := exec.KernelInitArgs{Kernel: k, Inputs: []arrow.DataType{sc.DataType()}, Options: opts}
		kctx.State, err = k.GetInitFn()(kctx, initArgs)
		require.NoError(t, err)

		executor := compute.NewScalarAggExecutor()
		defer executor.Clear()
		require.NoError(t, executor.Init(kctx, initArgs))

		batch := &compute.ExecBatch{Values: []compute.Datum{compute.NewDatumWithoutOwning(sc)}, Len: spanLen}
		ch := make(chan compute.Datum, 1)
		go func() {
			defer close(ch)
			require.NoError(t, executor.Execute(ctx, batch, ch))
		}()
		out := executor.WrapResults(ctx, ch, false)
		require.NotNil(t, out)
		defer out.Release()
		return out.(*compute.ScalarDatum).Value
	}

	// the scalar counts once per row of the span
	assertInt64Result(t, run(t, "sum", nil, scalar.NewInt64Scalar(3)), 3*spanLen)
	assertUint64Result(t, run(t, "sum", nil, scalar.NewUint8Scalar(2)), 2*spanLen)
	assertFloat64Result(t, run(t, "sum", nil, scalar.NewFloat64Scalar(1.5)), 1.5*spanLen)
	assertUint64Result(t, run(t, "sum", nil, scalar.NewBooleanScalar(true)), spanLen)
	assertInt64Result(t, run(t, "count", nil, scalar.NewInt64Scalar(3)), spanLen)
	assertInt64Result(t, run(t, "count", &compute.CountOptions{Mode: compute.CountAllRows}, scalar.NewInt64Scalar(3)), spanLen)
	assertInt64Result(t, run(t, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, scalar.NewInt64Scalar(3)), 0)

	nullSc := scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64)
	assertInt64Result(t, run(t, "count", &compute.CountOptions{Mode: compute.CountOnlyNull}, nullSc), spanLen)
	assertInt64Result(t, run(t, "count", nil, nullSc), 0)
	assertNullResult(t, run(t, "sum", nil, nullSc), arrow.PrimitiveTypes.Int64)

	// the same result whatever the chunk size the span is cut into
	small := compute.DefaultExecCtx()
	small.ChunkSize = 2
	prevCtx := ctx
	ctx = compute.SetExecCtx(prevCtx, small)
	assertInt64Result(t, run(t, "sum", nil, scalar.NewInt64Scalar(3)), 3*spanLen)
	assertInt64Result(t, run(t, "count", nil, scalar.NewInt64Scalar(3)), spanLen)
	ctx = prevCtx
}
