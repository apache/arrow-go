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

package kernels

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// CumulativeOptions controls cumulative operations.
type CumulativeOptions struct {
	// Start is the initial value. A nil value uses the zero value for the
	// input type.
	Start scalar.Scalar `compute:"start"`
	// SkipNulls controls whether a null input stops the cumulative operation.
	// Null positions are still null in the output when this is true.
	SkipNulls bool `compute:"skip_nulls"`
}

func (CumulativeOptions) TypeName() string { return "CumulativeOptions" }

type cumulativeSumState[T arrow.NumericType] struct {
	current         T
	skipNulls       bool
	encounteredNull bool
	checked         bool
	add             func(T, T) (T, error)
}

func safeCastKernels(typ arrow.DataType) []exec.ScalarKernel {
	switch typ.ID() {
	case arrow.INT8:
		return GetCastToInteger[int8](typ)
	case arrow.INT16:
		return GetCastToInteger[int16](typ)
	case arrow.INT32:
		return GetCastToInteger[int32](typ)
	case arrow.INT64:
		return GetCastToInteger[int64](typ)
	case arrow.UINT8:
		return GetCastToInteger[uint8](typ)
	case arrow.UINT16:
		return GetCastToInteger[uint16](typ)
	case arrow.UINT32:
		return GetCastToInteger[uint32](typ)
	case arrow.UINT64:
		return GetCastToInteger[uint64](typ)
	case arrow.FLOAT32:
		return GetCastToFloating[float32](typ)
	case arrow.FLOAT64:
		return GetCastToFloating[float64](typ)
	default:
		return nil
	}
}

func safeCastScalar(ctx *exec.KernelCtx, start scalar.Scalar, typ arrow.DataType) (scalar.Scalar, error) {
	kernels := safeCastKernels(typ)
	var castKernel *exec.ScalarKernel
	for i := range kernels {
		if kernels[i].GetSig().MatchesInputs([]arrow.DataType{start.DataType()}) {
			castKernel = &kernels[i]
			break
		}
	}
	if castKernel == nil {
		return nil, fmt.Errorf("%w: cannot safely cast cumulative sum start value from %s to %s",
			arrow.ErrInvalid, start.DataType(), typ)
	}

	input := exec.ArraySpan{}
	input.FillFromScalar(start)
	output := exec.ArraySpan{Type: typ, Len: 1}
	output.Buffers[1].WrapBuffer(ctx.Allocate(typ.(arrow.FixedWidthDataType).Bytes()))

	castCtx := *ctx
	castCtx.Kernel = castKernel
	castCtx.State = CastOptions{ToType: typ}
	batch := &exec.ExecSpan{
		Len:    1,
		Values: []exec.ExecValue{{Array: input}},
	}
	if err := castKernel.Exec(&castCtx, batch, &output); err != nil {
		output.Release()
		return nil, err
	}

	arr := output.MakeArray()
	defer arr.Release()
	return scalar.GetScalar(arr, 0)
}

func safeNumericCastScalar(ctx *exec.KernelCtx, start scalar.Scalar, typ arrow.DataType) (scalar.Scalar, error) {
	targetID := typ.ID()
	if !arrow.IsInteger(targetID) && !arrow.IsFloating(targetID) {
		return nil, fmt.Errorf("%w: cumulative sum input type must be numeric, got %s", arrow.ErrType, typ)
	}
	if arrow.TypeEqual(start.DataType(), typ) {
		return start, nil
	}

	casted, err := safeCastScalar(ctx, start, typ)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot cast cumulative sum start value to %s: %v", arrow.ErrInvalid, typ, err)
	}
	return casted, nil
}

func cumulativeStartValue[T arrow.NumericType](ctx *exec.KernelCtx, start scalar.Scalar, typ arrow.DataType) (T, error) {
	var zero T
	if start == nil {
		return zero, nil
	}
	if !start.IsValid() {
		return zero, fmt.Errorf("%w: cumulative sum start value must be valid", arrow.ErrInvalid)
	}

	casted, err := safeNumericCastScalar(ctx, start, typ)
	if err != nil {
		return zero, err
	}
	if releasable, ok := casted.(scalar.Releasable); ok {
		defer releasable.Release()
	}

	primitive, ok := casted.(scalar.PrimitiveScalar)
	if !ok {
		return zero, fmt.Errorf("%w: cumulative sum start value is not primitive", arrow.ErrInvalid)
	}

	span := &exec.ArraySpan{Type: typ, Len: 1}
	span.Buffers[1].Buf = primitive.Data()
	return exec.GetSpanValues[T](span, 1)[0], nil
}

func initCumulativeSum[T arrow.NumericType](checked bool) exec.KernelInitFn {
	return func(ctx *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
		opts := &CumulativeOptions{}
		if args.Options != nil {
			var ok bool
			opts, ok = args.Options.(*CumulativeOptions)
			if !ok {
				return nil, fmt.Errorf("%w: attempted to initialize cumulative sum from invalid function options", arrow.ErrInvalid)
			}
		}

		start, err := cumulativeStartValue[T](ctx, opts.Start, args.Inputs[0])
		if err != nil {
			return nil, err
		}

		return &cumulativeSumState[T]{
			current:   start,
			skipNulls: opts.SkipNulls,
			checked:   checked,
			add:       checkedAdder[T](),
		}, nil
	}
}

func checkedAddSigned[T arrow.IntType](left, right T) (T, error) {
	if (right > 0 && left > MaxOf[T]()-right) || (right < 0 && left < MinOf[T]()-right) {
		return 0, errOverflow
	}
	return left + right, nil
}

func checkedAddUnsigned[T arrow.UintType](left, right T) (T, error) {
	if left > MaxOf[T]()-right {
		return 0, errOverflow
	}
	return left + right, nil
}

func checkedAdder[T arrow.NumericType]() func(T, T) (T, error) {
	var zero T
	switch any(zero).(type) {
	case int8:
		return func(left, right T) (T, error) {
			value, err := checkedAddSigned(int8(left), int8(right))
			return T(value), err
		}
	case int16:
		return func(left, right T) (T, error) {
			value, err := checkedAddSigned(int16(left), int16(right))
			return T(value), err
		}
	case int32:
		return func(left, right T) (T, error) {
			value, err := checkedAddSigned(int32(left), int32(right))
			return T(value), err
		}
	case int64:
		return func(left, right T) (T, error) {
			value, err := checkedAddSigned(int64(left), int64(right))
			return T(value), err
		}
	case uint8:
		return func(left, right T) (T, error) {
			value, err := checkedAddUnsigned(uint8(left), uint8(right))
			return T(value), err
		}
	case uint16:
		return func(left, right T) (T, error) {
			value, err := checkedAddUnsigned(uint16(left), uint16(right))
			return T(value), err
		}
	case uint32:
		return func(left, right T) (T, error) {
			value, err := checkedAddUnsigned(uint32(left), uint32(right))
			return T(value), err
		}
	case uint64:
		return func(left, right T) (T, error) {
			value, err := checkedAddUnsigned(uint64(left), uint64(right))
			return T(value), err
		}
	default:
		return func(left, right T) (T, error) { return left + right, nil }
	}
}

func cumulativeSumExec[T arrow.NumericType](ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	state := ctx.State.(*cumulativeSumState[T])
	input := &batch.Values[0].Array

	out.Len = input.Len
	if input.Len == 0 {
		return nil
	}

	data := ctx.Allocate(int(input.Len) * arrow.GetDataType[T]().(arrow.FixedWidthDataType).Bytes())
	out.Buffers[1].WrapBuffer(data)
	values := exec.GetSpanValues[T](out, 1)
	inputValues := exec.GetSpanValues[T](input, 1)

	needsValidity := state.encounteredNull || input.MayHaveNulls()
	if needsValidity {
		validity := ctx.AllocateBitmap(input.Len)
		validityBytes := validity.Bytes()
		for i := range validityBytes {
			validityBytes[i] = 0xFF
		}
		out.Buffers[0].WrapBuffer(validity)
	}

	var nulls int64
	for i := int64(0); i < input.Len; i++ {
		valid := len(input.Buffers[0].Buf) == 0 || bitutil.BitIsSet(input.Buffers[0].Buf, int(input.Offset+i))
		if !valid || state.encounteredNull {
			if needsValidity {
				bitutil.ClearBit(out.Buffers[0].Buf, int(i))
			}
			nulls++
			if !valid && !state.skipNulls {
				state.encounteredNull = true
			}
			continue
		}

		current := state.current
		value := inputValues[i]
		var err error
		if state.checked {
			current, err = state.add(current, value)
		} else {
			current += value
		}
		if err != nil {
			out.Release()
			return err
		}

		state.current = current
		values[i] = current
	}

	out.Nulls = nulls
	return nil
}

func newCumulativeSumKernel[T arrow.NumericType](typ arrow.DataType, checked bool) exec.VectorKernel {
	kernel := exec.NewVectorKernel(
		[]exec.InputType{exec.NewExactInput(typ)},
		exec.NewOutputType(typ),
		cumulativeSumExec[T],
		initCumulativeSum[T](checked))
	kernel.Parallelizable = false
	return kernel
}

func cumulativeSumKernels(checked bool) []exec.VectorKernel {
	return []exec.VectorKernel{
		newCumulativeSumKernel[int8](arrow.PrimitiveTypes.Int8, checked),
		newCumulativeSumKernel[int16](arrow.PrimitiveTypes.Int16, checked),
		newCumulativeSumKernel[int32](arrow.PrimitiveTypes.Int32, checked),
		newCumulativeSumKernel[int64](arrow.PrimitiveTypes.Int64, checked),
		newCumulativeSumKernel[uint8](arrow.PrimitiveTypes.Uint8, checked),
		newCumulativeSumKernel[uint16](arrow.PrimitiveTypes.Uint16, checked),
		newCumulativeSumKernel[uint32](arrow.PrimitiveTypes.Uint32, checked),
		newCumulativeSumKernel[uint64](arrow.PrimitiveTypes.Uint64, checked),
		newCumulativeSumKernel[float32](arrow.PrimitiveTypes.Float32, checked),
		newCumulativeSumKernel[float64](arrow.PrimitiveTypes.Float64, checked),
	}
}

func GetVectorCumulativeKernels() (sum, checked []exec.VectorKernel) {
	return cumulativeSumKernels(false), cumulativeSumKernels(true)
}
