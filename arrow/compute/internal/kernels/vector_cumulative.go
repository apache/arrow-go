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
	Start scalar.Scalar
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

func safeNumericCastScalar(start scalar.Scalar, typ arrow.DataType) (scalar.Scalar, error) {
	sourceID := start.DataType().ID()
	targetID := typ.ID()
	if !arrow.IsInteger(targetID) && !arrow.IsFloating(targetID) {
		return nil, fmt.Errorf("%w: cumulative sum input type must be numeric, got %s", arrow.ErrType, typ)
	}
	if sourceID == targetID {
		return start, nil
	}

	casted, err := start.CastTo(typ)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot cast cumulative sum start value to %s: %v", arrow.ErrInvalid, typ, err)
	}

	// Floating-point to floating-point casts follow the existing compute cast
	// behavior, which does not reject precision loss. The other numeric casts
	// use the same safe checks as the compute cast kernels.
	if arrow.IsFloating(sourceID) && arrow.IsFloating(targetID) {
		return casted, nil
	}
	if !arrow.IsInteger(sourceID) && !arrow.IsFloating(sourceID) {
		// Non-numeric scalar types such as strings and booleans validate their
		// conversion while producing the target scalar. Keep that behavior
		// aligned with scalar safe-cast dispatch instead of rejecting them based
		// only on their source type.
		return casted, nil
	}

	var sourceSpan exec.ArraySpan
	sourceSpan.FillFromScalar(start)
	var checkErr error
	switch {
	case arrow.IsInteger(sourceID) && arrow.IsInteger(targetID):
		checkErr = intsCanFit(&sourceSpan, targetID)
	case arrow.IsInteger(sourceID) && arrow.IsFloating(targetID):
		checkErr = checkIntToFloatTrunc(&sourceSpan, targetID)
	case arrow.IsFloating(sourceID) && arrow.IsInteger(targetID):
		var roundTrip scalar.Scalar
		roundTrip, checkErr = casted.CastTo(start.DataType())
		if checkErr == nil && !scalar.Equals(start, roundTrip) {
			checkErr = fmt.Errorf("%w: float value %s was truncated converting to %s", arrow.ErrInvalid, start, typ)
		}
	}
	if checkErr != nil {
		return nil, fmt.Errorf("%w: cannot safely cast cumulative sum start value to %s: %v", arrow.ErrInvalid, typ, checkErr)
	}
	return casted, nil
}

func cumulativeStartValue[T arrow.NumericType](start scalar.Scalar, typ arrow.DataType) (T, error) {
	var zero T
	if start == nil {
		return zero, nil
	}
	if !start.IsValid() {
		return zero, fmt.Errorf("%w: cumulative sum start value must be valid", arrow.ErrInvalid)
	}

	casted, err := safeNumericCastScalar(start, typ)
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
	return func(_ *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
		opts := &CumulativeOptions{}
		if args.Options != nil {
			var ok bool
			opts, ok = args.Options.(*CumulativeOptions)
			if !ok {
				return nil, fmt.Errorf("%w: attempted to initialize cumulative sum from invalid function options", arrow.ErrInvalid)
			}
		}

		start, err := cumulativeStartValue[T](opts.Start, args.Inputs[0])
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
	return exec.NewVectorKernel(
		[]exec.InputType{exec.NewExactInput(typ)},
		exec.NewOutputType(typ),
		cumulativeSumExec[T],
		initCumulativeSum[T](checked))
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
