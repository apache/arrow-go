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
}

func cumulativeStartValue[T arrow.NumericType](start scalar.Scalar, typ arrow.DataType) (T, error) {
	var zero T
	if start == nil {
		return zero, nil
	}
	if !start.IsValid() {
		return zero, fmt.Errorf("%w: cumulative sum start value must be valid", arrow.ErrInvalid)
	}

	casted, err := start.CastTo(typ)
	if err != nil {
		return zero, fmt.Errorf("%w: cannot cast cumulative sum start value to %s: %v", arrow.ErrInvalid, typ, err)
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
		}, nil
	}
}

func checkedAdd[T arrow.NumericType](left, right T) (T, error) {
	switch l := any(left).(type) {
	case int8:
		r := any(right).(int8)
		if (r > 0 && l > int8(127)-r) || (r < 0 && l < int8(-128)-r) {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case int16:
		r := any(right).(int16)
		if (r > 0 && l > int16(32767)-r) || (r < 0 && l < int16(-32768)-r) {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case int32:
		r := any(right).(int32)
		if (r > 0 && l > int32(2147483647)-r) || (r < 0 && l < int32(-2147483648)-r) {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case int64:
		r := any(right).(int64)
		if (r > 0 && l > int64(9223372036854775807)-r) || (r < 0 && l < int64(-9223372036854775807-1)-r) {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case uint8:
		r := any(right).(uint8)
		if l > ^uint8(0)-r {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case uint16:
		r := any(right).(uint16)
		if l > ^uint16(0)-r {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case uint32:
		r := any(right).(uint32)
		if l > ^uint32(0)-r {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case uint64:
		r := any(right).(uint64)
		if l > ^uint64(0)-r {
			return 0, fmt.Errorf("%w: cumulative sum overflow", arrow.ErrInvalid)
		}
		return T(l + r), nil
	case float32:
		return T(l + any(right).(float32)), nil
	case float64:
		return T(l + any(right).(float64)), nil
	default:
		panic("unsupported cumulative sum type")
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
		value := exec.GetSpanValues[T](input, 1)[i]
		var err error
		if state.checked {
			current, err = checkedAdd(current, value)
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
