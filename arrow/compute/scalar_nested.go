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

package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/internal/kernels"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

var listElementDoc = FunctionDoc{
	Summary:     "Compute elements using nested list values and an index",
	Description: "For each list value, return the element at the requested index. The index must be a scalar or a one-element array-like datum.",
	ArgNames:    []string{"lists", "index"},
}

func listElementIndexScalar(index Datum) (scalar.Scalar, bool, error) {
	switch index.Kind() {
	case KindScalar:
		return nil, false, nil
	case KindArray:
		if index.Len() == 0 {
			return nil, false, fmt.Errorf("%w: list_element index array is empty", arrow.ErrInvalid)
		}
		if index.Len() > 1 {
			return nil, false, fmt.Errorf("%w: list_element does not support arrays of list indices", arrow.ErrNotImplemented)
		}

		arr := index.(*ArrayDatum).MakeArray()
		defer arr.Release()
		value, err := scalar.GetScalar(arr, 0)
		return value, true, err
	case KindChunked:
		if index.Len() == 0 {
			return nil, false, fmt.Errorf("%w: list_element index array is empty", arrow.ErrInvalid)
		}
		if index.Len() > 1 {
			return nil, false, fmt.Errorf("%w: list_element does not support arrays of list indices", arrow.ErrNotImplemented)
		}

		for _, chunk := range index.(*ChunkedDatum).Chunks() {
			if chunk.Len() == 0 {
				continue
			}
			value, err := scalar.GetScalar(chunk, 0)
			return value, true, err
		}
		return nil, false, fmt.Errorf("%w: list_element index array is empty", arrow.ErrInvalid)
	default:
		return nil, false, nil
	}
}

func RegisterScalarNested(reg FunctionRegistry) {
	kernelFn := NewScalarFunction("list_element", Binary(), listElementDoc)
	for _, kernel := range kernels.GetListElementKernels() {
		if err := kernelFn.AddKernel(kernel); err != nil {
			panic(err)
		}
	}

	// Normalize the index before scalar execution splits arguments into spans.
	// The index contract is defined by the original Datum, not by the length of
	// an execution span.
	fn := NewMetaFunction("list_element", Binary(), listElementDoc,
		func(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
			indexScalar, normalized, err := listElementIndexScalar(args[1])
			if err != nil {
				return nil, err
			}
			if normalized {
				indexDatum := &ScalarDatum{Value: indexScalar}
				defer indexDatum.Release()
				return kernelFn.Execute(ctx, opts, args[0], indexDatum)
			}

			return kernelFn.Execute(ctx, opts, args...)
		})
	reg.AddFunction(fn, false)
}

func ListElement(ctx context.Context, lists, index Datum) (Datum, error) {
	return CallFunction(ctx, "list_element", nil, lists, index)
}
