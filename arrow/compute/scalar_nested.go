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
)

var listElementDoc = FunctionDoc{
	Summary: "Compute elements using nested list values and an index",
	Description: "For each list value, return the element at the requested index.\n" +
		"The index must be an integral scalar or a one-element array-like datum.",
	ArgNames: []string{"lists", "index"},
}

func validateListElementIndex(index Datum) error {
	switch index.Kind() {
	case KindScalar:
		return kernels.ValidateListElementScalarIndex(index.(*ScalarDatum).Value)
	case KindArray, KindChunked:
		if index.Len() == 0 {
			return fmt.Errorf("%w: list_element index array is empty", arrow.ErrInvalid)
		}
		if index.Len() > 1 {
			return fmt.Errorf("%w: list_element does not support arrays of list indices", arrow.ErrNotImplemented)
		}
	}
	return nil
}

type listElementFunction struct {
	ScalarFunction
}

func listElementScalarResultSupported(typ arrow.DataType) bool {
	switch typ.ID() {
	case arrow.BINARY_VIEW, arrow.STRING_VIEW, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		return false
	case arrow.EXTENSION:
		return listElementScalarResultSupported(typ.(arrow.ExtensionType).StorageType())
	case arrow.STRUCT:
		for _, field := range typ.(*arrow.StructType).Fields() {
			if !listElementScalarResultSupported(field.Type) {
				return false
			}
		}
	case arrow.SPARSE_UNION:
		for _, field := range typ.(arrow.UnionType).Fields() {
			if !listElementScalarResultSupported(field.Type) {
				return false
			}
		}
	case arrow.DENSE_UNION:
		return true
	case arrow.RUN_END_ENCODED:
		return listElementScalarResultSupported(typ.(*arrow.RunEndEncodedType).Encoded())
	}
	return true
}

// Validate the index before scalar execution splits arguments into spans. The
// index contract is defined by the original Datum, not by the length of an
// execution span.
func (fn *listElementFunction) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	if err := fn.checkArity(len(args)); err != nil {
		return nil, err
	}
	if err := checkOptions(fn, opts); err != nil {
		return nil, err
	}

	if err := validateListElementIndex(args[1]); err != nil {
		return nil, err
	}
	if args[0].Kind() == KindScalar && args[1].Kind() == KindScalar {
		if listType, ok := args[0].(ArrayLikeDatum).Type().(arrow.ListLikeType); ok &&
			!listElementScalarResultSupported(listType.Elem()) {
			return nil, fmt.Errorf("%w: list_element scalar output type %s is not supported", arrow.ErrNotImplemented, listType.Elem())
		}
	}

	return fn.ScalarFunction.Execute(ctx, opts, args...)
}

func RegisterScalarNested(reg FunctionRegistry) {
	fn := &listElementFunction{ScalarFunction: *NewScalarFunction("list_element", Binary(), listElementDoc)}
	for _, kernel := range kernels.GetListElementKernels() {
		if err := fn.AddKernel(kernel); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(fn, false)
}

func ListElement(ctx context.Context, lists, index Datum) (Datum, error) {
	return CallFunction(ctx, "list_element", nil, lists, index)
}
