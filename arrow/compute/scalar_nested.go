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

	"github.com/apache/arrow-go/v18/arrow/compute/internal/kernels"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

var listElementDoc = FunctionDoc{
	Summary:     "Compute elements using nested list values and an index",
	Description: "For each list value, return the element at the requested index",
	ArgNames:    []string{"lists", "index"},
}

func RegisterScalarNested(reg FunctionRegistry) {
	fn := NewScalarFunction("list_element", Binary(), listElementDoc)
	for _, kernel := range kernels.GetListElementKernels() {
		if err := fn.AddKernel(kernel); err != nil {
			panic(err)
		}
	}
	reg.AddFunction(fn, false)
}

func ListElement(ctx context.Context, lists, index Datum) (Datum, error) {
	if indexArray, ok := index.(*ArrayDatum); ok && indexArray.Len() == 1 {
		arr := indexArray.MakeArray()
		defer arr.Release()

		indexScalar, err := scalar.GetScalar(arr, 0)
		if err != nil {
			return nil, err
		}
		if releasable, ok := indexScalar.(scalar.Releasable); ok {
			defer releasable.Release()
		}
		return CallFunction(ctx, "list_element", nil, lists, &ScalarDatum{Value: indexScalar})
	}

	return CallFunction(ctx, "list_element", nil, lists, index)
}
