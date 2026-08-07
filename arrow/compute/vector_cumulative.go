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
)

var (
	cumulativeSumDoc = FunctionDoc{
		Summary:     "Compute the cumulative sum of an array",
		Description: "Return the cumulative sum of the input array",
		ArgNames:    []string{"array"},
		OptionsType: "CumulativeOptions",
	}
	cumulativeSumCheckedDoc = FunctionDoc{
		Summary:     "Compute the cumulative sum of an array with overflow checking",
		Description: "Return the cumulative sum of the input array and report integer overflow",
		ArgNames:    []string{"array"},
		OptionsType: "CumulativeOptions",
	}
)

type CumulativeOptions = kernels.CumulativeOptions

func RegisterVectorCumulative(reg FunctionRegistry) {
	sum, checked := kernels.GetVectorCumulativeKernels()

	sumFn := NewVectorFunction("cumulative_sum", Unary(), cumulativeSumDoc)
	sumFn.SetDefaultOptions(&CumulativeOptions{})
	for _, k := range sum {
		if err := sumFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	reg.AddFunction(sumFn, false)

	checkedFn := NewVectorFunction("cumulative_sum_checked", Unary(), cumulativeSumCheckedDoc)
	checkedFn.SetDefaultOptions(&CumulativeOptions{})
	for _, k := range checked {
		if err := checkedFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	reg.AddFunction(checkedFn, false)
}

func CumulativeSum(ctx context.Context, opts CumulativeOptions, values Datum) (Datum, error) {
	return CallFunction(ctx, "cumulative_sum", &opts, values)
}

func CumulativeSumChecked(ctx context.Context, opts CumulativeOptions, values Datum) (Datum, error) {
	return CallFunction(ctx, "cumulative_sum_checked", &opts, values)
}
