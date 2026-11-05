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

package kernels

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
)

// MatchSubstringOptions controls the substring matching functions such as
// starts_with, which match Pattern as a literal byte sequence.
type MatchSubstringOptions struct {
	Pattern    string `compute:"pattern"`
	IgnoreCase bool   `compute:"ignore_case"`
}

func (MatchSubstringOptions) TypeName() string { return "MatchSubstringOptions" }

type matchSubstringState struct {
	pattern []byte
}

func initMatchSubstring(_ *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
	opts, ok := args.Options.(MatchSubstringOptions)
	if p, isPtr := args.Options.(*MatchSubstringOptions); isPtr && p != nil {
		opts, ok = *p, true
	}
	if !ok {
		return nil, fmt.Errorf("%w: attempted to initialize kernel state from invalid function options",
			arrow.ErrInvalid)
	}

	if opts.IgnoreCase {
		return nil, fmt.Errorf("%w: case-insensitive substring matching", arrow.ErrNotImplemented)
	}
	return matchSubstringState{pattern: []byte(opts.Pattern)}, nil
}

func startsWith(ctx *exec.KernelCtx, v []byte, _ *error) bool {
	return bytes.HasPrefix(v, ctx.State.(matchSubstringState).pattern)
}

// GetStartsWithKernels returns the kernels for the starts_with function
// over the base binary and string types.
func GetStartsWithKernels() []exec.ScalarKernel {
	out := exec.NewOutputType(arrow.FixedWidthTypes.Boolean)
	kernels := make([]exec.ScalarKernel, 0, len(baseBinaryTypes))
	for _, ty := range baseBinaryTypes {
		var ex exec.ArrayKernelExec
		if arrow.IsLargeBinaryLike(ty.ID()) {
			ex = ScalarUnaryNotNullBinaryArgBoolOut[int64](false, startsWith)
		} else {
			ex = ScalarUnaryNotNullBinaryArgBoolOut[int32](false, startsWith)
		}
		k := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(ty)}, out, ex, initMatchSubstring)
		k.NullHandling = exec.NullIntersection
		k.MemAlloc = exec.MemPrealloc
		kernels = append(kernels, k)
	}
	return kernels
}
