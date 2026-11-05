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
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/internal/bitutils"
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

// viewStartsWith decides from the view header alone when it can, and
// reads the data buffer only for the part of pattern past the prefix.
func viewStartsWith(h *arrow.ViewHeader, data, pattern []byte) bool {
	if h.Len() < len(pattern) {
		return false
	}
	if h.IsInline() {
		return bytes.HasPrefix(h.InlineBytes(), pattern)
	}
	prefix := h.Prefix()
	n := min(len(pattern), arrow.ViewPrefixLen)
	if !bytes.Equal(prefix[:n], pattern[:n]) {
		return false
	}
	off := int(h.BufferOffset())
	return bytes.Equal(data[off+n:off+len(pattern)], pattern[n:])
}

// startsWithView reads out-of-line values from Buffers[2], so inputs with
// more than one data buffer must be coalesced before execution.
func startsWithView(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		arg0    = &in.Values[0].Array
		pattern = ctx.State.(matchSubstringState).pattern
		views   = arrow.ViewHeaderTraits.CastFromBytes(arg0.Buffers[1].Buf)[arg0.Offset:]
		data    = arg0.Buffers[2].Buf
		outData = out.Buffers[1].Buf
		outPos  = int(out.Offset)
	)
	bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len,
		func(pos int64) {
			bitutil.SetBitTo(outData, outPos, viewStartsWith(&views[pos], data, pattern))
			outPos++
		}, func() {
			bitutil.ClearBit(outData, outPos)
			outPos++
		})
	return nil
}

// GetStartsWithKernels returns the kernels for the starts_with function
// over the binary, string and view types.
func GetStartsWithKernels() []exec.ScalarKernel {
	out := exec.NewOutputType(arrow.FixedWidthTypes.Boolean)
	types := append(slices.Clip(baseBinaryTypes), arrow.BinaryTypes.BinaryView, arrow.BinaryTypes.StringView)
	kernels := make([]exec.ScalarKernel, 0, len(types))
	for _, ty := range types {
		var ex exec.ArrayKernelExec
		switch ty.ID() {
		case arrow.BINARY_VIEW, arrow.STRING_VIEW:
			ex = startsWithView
		case arrow.LARGE_BINARY, arrow.LARGE_STRING:
			ex = ScalarUnaryNotNullBinaryArgBoolOut[int64](false, startsWith)
		default:
			ex = ScalarUnaryNotNullBinaryArgBoolOut[int32](false, startsWith)
		}
		k := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(ty)}, out, ex, initMatchSubstring)
		k.NullHandling = exec.NullIntersection
		k.MemAlloc = exec.MemPrealloc
		kernels = append(kernels, k)
	}
	return kernels
}
