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
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/internal/bitutils"
)

// MatchSubstringOptions controls the substring matching functions such as
// starts_with, which match Pattern literally, not as a regular
// expression. With IgnoreCase, string input matches Unicode simple case
// folds and binary input matches ASCII case folds.
type MatchSubstringOptions struct {
	Pattern    string `compute:"pattern"`
	IgnoreCase bool   `compute:"ignore_case"`
}

func (MatchSubstringOptions) TypeName() string { return "MatchSubstringOptions" }

type matchSubstringState struct {
	pattern []byte
}

type foldUTF8State struct {
	pattern []rune
}

// foldASCIIState holds the pattern with each byte lowered by foldASCII.
type foldASCIIState struct {
	pattern []byte
}

func initMatchSubstring(utf8Input bool) exec.KernelInitFn {
	return func(_ *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
		opts, ok := args.Options.(MatchSubstringOptions)
		if p, isPtr := args.Options.(*MatchSubstringOptions); isPtr && p != nil {
			opts, ok = *p, true
		}
		if !ok {
			return nil, fmt.Errorf("%w: attempted to initialize kernel state from invalid function options",
				arrow.ErrInvalid)
		}

		switch {
		case !opts.IgnoreCase:
			return matchSubstringState{pattern: []byte(opts.Pattern)}, nil
		case utf8Input:
			if !utf8.ValidString(opts.Pattern) {
				return nil, fmt.Errorf("%w: case-insensitive pattern must be valid UTF-8 for string input",
					arrow.ErrInvalid)
			}
			return foldUTF8State{pattern: []rune(opts.Pattern)}, nil
		default:
			pattern := []byte(opts.Pattern)
			for i, b := range pattern {
				pattern[i] = foldASCII(b)
			}
			return foldASCIIState{pattern: pattern}, nil
		}
	}
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

func viewBytes(h *arrow.ViewHeader, data []byte) []byte {
	if h.IsInline() {
		return h.InlineBytes()
	}
	off := int(h.BufferOffset())
	return data[off : off+h.Len()]
}

// startsWithView reads out-of-line values from Buffers[2], so inputs with
// more than one data buffer must be coalesced before execution.
func startsWithView(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		arg0       = &in.Values[0].Array
		views      = arrow.ViewHeaderTraits.CastFromBytes(arg0.Buffers[1].Buf)[arg0.Offset:]
		data       = arg0.Buffers[2].Buf
		outData    = out.Buffers[1].Buf
		outPos     = int(out.Offset)
		visitValid func(pos int64)
	)
	visitFold := func(fold func(*exec.KernelCtx, []byte, *error) bool) func(int64) {
		return func(pos int64) {
			bitutil.SetBitTo(outData, outPos, fold(ctx, viewBytes(&views[pos], data), nil))
			outPos++
		}
	}
	switch st := ctx.State.(type) {
	case matchSubstringState:
		pattern := st.pattern
		visitValid = func(pos int64) {
			bitutil.SetBitTo(outData, outPos, viewStartsWith(&views[pos], data, pattern))
			outPos++
		}
	case foldUTF8State:
		visitValid = visitFold(startsWithFoldUTF8)
	case foldASCIIState:
		visitValid = visitFold(startsWithFoldASCII)
	}
	bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len, visitValid,
		func() {
			bitutil.ClearBit(outData, outPos)
			outPos++
		})
	return nil
}

func startsWithFoldUTF8(ctx *exec.KernelCtx, v []byte, _ *error) bool {
	for _, p := range ctx.State.(foldUTF8State).pattern {
		r, size := utf8.DecodeRune(v)
		if (r == utf8.RuneError && size <= 1) || !equalSimpleFold(r, p) {
			return false
		}
		v = v[size:]
	}
	return true
}

func equalSimpleFold(r, p rune) bool {
	for f := r; ; {
		if f == p {
			return true
		}
		if f = unicode.SimpleFold(f); f == r {
			return false
		}
	}
}

func startsWithFoldASCII(ctx *exec.KernelCtx, v []byte, _ *error) bool {
	pattern := ctx.State.(foldASCIIState).pattern
	if len(v) < len(pattern) {
		return false
	}
	for i, p := range pattern {
		if foldASCII(v[i]) != p {
			return false
		}
	}
	return true
}

func foldASCII(b byte) byte {
	if 'A' <= b && b <= 'Z' {
		return b + 0x20
	}
	return b
}

func startsWithExec[OffsetT int32 | int64]() exec.ArrayKernelExec {
	caseSensitive := ScalarUnaryNotNullBinaryArgBoolOut[OffsetT](false, startsWith)
	foldUTF8 := ScalarUnaryNotNullBinaryArgBoolOut[OffsetT](false, startsWithFoldUTF8)
	foldASCII := ScalarUnaryNotNullBinaryArgBoolOut[OffsetT](false, startsWithFoldASCII)
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		switch ctx.State.(type) {
		case foldUTF8State:
			return foldUTF8(ctx, in, out)
		case foldASCIIState:
			return foldASCII(ctx, in, out)
		default:
			return caseSensitive(ctx, in, out)
		}
	}
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
			ex = startsWithExec[int64]()
		default:
			ex = startsWithExec[int32]()
		}
		k := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(ty)}, out, ex,
			initMatchSubstring(ty.(arrow.BinaryDataType).IsUtf8()))
		k.NullHandling = exec.NullIntersection
		k.MemAlloc = exec.MemPrealloc
		kernels = append(kernels, k)
	}
	return kernels
}

// utf8Length returns the number of Unicode code points in v by counting
// the bytes that are not UTF-8 continuation bytes. This matches the C++
// kernels, which count the same way and only assume valid UTF-8.
func utf8Length(v []byte) int {
	var n int
	for _, b := range v {
		if b&0xc0 != 0x80 {
			n++
		}
	}
	return n
}

func utf8Length32(_ *exec.KernelCtx, v []byte, _ *error) int32 {
	return int32(utf8Length(v))
}

func utf8Length64(_ *exec.KernelCtx, v []byte, _ *error) int64 {
	return int64(utf8Length(v))
}

// utf8LengthView is the string_view kernel of the utf8_length function. It
// reads out-of-line values from Buffers[2], so inputs with more than one
// data buffer must be coalesced before execution.
func utf8LengthView(_ *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		arg0    = &in.Values[0].Array
		views   = arrow.ViewHeaderTraits.CastFromBytes(arg0.Buffers[1].Buf)[arg0.Offset:]
		data    = arg0.Buffers[2].Buf
		outVals = exec.GetSpanValues[int32](out, 1)
		outPos  int
	)

	bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len,
		func(pos int64) {
			outVals[outPos] = int32(utf8Length(viewBytes(&views[pos], data)))
			outPos++
		}, func() { outPos++ })
	return nil
}

// GetUtf8LengthKernels returns the kernels for the utf8_length function
// over the string types. The output width follows the offset width of the
// input type, matching the C++ kernels (utf8 -> int32, large_utf8 ->
// int64). The string_view kernel is a Go addition; the C++ registry
// registers utf8_length for utf8 and large_utf8 only.
func GetUtf8LengthKernels() []exec.ScalarKernel {
	mk := func(inType, outType arrow.DataType, ex exec.ArrayKernelExec) exec.ScalarKernel {
		k := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(inType)},
			exec.NewOutputType(outType), ex, nil)
		k.NullHandling = exec.NullIntersection
		k.MemAlloc = exec.MemPrealloc
		return k
	}
	return []exec.ScalarKernel{
		mk(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32,
			ScalarUnaryNotNullBinaryArg[int32, int32](utf8Length32)),
		mk(arrow.BinaryTypes.LargeString, arrow.PrimitiveTypes.Int64,
			ScalarUnaryNotNullBinaryArg[int64, int64](utf8Length64)),
		mk(arrow.BinaryTypes.StringView, arrow.PrimitiveTypes.Int32, utf8LengthView),
	}
}

// utf8CodePointMapper is a per-code-point case mapping such as
// unicode.ToUpper or unicode.ToLower. Like the utf8proc calls used by the
// C++ kernels, these are simple one-to-one Unicode case mappings.
type utf8CodePointMapper func(rune) rune

// appendMapped appends v to dst with each of its code points mapped
// through mapRune. ASCII bytes are mapped without decoding, which relies
// on mapRune keeping ASCII closed, as unicode.ToUpper and unicode.ToLower
// do for simple Unicode case mappings. Returns an error wrapping
// arrow.ErrInvalid when v is not valid UTF-8, matching the C++ kernels
// which fail on invalid UTF-8 input.
func appendMapped(dst, v []byte, mapRune utf8CodePointMapper) ([]byte, error) {
	for len(v) > 0 {
		if v[0] < utf8.RuneSelf {
			dst = append(dst, byte(mapRune(rune(v[0]))))
			v = v[1:]
			continue
		}
		r, size := utf8.DecodeRune(v)
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("%w: Invalid UTF8 sequence in input", arrow.ErrInvalid)
		}
		if u := mapRune(r); u != r {
			dst = utf8.AppendRune(dst, u)
		} else {
			dst = append(dst, v[:size]...)
		}
		v = v[size:]
	}
	return dst, nil
}

// visitStringValues calls visit for each valid value of the utf8 or
// large_utf8 array arg0, and nullFn for each null value.
func visitStringValues[OffsetT int32 | int64](arg0 *exec.ArraySpan,
	visit func([]byte), nullFn func()) {
	offsets := exec.GetSpanOffsets[OffsetT](arg0, 1)
	data := arg0.Buffers[2].Buf
	bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len,
		func(pos int64) { visit(data[offsets[pos]:offsets[pos+1]]) }, nullFn)
}

// utf8CaseExecOffset is the utf8 or large_utf8 kernel of the utf8_upper
// and utf8_lower functions.
func utf8CaseExecOffset[OffsetT int32 | int64](mapRune utf8CodePointMapper) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := &in.Values[0].Array
		bldr := array.NewBuilder(exec.GetAllocator(ctx.Ctx), out.Type).(array.StringLikeBuilder)
		defer bldr.Release()

		// A Unicode simple case mapping grows a two-byte code point to at
		// most three bytes, so half again the input bytes is an upper
		// bound on the output size.
		var total int64
		visitStringValues[OffsetT](arg0, func(v []byte) { total += int64(len(v)) }, func() {})
		bldr.Reserve(int(arg0.Len))
		if err := reserveFormattedDataExact(bldr, total*3/2); err != nil {
			return err
		}

		var (
			scratch []byte
			err     error
		)
		visitStringValues[OffsetT](arg0, func(v []byte) {
			if err != nil {
				return
			}
			if scratch, err = appendMapped(scratch[:0], v, mapRune); err == nil {
				bldr.UnsafeAppend(scratch)
			}
		}, bldr.AppendNull)
		if err != nil {
			return err
		}

		arr := bldr.NewArray()
		out.TakeOwnership(arr.Data())
		arr.Release()
		return nil
	}
}

// utf8CaseExecView is the string_view kernel of the utf8_upper and
// utf8_lower functions. It reads out-of-line values from Buffers[2], so
// inputs with more than one data buffer must be coalesced before
// execution.
func utf8CaseExecView(mapRune utf8CodePointMapper) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := &in.Values[0].Array
		views := arrow.ViewHeaderTraits.CastFromBytes(arg0.Buffers[1].Buf)[arg0.Offset:]
		data := arg0.Buffers[2].Buf
		bldr := array.NewBuilder(exec.GetAllocator(ctx.Ctx), out.Type).(array.StringLikeBuilder)
		defer bldr.Release()

		var total int64
		bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len,
			func(pos int64) { total += int64(views[pos].Len()) }, func() {})
		bldr.Reserve(int(arg0.Len))
		if err := reserveFormattedDataExact(bldr, total*3/2); err != nil {
			return err
		}

		var (
			scratch []byte
			err     error
		)
		bitutils.VisitBitBlocks(arg0.Buffers[0].Buf, arg0.Offset, arg0.Len,
			func(pos int64) {
				if err != nil {
					return
				}
				if scratch, err = appendMapped(scratch[:0], viewBytes(&views[pos], data), mapRune); err == nil {
					bldr.UnsafeAppend(scratch)
				}
			}, bldr.AppendNull)
		if err != nil {
			// Release on a discarded view builder does not free its
			// out-of-line blocks, so materialize the partial result to
			// transfer the blocks somewhere they get freed.
			arr := bldr.NewArray()
			arr.Release()
			return err
		}

		arr := bldr.NewArray()
		out.TakeOwnership(arr.Data())
		arr.Release()
		return nil
	}
}

// GetUtf8CaseKernels returns the kernels for the utf8_upper and utf8_lower
// functions over the string types. The output type of each kernel matches
// its input type, like the C++ kernels, which register utf8_upper and
// utf8_lower for utf8 and large_utf8 only; the string_view kernels are a
// Go addition.
func GetUtf8CaseKernels(mapRune utf8CodePointMapper) []exec.ScalarKernel {
	types := []arrow.DataType{
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView,
	}
	kernels := make([]exec.ScalarKernel, 0, len(types))
	for _, ty := range types {
		var ex exec.ArrayKernelExec
		switch ty.ID() {
		case arrow.STRING_VIEW:
			ex = utf8CaseExecView(mapRune)
		case arrow.LARGE_STRING:
			ex = utf8CaseExecOffset[int64](mapRune)
		default:
			ex = utf8CaseExecOffset[int32](mapRune)
		}
		k := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(ty)},
			exec.NewOutputType(ty), ex, nil)
		k.NullHandling = exec.NullComputedNoPrealloc
		k.MemAlloc = exec.MemNoPrealloc
		kernels = append(kernels, k)
	}
	return kernels
}
