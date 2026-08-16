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

package extensions

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

// VariantPathElement is a single step of a variant path: either an object field
// name or an array index.
type VariantPathElement struct {
	name    string
	index   int
	isIndex bool
}

// VariantPathField returns a path element selecting the named object field.
func VariantPathField(name string) VariantPathElement {
	return VariantPathElement{name: name}
}

// VariantPathIndex returns a path element selecting the array element at index.
func VariantPathIndex(index int) VariantPathElement {
	return VariantPathElement{index: index, isIndex: true}
}

// VariantPath is an ordered list of path elements to extract from a variant value.
type VariantPath []VariantPathElement

// GetOptions controls VariantGet.
type GetOptions struct {
	// Path is the path to extract from each variant value.
	Path VariantPath
	// AsType, when nil, makes VariantGet return a VariantArray pointing at the path.
	// When set, the extracted value is cast to this type. Nested (struct/list) types
	// are not yet supported and yield arrow.ErrNotImplemented.
	AsType arrow.DataType
	// Safe makes cast failures produce null; when false a failure returns an error.
	Safe bool
	// Mem is the allocator for output arrays; nil uses memory.DefaultAllocator.
	Mem memory.Allocator
}

// VariantGet extracts opts.Path from each value of a VariantArray. It follows the
// shredded typed_value columns as far as the path allows, then falls back to a
// per-row walk of the residual value for the remainder.
func VariantGet(input arrow.Array, opts GetOptions) (arrow.Array, error) {
	va, ok := input.(*VariantArray)
	if !ok {
		return nil, fmt.Errorf("%w: VariantGet input must be a VariantArray, got %T", arrow.ErrInvalid, input)
	}

	if opts.Mem == nil {
		opts.Mem = memory.DefaultAllocator
	}

	return shreddedGetPath(va, opts)
}

// shreddingState is a (value?, typed_value?) column pair at one level of a shredded
// variant, mirroring arrow-rs ShreddingState.
type shreddingState struct {
	value      arrow.TypedArray[[]byte]
	typedValue arrow.Array
	length     int
}

func stateFromVariant(va *VariantArray) shreddingState {
	vt := va.ExtensionType().(*VariantType)
	st := va.Storage().(*array.Struct)

	var value arrow.TypedArray[[]byte]
	if vt.valueFieldIdx != -1 {
		value = st.Field(vt.valueFieldIdx).(arrow.TypedArray[[]byte])
	}

	var typed arrow.Array
	if vt.typedValueFieldIdx != -1 {
		typed = st.Field(vt.typedValueFieldIdx)
	}

	return shreddingState{value: value, typedValue: typed, length: va.Len()}
}

func stateFromFieldStruct(child *array.Struct) shreddingState {
	ct := child.DataType().(*arrow.StructType)

	var value arrow.TypedArray[[]byte]
	if idx, ok := ct.FieldIdx("value"); ok {
		value = child.Field(idx).(arrow.TypedArray[[]byte])
	}

	var typed arrow.Array
	if idx, ok := ct.FieldIdx("typed_value"); ok {
		typed = child.Field(idx)
	}

	return shreddingState{value: value, typedValue: typed, length: child.Len()}
}

type pathStepKind int

const (
	stepSuccess pathStepKind = iota
	stepMissing
	stepNotShredded
)

type pathStep struct {
	kind  pathStepKind
	state shreddingState
}

// missingStep decides whether an absent typed field means the value is provably
// missing (value column all-null) or merely not shredded (residual may hold it).
func (s shreddingState) missingStep() pathStep {
	if s.value == nil || s.value.NullN() == s.value.Len() {
		return pathStep{kind: stepMissing}
	}

	return pathStep{kind: stepNotShredded}
}

// followFieldElement takes one field step deeper into the shredded columns.
func followFieldElement(s shreddingState, name string) (pathStep, error) {
	if s.typedValue == nil {
		return s.missingStep(), nil
	}

	st, ok := s.typedValue.(*array.Struct)
	if !ok {
		return s.missingStep(), nil
	}

	idx, ok := st.DataType().(*arrow.StructType).FieldIdx(name)
	if !ok {
		return s.missingStep(), nil
	}

	child, ok := st.Field(idx).(*array.Struct)
	if !ok {
		return pathStep{}, fmt.Errorf("%w: expected struct field %q while following path, got %s",
			arrow.ErrInvalid, name, st.Field(idx).DataType())
	}

	return pathStep{kind: stepSuccess, state: stateFromFieldStruct(child)}, nil
}

func shreddedGetPath(va *VariantArray, opts GetOptions) (arrow.Array, error) {
	state := stateFromVariant(va)
	nulls := newNullTracker(va.Len())
	nulls.apply(va.Storage())

	// Peel the field prefix of the path through the shredded columns. Index steps
	// and non-shredded fields stop the columnar walk and hand the rest to a per-row
	// fallback over the fully reassembled value at the current node.
	idx := 0
	for idx < len(opts.Path) {
		elem := opts.Path[idx]
		if elem.isIndex {
			break
		}

		step, err := followFieldElement(state, elem.name)
		if err != nil {
			return nil, err
		}

		switch step.kind {
		case stepSuccess:
			nulls.apply(state.typedValue)
			state = step.state
			idx++

			continue
		case stepMissing:
			return allNullResult(va, opts)
		}

		break // stepNotShredded
	}

	remaining := opts.Path[idx:]
	target, err := buildTargetVariant(va, state, nulls, opts.Mem)
	if err != nil {
		return nil, err
	}
	defer target.Release()

	if len(remaining) == 0 {
		if opts.AsType == nil {
			target.Retain()

			return target, nil
		}

		if shredded := tryPerfectShredding(state, nulls, opts.AsType); shredded != nil {
			return shredded, nil
		}
	}

	return shredBasicVariant(target, remaining, opts)
}

// shredBasicVariant walks the remaining path per row and produces either a
// VariantArray (AsType nil) or a typed array.
func shredBasicVariant(target *VariantArray, remaining VariantPath, opts GetOptions) (arrow.Array, error) {
	if opts.AsType == nil {
		bldr := NewVariantBuilder(opts.Mem, NewDefaultVariantType())
		defer bldr.Release()
		bldr.Reserve(target.Len())

		for i := 0; i < target.Len(); i++ {
			leaf, ok, err := navigateRow(target, i, remaining)
			if err != nil {
				return nil, err
			}
			if !ok {
				bldr.AppendNull()

				continue
			}
			bldr.Append(leaf)
		}

		return bldr.NewArray(), nil
	}

	if _, ok := opts.AsType.(arrow.NestedType); ok {
		return nil, fmt.Errorf("%w: VariantGet cast to nested type %s", arrow.ErrNotImplemented, opts.AsType)
	}

	bldr := array.NewBuilder(opts.Mem, opts.AsType)
	defer bldr.Release()
	bldr.Reserve(target.Len())

	for i := 0; i < target.Len(); i++ {
		leaf, ok, err := navigateRow(target, i, remaining)
		if err != nil {
			return nil, err
		}
		if !ok || leaf.Type() == variant.Null {
			bldr.AppendNull()

			continue
		}

		if appendVariantToTypedBuilder(bldr, leaf) {
			continue
		}

		if opts.Safe {
			bldr.AppendNull()

			continue
		}

		return nil, fmt.Errorf("%w: cannot cast variant %v to %s", arrow.ErrInvalid, leaf.Type(), opts.AsType)
	}

	return bldr.NewArray(), nil
}

// navigateRow reassembles row i of target and walks path into it. It returns
// (value, false) when the row is null or the path is absent.
func navigateRow(target *VariantArray, i int, path VariantPath) (variant.Value, bool, error) {
	if target.IsNull(i) {
		return variant.Value{}, false, nil
	}

	v, err := target.Value(i)
	if err != nil {
		return variant.Value{}, false, fmt.Errorf("variant: reassembling row %d: %w", i, err)
	}

	return navigateValue(v, path)
}

// navigateValue walks path into a fully reassembled variant value.
func navigateValue(v variant.Value, path VariantPath) (variant.Value, bool, error) {
	cur := v
	for _, elem := range path {
		if elem.isIndex {
			arr, ok := cur.Value().(variant.ArrayValue)
			if !ok || elem.index < 0 || uint32(elem.index) >= arr.Len() {
				return variant.Value{}, false, nil
			}
			el, err := arr.Value(uint32(elem.index))
			if err != nil {
				return variant.Value{}, false, nil
			}
			cur = el

			continue
		}

		obj, ok := cur.Value().(variant.ObjectValue)
		if !ok {
			return variant.Value{}, false, nil
		}
		field, err := obj.ValueByKey(elem.name)
		if err != nil {
			return variant.Value{}, false, nil
		}
		cur = field.Value
	}

	return cur, true, nil
}

// tryPerfectShredding returns the typed_value column directly when the target is
// perfectly shredded to AsType. It only fires when no ancestor nulls need merging;
// otherwise the caller's per-row path produces the same values.
func tryPerfectShredding(state shreddingState, nulls *nullTracker, asType arrow.DataType) arrow.Array {
	if _, ok := asType.(arrow.NestedType); ok {
		return nil
	}
	if state.typedValue == nil || !nulls.allValid() {
		return nil
	}
	if !arrow.TypeEqual(state.typedValue.DataType(), asType) {
		return nil
	}
	if state.value != nil && state.value.NullN() != state.value.Len() {
		return nil
	}

	state.typedValue.Retain()

	return state.typedValue
}

// buildTargetVariant wraps the current shredding state as a VariantArray, carrying
// the accumulated ancestor nulls onto the storage struct.
func buildTargetVariant(va *VariantArray, state shreddingState, nulls *nullTracker, mem memory.Allocator) (*VariantArray, error) {
	// Take the raw metadata array (not va.Metadata) so dictionary- or large-binary-
	// encoded metadata is preserved and decoded by the target's own reader.
	srcVT := va.ExtensionType().(*VariantType)
	metadata := va.Storage().(*array.Struct).Field(srcVT.metadataFieldIdx)

	fields := []arrow.Field{{Name: "metadata", Type: metadata.DataType(), Nullable: false}}
	cols := []arrow.Array{metadata}

	if state.value != nil {
		fields = append(fields, arrow.Field{Name: "value", Type: state.value.DataType(), Nullable: true})
		cols = append(cols, state.value)
	}
	if state.typedValue != nil {
		fields = append(fields, arrow.Field{Name: "typed_value", Type: state.typedValue.DataType(), Nullable: true})
		cols = append(cols, state.typedValue)
	}

	bitmap, nullCount := nulls.bitmap(mem)
	if bitmap != nil {
		defer bitmap.Release()
	}

	st, err := array.NewStructArrayWithFieldsAndNulls(cols, fields, bitmap, nullCount, 0)
	if err != nil {
		return nil, err
	}
	defer st.Release()

	vt, err := NewVariantType(st.DataType())
	if err != nil {
		return nil, err
	}

	return array.NewExtensionArrayWithStorage(vt, st).(*VariantArray), nil
}

// allNullResult builds the all-null output for a provably missing path.
func allNullResult(va *VariantArray, opts GetOptions) (arrow.Array, error) {
	if opts.AsType != nil {
		return array.MakeArrayOfNull(opts.Mem, opts.AsType, va.Len()), nil
	}

	bldr := NewVariantBuilder(opts.Mem, NewDefaultVariantType())
	defer bldr.Release()
	for i := 0; i < va.Len(); i++ {
		bldr.AppendNull()
	}

	return bldr.NewArray(), nil
}

// nullTracker accumulates ancestor null masks encountered while walking the path.
type nullTracker struct {
	length int
	valid  []bool // nil means all valid
}

func newNullTracker(length int) *nullTracker {
	return &nullTracker{length: length}
}

func (n *nullTracker) apply(arr arrow.Array) {
	if arr == nil || arr.NullN() == 0 {
		return
	}
	if n.valid == nil {
		n.valid = make([]bool, n.length)
		for i := range n.valid {
			n.valid[i] = true
		}
	}
	for i := 0; i < n.length; i++ {
		if arr.IsNull(i) {
			n.valid[i] = false
		}
	}
}

func (n *nullTracker) allValid() bool { return n.valid == nil }

func (n *nullTracker) bitmap(mem memory.Allocator) (*memory.Buffer, int) {
	if n.valid == nil {
		return nil, 0
	}

	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(n.length))))
	nullCount := 0
	for i, v := range n.valid {
		if v {
			bitutil.SetBit(buf.Bytes(), i)
		} else {
			bitutil.ClearBit(buf.Bytes(), i)
			nullCount++
		}
	}

	return buf, nullCount
}
