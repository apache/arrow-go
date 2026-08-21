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

package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
)

// VariantGetOptions controls VariantGet.
type VariantGetOptions struct {
	// Path is the path to extract from every variant value.
	Path variant.VariantPath
	// AsType, when nil, makes VariantGet return a VariantArray pointing at the path;
	// when set, the extracted values are cast to it via the cast kernels.
	AsType arrow.DataType
	// Strict makes a lossy cast fail; the default allows overflow and truncation via
	// the cast kernels. Unlike arrow-rs safe mode there is no null-on-failure: an
	// impossible cast always errors, since arrow-go's cast kernels have no safe flag.
	Strict bool
}

// VariantGet extracts opts.Path from every value of input. It follows the shredded
// typed_value columns as far as the path allows - stepping into struct fields
// directly and gathering list elements with the take kernel - then reassembles only
// the residual for any remaining path. With AsType nil it returns a VariantArray of
// the extracted values; otherwise it casts them to AsType with the cast kernels.
func VariantGet(ctx context.Context, input *extensions.VariantArray, opts VariantGetOptions) (arrow.Array, error) {
	if input == nil {
		return nil, fmt.Errorf("%w: VariantGet requires a non-nil VariantArray", arrow.ErrInvalid)
	}

	// Empty path, no cast: the values are returned unchanged.
	if opts.Path.Len() == 0 && opts.AsType == nil {
		input.Retain()

		return input, nil
	}

	return shreddedGetPath(ctx, input, opts)
}

// shreddingState is a (value?, typed_value?) column pair at one level of a shredded
// variant, mirroring arrow-rs ShreddingState.
type shreddingState struct {
	value      arrow.TypedArray[[]byte]
	typedValue arrow.Array
	length     int
}

func stateFromInput(input *extensions.VariantArray) shreddingState {
	return shreddingState{
		value:      input.UntypedValues(),
		typedValue: input.Shredded(),
		length:     input.Len(),
	}
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
	owned []arrow.Array // intermediate take results the caller must release
}

// missingStep reports whether an absent typed field is provably missing (the value
// column is all-null) or merely not shredded (a residual may hold it).
func (s shreddingState) missingStep() pathStep {
	if s.value == nil || s.value.NullN() == s.value.Len() {
		return pathStep{kind: stepMissing}
	}

	return pathStep{kind: stepNotShredded}
}

func fieldStep(s shreddingState, name string) (pathStep, error) {
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

// indexStep gathers element index from every row of a shredded list with the take
// kernel, producing the shredding state one level deeper.
func indexStep(ctx context.Context, mem memory.Allocator, s shreddingState, index int) (pathStep, error) {
	if s.typedValue == nil {
		return s.missingStep(), nil
	}
	list, ok := s.typedValue.(array.ListLike)
	if !ok {
		return s.missingStep(), nil
	}
	elems, ok := list.ListValues().(*array.Struct)
	if !ok {
		return s.missingStep(), nil
	}

	ib := array.NewUint64Builder(mem)
	defer ib.Release()
	ib.Reserve(s.length)
	for row := 0; row < s.length; row++ {
		start, end := list.ValueOffsets(row)
		if list.IsValid(row) && index >= 0 && int64(index) < end-start {
			ib.Append(uint64(start + int64(index)))
		} else {
			ib.AppendNull()
		}
	}
	indices := ib.NewArray()
	defer indices.Release()

	et := elems.DataType().(*arrow.StructType)
	var owned []arrow.Array
	var next shreddingState
	next.length = s.length

	if vi, ok := et.FieldIdx("value"); ok {
		taken, err := TakeArray(ctx, elems.Field(vi), indices)
		if err != nil {
			return pathStep{}, err
		}
		owned = append(owned, taken)
		next.value = taken.(arrow.TypedArray[[]byte])
	}
	if ti, ok := et.FieldIdx("typed_value"); ok {
		taken, err := TakeArray(ctx, elems.Field(ti), indices)
		if err != nil {
			releaseAll(owned)

			return pathStep{}, err
		}
		owned = append(owned, taken)
		next.typedValue = taken
	}

	return pathStep{kind: stepSuccess, state: next, owned: owned}, nil
}

func releaseAll(arrs []arrow.Array) {
	for _, a := range arrs {
		a.Release()
	}
}

func shreddedGetPath(ctx context.Context, input *extensions.VariantArray, opts VariantGetOptions) (arrow.Array, error) {
	mem := GetAllocator(ctx)
	state := stateFromInput(input)
	nulls := newNullTracker(input.Len(), mem)
	defer nulls.release()
	nulls.merge(input.Storage())

	var owned []arrow.Array
	defer func() { releaseAll(owned) }()

	idx := 0
	for idx < opts.Path.Len() {
		name, index := opts.Path.StepAt(idx)
		var (
			step pathStep
			err  error
		)
		if name != "" {
			step, err = fieldStep(state, name)
		} else {
			step, err = indexStep(ctx, mem, state, index)
		}
		if err != nil {
			return nil, err
		}

		if step.kind == stepSuccess {
			nulls.merge(state.typedValue)
			state = step.state
			owned = append(owned, step.owned...)
			idx++

			continue
		}
		if step.kind == stepMissing {
			return allNullResult(mem, input.Len(), opts.AsType), nil
		}

		break // stepNotShredded
	}

	remaining := subPath(opts.Path, idx)

	// Try to return the typed column directly before building the target array,
	// so a perfect shredding does not allocate a struct and bitmap it discards.
	if remaining.Len() == 0 && opts.AsType != nil {
		if col := perfectShredded(state, nulls, opts.AsType); col != nil {
			defer col.Release()

			return CastArray(ctx, col, NewCastOptions(opts.AsType, opts.Strict))
		}
	}

	target, err := buildTargetVariant(input, state, nulls, mem)
	if err != nil {
		return nil, err
	}
	defer target.Release()

	if remaining.Len() == 0 && opts.AsType == nil {
		target.Retain()

		return target, nil
	}

	leaves, err := extractLeaves(target, remaining)
	if err != nil {
		return nil, err
	}
	if opts.AsType == nil {
		return buildLeafVariantArray(mem, leaves), nil
	}

	src := buildNaturalArray(mem, leaves)
	if src == nil {
		return allNullResult(mem, len(leaves), opts.AsType), nil
	}
	defer src.Release()

	return CastArray(ctx, src, NewCastOptions(opts.AsType, opts.Strict))
}

// perfectShredded returns the typed_value column when the path landed on a fully
// shredded value of exactly AsType and no ancestor nulls need merging; otherwise
// the caller's reassembly path produces the same values.
func perfectShredded(s shreddingState, nulls *nullTracker, asType arrow.DataType) arrow.Array {
	if _, ok := asType.(arrow.NestedType); ok {
		return nil
	}
	if s.typedValue == nil || !nulls.allValid() {
		return nil
	}
	if !arrow.TypeEqual(s.typedValue.DataType(), asType) {
		return nil
	}
	if s.value != nil && s.value.NullN() != s.value.Len() {
		return nil
	}

	s.typedValue.Retain()

	return s.typedValue
}

func buildTargetVariant(input *extensions.VariantArray, s shreddingState, nulls *nullTracker, mem memory.Allocator) (*extensions.VariantArray, error) {
	// Read the raw metadata column rather than input.Metadata(), which asserts plain
	// binary and panics on dictionary-encoded metadata; the raw column preserves
	// dictionary/large-binary encoding and is decoded by the target's own reader.
	storage := input.Storage().(*array.Struct)
	mdIdx, ok := storage.DataType().(*arrow.StructType).FieldIdx("metadata")
	if !ok {
		return nil, fmt.Errorf("%w: variant storage is missing its metadata field", arrow.ErrInvalid)
	}
	metadata := storage.Field(mdIdx)

	fields := []arrow.Field{{Name: "metadata", Type: metadata.DataType(), Nullable: false}}
	cols := []arrow.Array{metadata}
	if s.value != nil {
		fields = append(fields, arrow.Field{Name: "value", Type: s.value.DataType(), Nullable: true})
		cols = append(cols, s.value)
	}
	if s.typedValue != nil {
		fields = append(fields, arrow.Field{Name: "typed_value", Type: s.typedValue.DataType(), Nullable: true})
		cols = append(cols, s.typedValue)
	}

	bitmap, nullCount := nulls.validityBitmap()
	st, err := array.NewStructArrayWithFieldsAndNulls(cols, fields, bitmap, nullCount, 0)
	if err != nil {
		return nil, err
	}
	defer st.Release()

	vt, err := extensions.NewVariantType(st.DataType())
	if err != nil {
		return nil, err
	}

	return array.NewExtensionArrayWithStorage(vt, st).(*extensions.VariantArray), nil
}

// subPath returns the suffix of p starting at from, rebuilt through the opaque API.
func subPath(p variant.VariantPath, from int) variant.VariantPath {
	var out variant.VariantPath
	for i := from; i < p.Len(); i++ {
		if name, index := p.StepAt(i); name != "" {
			out = out.Field(name)
		} else {
			out = out.Index(index)
		}
	}

	return out
}

// variantLeaf is one row's extracted value; present is false when the path is
// absent for that row (or the row is null).
type variantLeaf struct {
	value   variant.Value
	present bool
}

func extractLeaves(target *extensions.VariantArray, path variant.VariantPath) ([]variantLeaf, error) {
	leaves := make([]variantLeaf, target.Len())
	for i := range leaves {
		if target.IsNull(i) {
			continue
		}
		v, err := target.Value(i)
		if err != nil {
			return nil, fmt.Errorf("variant: reassembling row %d: %w", i, err)
		}
		leaf, found, err := v.GetByPath(path)
		if err != nil {
			return nil, err
		}
		leaves[i] = variantLeaf{value: leaf, present: found}
	}

	return leaves, nil
}

func buildLeafVariantArray(mem memory.Allocator, leaves []variantLeaf) arrow.Array {
	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	bldr.Reserve(len(leaves))
	for _, l := range leaves {
		if !l.present {
			bldr.AppendNull()

			continue
		}
		bldr.Append(l.value)
	}

	return bldr.NewArray()
}

// buildNaturalArray materializes the leaves as an array of the first present leaf's
// natural Arrow type so the cast kernels can convert it. Rows whose value does not
// match that natural type become null. Returns nil when no leaf is present.
func buildNaturalArray(mem memory.Allocator, leaves []variantLeaf) arrow.Array {
	var natural arrow.DataType
	for _, l := range leaves {
		if l.present && l.value.Type() != variant.Null {
			natural = naturalArrowType(l.value)

			break
		}
	}
	if natural == nil {
		return nil
	}

	bldr := array.NewBuilder(mem, natural)
	defer bldr.Release()
	bldr.Reserve(len(leaves))
	for _, l := range leaves {
		if !l.present || !appendNatural(bldr, l.value) {
			bldr.AppendNull()
		}
	}

	return bldr.NewArray()
}

// allNullResult builds the all-null output for a provably missing path.
func allNullResult(mem memory.Allocator, n int, asType arrow.DataType) arrow.Array {
	if asType != nil {
		return array.MakeArrayOfNull(mem, asType, n)
	}

	// MakeArrayOfNull cannot build the variant extension type (its storage struct's
	// metadata/value are non-nullable), so append encoded variant nulls instead.
	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	for range n {
		bldr.AppendNull()
	}

	return bldr.NewArray()
}

// nullTracker accumulates ancestor validity bitmaps with a bitmap AND.
type nullTracker struct {
	length int
	mem    memory.Allocator
	buf    *memory.Buffer // validity bitmap (1 = valid); nil means all valid
}

func newNullTracker(length int, mem memory.Allocator) *nullTracker {
	return &nullTracker{length: length, mem: mem}
}

// merge folds arr's validity into the accumulated mask. Arrow validity bits are
// 1=valid, so accumulating ancestor nulls is a bitmap AND (a row is null in the
// result when it is null at any level) - the validity-space equivalent of OR-ing
// null masks.
func (n *nullTracker) merge(arr arrow.Array) {
	if arr == nil {
		return
	}
	vb := arr.Data().Buffers()[0]
	if vb == nil {
		return // all valid
	}
	off := int64(arr.Data().Offset())
	if n.buf == nil {
		n.buf = bitutil.BitmapAndAlloc(n.mem, vb.Bytes(), vb.Bytes(), off, off, int64(n.length), 0)

		return
	}
	merged := bitutil.BitmapAndAlloc(n.mem, n.buf.Bytes(), vb.Bytes(), 0, off, int64(n.length), 0)
	n.buf.Release()
	n.buf = merged
}

func (n *nullTracker) allValid() bool { return n.buf == nil }

func (n *nullTracker) validityBitmap() (*memory.Buffer, int) {
	if n.buf == nil {
		return nil, 0
	}

	return n.buf, n.length - bitutil.CountSetBits(n.buf.Bytes(), 0, n.length)
}

func (n *nullTracker) release() {
	if n.buf != nil {
		n.buf.Release()
		n.buf = nil
	}
}

func naturalArrowType(v variant.Value) arrow.DataType {
	switch v.Type() {
	case variant.Bool:
		return arrow.FixedWidthTypes.Boolean
	case variant.Int8:
		return arrow.PrimitiveTypes.Int8
	case variant.Int16:
		return arrow.PrimitiveTypes.Int16
	case variant.Int32:
		return arrow.PrimitiveTypes.Int32
	case variant.Int64:
		return arrow.PrimitiveTypes.Int64
	case variant.Float:
		return arrow.PrimitiveTypes.Float32
	case variant.Double:
		return arrow.PrimitiveTypes.Float64
	case variant.String:
		return arrow.BinaryTypes.String
	case variant.Binary:
		return arrow.BinaryTypes.Binary
	case variant.Date:
		return arrow.FixedWidthTypes.Date32
	case variant.Time:
		return arrow.FixedWidthTypes.Time64us
	case variant.TimestampMicros:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case variant.TimestampMicrosNTZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case variant.TimestampNanos:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	case variant.TimestampNanosNTZ:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case variant.UUID:
		return extensions.NewUUIDType()
	case variant.Decimal4, variant.Decimal8, variant.Decimal16:
		return &arrow.Decimal128Type{Precision: 38, Scale: int32(decimalScale(v))}
	}

	return nil
}

// appendNatural appends v to bldr when v matches bldr's natural type, reporting
// whether it did; a non-matching value is left for the caller to null.
func appendNatural(bldr array.Builder, v variant.Value) bool {
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		if x, ok := v.Value().(bool); ok {
			b.Append(x)

			return true
		}
	case *array.Int8Builder:
		if x, ok := v.Value().(int8); ok {
			b.Append(x)

			return true
		}
	case *array.Int16Builder:
		if x, ok := v.Value().(int16); ok {
			b.Append(x)

			return true
		}
	case *array.Int32Builder:
		if x, ok := v.Value().(int32); ok {
			b.Append(x)

			return true
		}
	case *array.Int64Builder:
		if x, ok := v.Value().(int64); ok {
			b.Append(x)

			return true
		}
	case *array.Float32Builder:
		if x, ok := v.Value().(float32); ok {
			b.Append(x)

			return true
		}
	case *array.Float64Builder:
		if x, ok := v.Value().(float64); ok {
			b.Append(x)

			return true
		}
	case *array.StringBuilder:
		if x, ok := v.Value().(string); ok {
			b.Append(x)

			return true
		}
	case *array.BinaryBuilder:
		if x, ok := v.Value().([]byte); ok {
			b.Append(x)

			return true
		}
	case *array.Date32Builder:
		if x, ok := v.Value().(arrow.Date32); ok {
			b.Append(x)

			return true
		}
	case *array.Time64Builder:
		if x, ok := v.Value().(arrow.Time64); ok {
			b.Append(x)

			return true
		}
	case *array.TimestampBuilder:
		if x, ok := v.Value().(arrow.Timestamp); ok {
			b.Append(x)

			return true
		}
	case *extensions.UUIDBuilder:
		if x, ok := v.Value().(uuid.UUID); ok {
			b.Append(x)

			return true
		}
	case *array.Decimal128Builder:
		if num, ok := decimalAsNum128(v); ok && int32(decimalScale(v)) == b.Type().(*arrow.Decimal128Type).Scale {
			b.Append(num)

			return true
		}
	}

	return false
}

func decimalScale(v variant.Value) uint8 {
	switch d := v.Value().(type) {
	case variant.DecimalValue[decimal.Decimal32]:
		return d.Scale
	case variant.DecimalValue[decimal.Decimal64]:
		return d.Scale
	case variant.DecimalValue[decimal.Decimal128]:
		return d.Scale
	}

	return 0
}

func decimalAsNum128(v variant.Value) (decimal128.Num, bool) {
	switch d := v.Value().(type) {
	case variant.DecimalValue[decimal.Decimal32]:
		return decimal128.FromI64(int64(d.Value.(decimal.Decimal32))), true
	case variant.DecimalValue[decimal.Decimal64]:
		return decimal128.FromI64(int64(d.Value.(decimal.Decimal64))), true
	case variant.DecimalValue[decimal.Decimal128]:
		return d.Value.(decimal.Decimal128), true
	}

	return decimal128.Num{}, false
}
