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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

func listElementOutputType(_ *exec.KernelCtx, inputTypes []arrow.DataType) (arrow.DataType, error) {
	listType, ok := inputTypes[0].(arrow.ListLikeType)
	if !ok {
		return nil, fmt.Errorf("%w: list_element requires a list-like input", arrow.ErrType)
	}
	return listType.Elem(), nil
}

func getListElementIndex(value *exec.ExecValue) (uint64, error) {
	if value.IsScalar() {
		if !value.Scalar.IsValid() {
			return 0, fmt.Errorf("%w: list_element index must not be null", arrow.ErrInvalid)
		}
		return scalarIndex(value.Scalar)
	}

	if value.Array.Len == 0 {
		return 0, fmt.Errorf("%w: list_element index array is empty", arrow.ErrInvalid)
	}
	if value.Array.Len > 1 {
		return 0, fmt.Errorf("%w: list_element does not support arrays of list indices", arrow.ErrNotImplemented)
	}
	if value.Array.UpdateNullCount() != 0 {
		return 0, fmt.Errorf("%w: list_element index must not contain nulls", arrow.ErrInvalid)
	}

	switch value.Array.Type.ID() {
	case arrow.INT8:
		return unsignedIndex(exec.GetSpanValues[int8](&value.Array, 1)[0])
	case arrow.INT16:
		return unsignedIndex(exec.GetSpanValues[int16](&value.Array, 1)[0])
	case arrow.INT32:
		return unsignedIndex(exec.GetSpanValues[int32](&value.Array, 1)[0])
	case arrow.INT64:
		return unsignedIndex(exec.GetSpanValues[int64](&value.Array, 1)[0])
	case arrow.UINT8:
		return uint64(exec.GetSpanValues[uint8](&value.Array, 1)[0]), nil
	case arrow.UINT16:
		return uint64(exec.GetSpanValues[uint16](&value.Array, 1)[0]), nil
	case arrow.UINT32:
		return uint64(exec.GetSpanValues[uint32](&value.Array, 1)[0]), nil
	case arrow.UINT64:
		return exec.GetSpanValues[uint64](&value.Array, 1)[0], nil
	default:
		return 0, fmt.Errorf("%w: invalid list_element index type %s", arrow.ErrType, value.Array.Type)
	}
}

func scalarIndex(value scalar.Scalar) (uint64, error) {
	switch value := value.(type) {
	case *scalar.Int8:
		return unsignedIndex(value.Value)
	case *scalar.Int16:
		return unsignedIndex(value.Value)
	case *scalar.Int32:
		return unsignedIndex(value.Value)
	case *scalar.Int64:
		return unsignedIndex(value.Value)
	case *scalar.Uint8:
		return uint64(value.Value), nil
	case *scalar.Uint16:
		return uint64(value.Value), nil
	case *scalar.Uint32:
		return uint64(value.Value), nil
	case *scalar.Uint64:
		return value.Value, nil
	default:
		return 0, fmt.Errorf("%w: invalid list_element index type %s", arrow.ErrType, value.DataType())
	}
}

func unsignedIndex[T arrow.IntType](value T) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: list_element index %d is out of bounds: should be greater than or equal to 0", arrow.ErrInvalid, value)
	}
	return uint64(value), nil
}

func listElementValueOffsets(list *exec.ArraySpan, i int64) (int64, int64, error) {
	switch list.Type.ID() {
	case arrow.LIST:
		offsets := exec.GetSpanOffsets[int32](list, 1)
		return int64(offsets[i]), int64(offsets[i+1]), nil
	case arrow.LARGE_LIST:
		offsets := exec.GetSpanOffsets[int64](list, 1)
		return offsets[i], offsets[i+1], nil
	case arrow.LIST_VIEW:
		offsets := exec.GetSpanValues[int32](list, 1)
		sizes := exec.GetSpanValues[int32](list, 2)
		start := int64(offsets[i])
		return start, start + int64(sizes[i]), nil
	case arrow.LARGE_LIST_VIEW:
		offsets := exec.GetSpanValues[int64](list, 1)
		sizes := exec.GetSpanValues[int64](list, 2)
		start := offsets[i]
		return start, start + sizes[i], nil
	case arrow.FIXED_SIZE_LIST:
		size := int64(list.Type.(*arrow.FixedSizeListType).Len())
		start := (list.Offset + i) * size
		return start, start + size, nil
	default:
		return 0, 0, fmt.Errorf("%w: unsupported list_element input type %s", arrow.ErrType, list.Type)
	}
}

func listElementExec(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	list := &batch.Values[0].Array
	if len(list.Children) == 0 {
		return fmt.Errorf("%w: list_element input has no values child", arrow.ErrInvalid)
	}

	index, err := getListElementIndex(&batch.Values[1])
	if err != nil {
		return err
	}

	elemType := list.Type.(arrow.ListLikeType).Elem()
	if list.Len == 0 {
		empty := array.MakeArrayOfNull(exec.GetAllocator(ctx.Ctx), elemType, 0)
		defer empty.Release()
		out.TakeOwnership(empty.Data())
		return nil
	}
	if !listElementTakeSupported(elemType) {
		return listElementConcat(ctx, list, index, elemType, out)
	}

	indexBuilder := array.NewInt64Builder(exec.GetAllocator(ctx.Ctx))
	defer indexBuilder.Release()
	indexBuilder.Reserve(int(list.Len))
	for i := int64(0); i < list.Len; i++ {
		if len(list.Buffers[0].Buf) != 0 && bitutil.BitIsNotSet(list.Buffers[0].Buf, int(list.Offset+i)) {
			indexBuilder.AppendNull()
			continue
		}

		start, end, err := listElementValueOffsets(list, i)
		if err != nil {
			return err
		}
		if end < start {
			return fmt.Errorf("%w: list_element input has invalid value offsets", arrow.ErrInvalid)
		}
		length := uint64(end - start)
		if index >= length {
			return fmt.Errorf("%w: list_element index %d is out of bounds: should be in [0, %d)", arrow.ErrInvalid, index, length)
		}
		indexBuilder.Append(start + int64(index))
	}

	indices := indexBuilder.NewArray()
	defer indices.Release()
	if handled, err := listElementTake(ctx, &list.Children[0], indices, out); handled {
		return err
	}
	return listElementTakeFallback(ctx, &list.Children[0], indices, out)
}

func listElementTakeSupported(typ arrow.DataType) bool {
	id := typ.ID()
	if id == arrow.NULL || arrow.IsBinaryLike(id) || arrow.IsLargeBinaryLike(id) ||
		arrow.IsFixedSizeBinary(id) || id == arrow.SPARSE_UNION || id == arrow.DENSE_UNION {
		return true
	}
	if !arrow.IsPrimitive(id) {
		return false
	}

	// PrimitiveTake has specialized implementations for these widths only.
	// In particular, INTERVAL_MONTH_DAY_NANO is a primitive 128-bit type and
	// must use the generic concatenation fallback below.
	fixed, ok := typ.(arrow.FixedWidthDataType)
	if !ok {
		return false
	}
	switch fixed.BitWidth() {
	case 1, 8, 16, 32, 64:
		return true
	default:
		return false
	}
}

func listElementConcat(ctx *exec.KernelCtx, list *exec.ArraySpan, index uint64, elemType arrow.DataType, out *exec.ExecResult) error {
	values := list.Children[0].MakeArray()
	defer values.Release()
	pieces := make([]arrow.Array, 0, int(list.Len))
	defer func() {
		for _, piece := range pieces {
			piece.Release()
		}
	}()

	for i := int64(0); i < list.Len; i++ {
		if len(list.Buffers[0].Buf) != 0 && bitutil.BitIsNotSet(list.Buffers[0].Buf, int(list.Offset+i)) {
			pieces = append(pieces, array.MakeArrayOfNull(exec.GetAllocator(ctx.Ctx), elemType, 1))
			continue
		}

		start, end, err := listElementValueOffsets(list, i)
		if err != nil {
			return err
		}
		if end < start {
			return fmt.Errorf("%w: list_element input has invalid value offsets", arrow.ErrInvalid)
		}
		length := uint64(end - start)
		if index >= length {
			return fmt.Errorf("%w: list_element index %d is out of bounds: should be in [0, %d)", arrow.ErrInvalid, index, length)
		}
		selected := start + int64(index)
		pieces = append(pieces, array.NewSlice(values, selected, selected+1))
	}

	result, err := array.Concatenate(pieces, exec.GetAllocator(ctx.Ctx))
	if err != nil {
		return err
	}
	defer result.Release()
	out.TakeOwnership(result.Data())
	return nil
}

func listElementTakeFallback(ctx *exec.KernelCtx, values *exec.ArraySpan, indices arrow.Array, out *exec.ExecResult) error {
	elemType := values.Type
	valuesArray := values.MakeArray()
	defer valuesArray.Release()
	pieces := make([]arrow.Array, 0, indices.Len())
	defer func() {
		for _, piece := range pieces {
			piece.Release()
		}
	}()

	for i := 0; i < indices.Len(); i++ {
		if indices.IsNull(i) {
			pieces = append(pieces, array.MakeArrayOfNull(exec.GetAllocator(ctx.Ctx), elemType, 1))
			continue
		}
		selected := indices.(*array.Int64).Value(i)
		pieces = append(pieces, array.NewSlice(valuesArray, selected, selected+1))
	}

	result, err := array.Concatenate(pieces, exec.GetAllocator(ctx.Ctx))
	if err != nil {
		return err
	}
	defer result.Release()
	out.TakeOwnership(result.Data())
	return nil
}

func listElementDenseUnionTake(ctx *exec.KernelCtx, values *exec.ArraySpan, indices arrow.Array, out *exec.ExecResult) error {
	var indexSpan exec.ArraySpan
	indexSpan.SetMembers(indices.Data())
	batch := &exec.ExecSpan{
		Len: int64(indices.Len()),
		Values: []exec.ExecValue{
			{Array: *values},
			{Array: indexSpan},
		},
	}
	takeCtx := *ctx
	takeCtx.State = TakeOptions{BoundsCheck: false}
	if err := TakeExec(DenseUnionImpl)(&takeCtx, batch, out); err != nil {
		return err
	}

	for i := range out.Children {
		childIndices := out.Children[i].MakeArray()
		childOut := &exec.ExecResult{Type: values.Children[i].Type}
		handled, err := listElementTake(ctx, &values.Children[i], childIndices, childOut)
		if err == nil && !handled {
			err = listElementTakeFallback(ctx, &values.Children[i], childIndices, childOut)
		}
		childIndices.Release()
		if err != nil {
			childOut.Release()
			return err
		}

		childData := childOut.MakeData()
		out.Children[i].Release()
		out.Children[i].TakeOwnership(childData)
		childData.Release()
	}
	return nil
}

func listElementSparseUnionTake(ctx *exec.KernelCtx, values *exec.ArraySpan, indices arrow.Array, out *exec.ExecResult) error {
	valuesArray := values.MakeArray()
	defer valuesArray.Release()

	unionType := values.Type.(*arrow.SparseUnionType)
	builder := array.NewSparseUnionBuilder(exec.GetAllocator(ctx.Ctx), unionType)
	defer builder.Release()
	builder.Reserve(indices.Len())
	for i := 0; i < builder.NumChildren(); i++ {
		builder.Child(i).Reserve(indices.Len())
	}

	indexArray := indices.(*array.Int64)
	for i := 0; i < indices.Len(); i++ {
		var value scalar.Scalar
		if indices.IsNull(i) {
			value = scalar.MakeNullScalar(unionType)
		} else {
			var err error
			value, err = scalar.GetScalar(valuesArray, int(indexArray.Value(i)))
			if err != nil {
				return err
			}
		}

		if err := listElementAppendSparseUnionValue(builder, value.(*scalar.SparseUnion)); err != nil {
			if releasable, ok := value.(scalar.Releasable); ok {
				releasable.Release()
			}
			return err
		}
		if releasable, ok := value.(scalar.Releasable); ok {
			releasable.Release()
		}
	}

	result := builder.NewArray()
	defer result.Release()
	out.TakeOwnership(result.Data())
	return nil
}

func listElementAppendSparseUnionValue(builder *array.SparseUnionBuilder, value *scalar.SparseUnion) error {
	builder.Append(value.TypeCode)
	for i := 0; i < builder.NumChildren(); i++ {
		child := builder.Child(i)
		if i != value.ChildID {
			child.AppendEmptyValue()
			continue
		}

		if !value.IsValid() {
			child.AppendNull()
			continue
		}
		if err := scalar.Append(child, value.Value[i]); err != nil {
			return err
		}
	}
	return nil
}

func listElementTake(ctx *exec.KernelCtx, values *exec.ArraySpan, indices arrow.Array, out *exec.ExecResult) (bool, error) {
	var indexSpan exec.ArraySpan
	indexSpan.SetMembers(indices.Data())
	batch := &exec.ExecSpan{
		Len: int64(indices.Len()),
		Values: []exec.ExecValue{
			{Array: *values},
			{Array: indexSpan},
		},
	}
	takeCtx := *ctx
	takeCtx.State = TakeOptions{BoundsCheck: false}

	switch id := values.Type.ID(); {
	case id == arrow.NULL:
		return true, NullTake(&takeCtx, batch, out)
	case arrow.IsPrimitive(id):
		return true, PrimitiveTake(&takeCtx, batch, out)
	case arrow.IsBinaryLike(id):
		return true, TakeExec(VarBinaryImpl[int32])(&takeCtx, batch, out)
	case arrow.IsLargeBinaryLike(id):
		return true, TakeExec(VarBinaryImpl[int64])(&takeCtx, batch, out)
	case arrow.IsFixedSizeBinary(id):
		return true, TakeExec(FSBImpl)(&takeCtx, batch, out)
	case id == arrow.SPARSE_UNION:
		return true, listElementSparseUnionTake(ctx, values, indices, out)
	case id == arrow.DENSE_UNION:
		return true, listElementDenseUnionTake(ctx, values, indices, out)
	default:
		return false, nil
	}
}

func GetListElementKernels() []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0, 5)
	for _, listID := range []arrow.Type{
		arrow.LIST,
		arrow.LARGE_LIST,
		arrow.LIST_VIEW,
		arrow.LARGE_LIST_VIEW,
		arrow.FIXED_SIZE_LIST,
	} {
		kernel := exec.NewScalarKernel(
			[]exec.InputType{
				exec.NewIDInput(listID),
				exec.NewMatchedInput(exec.Integer()),
			},
			exec.NewComputedOutputType(listElementOutputType),
			listElementExec,
			nil)
		kernel.NullHandling = exec.NullComputedNoPrealloc
		kernel.MemAlloc = exec.MemNoPrealloc
		kernels = append(kernels, kernel)
	}
	return kernels
}
