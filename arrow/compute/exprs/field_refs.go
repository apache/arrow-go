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

package exprs

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/internal"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/substrait-io/substrait-go/v8/expr"
)

func getFields(typ arrow.DataType) []arrow.Field {
	if nested, ok := typ.(arrow.NestedType); ok {
		return nested.Fields()
	}
	return nil
}

// GetRefField evaluates the substrait field reference to retrieve the
// referenced field or return an error.
func GetRefField(ref expr.ReferenceSegment, fields []arrow.Field) (*arrow.Field, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	var (
		out *arrow.Field
	)

	for ref != nil {
		if len(fields) == 0 {
			if out == nil {
				return nil, compute.ErrNoChildren
			}
			return nil, fmt.Errorf("%w: %s", compute.ErrNoChildren, out.Type)
		}

		switch f := ref.(type) {
		case *expr.StructFieldRef:
			if f.Field < 0 || f.Field >= int32(len(fields)) {
				return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, f)
			}

			out = &fields[f.Field]
			fields = getFields(out.Type)
		default:
			return nil, arrow.ErrNotImplemented
		}

		ref = ref.GetChild()
	}

	return out, nil
}

// GetRefSchema evaluates the provided substrait field reference against
// the schema to retrieve the referenced (potentially nested) field.
func GetRefSchema(ref expr.ReferenceSegment, schema *arrow.Schema) (*arrow.Field, error) {
	return GetRefField(ref, schema.Fields())
}

// GetScalar returns the evaluated referenced scalar value from the provided
// scalar which must be appropriate to the type of reference.
//
// A StructFieldRef can only reference against a Struct-type scalar, a
// ListElementRef can only reference against a List or LargeList scalar,
// and a MapKeyRef will only reference against a Map scalar. An error is
// returned if following the reference children ends up with an invalid
// nested reference object.
//
// The result is a new reference: if it implements scalar.Releasable, the
// caller must release it.
func GetScalar(ref expr.ReferenceSegment, s scalar.Scalar, mem memory.Allocator, ext ExtensionIDSet) (scalar.Scalar, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	// once owned is set, s is a reference this function must release when it
	// moves past it or fails
	owned := false
	for ; ref != nil; ref = ref.GetChild() {
		next, borrowed, err := scalarStep(ref, s, mem, ext)
		if err == nil && borrowed {
			retainScalar(next)
		}
		if owned {
			releaseScalar(s)
		}
		if err != nil {
			return nil, err
		}
		s, owned = next, true
	}
	return s, nil
}

// scalarStep follows one reference segment from s. borrowed reports whether
// next is part of s, rather than a new scalar the caller owns.
func scalarStep(ref expr.ReferenceSegment, s scalar.Scalar, mem memory.Allocator, ext ExtensionIDSet) (next scalar.Scalar, borrowed bool, err error) {
	switch f := ref.(type) {
	case *expr.StructFieldRef:
		if s.DataType().ID() != arrow.STRUCT {
			return nil, false, fmt.Errorf("%w: attempting to reference field from non-struct scalar %s",
				arrow.ErrInvalid, s)
		}

		st := s.(*scalar.Struct)
		fields := st.Type.(*arrow.StructType).Fields()
		if f.Field < 0 || f.Field >= int32(len(fields)) {
			return nil, false, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
		}
		if !st.IsValid() {
			return scalar.MakeNullScalar(fields[f.Field].Type), false, nil
		}
		return st.Value[f.Field], true, nil
	case *expr.ListElementRef:
		var values arrow.Array
		switch v := s.(type) {
		case *scalar.List:
			values = v.Value
		case *scalar.LargeList:
			values = v.Value
		default:
			return nil, false, fmt.Errorf("%w: cannot get ListElementRef from non-list scalar %s",
				arrow.ErrInvalid, s)
		}
		if !s.IsValid() {
			return scalar.MakeNullScalar(s.DataType().(arrow.ListLikeType).Elem()), false, nil
		}
		sc, err := scalar.GetScalar(values, int(f.Offset))
		return sc, false, err
	case *expr.MapKeyRef:
		v, ok := s.(*scalar.Map)
		if !ok {
			return nil, false, arrow.ErrInvalid
		}

		dt, _, err := FromSubstraitType(f.MapKey.GetType(), ext)
		if err != nil {
			return nil, false, err
		}

		mapType := v.Type.(*arrow.MapType)
		if !arrow.TypeEqual(dt, mapType.KeyType()) {
			return nil, false, arrow.ErrInvalid
		}
		if !v.IsValid() {
			return scalar.MakeNullScalar(mapType.ItemType()), false, nil
		}

		keyvalDatum, err := literalToDatum(mem, f.MapKey, ext)
		if err != nil {
			return nil, false, err
		}
		defer keyvalDatum.Release()

		var (
			keyval = keyvalDatum.(*compute.ScalarDatum)
			m      = v.Value.(*array.Struct)
			keys   = m.Field(0)
		)
		for i := 0; i < v.Value.Len(); i++ {
			kv, err := scalar.GetScalar(keys, i)
			if err != nil {
				return nil, false, err
			}
			found := scalar.Equals(kv, keyval.Value)
			releaseScalar(kv)
			if found {
				sc, err := scalar.GetScalar(m.Field(1), i)
				return sc, false, err
			}
		}
		return nil, false, arrow.ErrNotFound
	}
	return nil, false, arrow.ErrNotImplemented
}

func retainScalar(s scalar.Scalar) {
	if r, ok := s.(scalar.Releasable); ok {
		r.Retain()
	}
}

func releaseScalar(s scalar.Scalar) {
	if r, ok := s.(scalar.Releasable); ok {
		r.Release()
	}
}

// GetReferencedValue retrieves the referenced (potentially nested) value from
// the provided datum which may be a scalar, array, or record batch. The
// result is a new reference that the caller must release.
func GetReferencedValue(mem memory.Allocator, ref expr.ReferenceSegment, value compute.Datum, ext ExtensionIDSet) (compute.Datum, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	// once owned is set, cur holds a reference this function must release
	// when it moves past it or fails
	cur, owned := value, false
	for ; ref != nil; ref = ref.GetChild() {
		var (
			next compute.Datum
			err  error
		)
		switch v := cur.(type) {
		case *compute.ScalarDatum:
			var (
				sc       scalar.Scalar
				borrowed bool
			)
			if sc, borrowed, err = scalarStep(ref, v.Value, mem, ext); err == nil {
				if borrowed {
					retainScalar(sc)
				}
				next = &compute.ScalarDatum{Value: sc}
			}
		case *compute.ArrayDatum:
			next, err = arrayStep(mem, ref, v)
		case *compute.RecordDatum:
			next, err = recordStep(ref, v)
		default:
			err = arrow.ErrNotImplemented
		}

		if owned {
			cur.Release()
		}
		if err != nil {
			return nil, err
		}
		cur, owned = next, true
	}
	return cur, nil
}

// arrayStep follows one reference segment from an array, returning a new
// reference.
func arrayStep(mem memory.Allocator, ref expr.ReferenceSegment, v *compute.ArrayDatum) (compute.Datum, error) {
	switch r := ref.(type) {
	case *expr.StructFieldRef:
		if v.Type().ID() != arrow.STRUCT {
			return nil, fmt.Errorf("%w: struct field ref for non struct type %s",
				arrow.ErrInvalid, v.Type())
		}
		if r.Field < 0 || r.Field >= int32(len(v.Value.Children())) {
			return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
		}
		field, err := structField(mem, v.Value, int(r.Field))
		if err != nil {
			return nil, err
		}
		return &compute.ArrayDatum{Value: field}, nil
	case *expr.ListElementRef:
		switch v.Type().ID() {
		case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		default:
			return nil, fmt.Errorf("%w: cannot reference list element in non-list array type %s",
				arrow.ErrInvalid, v.Type())
		}
		if r.Offset < 0 || int64(r.Offset) >= v.Len() {
			return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
		}
		arr := v.MakeArray()
		defer arr.Release()
		sc, err := scalar.GetScalar(arr, int(r.Offset))
		if err != nil {
			return nil, err
		}
		return &compute.ScalarDatum{Value: sc}, nil
	}
	return nil, arrow.ErrNotImplemented
}

// recordStep follows a struct field reference into a record batch column,
// returning a new reference.
func recordStep(ref expr.ReferenceSegment, v *compute.RecordDatum) (compute.Datum, error) {
	r, ok := ref.(*expr.StructFieldRef)
	if !ok {
		return nil, arrow.ErrNotImplemented
	}
	if r.Field < 0 || r.Field >= int32(v.Value.NumCols()) {
		return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
	}
	col := v.Value.Column(int(r.Field)).Data()
	col.Retain()
	return &compute.ArrayDatum{Value: col}, nil
}

// structField returns field i of a struct array as a new array: sliced to
// the struct's offset and length, and null wherever the struct is null.
func structField(mem memory.Allocator, parent arrow.ArrayData, i int) (arrow.ArrayData, error) {
	off, n := parent.Offset(), parent.Len()
	child := array.NewSliceData(parent.Children()[i], int64(off), int64(off+n))

	if parent.NullN() == 0 || len(parent.Buffers()) == 0 || parent.Buffers()[0] == nil {
		return child, nil
	}
	parentBits := parent.Buffers()[0].Bytes()
	parentNulls := parent.NullN()
	if parentNulls == array.UnknownNullCount {
		parentNulls = n - bitutil.CountSetBits(parentBits, off, n)
	}
	if parentNulls == 0 {
		return child, nil
	}

	storage := child.DataType()
	if ext, ok := storage.(arrow.ExtensionType); ok {
		storage = ext.StorageType()
	}
	if storage.ID() == arrow.NULL {
		return child, nil
	}
	if !internal.DefaultHasValidityBitmap(storage.ID()) {
		err := fmt.Errorf("%w: cannot propagate struct nulls to field type %s", arrow.ErrNotImplemented, child.DataType())
		child.Release()
		return nil, err
	}
	defer child.Release()

	// keep the child's offset so its other buffers stay valid
	cOff := child.Offset()
	bits := memory.NewResizableBuffer(mem)
	defer bits.Release()
	bits.Resize(int(bitutil.BytesForBits(int64(cOff + n))))
	if cb := child.Buffers()[0]; cb != nil {
		bitutil.BitmapAnd(parentBits, cb.Bytes(), int64(off), int64(cOff), bits.Bytes(), int64(cOff), int64(n))
	} else {
		bitutil.CopyBitmap(parentBits, off, n, bits.Bytes(), cOff)
	}
	nulls := n - bitutil.CountSetBits(bits.Bytes(), cOff, n)

	buffers := slices.Clone(child.Buffers())
	buffers[0] = bits
	out := array.NewData(child.DataType(), n, buffers, child.Children(), nulls, cOff)
	if dict := child.Dictionary(); dict != nil {
		out.SetDictionary(dict)
	}
	return out, nil
}
