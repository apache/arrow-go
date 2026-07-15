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

//go:build cgo
// +build cgo

package cdata

// implement handling of the Arrow C Data Interface. At least from a consuming side.

// #include "abi.h"
// #include "helpers.h"
// #include <stdlib.h>
// int stream_get_schema(struct ArrowArrayStream* st, struct ArrowSchema* out) { return st->get_schema(st, out); }
// int stream_get_next(struct ArrowArrayStream* st, struct ArrowArray* out) { return st->get_next(st, out); }
// const char* stream_get_last_error(struct ArrowArrayStream* st) { return st->get_last_error(st); }
// struct ArrowArray* get_arr() {
//	struct ArrowArray* out = (struct ArrowArray*)(malloc(sizeof(struct ArrowArray)));
//	memset(out, 0, sizeof(struct ArrowArray));
//	return out;
// }
// struct ArrowArrayStream* get_stream() {
//	struct ArrowArrayStream* out = (struct ArrowArrayStream*)malloc(sizeof(struct ArrowArrayStream));
//	memset(out, 0, sizeof(struct ArrowArrayStream));
//	return out;
// }
//
import "C"

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/internal/debug"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type (
	// CArrowSchema is the C Data Interface for ArrowSchemas defined in abi.h
	CArrowSchema = C.struct_ArrowSchema
	// CArrowArray is the C Data Interface object for Arrow Arrays as defined in abi.h
	CArrowArray = C.struct_ArrowArray
	// CArrowArrayStream is the C Stream Interface object for handling streams of record batches.
	CArrowArrayStream = C.struct_ArrowArrayStream

	CArrowAsyncDeviceStreamHandler = C.struct_ArrowAsyncDeviceStreamHandler
	CArrowAsyncProducer            = C.struct_ArrowAsyncProducer
	CArrowAsyncTask                = C.struct_ArrowAsyncTask
	CArrowDeviceArray              = C.struct_ArrowDeviceArray
)

// Map from the defined strings to their corresponding arrow.DataType interface
// object instances, for types that don't require params.
var formatToSimpleType = map[string]arrow.DataType{
	"n":   arrow.Null,
	"b":   arrow.FixedWidthTypes.Boolean,
	"c":   arrow.PrimitiveTypes.Int8,
	"C":   arrow.PrimitiveTypes.Uint8,
	"s":   arrow.PrimitiveTypes.Int16,
	"S":   arrow.PrimitiveTypes.Uint16,
	"i":   arrow.PrimitiveTypes.Int32,
	"I":   arrow.PrimitiveTypes.Uint32,
	"l":   arrow.PrimitiveTypes.Int64,
	"L":   arrow.PrimitiveTypes.Uint64,
	"e":   arrow.FixedWidthTypes.Float16,
	"f":   arrow.PrimitiveTypes.Float32,
	"g":   arrow.PrimitiveTypes.Float64,
	"z":   arrow.BinaryTypes.Binary,
	"Z":   arrow.BinaryTypes.LargeBinary,
	"u":   arrow.BinaryTypes.String,
	"U":   arrow.BinaryTypes.LargeString,
	"vz":  arrow.BinaryTypes.BinaryView,
	"vu":  arrow.BinaryTypes.StringView,
	"tdD": arrow.FixedWidthTypes.Date32,
	"tdm": arrow.FixedWidthTypes.Date64,
	"tts": arrow.FixedWidthTypes.Time32s,
	"ttm": arrow.FixedWidthTypes.Time32ms,
	"ttu": arrow.FixedWidthTypes.Time64us,
	"ttn": arrow.FixedWidthTypes.Time64ns,
	"tDs": arrow.FixedWidthTypes.Duration_s,
	"tDm": arrow.FixedWidthTypes.Duration_ms,
	"tDu": arrow.FixedWidthTypes.Duration_us,
	"tDn": arrow.FixedWidthTypes.Duration_ns,
	"tiM": arrow.FixedWidthTypes.MonthInterval,
	"tiD": arrow.FixedWidthTypes.DayTimeInterval,
	"tin": arrow.FixedWidthTypes.MonthDayNanoInterval,
}

// decode metadata from C which is encoded as
//
//	 [int32] -> number of metadata pairs
//		for 0..n
//			[int32] -> number of bytes in key
//			[n bytes] -> key value
//			[int32] -> number of bytes in value
//			[n bytes] -> value
func decodeCMetadata(md *C.char) arrow.Metadata {
	if md == nil {
		return arrow.Metadata{}
	}

	// don't copy the bytes, just reference them directly
	const maxlen = 0x7fffffff
	data := (*[maxlen]byte)(unsafe.Pointer(md))[:]

	readint32 := func() int32 {
		v := *(*int32)(unsafe.Pointer(&data[0]))
		data = data[arrow.Int32SizeBytes:]
		return v
	}

	readstr := func() string {
		l := readint32()
		s := string(data[:l])
		data = data[l:]
		return s
	}

	npairs := readint32()
	if npairs == 0 {
		return arrow.Metadata{}
	}

	keys := make([]string, npairs)
	vals := make([]string, npairs)

	for i := int32(0); i < npairs; i++ {
		keys[i] = readstr()
		vals[i] = readstr()
	}

	return arrow.NewMetadata(keys, vals)
}

// convert a C.ArrowSchema to an arrow.Field to maintain metadata with the schema
func importSchema(schema *CArrowSchema) (ret arrow.Field, err error) {
	// always release, even on error
	defer C.ArrowSchemaRelease(schema)

	var childFields []arrow.Field
	if schema.n_children > 0 {
		// call ourselves recursively if there are children.
		// set up a slice to reference safely
		schemaChildren := unsafe.Slice(schema.children, schema.n_children)
		childFields = make([]arrow.Field, schema.n_children)
		for i, c := range schemaChildren {
			childFields[i], err = importSchema((*CArrowSchema)(c))
			if err != nil {
				return
			}
		}
	}

	// copy the schema name from the c-string
	ret.Name = C.GoString(schema.name)
	ret.Nullable = (schema.flags & C.ARROW_FLAG_NULLABLE) != 0
	ret.Metadata = decodeCMetadata(schema.metadata)

	// copies the c-string here, but it's very small
	f := C.GoString(schema.format)
	// handle our non-parameterized simple types.
	dt, ok := formatToSimpleType[f]
	if ok {
		ret.Type = dt

		if schema.dictionary != nil {
			valueField, err := importSchema(schema.dictionary)
			if err != nil {
				return ret, err
			}

			ret.Type = &arrow.DictionaryType{
				IndexType: ret.Type,
				ValueType: valueField.Type,
				Ordered:   schema.dictionary.flags&C.ARROW_FLAG_DICTIONARY_ORDERED != 0,
			}
		}

		return
	}

	// handle types with params via colon
	switch key, val, _ := strings.Cut(f, ":"); key {
	case "tss":
		dt = &arrow.TimestampType{Unit: arrow.Second, TimeZone: val}
	case "tsm":
		dt = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: val}
	case "tsu":
		dt = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: val}
	case "tsn":
		dt = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: val}
	case "w": // fixed size binary is "w:##" where ## is the byteWidth
		byteWidth, err := strconv.Atoi(val)
		if err != nil {
			return ret, err
		}
		dt = &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}
	case "d": // decimal types are d:<precision>,<scale>[,<bitsize>] size is assumed 128 if left out
		props := val
		propList := strings.Split(props, ",")
		bitwidth := 128
		var precision, scale int

		if len(propList) < 2 || len(propList) > 3 {
			return ret, fmt.Errorf("invalid decimal spec '%s': wrong number of properties", f)
		} else if len(propList) == 3 {
			bitwidth, err = strconv.Atoi(propList[2])
			if err != nil {
				return ret, fmt.Errorf("could not parse decimal bitwidth in '%s': %w", f, err)
			}
		}

		precision, err = strconv.Atoi(propList[0])
		if err != nil {
			return ret, fmt.Errorf("could not parse decimal precision in '%s': %w", f, err)
		}

		scale, err = strconv.Atoi(propList[1])
		if err != nil {
			return ret, fmt.Errorf("could not parse decimal scale in '%s': %w", f, err)
		}

		switch bitwidth {
		case 32:
			dt = &arrow.Decimal32Type{Precision: int32(precision), Scale: int32(scale)}
		case 64:
			dt = &arrow.Decimal64Type{Precision: int32(precision), Scale: int32(scale)}
		case 128:
			dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
		case 256:
			dt = &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}
		default:
			return ret, fmt.Errorf("unsupported decimal bitwidth, got '%s'", f)
		}
	}

	if f[0] == '+' { // types with children
		switch f[1] {
		case 'l': // list
			dt = arrow.ListOfField(childFields[0])
		case 'L': // large list
			dt = arrow.LargeListOfField(childFields[0])
		case 'v': // list view/large list view
			switch f[2] {
			case 'l':
				dt = arrow.ListViewOfField(childFields[0])
			case 'L':
				dt = arrow.LargeListViewOfField(childFields[0])
			}
		case 'w': // fixed size list is w:# where # is the list size.
			listSize, err := strconv.Atoi(strings.Split(f, ":")[1])
			if err != nil {
				return ret, err
			}

			dt = arrow.FixedSizeListOfField(int32(listSize), childFields[0])
		case 's': // struct
			dt = arrow.StructOf(childFields...)
		case 'r': // run-end encoded
			if len(childFields) != 2 {
				return ret, fmt.Errorf("%w: run-end encoded arrays must have 2 children", arrow.ErrInvalid)
			}
			dt = arrow.RunEndEncodedOf(childFields[0].Type, childFields[1].Type)
		case 'm': // map type is basically a list of structs.
			st := childFields[0].Type.(*arrow.StructType)
			dt = arrow.MapOf(st.Field(0).Type, st.Field(1).Type)
			dt.(*arrow.MapType).KeysSorted = (schema.flags & C.ARROW_FLAG_MAP_KEYS_SORTED) != 0
		case 'u': // union
			var mode arrow.UnionMode
			switch f[2] {
			case 'd':
				mode = arrow.DenseMode
			case 's':
				mode = arrow.SparseMode
			default:
				err = fmt.Errorf("%w: invalid union type", arrow.ErrInvalid)
				return
			}

			_, val, ok := strings.Cut(f, ":")
			if !ok {
				return ret, fmt.Errorf("invalid union type code spec %q", f)
			}
			var typeCodes []arrow.UnionTypeCode
			for i := range strings.SplitSeq(val, ",") {
				v, e := strconv.ParseInt(i, 10, 8)
				if e != nil {
					err = fmt.Errorf("%w: invalid type code: %s", arrow.ErrInvalid, e)
					return
				}
				if v < 0 {
					err = fmt.Errorf("%w: negative type code in union: format string %s", arrow.ErrInvalid, f)
					return
				}
				typeCodes = append(typeCodes, arrow.UnionTypeCode(v))
			}

			if len(childFields) != len(typeCodes) {
				err = fmt.Errorf("%w: ArrowArray struct number of children incompatible with format string", arrow.ErrInvalid)
				return
			}

			dt = arrow.UnionOf(mode, childFields, typeCodes)
		}
	}

	if dt == nil {
		// if we didn't find a type, then it's something we haven't implemented.
		err = errors.New("unimplemented type")
	} else {
		ret.Type = dt
	}

	return
}

// importer to keep track when importing C ArrowArray objects.
type cimporter struct {
	dt       arrow.DataType
	arr      *CArrowArray
	data     arrow.ArrayData
	parent   *cimporter
	children []cimporter
	cbuffers []*C.void

	alloc *importAllocator
}

const maxInt64Value = int64(1<<63 - 1)

func (imp *cimporter) importChild(parent *cimporter, src *CArrowArray) error {
	imp.parent, imp.arr, imp.alloc = parent, src, parent.alloc
	return imp.doImport()
}

func validateCArrayHeader(arr *CArrowArray) error {
	if arr == nil {
		return fmt.Errorf("%w: nil ArrowArray", arrow.ErrInvalid)
	}
	length := int64(arr.length)
	offset := int64(arr.offset)
	nullCount := int64(arr.null_count)
	nBuffers := int64(arr.n_buffers)
	nChildren := int64(arr.n_children)

	if length < 0 {
		return fmt.Errorf("%w: ArrowArray length cannot be negative: %d", arrow.ErrInvalid, length)
	}
	if offset < 0 {
		return fmt.Errorf("%w: ArrowArray offset cannot be negative: %d", arrow.ErrInvalid, offset)
	}
	if nullCount < -1 || nullCount > length {
		return fmt.Errorf("%w: ArrowArray null_count %d is outside [-1, %d]", arrow.ErrInvalid, nullCount, length)
	}
	if nBuffers < 0 {
		return fmt.Errorf("%w: ArrowArray n_buffers cannot be negative: %d", arrow.ErrInvalid, nBuffers)
	}
	if nChildren < 0 {
		return fmt.Errorf("%w: ArrowArray n_children cannot be negative: %d", arrow.ErrInvalid, nChildren)
	}
	if nBuffers > maxIntValue() {
		return fmt.Errorf("%w: ArrowArray n_buffers is too large: %d", arrow.ErrInvalid, nBuffers)
	}
	if nChildren > maxIntValue() {
		return fmt.Errorf("%w: ArrowArray n_children is too large: %d", arrow.ErrInvalid, nChildren)
	}
	if _, err := checkedMul(nBuffers, int64(unsafe.Sizeof(uintptr(0)))); err != nil {
		return fmt.Errorf("%w: ArrowArray buffers pointer array is too large", arrow.ErrInvalid)
	}
	if _, err := checkedMul(nChildren, int64(unsafe.Sizeof(uintptr(0)))); err != nil {
		return fmt.Errorf("%w: ArrowArray children pointer array is too large", arrow.ErrInvalid)
	}
	if nBuffers > 0 && arr.buffers == nil {
		return fmt.Errorf("%w: ArrowArray buffers is nil with n_buffers %d", arrow.ErrInvalid, nBuffers)
	}
	if nChildren > 0 && arr.children == nil {
		return fmt.Errorf("%w: ArrowArray children is nil with n_children %d", arrow.ErrInvalid, nChildren)
	}
	if offset > maxInt64Value-length {
		return fmt.Errorf("%w: ArrowArray length and offset overflow: length %d, offset %d", arrow.ErrInvalid, length, offset)
	}
	if length+offset > maxIntValue() {
		return fmt.Errorf("%w: ArrowArray length and offset exceed native int: length %d, offset %d", arrow.ErrInvalid, length, offset)
	}

	return nil
}

func maxIntValue() int64 {
	return int64(^uint(0) >> 1)
}

func checkedAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > maxInt64Value-b {
		return 0, fmt.Errorf("%w: integer addition overflow: %d + %d", arrow.ErrInvalid, a, b)
	}
	return a + b, nil
}

func checkedMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 || (a != 0 && b > maxInt64Value/a) {
		return 0, fmt.Errorf("%w: integer multiplication overflow: %d * %d", arrow.ErrInvalid, a, b)
	}
	return a * b, nil
}

func checkedBytesForBits(bits int64) (int64, error) {
	bits, err := checkedAdd(bits, 7)
	if err != nil {
		return 0, err
	}
	return bits >> 3, nil
}

func (imp *cimporter) checkExpectedChildren() error {
	var expected int64
	switch imp.dt.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW, arrow.FIXED_SIZE_LIST, arrow.MAP:
		expected = 1
	case arrow.STRUCT:
		expected = int64(len(imp.dt.(*arrow.StructType).Fields()))
	case arrow.RUN_END_ENCODED:
		expected = 2
	case arrow.DENSE_UNION:
		expected = int64(len(imp.dt.(*arrow.DenseUnionType).Fields()))
	case arrow.SPARSE_UNION:
		expected = int64(len(imp.dt.(*arrow.SparseUnionType).Fields()))
	}
	return imp.checkNumChildren(expected)
}

func (imp *cimporter) checkExpectedBuffers() error {
	switch imp.dt.(type) {
	case *arrow.NullType, *arrow.RunEndEncodedType:
		return imp.checkNumBuffers(0)
	case arrow.FixedWidthDataType:
		return imp.checkNumBuffers(2)
	case *arrow.StringType, *arrow.BinaryType, *arrow.LargeStringType, *arrow.LargeBinaryType:
		return imp.checkNumBuffers(3)
	case *arrow.StringViewType, *arrow.BinaryViewType:
		if imp.arr.n_buffers < 3 {
			return fmt.Errorf("%w: expected at least 3 buffers for imported type %s, ArrowArray has %d", arrow.ErrInvalid, imp.dt, imp.arr.n_buffers)
		}
	case *arrow.ListType, *arrow.LargeListType, *arrow.MapType:
		return imp.checkNumBuffers(2)
	case *arrow.ListViewType, *arrow.LargeListViewType:
		return imp.checkNumBuffers(3)
	case *arrow.FixedSizeListType, *arrow.StructType:
		return imp.checkNumBuffers(1)
	case *arrow.DenseUnionType:
		if imp.arr.n_buffers != 2 && imp.arr.n_buffers != 3 {
			return fmt.Errorf("%w: expected 2 or 3 buffers for imported type %s, ArrowArray has %d", arrow.ErrInvalid, imp.dt, imp.arr.n_buffers)
		}
	case *arrow.SparseUnionType:
		if imp.arr.n_buffers != 1 && imp.arr.n_buffers != 2 {
			return fmt.Errorf("%w: expected 1 or 2 buffers for imported type %s, ArrowArray has %d", arrow.ErrInvalid, imp.dt, imp.arr.n_buffers)
		}
	default:
		return fmt.Errorf("unimplemented type %s", imp.dt)
	}
	return nil
}

// import any child arrays for lists, structs, and so on.
func (imp *cimporter) doImportChildren() error {
	if err := imp.checkExpectedChildren(); err != nil {
		return err
	}

	var children []*CArrowArray
	if imp.arr.n_children > 0 {
		children = unsafe.Slice(imp.arr.children, imp.arr.n_children)
		imp.children = make([]cimporter, imp.arr.n_children)
	}

	// handle the cases
	switch imp.dt.ID() {
	case arrow.LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.ListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.LARGE_LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.LargeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.LIST_VIEW: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.ListViewType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.LARGE_LIST_VIEW: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.LargeListViewType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.FIXED_SIZE_LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.FixedSizeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.STRUCT: // import all the children
		st := imp.dt.(*arrow.StructType)
		for i, c := range children {
			imp.children[i].dt = st.Field(i).Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	case arrow.RUN_END_ENCODED: // import run-ends and values
		st := imp.dt.(*arrow.RunEndEncodedType)
		imp.children[0].dt = st.RunEnds()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
		imp.children[1].dt = st.Encoded()
		if err := imp.children[1].importChild(imp, children[1]); err != nil {
			return err
		}
	case arrow.MAP: // only one child to import, it's a struct array
		imp.children[0].dt = imp.dt.(*arrow.MapType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.DENSE_UNION:
		dt := imp.dt.(*arrow.DenseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	case arrow.SPARSE_UNION:
		dt := imp.dt.(*arrow.SparseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	}

	return nil
}

func (imp *cimporter) initarr() {
	imp.arr = C.get_arr()
	if imp.alloc == nil {
		imp.alloc = &importAllocator{arr: imp.arr}
	}
}

func (imp *cimporter) doImportArr(src *CArrowArray) error {
	if err := validateCArrayHeader(src); err != nil {
		return err
	}
	imp.arr = C.get_arr()
	C.ArrowArrayMove(src, imp.arr)
	if imp.alloc == nil {
		imp.alloc = &importAllocator{arr: imp.arr}
	}

	// we tie the releasing of the array to when the buffers are
	// cleaned up, so if there are no buffers that we've imported
	// such as for a null array or a nested array with no bitmap
	// and only null columns, then we can release the CArrowArray
	// struct immediately after import, since we have no imported
	// memory that we have to track the lifetime of.
	// On error, we always release regardless of buffer count to avoid leaks.
	var importErr error
	defer func() {
		if importErr != nil || imp.alloc.bufCount.Load() == 0 {
			imp.alloc.forceRelease()
		}
	}()

	importErr = imp.doImport()
	return importErr
}

// import is called recursively as needed for importing an array and its children
// in order to generate array.Data objects
func (imp *cimporter) doImport() error {
	if err := validateCArrayHeader(imp.arr); err != nil {
		return err
	}
	if _, ok := imp.dt.(*arrow.DictionaryType); ok && imp.arr.dictionary == nil {
		return fmt.Errorf("%w: dictionary ArrowArray has no dictionary", arrow.ErrInvalid)
	}
	if err := imp.checkExpectedBuffers(); err != nil {
		return err
	}

	// import any children
	if err := imp.doImportChildren(); err != nil {
		for _, c := range imp.children {
			if c.data != nil {
				c.data.Release()
			}
		}
		return err
	}

	for _, c := range imp.children {
		if c.data != nil {
			defer c.data.Release()
		}
	}

	if imp.arr.n_buffers > 0 {
		// get a view of the buffers, zero-copy. we're just looking at the pointers
		imp.cbuffers = unsafe.Slice((**C.void)(unsafe.Pointer(imp.arr.buffers)), imp.arr.n_buffers)
	}

	// handle each of our type cases
	switch dt := imp.dt.(type) {
	case *arrow.NullType:
		if err := imp.checkNoChildren(); err != nil {
			return err
		}

		imp.data = array.NewData(dt, int(imp.arr.length), nil, nil, int(imp.arr.null_count), int(imp.arr.offset))
	case arrow.FixedWidthDataType:
		return imp.importFixedSizePrimitive()
	case *arrow.StringType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.BinaryType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.LargeStringType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.LargeBinaryType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.StringViewType:
		return imp.importBinaryViewLike()
	case *arrow.BinaryViewType:
		return imp.importBinaryViewLike()
	case *arrow.ListType:
		return imp.importListLike()
	case *arrow.LargeListType:
		return imp.importListLike()
	case *arrow.ListViewType:
		return imp.importListViewLike()
	case *arrow.LargeListViewType:
		return imp.importListViewLike()
	case *arrow.MapType:
		return imp.importListLike()
	case *arrow.FixedSizeListType:
		if err := imp.checkNumChildren(1); err != nil {
			return err
		}

		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}

		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}
		if nulls != nil {
			defer nulls.Release()
		}

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.StructType:
		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}

		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}
		if nulls != nil {
			defer nulls.Release()
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, children, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.RunEndEncodedType:
		if err := imp.checkNumBuffers(0); err != nil {
			return err
		}

		if len(imp.children) != 2 {
			return fmt.Errorf("%w: run-end encoded array should have 2 children", arrow.ErrInvalid)
		}

		children := []arrow.ArrayData{imp.children[0].data, imp.children[1].data}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{}, children, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.DenseUnionType:
		if err := imp.checkNoNulls(); err != nil {
			return err
		}

		bufs := []*memory.Buffer{nil, nil, nil}
		var err error
		if imp.arr.n_buffers == 3 {
			// legacy format exported by older arrow c++ versions
			if bufs[1], err = imp.importFixedSizeBuffer(1, 1); err != nil {
				return err
			}
			defer bufs[1].Release()
			if bufs[2], err = imp.importFixedSizeBuffer(2, int64(arrow.Int32SizeBytes)); err != nil {
				return err
			}
			defer bufs[2].Release()
		} else {
			if err := imp.checkNumBuffers(2); err != nil {
				return err
			}

			if bufs[1], err = imp.importFixedSizeBuffer(0, 1); err != nil {
				return err
			}
			defer bufs[1].Release()
			if bufs[2], err = imp.importFixedSizeBuffer(1, int64(arrow.Int32SizeBytes)); err != nil {
				return err
			}
			defer bufs[2].Release()
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}
		imp.data = array.NewData(dt, int(imp.arr.length), bufs, children, 0, int(imp.arr.offset))
	case *arrow.SparseUnionType:
		if err := imp.checkNoNulls(); err != nil {
			return err
		}

		var buf *memory.Buffer
		var err error
		if imp.arr.n_buffers == 2 {
			// legacy format exported by older Arrow C++ versions
			if buf, err = imp.importFixedSizeBuffer(1, 1); err != nil {
				return err
			}
			defer buf.Release()
		} else {
			if err := imp.checkNumBuffers(1); err != nil {
				return err
			}

			if buf, err = imp.importFixedSizeBuffer(0, 1); err != nil {
				return err
			}
			defer buf.Release()
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nil, buf}, children, 0, int(imp.arr.offset))
	default:
		return fmt.Errorf("unimplemented type %s", dt)
	}

	return nil
}

func (imp *cimporter) importStringLike(offsetByteWidth int64) (err error) {
	if err = imp.checkNoChildren(); err != nil {
		return
	}

	if err = imp.checkNumBuffers(3); err != nil {
		return
	}

	var nulls, offsets, values *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}
	if nulls != nil {
		defer nulls.Release()
	}

	if offsets, err = imp.importOffsetsBuffer(1, offsetByteWidth); err != nil {
		return
	}
	defer offsets.Release()

	var nvals int64
	switch offsetByteWidth {
	case 4:
		typedOffsets := arrow.Int32Traits.CastFromBytes(offsets.Bytes())
		nvals = int64(typedOffsets[imp.arr.offset+imp.arr.length])
	case 8:
		typedOffsets := arrow.Int64Traits.CastFromBytes(offsets.Bytes())
		nvals = typedOffsets[imp.arr.offset+imp.arr.length]
	}
	if values, err = imp.importVariableValuesBuffer(2, 1, nvals); err != nil {
		return
	}
	defer values.Release()

	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets, values}, nil, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importBinaryViewLike() (err error) {
	if err = imp.checkNoChildren(); err != nil {
		return
	}
	if _, err = checkedMul(int64(imp.arr.n_buffers)-3, int64(arrow.Int64SizeBytes)); err != nil {
		return err
	}

	buffers := make([]*memory.Buffer, len(imp.cbuffers)-1)
	defer memory.ReleaseBuffers(buffers)

	if buffers[0], err = imp.importNullBitmap(0); err != nil {
		return
	}

	if buffers[1], err = imp.importFixedSizeBuffer(1, int64(arrow.ViewHeaderSizeBytes)); err != nil {
		return
	}

	if len(buffers) > 2 {
		if imp.cbuffers[len(buffers)] == nil {
			return fmt.Errorf("%w: invalid data buffer sizes", arrow.ErrInvalid)
		}
		if _, err = checkedMul(int64(len(buffers)-2), int64(arrow.Int64SizeBytes)); err != nil {
			return err
		}
		dataBufferSizes := unsafe.Slice((*int64)(unsafe.Pointer(imp.cbuffers[len(buffers)])), len(buffers)-2)
		for i, size := range dataBufferSizes {
			if buffers[i+2], err = imp.importVariableValuesBuffer(i+2, 1, size); err != nil {
				return
			}
		}
	}

	imp.data = array.NewData(imp.dt, int(imp.arr.length), buffers, nil, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importListLike() (err error) {
	if err = imp.checkNumChildren(1); err != nil {
		return err
	}

	if err = imp.checkNumBuffers(2); err != nil {
		return err
	}

	var nulls, offsets *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}
	if nulls != nil {
		defer nulls.Release()
	}

	offsetSize := imp.dt.Layout().Buffers[1].ByteWidth
	if offsets, err = imp.importOffsetsBuffer(1, int64(offsetSize)); err != nil {
		return
	}
	if offsets != nil {
		defer offsets.Release()
	}

	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importListViewLike() (err error) {
	offsetSize := int64(imp.dt.Layout().Buffers[1].ByteWidth)

	if err = imp.checkNumChildren(1); err != nil {
		return err
	}

	if err = imp.checkNumBuffers(3); err != nil {
		return err
	}

	var nulls, offsets, sizes *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}
	if nulls != nil {
		defer nulls.Release()
	}

	if offsets, err = imp.importFixedSizeBuffer(1, offsetSize); err != nil {
		return
	}
	if offsets != nil {
		defer offsets.Release()
	}

	if sizes, err = imp.importFixedSizeBuffer(2, offsetSize); err != nil {
		return
	}
	if sizes != nil {
		defer sizes.Release()
	}

	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets, sizes}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importFixedSizePrimitive() error {
	if err := imp.checkNoChildren(); err != nil {
		return err
	}

	if err := imp.checkNumBuffers(2); err != nil {
		return err
	}

	nulls, err := imp.importNullBitmap(0)
	if err != nil {
		return err
	}

	var values *memory.Buffer

	fw := imp.dt.(arrow.FixedWidthDataType)
	if bitutil.IsMultipleOf8(int64(fw.BitWidth())) {
		var byteWidth int64
		byteWidth, err = checkedBytesForBits(int64(fw.BitWidth()))
		if err != nil {
			return err
		}
		values, err = imp.importFixedSizeBuffer(1, byteWidth)
	} else {
		if fw.BitWidth() != 1 {
			return errors.New("invalid bitwidth")
		}
		values, err = imp.importBitsBuffer(1)
	}

	if err != nil {
		return err
	}

	var dict *array.Data
	if dt, ok := imp.dt.(*arrow.DictionaryType); ok {
		dictImp := &cimporter{dt: dt.ValueType}
		if err := dictImp.importChild(imp, imp.arr.dictionary); err != nil {
			return err
		}
		defer dictImp.data.Release()

		dict = dictImp.data.(*array.Data)
	}

	if nulls != nil {
		defer nulls.Release()
	}
	if values != nil {
		defer values.Release()
	}

	imp.data = array.NewDataWithDictionary(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, values}, int(imp.arr.null_count), int(imp.arr.offset), dict)
	return nil
}

func (imp *cimporter) checkNoChildren() error { return imp.checkNumChildren(0) }

func (imp *cimporter) checkNoNulls() error {
	if imp.arr.null_count != 0 {
		return fmt.Errorf("%w: unexpected non-zero null count for imported type %s", arrow.ErrInvalid, imp.dt)
	}
	return nil
}

func (imp *cimporter) checkNumChildren(n int64) error {
	if int64(imp.arr.n_children) != n {
		return fmt.Errorf("expected %d children, for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_children)
	}
	return nil
}

func (imp *cimporter) checkNumBuffers(n int64) error {
	if int64(imp.arr.n_buffers) != n {
		return fmt.Errorf("expected %d buffers for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_buffers)
	}
	return nil
}

func (imp *cimporter) importBuffer(bufferID int, sz int64) (*memory.Buffer, error) {
	// Buffer sizes come from foreign memory and cannot be independently verified;
	// these checks only prevent invalid metadata from creating unsafe Go slices.
	// this is not a copy, we're just having a slice which points at the data
	// it's still owned by the C.ArrowArray object and its backing C++ object.
	if bufferID < 0 || bufferID >= len(imp.cbuffers) {
		return nil, fmt.Errorf("%w: invalid buffer index %d", arrow.ErrInvalid, bufferID)
	}
	if sz < 0 || sz > maxIntValue() {
		return nil, fmt.Errorf("%w: invalid buffer size %d", arrow.ErrInvalid, sz)
	}
	if imp.cbuffers[bufferID] == nil || sz == 0 {
		if sz != 0 {
			return nil, fmt.Errorf("%w: invalid buffer", arrow.ErrInvalid)
		}
		return memory.NewBufferBytes([]byte{}), nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(imp.cbuffers[bufferID])), sz)
	imp.alloc.addBuffer()
	return memory.NewBufferWithAllocator(data, imp.alloc), nil
}

func (imp *cimporter) importBitsBuffer(bufferID int) (*memory.Buffer, error) {
	total, err := checkedAdd(int64(imp.arr.length), int64(imp.arr.offset))
	if err != nil {
		return nil, err
	}
	bufsize, err := checkedBytesForBits(total)
	if err != nil {
		return nil, err
	}
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importNullBitmap(bufferID int) (*memory.Buffer, error) {
	if imp.arr.null_count > 0 && imp.cbuffers[bufferID] == nil {
		return nil, fmt.Errorf("arrowarray struct has null bitmap buffer, but non-zero null_count %d", imp.arr.null_count)
	}

	if imp.arr.null_count == 0 && imp.cbuffers[bufferID] == nil {
		return nil, nil
	}

	return imp.importBitsBuffer(bufferID)
}

func (imp *cimporter) importFixedSizeBuffer(bufferID int, byteWidth int64) (*memory.Buffer, error) {
	total, err := checkedAdd(int64(imp.arr.length), int64(imp.arr.offset))
	if err != nil {
		return nil, err
	}
	bufsize, err := checkedMul(byteWidth, total)
	if err != nil {
		return nil, err
	}
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importOffsetsBuffer(bufferID int, offsetsize int64) (*memory.Buffer, error) {
	total, err := checkedAdd(int64(imp.arr.length), int64(imp.arr.offset))
	if err != nil {
		return nil, err
	}
	total, err = checkedAdd(total, 1)
	if err != nil {
		return nil, err
	}
	bufsize, err := checkedMul(offsetsize, total)
	if err != nil {
		return nil, err
	}
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importVariableValuesBuffer(bufferID int, byteWidth, nvals int64) (*memory.Buffer, error) {
	bufsize, err := checkedMul(byteWidth, nvals)
	if err != nil {
		return nil, err
	}
	return imp.importBuffer(bufferID, bufsize)
}

func importCArrayAsType(arr *CArrowArray, dt arrow.DataType) (imp *cimporter, err error) {
	imp = &cimporter{dt: dt}
	err = imp.doImportArr(arr)
	return
}

func initReader(rdr *nativeCRecordBatchReader, stream *CArrowArrayStream) error {
	rdr.refCount.Store(1)
	rdr.stream = C.get_stream()
	C.ArrowArrayStreamMove(stream, rdr.stream)
	rdr.arr = C.get_arr()

	rdr.cleanUps[0] = runtime.AddCleanup(rdr, func(s *CArrowArrayStream) {
		C.ArrowArrayStreamRelease(s)
		C.free(unsafe.Pointer(s))
	}, rdr.stream)
	rdr.cleanUps[1] = runtime.AddCleanup(rdr, func(a *CArrowArray) {
		C.ArrowArrayRelease(a)
		C.free(unsafe.Pointer(a))
	}, rdr.arr)

	var sc CArrowSchema
	errno := C.stream_get_schema(rdr.stream, &sc)
	if errno != 0 {
		return rdr.getError(int(errno))
	}
	defer C.ArrowSchemaRelease(&sc)

	s, err := ImportCArrowSchema((*CArrowSchema)(&sc))
	if err != nil {
		return err
	}
	rdr.schema = s

	return nil
}

// Record Batch reader that conforms to arrio.Reader for the ArrowArrayStream interface
type nativeCRecordBatchReader struct {
	stream *CArrowArrayStream
	arr    *CArrowArray
	schema *arrow.Schema

	cur arrow.RecordBatch
	err error

	refCount atomic.Int64
	cleanUps [2]runtime.Cleanup
}

func (n *nativeCRecordBatchReader) Retain() {
	n.refCount.Add(1)
}

func (n *nativeCRecordBatchReader) Release() {
	rc := n.refCount.Add(-1)
	debug.Assert(rc >= 0, "too many releases")

	if rc == 0 {
		n.cleanUps[0].Stop()
		n.cleanUps[1].Stop()
		if n.cur != nil {
			n.cur.Release()
		}

		C.ArrowArrayStreamRelease(n.stream)
		C.ArrowArrayRelease(n.arr)
		C.free(unsafe.Pointer(n.stream))
		C.free(unsafe.Pointer(n.arr))
	}
}

func (n *nativeCRecordBatchReader) Err() error                     { return n.err }
func (n *nativeCRecordBatchReader) RecordBatch() arrow.RecordBatch { return n.cur }

// Deprecated: Use [RecordBatch] instead.
func (n *nativeCRecordBatchReader) Record() arrow.Record { return n.RecordBatch() }

func (n *nativeCRecordBatchReader) Next() bool {
	err := n.next()
	switch err {
	case nil:
		return true
	case io.EOF:
		return false
	}
	n.err = err
	return false
}

func (n *nativeCRecordBatchReader) next() error {
	if n.schema == nil {
		var sc CArrowSchema
		errno := C.stream_get_schema(n.stream, &sc)
		if errno != 0 {
			return n.getError(int(errno))
		}
		defer C.ArrowSchemaRelease(&sc)
		s, err := ImportCArrowSchema((*CArrowSchema)(&sc))
		if err != nil {
			return err
		}

		n.schema = s
	}

	if n.cur != nil {
		n.cur.Release()
		n.cur = nil
	}

	errno := C.stream_get_next(n.stream, n.arr)
	if errno != 0 {
		return n.getError(int(errno))
	}

	if C.ArrowArrayIsReleased(n.arr) == 1 {
		return io.EOF
	}

	rec, err := ImportCRecordBatchWithSchema(n.arr, n.schema)
	if err != nil {
		return err
	}

	n.cur = rec
	return nil
}

func (n *nativeCRecordBatchReader) Schema() *arrow.Schema {
	return n.schema
}

func (n *nativeCRecordBatchReader) getError(errno int) error {
	return fmt.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.stream_get_last_error(n.stream)))
}

func (n *nativeCRecordBatchReader) Read() (arrow.RecordBatch, error) {
	if err := n.next(); err != nil {
		n.err = err
		return nil, err
	}
	return n.cur, nil
}

func releaseArr(arr *CArrowArray) {
	C.ArrowArrayRelease(arr)
}

func releaseSchema(schema *CArrowSchema) {
	C.ArrowSchemaRelease(schema)
}

func releaseStream(stream *CArrowArrayStream) {
	C.ArrowArrayStreamRelease(stream)
}
