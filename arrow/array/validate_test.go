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

package array

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/endian"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeBinaryArrayRaw creates a Binary array directly from raw buffers,
// bypassing builder validation. Used to simulate corrupted IPC data.
func makeBinaryArrayRaw(t *testing.T, offsets []int32, data []byte, length, offset int) *Binary {
	t.Helper()
	offsetBuf := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
	dataBuf := memory.NewBufferBytes(data)
	d := NewData(arrow.BinaryTypes.Binary, length, []*memory.Buffer{nil, offsetBuf, dataBuf}, nil, 0, offset)
	return NewBinaryData(d)
}

// makeLargeBinaryArrayRaw creates a LargeBinary array directly from raw buffers.
func makeLargeBinaryArrayRaw(t *testing.T, offsets []int64, data []byte, length, offset int) *LargeBinary {
	t.Helper()
	offsetBuf := memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(offsets))
	dataBuf := memory.NewBufferBytes(data)
	d := NewData(arrow.BinaryTypes.LargeBinary, length, []*memory.Buffer{nil, offsetBuf, dataBuf}, nil, 0, offset)
	return NewLargeBinaryData(d)
}

// makeStringArrayRaw creates a String array directly from raw buffers.
func makeStringArrayRaw(t *testing.T, offsets []int32, data string, length, offset int) *String {
	t.Helper()
	offsetBuf := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
	dataBuf := memory.NewBufferBytes([]byte(data))
	d := NewData(arrow.BinaryTypes.String, length, []*memory.Buffer{nil, offsetBuf, dataBuf}, nil, 0, offset)
	return NewStringData(d)
}

// makeLargeStringArrayRaw creates a LargeString array directly from raw buffers.
func makeLargeStringArrayRaw(t *testing.T, offsets []int64, data string, length, offset int) *LargeString {
	t.Helper()
	offsetBuf := memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(offsets))
	dataBuf := memory.NewBufferBytes([]byte(data))
	d := NewData(arrow.BinaryTypes.LargeString, length, []*memory.Buffer{nil, offsetBuf, dataBuf}, nil, 0, offset)
	return NewLargeStringData(d)
}

func makeInt32ArrayRaw(t *testing.T, values []int32, validity []byte, nulls, length, offset int) *Int32 {
	t.Helper()
	valueBuf := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(values))
	var validityBuf *memory.Buffer
	if validity != nil {
		validityBuf = memory.NewBufferBytes(validity)
	}
	data := NewData(arrow.PrimitiveTypes.Int32, length, []*memory.Buffer{validityBuf, valueBuf}, nil, nulls, offset)
	arr := NewInt32Data(data)
	data.Release()
	return arr
}

func makeBinaryViewArrayRaw(t *testing.T, headerBytes []byte, dataBuffers []*memory.Buffer, validity []byte, nulls, length, offset int) *BinaryView {
	t.Helper()
	var validityBuf *memory.Buffer
	if validity != nil {
		validityBuf = memory.NewBufferBytes(validity)
	}
	viewBuf := memory.NewBufferBytes(headerBytes)
	buffers := append([]*memory.Buffer{validityBuf, viewBuf}, dataBuffers...)
	data := NewData(arrow.BinaryTypes.BinaryView, length, buffers, nil, nulls, offset)
	arr := NewBinaryViewData(data)
	data.Release()
	return arr
}

func makeStringViewArrayRaw(t *testing.T, headerBytes []byte, dataBuffers []*memory.Buffer, validity []byte, nulls, length, offset int) *StringView {
	t.Helper()
	var validityBuf *memory.Buffer
	if validity != nil {
		validityBuf = memory.NewBufferBytes(validity)
	}
	viewBuf := memory.NewBufferBytes(headerBytes)
	buffers := append([]*memory.Buffer{validityBuf, viewBuf}, dataBuffers...)
	data := NewData(arrow.BinaryTypes.StringView, length, buffers, nil, nulls, offset)
	arr := NewStringViewData(data)
	data.Release()
	return arr
}

func TestBinaryValidate(t *testing.T) {
	t.Run("valid array passes", func(t *testing.T) {
		// offsets [0,3,6,9], data "abcdefghi" — 3 elements of 3 bytes each
		arr := makeBinaryArrayRaw(t, []int32{0, 3, 6, 9}, []byte("abcdefghi"), 3, 0)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("valid sliced array passes", func(t *testing.T) {
		arr := makeBinaryArrayRaw(t, []int32{0, 3, 6, 9}, []byte("abcdefghi"), 1, 1)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("empty array passes", func(t *testing.T) {
		arr := makeBinaryArrayRaw(t, nil, nil, 0, 0)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("non-monotonic offsets pass Validate but fail ValidateFull", func(t *testing.T) {
		// last offset (5) is within data bounds so setData/Validate pass,
		// but offset[1]=5 then offset[2]=3 is decreasing — ValidateFull must catch this.
		arr := makeBinaryArrayRaw(t, []int32{0, 5, 3, 5}, []byte("hello"), 3, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not monotonically non-decreasing")
	})

	t.Run("negative first offset passes Validate but fails ValidateFull", func(t *testing.T) {
		// last offset (5) is within bounds, but first offset is negative.
		arr := makeBinaryArrayRaw(t, []int32{-1, 2, 5}, []byte("hello"), 2, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})
}

func TestLargeBinaryValidate(t *testing.T) {
	t.Run("valid array passes", func(t *testing.T) {
		arr := makeLargeBinaryArrayRaw(t, []int64{0, 3, 6, 9}, []byte("abcdefghi"), 3, 0)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("non-monotonic offsets pass Validate but fail ValidateFull", func(t *testing.T) {
		arr := makeLargeBinaryArrayRaw(t, []int64{0, 5, 3, 5}, []byte("hello"), 3, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not monotonically non-decreasing")
	})

	t.Run("negative first offset passes Validate but fails ValidateFull", func(t *testing.T) {
		arr := makeLargeBinaryArrayRaw(t, []int64{-1, 2, 5}, []byte("hello"), 2, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})
}

func TestStringValidate(t *testing.T) {
	t.Run("empty values allow a nil data buffer", func(t *testing.T) {
		offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 0, 0}))
		data := NewData(arrow.BinaryTypes.String, 2, []*memory.Buffer{nil, offsets, nil}, nil, 0, 0)
		offsets.Release()
		arr := NewStringData(data)
		data.Release()
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("valid array passes", func(t *testing.T) {
		arr := makeStringArrayRaw(t, []int32{0, 3, 5, 10}, "abcdeabcde", 3, 0)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("non-monotonic offsets pass Validate but fail ValidateFull", func(t *testing.T) {
		arr := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not monotonically non-decreasing")
	})

	t.Run("negative first offset passes Validate but fails ValidateFull", func(t *testing.T) {
		arr := makeStringArrayRaw(t, []int32{-1, 2, 5}, "hello", 2, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})
}

func TestLargeStringValidate(t *testing.T) {
	t.Run("valid array passes", func(t *testing.T) {
		arr := makeLargeStringArrayRaw(t, []int64{0, 3, 5, 10}, "abcdeabcde", 3, 0)
		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("non-monotonic offsets pass Validate but fail ValidateFull", func(t *testing.T) {
		arr := makeLargeStringArrayRaw(t, []int64{0, 5, 3, 5}, "hello", 3, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not monotonically non-decreasing")
	})

	t.Run("negative first offset passes Validate but fails ValidateFull", func(t *testing.T) {
		arr := makeLargeStringArrayRaw(t, []int64{-1, 2, 5}, "hello", 2, 0)
		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})
}

func TestBinaryViewValidate(t *testing.T) {
	t.Run("valid array passes", func(t *testing.T) {
		var headers [1]arrow.ViewHeader
		headers[0].SetBytes([]byte("hello"))
		arr := makeBinaryViewArrayRaw(t, arrow.ViewHeaderTraits.CastToBytes(headers[:]), nil, nil, 0, 1, 0)
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		assert.NoError(t, arr.ValidateFull())
	})

	t.Run("negative size passes Validate but fails ValidateFull", func(t *testing.T) {
		headerBytes := make([]byte, arrow.ViewHeaderSizeBytes)
		endian.Native.PutUint32(headerBytes[:4], ^uint32(0))
		arr := makeBinaryViewArrayRaw(t, headerBytes, nil, nil, 0, 1, 0)
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative size")
	})

	t.Run("missing referenced buffer passes Validate but fails ValidateFull", func(t *testing.T) {
		var headers [1]arrow.ViewHeader
		headers[0].SetBytes([]byte("this is longer than twelve"))
		headers[0].SetIndexOffset(0, 0)
		arr := makeBinaryViewArrayRaw(t, arrow.ViewHeaderTraits.CastToBytes(headers[:]), nil, nil, 0, 1, 0)
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "references buffer 0")
	})

	t.Run("inline padding bytes fail ValidateFull", func(t *testing.T) {
		var headers [1]arrow.ViewHeader
		headers[0].SetBytes([]byte("x"))
		headerBytes := append([]byte(nil), arrow.ViewHeaderTraits.CastToBytes(headers[:])...)
		headerBytes[8] = 1
		arr := makeBinaryViewArrayRaw(t, headerBytes, nil, nil, 0, 1, 0)
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "padding bytes were not all zero")
	})
}

func TestStringViewValidate(t *testing.T) {
	t.Run("invalid utf8 passes Validate but fails ValidateFull", func(t *testing.T) {
		var headers [1]arrow.ViewHeader
		headers[0].SetBytes([]byte{0xff})
		arr := makeStringViewArrayRaw(t, arrow.ViewHeaderTraits.CastToBytes(headers[:]), nil, nil, 0, 1, 0)
		defer arr.Release()

		assert.NoError(t, arr.Validate())
		err := arr.ValidateFull()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not valid utf8")
	})
}

func TestTopLevelValidate(t *testing.T) {
	t.Run("Validate dispatches to Validator", func(t *testing.T) {
		// non-monotonic string array: passes setData but ValidateFull must fail
		arr := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)
		assert.NoError(t, Validate(arr))
		require.Error(t, ValidateFull(arr))
	})

	t.Run("known null count mismatch passes Validate but fails ValidateFull", func(t *testing.T) {
		validity := make([]byte, bitutil.BytesForBits(2))
		bitutil.SetBit(validity, 0)
		arr := makeInt32ArrayRaw(t, []int32{10, 20}, validity, 0, 2, 0)
		defer arr.Release()

		assert.NoError(t, Validate(arr))
		err := ValidateFull(arr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match actual number of nulls")
	})

	t.Run("Validate returns nil for non-Validator types", func(t *testing.T) {
		// Bool arrays don't implement Validator — should return nil
		bldr := NewBooleanBuilder(memory.NewGoAllocator())
		bldr.AppendValues([]bool{true, false}, nil)
		arr := bldr.NewBooleanArray()
		defer arr.Release()
		assert.NoError(t, Validate(arr))
		assert.NoError(t, ValidateFull(arr))
	})

	t.Run("ValidateRecord validates all columns", func(t *testing.T) {
		validArr := makeStringArrayRaw(t, []int32{0, 3, 6}, "abcdef", 2, 0)
		corruptArr := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "ok", Type: arrow.BinaryTypes.String},
			{Name: "bad", Type: arrow.BinaryTypes.String},
		}, nil)
		rec := NewRecordBatch(schema, []arrow.Array{validArr, corruptArr}, 2)
		defer rec.Release()

		assert.NoError(t, ValidateRecord(rec))
		err := ValidateRecordFull(rec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 1 (bad)")
	})
}

func TestTopLevelValidateUnions(t *testing.T) {
	type unionCase struct {
		name string
		arr  arrow.Array
	}

	typeIDs, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8, strings.NewReader(`[0, 1]`))
	require.NoError(t, err)
	defer typeIDs.Release()

	sparseChild0, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[10, 20]`))
	require.NoError(t, err)
	defer sparseChild0.Release()
	sparseChild1, _, err := FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(`["one", "two"]`))
	require.NoError(t, err)
	defer sparseChild1.Release()

	sparse, err := NewSparseUnionFromArrays(typeIDs, []arrow.Array{sparseChild0, sparseChild1})
	require.NoError(t, err)
	defer sparse.Release()

	offsets, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0]`))
	require.NoError(t, err)
	defer offsets.Release()
	denseChild0, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[10]`))
	require.NoError(t, err)
	defer denseChild0.Release()
	denseChild1, _, err := FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(`["two"]`))
	require.NoError(t, err)
	defer denseChild1.Release()

	dense, err := NewDenseUnionFromArrays(typeIDs, offsets, []arrow.Array{denseChild0, denseChild1})
	require.NoError(t, err)
	defer dense.Release()

	for _, tc := range []unionCase{{"sparse", sparse}, {"dense", dense}} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NoError(t, Validate(tc.arr))
			assert.NoError(t, ValidateFull(tc.arr))

			nested, err := NewStructArrayWithFields([]arrow.Array{tc.arr}, []arrow.Field{
				{Name: "value", Type: tc.arr.DataType(), Nullable: true},
			})
			require.NoError(t, err)
			defer nested.Release()

			assert.NoError(t, Validate(nested))
			assert.NoError(t, ValidateFull(nested))
		})
	}
}

func TestTopLevelValidateFullRecursesThroughNestedChildren(t *testing.T) {
	badString := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)
	defer badString.Release()

	inner, err := NewStructArrayWithFields([]arrow.Array{badString}, []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	})
	require.NoError(t, err)
	defer inner.Release()

	offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 3}))
	data := NewData(arrow.ListOf(inner.DataType()), 1,
		[]*memory.Buffer{nil, offsets}, []arrow.ArrayData{inner.Data()}, 0, 0)
	offsets.Release()
	list := NewListData(data)
	data.Release()
	defer list.Release()

	outer, err := NewStructArrayWithFields([]arrow.Array{list}, []arrow.Field{
		{Name: "orders", Type: list.DataType(), Nullable: true},
	})
	require.NoError(t, err)
	defer outer.Release()

	err = ValidateFull(outer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `field "orders" -> list values -> field "name"`)
}

func TestListValidateFullChecksOffsets(t *testing.T) {
	values, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2]`))
	require.NoError(t, err)
	defer values.Release()

	offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 2, 1}))
	data := NewData(arrow.ListOf(arrow.PrimitiveTypes.Int32), 2,
		[]*memory.Buffer{nil, offsets}, []arrow.ArrayData{values.Data()}, 0, 0)
	offsets.Release()
	list := NewListData(data)
	data.Release()
	defer list.Release()

	assert.NoError(t, list.Validate())
	err = list.ValidateFull()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not monotonically non-decreasing")
}

func TestTopLevelValidateFullRecursesDictionaryValues(t *testing.T) {
	badDictionary := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)
	defer badDictionary.Release()
	indices, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8, strings.NewReader(`[0]`))
	require.NoError(t, err)
	defer indices.Release()

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dict := NewDictionaryArray(dt, indices, badDictionary)
	defer dict.Release()

	err = ValidateFull(dict)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dictionary")
	assert.Contains(t, err.Error(), "not monotonically non-decreasing")
}

func TestDictionaryIndexBoundsOnlyRunDuringValidateFull(t *testing.T) {
	indices, _, err := FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8, strings.NewReader(`[0, 1]`))
	require.NoError(t, err)
	defer indices.Release()
	values, _, err := FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(`["only"]`))
	require.NoError(t, err)
	defer values.Release()

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dict := NewDictionaryArray(dt, indices, values)
	defer dict.Release()

	assert.NoError(t, Validate(dict))
	err = ValidateFull(dict)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dictionary indices")
}
