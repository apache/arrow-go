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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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

func TestTopLevelValidate(t *testing.T) {
	t.Run("Validate dispatches to Validator", func(t *testing.T) {
		// non-monotonic string array: passes setData but ValidateFull must fail
		arr := makeStringArrayRaw(t, []int32{0, 5, 3, 5}, "hello", 3, 0)
		assert.NoError(t, Validate(arr))
		require.Error(t, ValidateFull(arr))
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
