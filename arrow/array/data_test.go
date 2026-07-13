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
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/utils/maphash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataReset(t *testing.T) {
	var (
		buffers1 = make([]*memory.Buffer, 0, 3)
		buffers2 = make([]*memory.Buffer, 0, 3)
	)
	for i := 0; i < cap(buffers1); i++ {
		buffers1 = append(buffers1, memory.NewBufferBytes([]byte("some-bytes1")))
		buffers2 = append(buffers2, memory.NewBufferBytes([]byte("some-bytes2")))
	}

	data := NewData(&arrow.StringType{}, 10, buffers1, nil, 0, 0)
	data.Reset(&arrow.Int64Type{}, 5, buffers2, nil, 1, 2)

	for i := 0; i < 2; i++ {
		assert.Equal(t, buffers2, data.Buffers())
		assert.Equal(t, &arrow.Int64Type{}, data.DataType())
		assert.Equal(t, 1, data.NullN())
		assert.Equal(t, 2, data.Offset())
		assert.Equal(t, 5, data.Len())

		// Make sure it works when resetting the data with its own buffers (new buffers are retained
		// before old ones are released.)
		data.Reset(&arrow.Int64Type{}, 5, data.Buffers(), nil, 1, 2)
	}
}

func TestNewSliceDataInvalidBounds(t *testing.T) {
	data := NewData(&arrow.Int64Type{}, 3, nil, nil, 0, 0)
	defer data.Release()

	tests := []struct {
		name string
		i, j int64
	}{
		{name: "negative start", i: -1, j: 0},
		{name: "negative end", i: -2, j: -1},
		{name: "end beyond length", i: 0, j: 4},
		{name: "start after end", i: 2, j: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t, "arrow/array: index out of range", func() {
				NewSliceData(data, tt.i, tt.j)
			})
		})
	}
}

func TestDataResetClearsDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	values := memory.NewResizableBuffer(mem)
	values.Resize(16)

	dictData := NewData(arrow.PrimitiveTypes.Int32, 4, []*memory.Buffer{nil, values}, nil, 0, 0)
	values.Release()

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.PrimitiveTypes.Int32,
	}
	data := NewDataWithDictionary(dictType, 0, []*memory.Buffer{nil, nil}, 0, 0, dictData)
	defer data.Release()

	assert.NotNil(t, data.Dictionary())

	data.Reset(arrow.PrimitiveTypes.Int8, 0, []*memory.Buffer{nil, nil}, nil, 0, 0)

	assert.Nil(t, data.Dictionary())

	dictData.Release()
	mem.AssertSize(t, 0)
}

func TestDataSetDictionaryWithSamePointerRetainsDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := memory.NewResizableBuffer(mem)
	values.Resize(4)
	copy(values.Bytes(), arrow.Int32Traits.CastToBytes([]int32{42}))
	dictData := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{nil, values}, nil, 0, 0)
	values.Release()

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.PrimitiveTypes.Int32,
	}
	data := NewDataWithDictionary(dictType, 1, []*memory.Buffer{nil, memory.NewBufferBytes([]byte{0})}, 0, 0, dictData)
	defer data.Release()
	dictData.Release()

	data.SetDictionary(dictData)
	require.Same(t, dictData, data.Dictionary())
	require.NotNil(t, dictData.Buffers()[1])

	valuesArray := NewInt32Data(dictData)
	defer valuesArray.Release()
	require.Equal(t, int32(42), valuesArray.Value(0))
}

func TestDataSetDictionaryNilClearsDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := memory.NewResizableBuffer(mem)
	values.Resize(4)
	dictData := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{nil, values}, nil, 0, 0)
	values.Release()

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.PrimitiveTypes.Int32,
	}
	data := NewDataWithDictionary(dictType, 1, []*memory.Buffer{nil, memory.NewBufferBytes([]byte{0})}, 0, 0, dictData)
	defer data.Release()
	dictData.Release()

	require.NotNil(t, data.Dictionary())

	data.SetDictionary(nil)

	require.Nil(t, data.Dictionary())
}

func TestHashIncludesDataMetadataAndBuffers(t *testing.T) {
	seed := maphash.MakeSeed()
	hash := func(data arrow.ArrayData) uint64 {
		var h maphash.MapHash
		h.SetSeed(seed)
		Hash(&h, data)
		return h.Sum64()
	}

	base := NewData(arrow.PrimitiveTypes.Int32, 2, []*memory.Buffer{
		nil,
		memory.NewBufferBytes([]byte{1, 0, 0, 0, 2, 0, 0, 0}),
	}, nil, 0, 0)
	defer base.Release()
	baseHash := hash(base)

	t.Run("value buffers", func(t *testing.T) {
		other := NewData(arrow.PrimitiveTypes.Int32, 2, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{3, 0, 0, 0, 4, 0, 0, 0}),
		}, nil, 0, 0)
		defer other.Release()

		assert.NotEqual(t, baseHash, hash(other))
	})

	t.Run("data type", func(t *testing.T) {
		other := NewData(arrow.PrimitiveTypes.Uint32, 2, base.Buffers(), nil, 0, 0)
		defer other.Release()

		assert.NotEqual(t, baseHash, hash(other))
	})

	t.Run("offset", func(t *testing.T) {
		other := NewData(arrow.PrimitiveTypes.Int32, 2, base.Buffers(), nil, 0, 1)
		defer other.Release()

		assert.NotEqual(t, baseHash, hash(other))
	})

	t.Run("children", func(t *testing.T) {
		childA := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{1, 0, 0, 0}),
		}, nil, 0, 0)
		defer childA.Release()
		childB := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{2, 0, 0, 0}),
		}, nil, 0, 0)
		defer childB.Release()

		offsets := memory.NewBufferBytes([]byte{0, 0, 0, 0, 1, 0, 0, 0})
		left := NewData(arrow.ListOf(arrow.PrimitiveTypes.Int32), 1, []*memory.Buffer{nil, offsets}, []arrow.ArrayData{childA}, 0, 0)
		defer left.Release()
		right := NewData(arrow.ListOf(arrow.PrimitiveTypes.Int32), 1, []*memory.Buffer{nil, offsets}, []arrow.ArrayData{childB}, 0, 0)
		defer right.Release()

		assert.NotEqual(t, hash(left), hash(right))
	})

	t.Run("dictionary", func(t *testing.T) {
		dictA := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{1, 0, 0, 0}),
		}, nil, 0, 0)
		defer dictA.Release()
		dictB := NewData(arrow.PrimitiveTypes.Int32, 1, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{2, 0, 0, 0}),
		}, nil, 0, 0)
		defer dictB.Release()

		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.PrimitiveTypes.Int32}
		indices := []*memory.Buffer{nil, memory.NewBufferBytes([]byte{0})}
		left := NewDataWithDictionary(dt, 1, indices, 0, 0, dictA)
		defer left.Release()
		right := NewDataWithDictionary(dt, 1, indices, 0, 0, dictB)
		defer right.Release()

		assert.NotEqual(t, hash(left), hash(right))
	})
}

func TestSizeInBytes(t *testing.T) {
	var buffers1 = make([]*memory.Buffer, 0, 3)

	for i := 0; i < cap(buffers1); i++ {
		buffers1 = append(buffers1, memory.NewBufferBytes([]byte("15-bytes-buffer")))
	}
	data := NewData(&arrow.StringType{}, 10, buffers1, nil, 0, 0)
	var arrayData arrow.ArrayData = data
	dataWithChild := NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{arrayData}, 0, 0)

	buffers2 := slices.Clone(buffers1)
	buffers2[0] = nil
	dataWithNilBuffer := NewData(&arrow.StringType{}, 10, buffers2, nil, 0, 0)

	t.Run("nil buffers", func(t *testing.T) {
		expectedSize := uint64(30)
		if actualSize := dataWithNilBuffer.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers only", func(t *testing.T) {
		expectedSize := uint64(45)
		if actualSize := data.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and child data", func(t *testing.T) {
		// 45 bytes in buffers, 45 bytes in child data
		expectedSize := uint64(90)
		if actualSize := dataWithChild.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and nested child data", func(t *testing.T) {
		var dataWithChildArrayData arrow.ArrayData = dataWithChild
		var dataWithNestedChild arrow.ArrayData = NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{dataWithChildArrayData}, 0, 0)
		// 45 bytes in buffers, 90 bytes in nested child data
		expectedSize := uint64(135)
		if actualSize := dataWithNestedChild.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and dictionary", func(t *testing.T) {
		dictData := data
		dataWithDict := NewDataWithDictionary(&arrow.StringType{}, 10, buffers1, 0, 0, dictData)
		// 45 bytes in buffers, 45 bytes in dictionary
		expectedSize := uint64(90)
		if actualSize := dataWithDict.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("sliced data", func(t *testing.T) {
		sliceData := NewSliceData(arrayData, 3, 5)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(45)
		if actualSize := sliceData.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("sliced data with children", func(t *testing.T) {
		var dataWithChildArrayData arrow.ArrayData = dataWithChild
		sliceData := NewSliceData(dataWithChildArrayData, 3, 5)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(90)
		if actualSize := sliceData.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers with children which are sliced data", func(t *testing.T) {
		sliceData := NewSliceData(arrayData, 3, 5)
		dataWithSlicedChildren := NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{sliceData}, 0, 0)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(90)
		if actualSize := dataWithSlicedChildren.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})
}

func TestDataReleaseWithNilChildData(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Create a Data object that simulates the state after a failed concatenation
	// where childData slice is allocated but contains nil elements
	buffers := []*memory.Buffer{memory.NewBufferBytes([]byte("test-buffer"))}
	data := NewData(arrow.ListOf(arrow.PrimitiveTypes.Int32), 1, buffers, nil, 0, 0)

	// Simulate the scenario where childData is allocated but elements remain nil
	// This happens in concat.go when childData is allocated but concat() fails
	data.childData = make([]arrow.ArrayData, 1)
	// data.childData[0] remains nil (simulating failed concat)

	assert.NotPanics(t, func() {
		data.Release()
	}, "Release() should not panic when childData contains nil elements")
}
