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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/internal/testing/tools"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Init(t *testing.T) {
	type exp struct{ size int }
	tests := []struct {
		name string
		cap  int

		exp exp
	}{
		{"07 bits", 07, exp{size: 1}},
		{"19 bits", 19, exp{size: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ab := &builder{mem: memory.NewGoAllocator()}
			ab.init(test.cap)
			assert.Equal(t, test.cap, ab.Cap(), "invalid capacity")
			assert.Equal(t, test.exp.size, ab.nullBitmap.Len(), "invalid length")
		})
	}
}

func TestBuilder_UnsafeSetValid(t *testing.T) {
	ab := &builder{mem: memory.NewGoAllocator()}
	ab.init(32)
	ab.unsafeAppendBoolsToBitmap(tools.Bools(0, 0, 0, 0, 0), 5)
	assert.Equal(t, 5, ab.Len())
	assert.Equal(t, []byte{0, 0, 0, 0}, ab.nullBitmap.Bytes())

	ab.unsafeSetValid(17)
	assert.Equal(t, []byte{0xe0, 0xff, 0x3f, 0}, ab.nullBitmap.Bytes())
}

func TestBuilder_resize(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 64

	b.init(n)
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, 0, b.Len())

	b.UnsafeAppendBoolToBitmap(true)
	for i := 1; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(false)
	}
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	n = 5
	b.resize(n, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	b.resize(32, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())
}

func TestBuilder_IsNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	assert.True(t, b.IsNull(0))
	assert.True(t, b.IsNull(1))

	for i := 0; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(i%2 == 0)
	}
	for i := 0; i < n; i++ {
		assert.Equal(t, i%2 != 0, b.IsNull(i))
	}
}

func TestBuilder_SetNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	for i := 0; i < n; i++ {
		// Set everything to true
		b.UnsafeAppendBoolToBitmap(true)
	}
	for i := 0; i < n; i++ {
		if i%2 == 0 { // Set all even numbers to null
			b.SetNull(i)
		}
	}
	assert.Equal(t, n/2, b.NullN())

	// idempotent SetNull
	b.SetNull(0)
	assert.Equal(t, n/2, b.NullN())

	for i := 0; i < n; i++ {
		if i%2 == 0 {
			assert.True(t, b.IsNull(i))
		} else {
			assert.False(t, b.IsNull(i))
		}
	}
}

func countNulls(arr arrow.Array) int {
	n := 0
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			n++
		}
	}
	return n
}

func TestBuilderTruncate(t *testing.T) {
	scalarValid := []bool{true, false, true, true, false, true, true, false}

	cases := []struct {
		name string
		// build returns a builder that already has `full` elements appended.
		build func(mem memory.Allocator) Builder
		full  int
	}{
		{
			name: "boolean",
			build: func(mem memory.Allocator) Builder {
				b := NewBooleanBuilder(mem)
				b.AppendValues([]bool{true, false, true, false, true, false, true, false}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "int8",
			build: func(mem memory.Allocator) Builder {
				b := NewInt8Builder(mem)
				b.AppendValues([]int8{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "int16",
			build: func(mem memory.Allocator) Builder {
				b := NewInt16Builder(mem)
				b.AppendValues([]int16{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "int32",
			build: func(mem memory.Allocator) Builder {
				b := NewInt32Builder(mem)
				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "int64",
			build: func(mem memory.Allocator) Builder {
				b := NewInt64Builder(mem)
				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "uint8",
			build: func(mem memory.Allocator) Builder {
				b := NewUint8Builder(mem)
				b.AppendValues([]uint8{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "uint16",
			build: func(mem memory.Allocator) Builder {
				b := NewUint16Builder(mem)
				b.AppendValues([]uint16{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "uint32",
			build: func(mem memory.Allocator) Builder {
				b := NewUint32Builder(mem)
				b.AppendValues([]uint32{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "uint64",
			build: func(mem memory.Allocator) Builder {
				b := NewUint64Builder(mem)
				b.AppendValues([]uint64{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "float32",
			build: func(mem memory.Allocator) Builder {
				b := NewFloat32Builder(mem)
				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "float64",
			build: func(mem memory.Allocator) Builder {
				b := NewFloat64Builder(mem)
				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "string",
			build: func(mem memory.Allocator) Builder {
				b := NewStringBuilder(mem)
				b.AppendValues([]string{"a", "bb", "ccc", "d", "ee", "fff", "g", "hh"}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "binary",
			build: func(mem memory.Allocator) Builder {
				b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				b.AppendValues([][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("d"), []byte("ee"), []byte("fff"), []byte("g"), []byte("hh")}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "fixed_size_binary",
			build: func(mem memory.Allocator) Builder {
				b := NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 3})
				for i, v := range scalarValid {
					if v {
						b.Append([]byte{byte(i), byte(i + 1), byte(i + 2)})
					} else {
						b.AppendNull()
					}
				}
				return b
			},
			full: 8,
		},
		{
			name: "decimal128",
			build: func(mem memory.Allocator) Builder {
				b := NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 1})
				for i, v := range scalarValid {
					if v {
						assert.NoError(t, b.AppendValueFromString(strconv.Itoa(i)+".0"))
					} else {
						b.AppendNull()
					}
				}
				return b
			},
			full: 8,
		},
		{
			name: "decimal256",
			build: func(mem memory.Allocator) Builder {
				b := NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 10, Scale: 1})
				for i, v := range scalarValid {
					if v {
						assert.NoError(t, b.AppendValueFromString(strconv.Itoa(i)+".0"))
					} else {
						b.AppendNull()
					}
				}
				return b
			},
			full: 8,
		},
		{
			name: "date32",
			build: func(mem memory.Allocator) Builder {
				b := NewDate32Builder(mem)
				b.AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "timestamp",
			build: func(mem memory.Allocator) Builder {
				b := NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Second})
				b.AppendValues([]arrow.Timestamp{1, 2, 3, 4, 5, 6, 7, 8}, scalarValid)
				return b
			},
			full: 8,
		},
		{
			name: "list",
			build: func(mem memory.Allocator) Builder {
				b := NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
				vb := b.ValueBuilder().(*Int32Builder)
				lengths := []int{3, 0, 2, 1, 4, 2}
				valid := []bool{true, false, true, true, true, false}
				v := int32(0)
				for i, l := range lengths {
					b.Append(valid[i])
					for j := 0; j < l; j++ {
						vb.Append(v)
						v++
					}
				}
				return b
			},
			full: 6,
		},
		{
			name: "large_list",
			build: func(mem memory.Allocator) Builder {
				b := NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
				vb := b.ValueBuilder().(*Int32Builder)
				lengths := []int{3, 0, 2, 1, 4, 2}
				valid := []bool{true, false, true, true, true, false}
				v := int32(0)
				for i, l := range lengths {
					b.Append(valid[i])
					for j := 0; j < l; j++ {
						vb.Append(v)
						v++
					}
				}
				return b
			},
			full: 6,
		},
		{
			name: "fixed_size_list",
			build: func(mem memory.Allocator) Builder {
				b := NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Int32)
				vb := b.ValueBuilder().(*Int32Builder)
				valid := []bool{true, false, true, true, false, true}
				v := int32(0)
				for _, ok := range valid {
					b.Append(ok)
					vb.Append(v)
					vb.Append(v + 1)
					v += 2
				}
				return b
			},
			full: 6,
		},
		{
			name: "map",
			build: func(mem memory.Allocator) Builder {
				b := NewMapBuilder(mem, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, false)
				kb := b.KeyBuilder().(*Int32Builder)
				ib := b.ItemBuilder().(*Int32Builder)
				lengths := []int{2, 0, 1, 3, 2, 1}
				valid := []bool{true, false, true, true, true, false}
				k := int32(0)
				for i, l := range lengths {
					b.Append(valid[i])
					for j := 0; j < l; j++ {
						kb.Append(k)
						ib.Append(k * 10)
						k++
					}
				}
				return b
			},
			full: 6,
		},
		{
			name: "struct",
			build: func(mem memory.Allocator) Builder {
				dt := arrow.StructOf(
					arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
				)
				b := NewStructBuilder(mem, dt)
				ab := b.FieldBuilder(0).(*Int32Builder)
				bb := b.FieldBuilder(1).(*StringBuilder)
				valid := []bool{true, false, true, true, false, true, true, false}
				for i, ok := range valid {
					b.Append(ok)
					ab.Append(int32(i))
					bb.Append(strconv.Itoa(i))
				}
				return b
			},
			full: 8,
		},
		{
			name: "dictionary",
			build: func(mem memory.Allocator) Builder {
				dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
				b := NewDictionaryBuilder(mem, dt).(*BinaryDictionaryBuilder)
				values := []string{"x", "y", "x", "z", "y", "x", "w", "z"}
				for i, ok := range scalarValid {
					if ok {
						assert.NoError(t, b.AppendString(values[i]))
					} else {
						b.AppendNull()
					}
				}
				return b
			},
			full: 8,
		},
		{
			name: "sparse_union",
			build: func(mem memory.Allocator) Builder {
				i8 := NewInt8Builder(mem)
				str := NewStringBuilder(mem)
				b := NewSparseUnionBuilderWithBuilders(mem,
					arrow.SparseUnionOf([]arrow.Field{
						{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
					}, []arrow.UnionTypeCode{0, 1}),
					[]Builder{i8, str})
				types := []arrow.UnionTypeCode{0, 1, 0, 1, 0, 1}
				for i, tc := range types {
					b.Append(tc)
					if tc == 0 {
						i8.Append(int8(i))
						str.AppendEmptyValue()
					} else {
						str.Append(strconv.Itoa(i))
						i8.AppendEmptyValue()
					}
				}
				i8.Release()
				str.Release()
				return b
			},
			full: 6,
		},
		{
			name: "dense_union",
			build: func(mem memory.Allocator) Builder {
				i8 := NewInt8Builder(mem)
				str := NewStringBuilder(mem)
				b := NewDenseUnionBuilderWithBuilders(mem,
					arrow.DenseUnionOf([]arrow.Field{
						{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
					}, []arrow.UnionTypeCode{0, 1}),
					[]Builder{i8, str})
				types := []arrow.UnionTypeCode{0, 1, 0, 1, 0, 1}
				for i, tc := range types {
					b.Append(tc)
					if tc == 0 {
						i8.Append(int8(i))
					} else {
						str.Append(strconv.Itoa(i))
					}
				}
				i8.Release()
				str.Release()
				return b
			},
			full: 6,
		},
		{
			name: "run_end_encoded",
			build: func(mem memory.Allocator) Builder {
				b := NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
				vb := b.ValueBuilder().(*StringBuilder)
				runs := []uint64{2, 3, 1, 4, 2, 3}
				for i, r := range runs {
					b.Append(r)
					vb.Append(strconv.Itoa(i))
				}
				return b
			},
			full: 15,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range []int{tc.full - 1, tc.full / 2, 0} {
				t.Run(strconv.Itoa(n), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
					defer mem.AssertSize(t, 0)

					b := tc.build(mem)
					defer b.Release()

					b.Truncate(n)

					arr := b.NewArray()
					defer arr.Release()

					assert.Equal(t, n, arr.Len(), "length after truncate")
					assert.Equal(t, countNulls(arr), arr.NullN(), "null count consistency")
				})
			}
		})
	}
}
