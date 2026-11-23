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

package encoding_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func encodeDictIndices(t testing.TB, values []parquet.ByteArray) encoding.Buffer {
	t.Helper()

	enc := encoding.NewEncoder(parquet.Types.ByteArray, parquet.Encodings.PlainDict, true, nil, memory.DefaultAllocator).(*encoding.DictByteArrayEncoder)
	defer enc.Release()

	enc.Put(values)
	buf, err := enc.FlushValues()
	if err != nil {
		t.Fatalf("could not encode dictionary indices: %v", err)
	}
	return buf
}

func newBinaryDictionaryBuilder(mem memory.Allocator) *array.BinaryDictionaryBuilder {
	dictBuilder := array.NewStringBuilder(mem)
	dictBuilder.Append("one")
	dictBuilder.Append("two")
	dictBuilder.Append("three")
	dict := dictBuilder.NewArray()
	dictBuilder.Release()

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	bldr := array.NewDictionaryBuilderWithDict(mem, dictType, dict).(*array.BinaryDictionaryBuilder)
	dict.Release()
	return bldr
}

func TestDictByteArrayDecoderDecodeIndices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := []parquet.ByteArray{
		[]byte("one"), []byte("two"), []byte("one"), []byte("three"),
		[]byte("two"), []byte("one"),
	}
	indices := encodeDictIndices(t, values)
	defer indices.Release()

	decoder := encoding.NewDictDecoder(parquet.Types.ByteArray, nil, mem).(*encoding.DictByteArrayDecoder)
	bldr := newBinaryDictionaryBuilder(mem)
	defer bldr.Release()

	require.NoError(t, decoder.SetData(len(values), indices.Bytes()))
	n, err := decoder.DecodeIndices(len(values), bldr)
	require.NoError(t, err)
	require.Equal(t, len(values), n)

	arr := bldr.NewDictionaryArray()
	for i, want := range []int{0, 1, 0, 2, 1, 0} {
		require.False(t, arr.IsNull(i))
		require.Equal(t, want, arr.GetValueIndex(i))
	}
	arr.Release()

	require.NoError(t, decoder.SetData(len(values), indices.Bytes()))
	n, err = decoder.DecodeIndices(len(values), bldr)
	require.NoError(t, err)
	require.Equal(t, len(values), n)
}

func TestDictByteArrayDecoderDecodeToBinaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	column := schema.NewColumn(schema.NewByteArrayNode("value", parquet.Repetitions.Required, -1), 0, 0)
	dictionary := encoding.NewDecoder(parquet.Types.ByteArray, parquet.Encodings.Plain, column, mem)
	require.NoError(t, dictionary.SetData(2, []byte{3, 0, 0, 0, 'o', 'n', 'e', 0, 0, 0, 0}))
	decoder := encoding.NewDictDecoder(parquet.Types.ByteArray, column, mem).(*encoding.DictByteArrayDecoder)
	decoder.SetDict(dictionary)

	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append([]byte("prefix"))

	indices := make([]uint64, 2051)
	for i := range indices {
		indices[i] = uint64(i % 2)
	}
	require.NoError(t, decoder.SetData(len(indices), encodeDictionaryIndexPage(t, indices, 1)))
	for _, batch := range []int{2049, 2} {
		n, err := decoder.DecodeToBinaryBuilder(builder, batch)
		require.NoError(t, err)
		require.Equal(t, batch, n)
	}
	result := builder.NewBinaryArray()
	defer result.Release()
	require.Equal(t, len(indices)+1, result.Len())
	require.Equal(t, "prefix", result.ValueString(0))
	for i, index := range indices {
		require.False(t, result.IsNull(i+1))
		require.Equal(t, []string{"one", ""}[index], result.ValueString(i+1))
	}

	builder.Append([]byte("keep"))
	builder.AppendNull()
	indices[1400] = 2
	require.NoError(t, decoder.SetData(len(indices), encodeDictionaryIndexPage(t, indices, 2)))
	n, err := decoder.DecodeToBinaryBuilder(builder, len(indices))
	require.ErrorContains(t, err, "out of range")
	require.Zero(t, n)
	require.Equal(t, 2, builder.Len())
	require.Equal(t, len("keep"), builder.DataLen())
	builder.Append([]byte("next"))
	restored := builder.NewBinaryArray()
	defer restored.Release()
	require.Equal(t, "keep", restored.ValueString(0))
	require.True(t, restored.IsNull(1))
	require.Equal(t, "next", restored.ValueString(2))
}

func TestReserveBinaryDataRejectsOffsetOverflow(t *testing.T) {
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)
	defer builder.Release()
	require.ErrorIs(t, encoding.ReserveBinaryData(builder, uint64(math.MaxInt32)+1), arrow.ErrInvalid)
	require.Zero(t, builder.DataCap())
}

func TestDictByteArrayDecoderDecodeIndicesSpaced(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := []parquet.ByteArray{
		[]byte("one"), []byte("two"), []byte("three"), []byte("one"),
	}
	indices := encodeDictIndices(t, values)
	defer indices.Release()

	decoder := encoding.NewDictDecoder(parquet.Types.ByteArray, nil, mem).(*encoding.DictByteArrayDecoder)
	bldr := newBinaryDictionaryBuilder(mem)
	defer bldr.Release()

	validBits := []byte{0x0f}
	require.NoError(t, decoder.SetData(len(values), indices.Bytes()))
	n, err := decoder.DecodeIndicesSpaced(8, 4, validBits, 0, bldr)
	require.NoError(t, err)
	require.Equal(t, 8, n)

	arr := bldr.NewDictionaryArray()
	for i := 0; i < 8; i++ {
		require.Equal(t, i < 4, !arr.IsNull(i))
	}
	arr.Release()

	validBits = []byte{0xf0}
	require.NoError(t, decoder.SetData(len(values), indices.Bytes()))
	n, err = decoder.DecodeIndicesSpaced(8, 4, validBits, 0, bldr)
	require.NoError(t, err)
	require.Equal(t, 8, n)

	arr = bldr.NewDictionaryArray()
	defer arr.Release()
	for i := 0; i < 8; i++ {
		require.Equal(t, i >= 4, !arr.IsNull(i))
	}
}

func BenchmarkDictByteArrayDecoderDecodeIndices(b *testing.B) {
	for _, nvalues := range []int{1, 64, 4096, 65536} {
		b.Run(fmt.Sprintf("dense/%d", nvalues), func(b *testing.B) {
			benchmarkDictByteArrayDecoderDecodeIndices(b, nvalues, 0)
		})
	}
	for _, nvalues := range []int{64, 4096, 65536} {
		b.Run(fmt.Sprintf("spaced/%d/25pct-null", nvalues), func(b *testing.B) {
			benchmarkDictByteArrayDecoderDecodeIndices(b, nvalues, nvalues/4)
		})
	}
}

func benchmarkDictByteArrayDecoderDecodeIndices(b *testing.B, nvalues, nullCount int) {
	values := make([]parquet.ByteArray, nvalues-nullCount)
	for i := range values {
		values[i] = []byte("one")
	}
	indices := encodeDictIndices(b, values)
	defer indices.Release()

	decoder := encoding.NewDictDecoder(parquet.Types.ByteArray, nil, memory.DefaultAllocator).(*encoding.DictByteArrayDecoder)
	bldr := newBinaryDictionaryBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	var validBits []byte
	if nullCount > 0 {
		validBits = make([]byte, bitutil.BytesForBits(int64(nvalues)))
		for i := nullCount; i < nvalues; i++ {
			bitutil.SetBit(validBits, i)
		}
	}

	decode := func() {
		bldr.Resize(0)
		if err := decoder.SetData(len(values), indices.Bytes()); err != nil {
			b.Fatal(err)
		}
		var (
			n   int
			err error
		)
		if nullCount == 0 {
			n, err = decoder.DecodeIndices(nvalues, bldr)
		} else {
			n, err = decoder.DecodeIndicesSpaced(nvalues, nullCount, validBits, 0, bldr)
		}
		if err != nil {
			b.Fatal(err)
		}
		if n != nvalues {
			b.Fatalf("decoded %d values, want %d", n, nvalues)
		}
	}

	decode()
	b.ReportAllocs()
	b.SetBytes(int64(nvalues))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decode()
	}
	b.StopTimer()
}
