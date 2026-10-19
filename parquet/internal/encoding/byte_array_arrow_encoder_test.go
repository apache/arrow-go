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

package encoding

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

type arrowByteArrayEncoder32Test interface {
	ByteArrayEncoder
	PutArrow([]byte, []int32)
	PutArrowSpaced([]byte, []int32, []byte, int64)
}

type arrowByteArrayEncoder64Test interface {
	ByteArrayEncoder
	PutArrow64([]byte, []int64)
	PutArrowSpaced64([]byte, []int64, []byte, int64)
}

func arrowByteArrayInput() ([]parquet.ByteArray, []byte, []int32, []int64) {
	values := []parquet.ByteArray{
		[]byte("prefix/000"),
		[]byte("prefix/001"),
		{},
		[]byte("prefix/003"),
		[]byte("other"),
		{},
		[]byte("other/longer"),
	}
	data := make([]byte, 0, 64)
	offsets32 := []int32{0}
	offsets64 := []int64{0}
	for _, value := range values {
		data = append(data, value...)
		offsets32 = append(offsets32, int32(len(data)))
		offsets64 = append(offsets64, int64(len(data)))
	}
	return values, data, offsets32, offsets64
}

func encodedByteArrays(t *testing.T, encoding parquet.Encoding, values []parquet.ByteArray, spaced bool, validBits []byte, validBitsOffset int64) []byte {
	t.Helper()
	enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer enc.Release()
	if spaced {
		enc.PutSpaced(values, validBits, validBitsOffset)
	} else {
		enc.Put(values)
	}
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()
	return append([]byte(nil), buf.Bytes()...)
}

func TestByteArrayArrowEncodersMatchByteArrayInput(t *testing.T) {
	values, data, offsets32, offsets64 := arrowByteArrayInput()
	validBits := make([]byte, bitutil.BytesForBits(int64(len(values)+5)))
	validBitsOffset := int64(3)
	for _, index := range []int{0, 1, 3, 4, 6} {
		bitutil.SetBit(validBits, int(validBitsOffset)+index)
	}

	for _, encoding := range []parquet.Encoding{
		parquet.Encodings.Plain,
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	} {
		t.Run(encoding.String(), func(t *testing.T) {
			want := encodedByteArrays(t, encoding, values, false, nil, 0)
			wantSpaced := encodedByteArrays(t, encoding, values, true, validBits, validBitsOffset)

			for _, width := range []string{"int32", "int64"} {
				t.Run(width, func(t *testing.T) {
					enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
					defer enc.Release()
					if width == "int32" {
						direct := enc.(arrowByteArrayEncoder32Test)
						direct.PutArrow(data, offsets32)
					} else {
						direct := enc.(arrowByteArrayEncoder64Test)
						direct.PutArrow64(data, offsets64)
					}
					buf, err := enc.FlushValues()
					require.NoError(t, err)
					require.Equal(t, want, buf.Bytes())
					buf.Release()

					if width == "int32" {
						direct := enc.(arrowByteArrayEncoder32Test)
						direct.PutArrowSpaced(data, offsets32, validBits, validBitsOffset)
					} else {
						direct := enc.(arrowByteArrayEncoder64Test)
						direct.PutArrowSpaced64(data, offsets64, validBits, validBitsOffset)
					}
					buf, err = enc.FlushValues()
					require.NoError(t, err)
					require.Equal(t, wantSpaced, buf.Bytes())
					buf.Release()
				})
			}
		})
	}
}

func TestPlainByteArrayArrowOffsets(t *testing.T) {
	t.Run("int32", testPlainByteArrayArrowOffsets[int32])
	t.Run("int64", testPlainByteArrayArrowOffsets[int64])
}

func testPlainByteArrayArrowOffsets[T arrowByteArrayOffset](t *testing.T) {
	t.Helper()
	values, data, offsets32, _ := arrowByteArrayInput()
	offsets := make([]T, len(offsets32))
	for i, offset := range offsets32 {
		offsets[i] = T(offset)
	}
	for _, tc := range []struct {
		name            string
		offsets         []T
		validBits       []byte
		validBitsOffset int64
		want            []parquet.ByteArray
	}{
		{name: "nil"},
		{name: "empty-slice", offsets: offsets[3:4]},
		{name: "empty-values", offsets: []T{offsets[2], offsets[2], offsets[2]}, want: []parquet.ByteArray{{}, {}}},
		{name: "sliced", offsets: offsets[1:7], want: values[1:6]},
		{name: "whole", offsets: offsets, want: values},
		{
			name: "spaced-slice", offsets: offsets[1:],
			validBits: []byte{0b01011000, 0}, validBitsOffset: 3,
			want: []parquet.ByteArray{values[1], values[2], values[4]},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.Plain, false, nil, memory.DefaultAllocator).(*PlainByteArrayEncoder)
			defer enc.Release()
			enc.PutByteArray([]byte("before"))
			want := []parquet.ByteArray{[]byte("before")}
			for range 2 {
				putArrowPlainSpaced(enc.sink, data, tc.offsets, tc.validBits, tc.validBitsOffset)
				want = append(want, tc.want...)
			}
			enc.PutByteArray([]byte("after"))
			want = append(want, []byte("after"))
			got, err := enc.FlushValues()
			require.NoError(t, err)
			defer got.Release()
			require.Equal(t, encodedByteArrays(t, parquet.Encodings.Plain, want, false, nil, 0), got.Bytes())
		})
	}
}

// Check that the total-span size calculation preserves the plain encoding
// for variable-width values, sliced offsets, and disjoint valid runs.
func TestPlainByteArrayArrowVariableLengthSlices(t *testing.T) {
	t.Run("int32", testPlainByteArrayArrowVariableLengthSlices[int32])
	t.Run("int64", testPlainByteArrayArrowVariableLengthSlices[int64])
}

func testPlainByteArrayArrowVariableLengthSlices[T arrowByteArrayOffset](t *testing.T) {
	t.Helper()
	const nvalues = 129
	values := make([]parquet.ByteArray, nvalues)
	data := []byte("prefix outside the first value")
	offsets := make([]T, nvalues+1)
	for i := range values {
		offsets[i] = T(len(data))
		values[i] = make([]byte, (i*i*17+i*3)%57)
		for j := range values[i] {
			values[i][j] = byte(i + j)
		}
		data = append(data, values[i]...)
	}
	offsets[nvalues] = T(len(data))

	for _, span := range []struct {
		name       string
		begin, end int
	}{
		{"whole", 0, nvalues},
		{"middle", 9, 93},
		{"single", 63, 64},
		{"empty", 48, 48},
		{"tail", nvalues - 12, nvalues},
	} {
		for _, selection := range []string{"all", "sparse", "long-runs", "none"} {
			t.Run(span.name+"/"+selection, func(t *testing.T) {
				var validBits []byte
				if selection != "all" {
					validBits = make([]byte, bitutil.BytesForBits(int64(span.end-span.begin+3)))
				}
				want := []parquet.ByteArray{[]byte("before")}
				for i := span.begin; i < span.end; i++ {
					valid := selection == "all" ||
						selection == "sparse" && i%11 < 3 ||
						selection == "long-runs" && (i/16)%2 == 0
					if valid {
						want = append(want, values[i])
						if validBits != nil {
							bitutil.SetBit(validBits, 3+i-span.begin)
						}
					}
				}
				want = append(want, []byte("after"))

				enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.Plain, false, nil, memory.DefaultAllocator).(*PlainByteArrayEncoder)
				defer enc.Release()
				enc.PutByteArray([]byte("before"))
				putArrowPlainSpaced(enc.sink, data, offsets[span.begin:span.end+1], validBits, 3)
				enc.PutByteArray([]byte("after"))
				got, err := enc.FlushValues()
				require.NoError(t, err)
				defer got.Release()
				require.Equal(t, encodedByteArrays(t, parquet.Encodings.Plain, want, false, nil, 0), got.Bytes())
			})
		}
	}
}

func BenchmarkPlainByteArrayPutArrow(b *testing.B) {
	b.Run("int32", benchmarkPlainByteArrayPutArrow[int32])
	b.Run("int64", benchmarkPlainByteArrayPutArrow[int64])
}

func benchmarkPlainByteArrayPutArrow[T arrowByteArrayOffset](b *testing.B) {
	for _, nvalues := range []int{64, 65536} {
		for _, width := range []int{0, 4, 16, 64} {
			b.Run(fmt.Sprintf("values=%d/bytes=%d", nvalues, width), func(b *testing.B) {
				data := make([]byte, 4+nvalues*width)
				offsets := make([]T, nvalues+1)
				for i := range offsets {
					offsets[i] = T(4 + i*width)
				}
				enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.Plain, false, nil, memory.DefaultAllocator).(*PlainByteArrayEncoder)
				defer enc.Release()
				enc.sink.Reserve(nvalues * (4 + width))
				b.SetBytes(int64(nvalues * (4 + width)))
				b.ReportAllocs()
				for b.Loop() {
					// Reuse capacity to isolate encoding from result-buffer allocation.
					enc.sink.pos = 0
					putArrowPlain(enc.sink, data, offsets)
				}
				b.ReportMetric(float64(nvalues), "values/op")
			})
		}
	}
}

func TestDeltaByteArrayArrowEncoderPreservesStateAcrossBatches(t *testing.T) {
	values, data, offsets32, offsets64 := arrowByteArrayInput()

	for _, width := range []string{"int32", "int64"} {
		t.Run(width, func(t *testing.T) {
			enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
			defer enc.Release()
			direct32, direct64 := enc.(arrowByteArrayEncoder32Test), enc.(arrowByteArrayEncoder64Test)
			if width == "int32" {
				direct32.PutArrow(data, offsets32[:3])
				direct32.PutArrow(data, offsets32[2:])
			} else {
				direct64.PutArrow64(data, offsets64[:3])
				direct64.PutArrow64(data, offsets64[2:])
			}
			got, err := enc.FlushValues()
			require.NoError(t, err)
			defer got.Release()
			want := encodedByteArrays(t, parquet.Encodings.DeltaByteArray, values, false, nil, 0)
			require.Equal(t, want, got.Bytes())
		})
	}
}

func TestDeltaArrowEncodersAcrossInternalBatches(t *testing.T) {
	const nvalues = deltaByteArrayBatchSize*2 + 17
	values := make([]parquet.ByteArray, nvalues)
	data := make([]byte, 0, nvalues*16)
	offsets32 := []int32{0}
	offsets64 := []int64{0}
	validBits := make([]byte, bitutil.BytesForBits(nvalues+1))
	validBitsOffset := int64(1)
	for i := range values {
		values[i] = parquet.ByteArray(fmt.Sprintf("partition/%03d/value", i))
		data = append(data, values[i]...)
		offsets32 = append(offsets32, int32(len(data)))
		offsets64 = append(offsets64, int64(len(data)))
		if i%5 != 0 {
			bitutil.SetBit(validBits, int(validBitsOffset)+i)
		}
	}

	for _, encoding := range []parquet.Encoding{
		parquet.Encodings.DeltaLengthByteArray,
		parquet.Encodings.DeltaByteArray,
	} {
		for _, width := range []string{"int32", "int64"} {
			t.Run(encoding.String()+"/"+width, func(t *testing.T) {
				want := encodedByteArrays(t, encoding, values, true, validBits, validBitsOffset)
				enc := NewEncoder(parquet.Types.ByteArray, encoding, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
				defer enc.Release()
				if width == "int32" {
					enc.(arrowByteArrayEncoder32Test).PutArrowSpaced(data, offsets32, validBits, validBitsOffset)
				} else {
					enc.(arrowByteArrayEncoder64Test).PutArrowSpaced64(data, offsets64, validBits, validBitsOffset)
				}
				got, err := enc.FlushValues()
				require.NoError(t, err)
				require.Equal(t, want, got.Bytes())
				got.Release()
			})
		}
	}
}
