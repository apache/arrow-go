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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func encodeDeltaByteArrayUnbatched(values []parquet.ByteArray) (Buffer, error) {
	prefixEncoder := NewEncoder(parquet.Types.Int32, parquet.Encodings.DeltaBinaryPacked,
		false, nil, memory.DefaultAllocator).(Int32Encoder)
	suffixEncoder := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
		false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
	defer prefixEncoder.Release()
	defer suffixEncoder.Release()

	var lastVal parquet.ByteArray
	for _, val := range values {
		prefixLength := 0
		for prefixLength < lastVal.Len() && prefixLength < val.Len() {
			if lastVal[prefixLength] != val[prefixLength] {
				break
			}
			prefixLength++
		}
		prefixEncoder.Put([]int32{int32(prefixLength)})
		suffixEncoder.Put([]parquet.ByteArray{val[prefixLength:]})
		lastVal = val
	}

	prefixBuf, err := prefixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer prefixBuf.Release()

	suffixBuf, err := suffixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer suffixBuf.Release()

	ret := bufferPool.Get().(*memory.Buffer)
	ret.ResizeNoShrink(prefixBuf.Len() + suffixBuf.Len())
	copy(ret.Bytes(), prefixBuf.Bytes())
	copy(ret.Bytes()[prefixBuf.Len():], suffixBuf.Bytes())
	return poolBuffer{ret}, nil
}

func BenchmarkDeltaByteArrayEncoding(b *testing.B) {
	for _, test := range []struct {
		name  string
		value func(int) []byte
	}{
		{
			name: "prefix-heavy",
			value: func(i int) []byte {
				return []byte(fmt.Sprintf("partition/%06d", i))
			},
		},
		{
			name: "low-prefix",
			value: func(i int) []byte {
				return []byte(fmt.Sprintf("%c/%06d", byte(i%251), i))
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			const nvalues = 64 * 1024
			values := make([]parquet.ByteArray, nvalues)
			var inputBytes int64
			for i := range values {
				values[i] = test.value(i)
				inputBytes += int64(values[i].Len())
			}

			for _, benchmark := range []struct {
				name   string
				encode func([]parquet.ByteArray) (Buffer, error)
			}{
				{name: "before", encode: encodeDeltaByteArrayUnbatched},
				{name: "after", encode: func(values []parquet.ByteArray) (Buffer, error) {
					enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray,
						false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
					defer enc.Release()
					enc.Put(values)
					return enc.FlushValues()
				}},
			} {
				b.Run(benchmark.name, func(b *testing.B) {
					b.SetBytes(inputBytes)
					b.ReportAllocs()
					for b.Loop() {
						buf, err := benchmark.encode(values)
						if err != nil {
							b.Fatal(err)
						}
						buf.Release()
					}
				})
			}
		})
	}
}
