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

package compress_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
)

func BenchmarkBrotliEncodeLevel(b *testing.B) {
	dataCases := []struct {
		name string
		data []byte
	}{
		{name: "compressible/64KiB", data: makeCompressibleData(64 * 1024)},
		{name: "compressible/256KiB", data: makeCompressibleData(256 * 1024)},
		{name: "compressible/1MiB", data: makeCompressibleData(1024 * 1024)},
		{name: "semi-random/64KiB", data: makeSemiRandomBrotliData(64 * 1024)},
		{name: "semi-random/256KiB", data: makeSemiRandomBrotliData(256 * 1024)},
		{name: "semi-random/1MiB", data: makeSemiRandomBrotliData(1024 * 1024)},
	}
	levels := []struct {
		name  string
		level int
	}{
		{name: "level=1", level: 1},
		{name: "level=default", level: compress.DefaultCompressionLevel},
		{name: "level=9", level: 9},
		{name: "level=11", level: 11},
	}

	codec, err := compress.GetCodec(compress.Codecs.Brotli)
	if err != nil {
		b.Fatal(err)
	}

	for _, dataCase := range dataCases {
		for _, level := range levels {
			b.Run(fmt.Sprintf("%s/%s", dataCase.name, level.name), func(b *testing.B) {
				dst := make([]byte, int(codec.CompressBound(int64(len(dataCase.data)))))
				b.SetBytes(int64(len(dataCase.data)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					codec.EncodeLevel(dst, dataCase.data, level.level)
				}
			})
		}
	}
}

func BenchmarkBrotliEncodeLevelParallel(b *testing.B) {
	data := makeSemiRandomBrotliData(256 * 1024)
	codec, err := compress.GetCodec(compress.Codecs.Brotli)
	if err != nil {
		b.Fatal(err)
	}

	for _, level := range []struct {
		name  string
		level int
	}{
		{name: "level=1", level: 1},
		{name: "level=default", level: compress.DefaultCompressionLevel},
		{name: "level=9", level: 9},
		{name: "level=11", level: 11},
	} {
		b.Run(level.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, int(codec.CompressBound(int64(len(data)))))
				for pb.Next() {
					codec.EncodeLevel(dst, data, level.level)
				}
			})
		})
	}
}

func makeSemiRandomBrotliData(size int) []byte {
	data := makeRandomData(size)
	pattern := []byte("parquet-page-data-pattern-0123456789abcdef")
	for i := 0; i < len(data)/4; i += len(pattern) {
		copy(data[i:], pattern)
	}
	return data
}
