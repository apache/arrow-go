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
	"github.com/klauspost/compress/gzip"
)

func BenchmarkGzipEncodePages(b *testing.B) {
	codec, err := compress.GetCodec(compress.Codecs.Gzip)
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		data func(int) []byte
	}{
		{"repeated", makeCompressibleData},
		{"random", makeRandomData},
	} {
		for _, pageSize := range []int{4 << 10, 64 << 10, 256 << 10} {
			b.Run(tc.name+"/page="+formatBytes(pageSize), func(b *testing.B) {
				src := tc.data(pageSize)
				dst := make([]byte, 0, codec.CompressBound(int64(len(src))))

				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst = codec.EncodeLevel(dst[:0], src, gzip.DefaultCompression)
				}
			})
		}
	}
}

func formatBytes(size int) string {
	if size >= 1<<20 {
		return fmt.Sprintf("%dMiB", size>>20)
	}
	return fmt.Sprintf("%dKiB", size>>10)
}
