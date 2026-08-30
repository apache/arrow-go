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
	"bytes"
	"fmt"
	"testing"
)

func TestEncodeByteStreamSplitWidth4(t *testing.T) {
	testEncodeByteStreamSplit(t, 4, encodeByteStreamSplitWidth4Impl)
}

func TestEncodeByteStreamSplitWidth8(t *testing.T) {
	testEncodeByteStreamSplit(t, 8, encodeByteStreamSplitWidth8Impl)
}

func testEncodeByteStreamSplit(t *testing.T, width int, implementation func([]byte, []byte)) {
	for _, nValues := range []int{0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1023, 1024, 1025} {
		t.Run(fmt.Sprintf("nValues=%d", nValues), func(t *testing.T) {
			input := bytes.Repeat([]byte{0xa5}, nValues*width+2)
			in := input[1 : len(input)-1]
			for i := range in {
				in[i] = byte((i*37 + width) ^ (i >> 3))
			}
			original := bytes.Clone(input)

			want := make([]byte, len(in))
			output := bytes.Repeat([]byte{0x5a}, len(in)+2)
			got := output[1 : len(output)-1]
			switch width {
			case 4:
				encodeByteStreamSplitWidth4(want, in)
			case 8:
				encodeByteStreamSplitWidth8(want, in)
			}
			implementation(got, in)
			if !bytes.Equal(got, want) {
				t.Fatalf("encoded output mismatch: got %x, want %x", got, want)
			}
			if output[0] != 0x5a || output[len(output)-1] != 0x5a {
				t.Fatal("encoding modified bytes outside the output slice")
			}
			if !bytes.Equal(input, original) {
				t.Fatal("encoding modified the input")
			}
		})
	}
}

func BenchmarkEncodeByteStreamSplitWidth4(b *testing.B) {
	benchmarkEncodeByteStreamSplit(b, 4)
}

func BenchmarkEncodeByteStreamSplitWidth8(b *testing.B) {
	benchmarkEncodeByteStreamSplit(b, 8)
}

func benchmarkEncodeByteStreamSplit(b *testing.B, width int) {
	for _, nValues := range []int{8, 1024, 65536} {
		b.Run(fmt.Sprintf("nValues=%d", nValues), func(b *testing.B) {
			in := make([]byte, nValues*width)
			for i := range in {
				in[i] = byte((i*37 + width) ^ (i >> 3))
			}
			out := make([]byte, len(in))

			b.Run("scalar", func(b *testing.B) {
				b.SetBytes(int64(len(in)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch width {
					case 4:
						encodeByteStreamSplitWidth4(out, in)
					case 8:
						encodeByteStreamSplitWidth8(out, in)
					}
				}
			})

			b.Run("dispatch", func(b *testing.B) {
				b.SetBytes(int64(len(in)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch width {
					case 4:
						encodeByteStreamSplitWidth4Impl(out, in)
					case 8:
						encodeByteStreamSplitWidth8Impl(out, in)
					}
				}
			})
		})
	}
}
