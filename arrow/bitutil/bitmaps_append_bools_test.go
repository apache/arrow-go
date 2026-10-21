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

package bitutil_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func TestBitmapWriterAppendBoolsChunking(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for offset := 0; offset < 16; offset++ {
		for length := 0; length <= 129; length++ {
			original := make([]byte, int(bitutil.BytesForBits(int64(offset+length)))+8)
			_, _ = rng.Read(original)
			values := make([]bool, length+7)
			for i := range values {
				values[i] = rng.Intn(2) == 0
			}

			want := append([]byte(nil), original...)
			for i, v := range values[:length] {
				bitutil.SetBitTo(want, offset+i, v)
			}

			for _, chunkSize := range []int{1, 2, 7, 8, 9, 31, 256} {
				got := append([]byte(nil), original...)
				writer := bitutil.NewBitmapWriter(got, offset, length)
				pos := 0
				if chunkSize == 256 {
					// Request extra values to cover the writer's capacity limit.
					pos = writer.AppendBools(values)
				} else {
					for pos < length {
						end := min(pos+chunkSize, length)
						written := writer.AppendBools(values[pos:end])
						if written != end-pos {
							t.Fatalf("offset=%d length=%d chunk=%d: wrote %d", offset, length, chunkSize, written)
						}
						pos += written
					}
				}
				if pos != length || writer.AppendBools(values[length:]) != 0 {
					t.Fatalf("offset=%d length=%d chunk=%d: wrong capacity", offset, length, chunkSize)
				}
				writer.Finish()
				if !bytes.Equal(got, want) {
					t.Fatalf("offset=%d length=%d chunk=%d: got %x, want %x", offset, length, chunkSize, got, want)
				}
			}
		}
	}
}

func TestBitmapWriterAppendBoolsWithOtherWrites(t *testing.T) {
	const length = 64
	values := make([]bool, length)
	for i := range values {
		values[i] = i%3 == 0
	}
	src := []byte{0x92, 0xb6, 0x03, 0x77}
	for offset := 0; offset < 10; offset++ {
		got := bytes.Repeat([]byte{0xa5}, 24)
		want := append([]byte(nil), got...)
		writer := bitutil.NewBitmapWriter(got, offset, length)
		for i := 0; i < 11; i++ {
			if values[i] {
				writer.Set()
			} else {
				writer.Clear()
			}
			writer.Next()
		}
		if n := writer.AppendBools(values[11:28]); n != 17 {
			t.Fatalf("expected 17 booleans, got %d", n)
		}
		if n := writer.AppendBitmap(src, 5, 19); n != 19 {
			t.Fatalf("expected 19 bitmap bits, got %d", n)
		}
		for i := 47; i < length; i++ {
			if values[i] {
				writer.Set()
			} else {
				writer.Clear()
			}
			writer.Next()
		}
		writer.Finish()

		for i := 0; i < length; i++ {
			value := values[i]
			if i >= 28 && i < 47 {
				value = bitutil.BitIsSet(src, 5+i-28)
			}
			bitutil.SetBitTo(want, offset+i, value)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("offset=%d: got %x, want %x", offset, got, want)
		}
	}
}

func BenchmarkBitmapWriterAppendBools(b *testing.B) {
	for _, size := range []int{64, 1024, 8192, 65536} {
		values := make([]bool, size)
		for i := range values {
			values[i] = (i*37)%11 < 5
		}
		for _, offset := range []int{0, 3} {
			for _, split := range []bool{false, true} {
				b.Run(fmt.Sprintf("size=%d/offset=%d/split=%t", size, offset, split), func(b *testing.B) {
					out := make([]byte, int(bitutil.BytesForBits(int64(offset+size)))+8)
					writer := bitutil.NewBitmapWriter(out, offset, size)
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						writer.Reset(offset, size)
						if split {
							oneThird := size / 3
							writer.AppendBools(values[:oneThird])
							writer.AppendBools(values[oneThird : oneThird*2])
							writer.AppendBools(values[oneThird*2:])
						} else {
							writer.AppendBools(values)
						}
						writer.Finish()
					}
				})
			}
		}
	}
}
