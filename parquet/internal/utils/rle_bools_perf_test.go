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

package utils_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
)

func appendRLELiteralBools(dst []byte, values []bool) []byte {
	if len(values)%8 != 0 {
		panic("literal length must be divisible by eight")
	}
	dst = binary.AppendUvarint(dst, uint64(len(values)/8*2+1))
	for i := 0; i < len(values); i += 8 {
		var packed byte
		for bit := 0; bit < 8; bit++ {
			if values[i+bit] {
				packed |= 1 << uint(bit)
			}
		}
		dst = append(dst, packed)
	}
	return dst
}

func appendRLERepeatedBool(dst []byte, count int, value bool) []byte {
	dst = binary.AppendUvarint(dst, uint64(count)*2)
	if value {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func TestBitReaderGetBatchBoolsResumeAfterPartialBytes(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i*73 + (i/11)*19 + 57)
	}
	for _, offset := range []int{0, 1, 2, 3, 7, 8, 9, 15, 31, 32, 55, 63, 64, 65} {
		for _, size := range []int{1, 3, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 4096} {
			t.Run(fmt.Sprintf("offset=%d/size=%d", offset, size), func(t *testing.T) {
				dec := utils.NewBitReader(bytes.NewReader(data))
				for range offset {
					if _, ok := dec.GetValue(1); !ok {
						t.Fatal("could not skip initial bits")
					}
				}
				pos := offset
				for range 3 {
					out := make([]bool, size)
					n, err := dec.GetBatchBools(out)
					if err != nil || n != size {
						t.Fatalf("read %d booleans: n=%d, err=%v", size, n, err)
					}
					for i, value := range out {
						bit := pos + i
						want := data[bit/8]&(1<<uint(bit%8)) != 0
						if value != want {
							t.Fatalf("bit %d: got %t, want %t", bit, value, want)
						}
					}
					pos += size
				}
			})
		}
	}
}

func TestBitReaderGetBatchBoolsTruncatedUnaligned(t *testing.T) {
	data := []byte{0xa5, 0x53, 0xcf, 0x91, 0x42}
	for _, offset := range []int{0, 1, 3, 7, 8, 9, 31, 32} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			dec := utils.NewBitReader(bytes.NewReader(data))
			for range offset {
				if _, ok := dec.GetValue(1); !ok {
					t.Fatal("could not skip initial bits")
				}
			}
			out := make([]bool, 64)
			n, err := dec.GetBatchBools(out)
			if err != io.ErrUnexpectedEOF || n != len(data)*8-offset {
				t.Fatalf("read n=%d, err=%v; want %d and unexpected EOF", n, err, len(data)*8-offset)
			}
			for i, value := range out[:n] {
				bit := offset + i
				if want := data[bit/8]&(1<<uint(bit%8)) != 0; value != want {
					t.Fatalf("bit %d: got %t, want %t", bit, value, want)
				}
			}
		})
	}
}

func rleBooleanFixture() ([]byte, []bool) {
	a := make([]bool, 2048)
	b := make([]bool, 1200)
	c := make([]bool, 4096)
	for i := range a {
		a[i] = (i*19+i/7)%23 < 9
	}
	for i := range b {
		b[i] = (i*7+i/11)%13 < 5
	}
	for i := range c {
		c[i] = (i*37+i/9)%17 < 8
	}

	encoded := appendRLELiteralBools(nil, a)
	encoded = appendRLERepeatedBool(encoded, 37, true)
	encoded = appendRLELiteralBools(encoded, b)
	encoded = appendRLERepeatedBool(encoded, 13, false)
	encoded = appendRLELiteralBools(encoded, c)

	want := append([]bool(nil), a...)
	for range 37 {
		want = append(want, true)
	}
	want = append(want, b...)
	for range 13 {
		want = append(want, false)
	}
	want = append(want, c...)
	return encoded, want
}

func TestRLEGetBatchBoolsLargeLiteralsAndRepeatedRuns(t *testing.T) {
	encoded, want := rleBooleanFixture()
	for _, batch := range []int{1, 2, 3, 7, 8, 9, 31, 32, 33, 63, 64, 65, 256, 4096, len(want)} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			dec := utils.NewRleDecoder(bytes.NewReader(encoded), 1)
			out := make([]bool, len(want))
			for pos := 0; pos < len(out); {
				size := min(batch, len(out)-pos)
				n, err := dec.GetBatchBools(out[pos : pos+size])
				if err != nil || n != size {
					t.Fatalf("read at %d: n=%d, want=%d, err=%v", pos, n, size, err)
				}
				pos += n
			}
			for i, value := range out {
				if value != want[i] {
					t.Fatalf("bit %d: got %t, want %t", i, value, want[i])
				}
			}
			if n, err := dec.GetBatchBools(make([]bool, 1)); n != 0 || err != nil {
				t.Fatalf("exhausted decoder: n=%d, err=%v", n, err)
			}
		})
	}

	dec := utils.NewRleDecoder(bytes.NewReader(encoded), 1)
	first := make([]bool, 19)
	if n, err := dec.GetBatchBools(first); n != len(first) || err != nil {
		t.Fatalf("first batch: n=%d, err=%v", n, err)
	}
	if n := dec.Discard(2050); n != 2050 {
		t.Fatalf("discarded %d values, want 2050", n)
	}
	rest := make([]bool, len(want)-len(first)-2050)
	if n, err := dec.GetBatchBools(rest); n != len(rest) || err != nil {
		t.Fatalf("after discard: n=%d, err=%v", n, err)
	}
	for i, value := range first {
		if value != want[i] {
			t.Fatalf("initial value %d differs", i)
		}
	}
	for i, value := range rest {
		if value != want[i+len(first)+2050] {
			t.Fatalf("value after discard at %d differs", i)
		}
	}
}

func BenchmarkRLEGetBatchBools(b *testing.B) {
	const count = 1 << 16
	input := make([]bool, count)
	for i := range input {
		input[i] = (i*29+i/17)%31 < 13
	}
	encoded := appendRLELiteralBools(nil, input)
	out := make([]bool, count)

	for _, batchSize := range []int{32, 256, 4096, count} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			source := bytes.NewReader(encoded)
			dec := utils.NewRleDecoder(source, 1)
			b.SetBytes(int64(count))
			b.ReportAllocs()
			for b.Loop() {
				source.Reset(encoded)
				dec.Reset(source, 1)
				for pos := 0; pos < count; {
					size := min(batchSize, count-pos)
					n, err := dec.GetBatchBools(out[pos : pos+size])
					if err != nil || n != size {
						b.Fatalf("read at %d: n=%d, want=%d, err=%v", pos, n, size, err)
					}
					pos += n
				}
			}
			if out[count-1] != input[count-1] {
				b.Fatal("decoded data differs")
			}
		})
	}
}
