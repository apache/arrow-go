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

package utils

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"testing"
)

// getBatchLevelsBeforeNarrow is the original GetBatchLevels implementation.
// Keeping it here enables exact before/after comparisons on the same machine.
func getBatchLevelsBeforeNarrow(b *BitReader, width uint, out []int16, maxLevel int16) (int, int64, error) {
	if width > 16 {
		return 0, 0, fmt.Errorf("must be 16 bits or less per read")
	}
	var maxCount int64
	length := len(out)
	i := 0
	for ; i < length && b.bitoffset != 0; i++ {
		val, err := b.next(width)
		if err != nil {
			return i, maxCount, err
		}
		level := int16(val)
		out[i] = level
		if level == maxLevel {
			maxCount++
		}
	}
	if _, err := b.reader.Seek(b.byteoffset, io.SeekStart); err != nil {
		return i, maxCount, err
	}
	for i < length {
		unpackSize := min(buflen, length-i)
		numUnpacked, err := unpack32(b.reader, b.unpackBuf[:unpackSize], int(width))
		for k, val := range b.unpackBuf[:numUnpacked] {
			level := int16(val)
			out[i+k] = level
			if level == maxLevel {
				maxCount++
			}
		}
		i += numUnpacked
		b.byteoffset += int64(numUnpacked * int(width) / 8)
		if err != nil {
			return i, maxCount, err
		}
		if numUnpacked == 0 {
			break
		}
	}
	if err := b.fillbuffer(); err != nil {
		return i, maxCount, err
	}
	for ; i < length; i++ {
		val, err := b.next(width)
		if err != nil {
			return i, maxCount, err
		}
		level := int16(val)
		out[i] = level
		if level == maxLevel {
			maxCount++
		}
	}
	return i, maxCount, nil
}

func packedLevelFixture(width uint, n int, pattern string) ([]int16, []byte) {
	values := make([]int16, n)
	for i := range values {
		switch pattern {
		case "defined":
			values[i] = int16((1 << width) - 1)
		case "mostly-defined":
			if i%20 != 0 {
				values[i] = int16((1 << width) - 1)
			}
		case "alternating":
			values[i] = int16(i % 2)
		case "varied":
			values[i] = int16((i*29 + i/5 + i*i/71) % (1 << width))
		}
	}
	packed := make([]byte, (n*int(width)+7)/8)
	for i, value := range values {
		bit := i * int(width)
		for j := uint(0); j < width; j++ {
			if value&(1<<j) != 0 {
				packed[(bit+int(j))/8] |= 1 << ((bit + int(j)) % 8)
			}
		}
	}
	return values, packed
}

func TestGetBatchLevelsNarrowParity(t *testing.T) {
	for _, width := range []uint{1, 2} {
		for _, pattern := range []string{"defined", "mostly-defined", "alternating", "varied"} {
			for _, n := range []int{0, 1, 7, 31, 32, 33, 63, 64, 65, 1023, 1024, 1025, 4097} {
				_, packed := packedLevelFixture(width, n+130, pattern)
				for _, prefix := range []int{0, 1, 7, 32, 64} {
					for _, maxLevel := range []int16{0, 1, 2, 3, 7} {
						name := fmt.Sprintf("width=%d/%s/len=%d/prefix=%d/max=%d", width, pattern, n, prefix, maxLevel)
						t.Run(name, func(t *testing.T) {
							got := make([]int16, n)
							want := make([]int16, n)
							for i := range got {
								got[i] = -123
								want[i] = -123
							}
							r1 := NewBitReader(bytes.NewReader(packed))
							r2 := NewBitReader(bytes.NewReader(packed))
							if prefix != 0 {
								if err := r1.fillbuffer(); err != nil {
									t.Fatal(err)
								}
								if err := r2.fillbuffer(); err != nil {
									t.Fatal(err)
								}
								for range prefix {
									if _, err := r1.next(width); err != nil {
										t.Fatal(err)
									}
									if _, err := r2.next(width); err != nil {
										t.Fatal(err)
									}
								}
							}
							gotN, gotCount, gotErr := r1.GetBatchLevels(width, got, maxLevel)
							wantN, wantCount, wantErr := getBatchLevelsBeforeNarrow(r2, width, want, maxLevel)
							if gotN != wantN || gotCount != wantCount || gotErr != wantErr || !slices.Equal(got, want) {
								t.Fatalf("got n=%d count=%d err=%v values=%v; want n=%d count=%d err=%v values=%v",
									gotN, gotCount, gotErr, got, wantN, wantCount, wantErr, want)
							}
							if r1.byteoffset != r2.byteoffset || r1.bitoffset != r2.bitoffset {
								t.Fatalf("reader offsets differ: byte=%d bit=%d vs byte=%d bit=%d",
									r1.byteoffset, r1.bitoffset, r2.byteoffset, r2.bitoffset)
							}
						})
					}
				}
			}
		}
	}
}

func TestGetBatchLevelsNarrowShortReads(t *testing.T) {
	for _, width := range []uint{1, 2} {
		_, packed := packedLevelFixture(width, 2050, "varied")
		for _, nbytes := range []int{0, 1, 3, 4, 5, 7, 8, 9, 63, 64, 65, 128, 129, 256} {
			if nbytes > len(packed) {
				continue
			}
			for _, nvalues := range []int{1, 31, 32, 33, 63, 64, 65, 1024, 2048} {
				t.Run(fmt.Sprintf("width=%d/bytes=%d/values=%d", width, nbytes, nvalues), func(t *testing.T) {
					data := packed[:nbytes]
					got := make([]int16, nvalues)
					want := make([]int16, nvalues)
					for i := range got {
						got[i], want[i] = -123, -123
					}
					r1 := NewBitReader(bytes.NewReader(data))
					r2 := NewBitReader(bytes.NewReader(data))
					gotN, gotCount, gotErr := r1.GetBatchLevels(width, got, int16((1<<width)-1))
					wantN, wantCount, wantErr := getBatchLevelsBeforeNarrow(r2, width, want, int16((1<<width)-1))
					if gotN != wantN || gotCount != wantCount || gotErr != wantErr || !slices.Equal(got, want) {
						t.Fatalf("got n=%d count=%d err=%v, want n=%d count=%d err=%v",
							gotN, gotCount, gotErr, wantN, wantCount, wantErr)
					}
				})
			}
		}
	}
}

func TestGetBatchLevelsOneBitResumeAfterPartialGroup(t *testing.T) {
	const width uint = 1
	values, packed := packedLevelFixture(width, 2048, "varied")
	for _, nbytes := range []int{1, 2, 3, 5, 7, 9, 63, 65, 129} {
		t.Run(fmt.Sprintf("bytes=%d", nbytes), func(t *testing.T) {
			source := bytes.NewReader(packed[:nbytes])
			reader := NewBitReader(source)
			out := make([]int16, 2048)
			n, maxCount, err := reader.GetBatchLevels(width, out, 1)
			wantN := nbytes / 4 * 32
			if n != wantN || err != io.ErrUnexpectedEOF {
				t.Fatalf("decoded %d, err=%v; want %d and unexpected EOF", n, err, wantN)
			}
			if !slices.Equal(out[:n], values[:n]) {
				t.Fatalf("decoded levels differ from expected values")
			}
			var wantCount int64
			for _, level := range values[:n] {
				if level == 1 {
					wantCount++
				}
			}
			if maxCount != wantCount {
				t.Fatalf("max level count=%d; want %d", maxCount, wantCount)
			}
			if reader.byteoffset != int64(wantN/8) || reader.bitoffset != 0 {
				t.Fatalf("reader offset=%d:%d; want %d:0", reader.byteoffset, reader.bitoffset, wantN/8)
			}
			cursor, seekErr := source.Seek(0, io.SeekCurrent)
			if seekErr != nil || cursor != reader.byteoffset {
				t.Fatalf("underlying cursor=%d, err=%v; want %d", cursor, seekErr, reader.byteoffset)
			}

			// The failed bulk read consumed part of the next group.
			// Those bytes must still be available to scalar decoding.
			for i := n; i < nbytes*8; i++ {
				value, err := reader.next(width)
				if err != nil || int16(value) != values[i] {
					t.Fatalf("subsequent level %d: got %d, err=%v; want %d", i, value, err, values[i])
				}
			}
			if _, err := reader.next(width); err != io.ErrUnexpectedEOF {
				t.Fatalf("read past available bits: got %v; want unexpected EOF", err)
			}
		})
	}
}

func TestGetBatchLevelsNarrowSequentialReads(t *testing.T) {
	for _, width := range []uint{1, 2} {
		_, packed := packedLevelFixture(width, 4096, "varied")
		gotReader := NewBitReader(bytes.NewReader(packed))
		wantReader := NewBitReader(bytes.NewReader(packed))
		for _, size := range []int{31, 1, 32, 7, 1024, 33, 2000, 968} {
			got := make([]int16, size)
			want := make([]int16, size)
			gotN, gotCount, gotErr := gotReader.GetBatchLevels(width, got, int16((1<<width)-1))
			wantN, wantCount, wantErr := getBatchLevelsBeforeNarrow(wantReader, width, want, int16((1<<width)-1))
			if gotN != wantN || gotCount != wantCount || gotErr != wantErr || !slices.Equal(got, want) {
				t.Fatalf("width=%d size=%d: got n=%d count=%d err=%v; want n=%d count=%d err=%v",
					width, size, gotN, gotCount, gotErr, wantN, wantCount, wantErr)
			}
		}
	}
}

func BenchmarkPackedLevelDecode(b *testing.B) {
	for _, width := range []uint{1, 2} {
		for _, pattern := range []string{"mostly-defined", "alternating", "varied"} {
			for _, size := range []int{1024, 65536} {
				_, packed := packedLevelFixture(width, size, pattern)
				maxLevel := int16((1 << width) - 1)
				for _, implementation := range []string{"before", "after"} {
					b.Run(fmt.Sprintf("width=%d/%s/n=%d/%s", width, pattern, size, implementation), func(b *testing.B) {
						reader := NewBitReader(bytes.NewReader(packed))
						output := make([]int16, size)
						b.ReportAllocs()
						b.SetBytes(int64(size * 2))
						for b.Loop() {
							reader.Reset(bytes.NewReader(packed))
							var n int
							var err error
							if implementation == "before" {
								n, _, err = getBatchLevelsBeforeNarrow(reader, width, output, maxLevel)
							} else {
								n, _, err = reader.GetBatchLevels(width, output, maxLevel)
							}
							if err != nil || n != size {
								b.Fatalf("decoded %d of %d, err=%v", n, size, err)
							}
						}
					})
				}
			}
		}
	}
}
