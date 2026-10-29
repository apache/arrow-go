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
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// writeValuesBefore is the previous implementation, retained for paired
// benchmarks of the baseline and the actual WriteValues method.
func writeValuesBefore(b *BitWriter, values []uint64, nbits uint) error {
	buffer := b.buffer
	bitoffset := b.bitoffset
	byteoffset := b.byteoffset
	for _, v := range values {
		buffer |= v << bitoffset
		bitoffset += nbits
		if bitoffset >= 64 {
			binary.LittleEndian.PutUint64(b.raw[:], buffer)
			if _, err := b.wr.WriteAt(b.raw[:], int64(byteoffset)); err != nil {
				b.buffer = buffer
				b.bitoffset = bitoffset
				b.byteoffset = byteoffset
				return err
			}
			bitoffset -= 64
			buffer = v >> (nbits - bitoffset)
			byteoffset += 8
		}
	}
	b.buffer = buffer
	b.bitoffset = bitoffset
	b.byteoffset = byteoffset
	return nil
}

func TestBitWriterWriteValuesDirectBufferParity(t *testing.T) {
	for width := uint(0); width <= 64; width++ {
		mask := uint64(math.MaxUint64)
		if width < 64 {
			mask = (uint64(1) << width) - 1
		}
		for _, size := range []int{0, 1, 7, 8, 15, 16, 17, 63, 128, 1024} {
			values := make([]uint64, size)
			for i := range values {
				values[i] = (uint64(i)*0x9e3779b97f4a7c15 + uint64(i/3)) & mask
			}
			for _, offset := range []uint{0, 1, 3, 7, 31, 63} {
				nbytes := (int(offset)+size*int(width)+7)/8 + 16
				oldBuf := make([]byte, nbytes)
				newBuf := make([]byte, nbytes)
				oldWriter := NewBitWriter(NewWriterAtBuffer(oldBuf))
				newWriter := NewBitWriter(NewWriterAtBuffer(newBuf))
				if offset != 0 {
					if err := oldWriter.WriteValue(5, offset); err != nil {
						t.Fatal(err)
					}
					if err := newWriter.WriteValue(5, offset); err != nil {
						t.Fatal(err)
					}
				}
				oldErr := writeValuesBefore(oldWriter, values, width)
				newErr := newWriter.WriteValues(values, width)
				if (oldErr == nil) != (newErr == nil) ||
					oldWriter.byteoffset != newWriter.byteoffset ||
					oldWriter.bitoffset != newWriter.bitoffset ||
					oldWriter.buffer != newWriter.buffer {
					t.Fatalf("width=%d size=%d offset=%d: old=%v new=%v", width, size, offset, oldErr, newErr)
				}
				oldWriter.Flush(false)
				newWriter.Flush(false)
				if !bytes.Equal(oldBuf, newBuf) || oldWriter.Written() != newWriter.Written() {
					t.Fatalf("encoded bytes differ: width=%d size=%d offset=%d", width, size, offset)
				}
			}
		}
	}
}

func TestBitWriterWriteValuesDirectBufferShortWrites(t *testing.T) {
	for _, width := range []uint{4, 7, 8, 12, 16, 32, 64} {
		for _, n := range []int{8, 16, 64, 127} {
			values := make([]uint64, n)
			for i := range values {
				values[i] = uint64(i * 17)
			}
			for _, capacity := range []int{0, 1, 4, 7, 8, 9, 16, 64, 128} {
				for _, offset := range []uint{0, 3, 63} {
					oldBuf := make([]byte, capacity)
					newBuf := make([]byte, capacity)
					oldWriter := NewBitWriter(NewWriterAtBuffer(oldBuf))
					newWriter := NewBitWriter(NewWriterAtBuffer(newBuf))
					oldWriter.buffer = 5
					newWriter.buffer = 5
					oldWriter.bitoffset = offset
					newWriter.bitoffset = offset
					oldErr := writeValuesBefore(oldWriter, values, width)
					newErr := newWriter.WriteValues(values, width)
					if fmt.Sprint(oldErr) != fmt.Sprint(newErr) ||
						oldWriter.byteoffset != newWriter.byteoffset ||
						oldWriter.bitoffset != newWriter.bitoffset ||
						oldWriter.buffer != newWriter.buffer ||
						!bytes.Equal(oldBuf, newBuf) {
						t.Fatalf("short write changed: width=%d n=%d capacity=%d offset=%d: %v / %v",
							width, n, capacity, offset, oldErr, newErr)
					}
				}
			}
		}
	}
}

var benchmarkBitWriterWritten int

func BenchmarkBitWriterWriteValues(b *testing.B) {
	for _, width := range []uint{4, 7, 8, 12, 16, 32, 64} {
		for _, count := range []int{1024, 65536} {
			values := make([]uint64, count)
			mask := uint64(math.MaxUint64)
			if width < 64 {
				mask = (uint64(1) << width) - 1
			}
			for i := range values {
				values[i] = (uint64(i) * 0x9e3779b97f4a7c15) & mask
			}
			for _, offset := range []uint{0, 3} {
				raw := make([]byte, (int(offset)+count*int(width)+7)/8+16)
				for _, mode := range []struct {
					name   string
					encode func(*BitWriter, []uint64, uint) error
				}{
					{"before", writeValuesBefore},
					{"after", (*BitWriter).WriteValues},
				} {
					b.Run(fmt.Sprintf("width=%d/values=%d/offset=%d/%s", width, count, offset, mode.name), func(b *testing.B) {
						b.SetBytes(int64((count*int(width) + 7) / 8))
						b.ReportAllocs()
						for b.Loop() {
							writer := NewBitWriter(NewWriterAtBuffer(raw))
							if offset > 0 {
								writer.bitoffset = offset
								writer.buffer = 5
							}
							if err := mode.encode(writer, values, width); err != nil {
								b.Fatal(err)
							}
							benchmarkBitWriterWritten = writer.Written()
						}
					})
				}
			}
		}
	}
}
