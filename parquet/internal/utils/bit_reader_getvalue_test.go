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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
)

func packedScalarValues(t testing.TB, width, count int) ([]byte, []uint64) {
	t.Helper()
	values := make([]uint64, count)
	packed := make([]byte, (width*count+7)/8)
	writer := utils.NewBitWriter(utils.NewWriterAtBuffer(packed))
	for i := range values {
		v := uint64(i)*0x9e3779b97f4a7c15 + uint64(i/3)
		if width < 64 {
			v &= (uint64(1) << uint(width)) - 1
		}
		values[i] = v
		if err := writer.WriteValue(v, uint(width)); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush(true)
	return packed, values
}

func TestBitReaderGetValueScalarAndBatch(t *testing.T) {
	const count = 129
	for width := 1; width <= 64; width++ {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			packed, want := packedScalarValues(t, width, count)
			reader := utils.NewBitReader(bytes.NewReader(packed))
			for i := 0; i < 17; i++ {
				value, ok := reader.GetValue(width)
				if !ok || value != want[i] {
					t.Fatalf("scalar %d: got %d, ok=%t; want %d", i, value, ok, want[i])
				}
			}

			batch := make([]uint64, 32)
			n, err := reader.GetBatch(uint(width), batch)
			if err != nil || n != len(batch) {
				t.Fatalf("batch: got %d values, err=%v", n, err)
			}
			for i, value := range batch {
				if value != want[17+i] {
					t.Fatalf("batch %d: got %d, want %d", i, value, want[17+i])
				}
			}
			for i := 49; i < count; i++ {
				value, ok := reader.GetValue(width)
				if !ok || value != want[i] {
					t.Fatalf("scalar %d: got %d, ok=%t; want %d", i, value, ok, want[i])
				}
			}

			reader.Reset(bytes.NewReader(packed))
			value, ok := reader.GetValue(width)
			if !ok || value != want[0] {
				t.Fatalf("after Reset: got %d, ok=%t; want %d", value, ok, want[0])
			}
		})
	}
}

func TestBitReaderGetValueBoundsAndTruncation(t *testing.T) {
	reader := utils.NewBitReader(bytes.NewReader([]byte{0xa5}))
	for _, width := range []int{-1, 65, 128} {
		if _, ok := reader.GetValue(width); ok {
			t.Fatalf("unexpected success for invalid width %d", width)
		}
	}
	if value, ok := reader.GetValue(0); !ok || value != 0 {
		t.Fatalf("zero width: got %d, ok=%t", value, ok)
	}
	if value, ok := reader.GetValue(8); !ok || value != 0xa5 {
		t.Fatalf("after invalid widths: got %d, ok=%t", value, ok)
	}
	if _, ok := reader.GetValue(8); ok {
		t.Fatal("read beyond the available byte")
	}

	for _, width := range []int{1, 33, 64} {
		if _, ok := utils.NewBitReader(bytes.NewReader(nil)).GetValue(width); ok {
			t.Fatalf("empty buffer accepted width %d", width)
		}
	}

	for _, width := range []int{9, 33, 63} {
		packed, want := packedScalarValues(t, width, 1)
		reader := utils.NewBitReader(bytes.NewReader(packed))
		if value, ok := reader.GetValue(width); !ok || value != want[0] {
			t.Fatalf("first value width %d: got %d, ok=%t; want %d", width, value, ok, want[0])
		}
		if _, ok := reader.GetValue(width); ok {
			t.Fatalf("truncated second value accepted for width %d", width)
		}
	}
}

var bitReaderGetValueSink uint64

func BenchmarkBitReaderGetValue(b *testing.B) {
	const valuesPerBuffer = 8192
	for _, width := range []int{1, 3, 8, 16, 32, 33, 64} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			packed, _ := packedScalarValues(b, width, valuesPerBuffer)
			source := bytes.NewReader(packed)
			reader := utils.NewBitReader(source)
			b.ReportAllocs()
			b.ResetTimer()

			var checksum uint64
			for i := 0; b.Loop(); i++ {
				if i%valuesPerBuffer == 0 {
					source.Reset(packed)
					reader.Reset(source)
				}
				value, ok := reader.GetValue(width)
				if !ok {
					b.Fatalf("failed to read value %d at width %d", i, width)
				}
				checksum += value
			}
			bitReaderGetValueSink = checksum
		})
	}
}

func TestBitReaderGetValueAfterTruncatedBatch(t *testing.T) {
	t.Run("boolean batch at EOF", func(t *testing.T) {
		reader := utils.NewBitReader(bytes.NewReader([]byte{0, 0, 0, 0, 0xff}))
		batch := make([]uint64, 32)
		if n, err := reader.GetBatch(1, batch); n != 32 || err != nil {
			t.Fatalf("initial batch: n=%d err=%v", n, err)
		}
		if n, err := reader.GetBatchBools(make([]bool, 16)); n != 8 || err == nil {
			t.Fatalf("truncated boolean batch: n=%d err=%v", n, err)
		}
		if value, ok := reader.GetValue(1); ok {
			t.Fatalf("unexpected scalar %d after EOF", value)
		}
	})
	t.Run("bitmap batch with a partial group", func(t *testing.T) {
		reader := utils.NewBitReader(bytes.NewReader([]byte{0, 0, 0, 0, 1, 2, 3, 4, 0x5a}))
		batch := make([]uint64, 32)
		if n, err := reader.GetBatch(1, batch); n != 32 || err != nil {
			t.Fatalf("initial batch: n=%d err=%v", n, err)
		}
		if n, err := reader.GetBatchBitmap(make([]byte, 8), 0, 64); n != 32 || err == nil {
			t.Fatalf("truncated bitmap batch: n=%d err=%v", n, err)
		}
		if value, ok := reader.GetValue(8); !ok || value != 0x5a {
			t.Fatalf("scalar after partial group: value=%d ok=%t", value, ok)
		}
	})
}
