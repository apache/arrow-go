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

package ipc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/internal/dictutils"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type dictionaryTrackingAllocator struct {
	memory.Allocator
	current int64
}

func (a *dictionaryTrackingAllocator) Allocate(size int) []byte {
	out := a.Allocator.Allocate(size)
	a.current += int64(size)
	return out
}

func (a *dictionaryTrackingAllocator) Reallocate(size int, b []byte) []byte {
	out := a.Allocator.Reallocate(size, b)
	a.current += int64(size - len(b))
	return out
}

func (a *dictionaryTrackingAllocator) Free(b []byte) {
	a.current -= int64(len(b))
	a.Allocator.Free(b)
}

type dictionaryPayloadTrackingWriter struct {
	allocator *dictionaryTrackingAllocator
	peak      int64
	payloads  []int64
}

func (w *dictionaryPayloadTrackingWriter) Start() error { return nil }
func (w *dictionaryPayloadTrackingWriter) Close() error { return nil }

func (w *dictionaryPayloadTrackingWriter) WritePayload(p Payload) error {
	if p.msg != MessageDictionaryBatch {
		return nil
	}

	current := w.allocator.current
	w.payloads = append(w.payloads, current)
	if current > w.peak {
		w.peak = current
	}
	return nil
}

func newDictionaryRecordBatch(mem memory.Allocator, numColumns, numValues int) arrow.RecordBatch {
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.PrimitiveTypes.Int64,
	}
	fields := make([]arrow.Field, numColumns)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("dict_%d", i), Type: dictType}
	}

	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(mem, schema)
	for i := 0; i < numColumns; i++ {
		column := builder.Field(i).(*array.Int64DictionaryBuilder)
		column.Reserve(numValues)
		for j := 0; j < numValues; j++ {
			column.UnsafeAppend(int64(j))
		}
	}

	record := builder.NewRecordBatch()
	builder.Release()
	return record
}

func TestWriteDictionaryPayloadsReleasesPayloadPerDictionary(t *testing.T) {
	const (
		numColumns = 8
		numValues  = 4096
	)

	mem := &dictionaryTrackingAllocator{Allocator: memory.DefaultAllocator}
	record := newDictionaryRecordBatch(mem, numColumns, numValues)
	defer record.Release()

	mapper := &dictutils.Mapper{}
	mapper.ImportSchema(record.Schema())
	encoder := newRecordEncoder(mem, 0, kMaxNestingDepth, true, -1, 1, 0, nil)
	lastWrittenDicts := make(map[int64]arrow.Array, numColumns)
	defer func() {
		for _, dict := range lastWrittenDicts {
			dict.Release()
		}
	}()

	writer := &dictionaryPayloadTrackingWriter{allocator: mem}
	before := mem.current
	require.NoError(t, writeDictionaryPayloads(mem, record, false, false, mapper, lastWrittenDicts, writer, encoder))
	require.Len(t, writer.payloads, numColumns)

	payloadBytes := writer.payloads[0] - before
	require.Positive(t, payloadBytes)
	lastPayloadGrowth := writer.payloads[len(writer.payloads)-1] - writer.payloads[0]
	require.Less(t, lastPayloadGrowth, payloadBytes)
}

func TestWriteDictionaryPayloadsReleasesPayloadOnFailure(t *testing.T) {
	const numColumns = 3
	for _, delta := range []bool{false, true} {
		for failAfter := 1; failAfter <= numColumns; failAfter++ {
			t.Run(fmt.Sprintf("delta=%t/failAfter=%d", delta, failAfter), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				record := newDictionaryRecordBatch(mem, numColumns, 64)
				defer record.Release()

				mapper := &dictutils.Mapper{}
				mapper.ImportSchema(record.Schema())
				encoder := newRecordEncoder(mem, 0, kMaxNestingDepth, true, -1, 1, 0, nil)
				lastWrittenDicts := make(map[int64]arrow.Array)
				defer func() {
					for _, dict := range lastWrittenDicts {
						dict.Release()
					}
				}()

				if delta {
					previous := newDictionaryRecordBatch(mem, numColumns, 32)
					defer previous.Release()
					require.NoError(t, writeDictionaryPayloads(mem, previous, false, true,
						mapper, lastWrittenDicts, &failingPayloadWriter{}, encoder))
				}

				before := mem.CurrentAlloc()
				want := errors.New("dictionary payload write failed")
				writer := &failingPayloadWriter{err: want, failAfter: failAfter}
				err := writeDictionaryPayloads(mem, record, false, delta,
					mapper, lastWrittenDicts, writer, encoder)
				require.ErrorIs(t, err, want)
				require.Equal(t, failAfter, writer.payloads)
				require.Equal(t, before, mem.CurrentAlloc())
			})
		}
	}
}

func BenchmarkWriteDictionaryPayloadsPeak(b *testing.B) {
	for _, tc := range []struct {
		name       string
		numColumns int
		numValues  int
	}{
		{name: "8cols", numColumns: 8, numValues: 4096},
		{name: "64cols", numColumns: 64, numValues: 4096},
		{name: "256cols", numColumns: 256, numValues: 4096},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			mem := &dictionaryTrackingAllocator{Allocator: memory.DefaultAllocator}
			record := newDictionaryRecordBatch(mem, tc.numColumns, tc.numValues)
			defer record.Release()

			mapper := &dictutils.Mapper{}
			mapper.ImportSchema(record.Schema())
			encoder := newRecordEncoder(mem, 0, kMaxNestingDepth, true, -1, 1, 0, nil)
			base := mem.current
			var peakPayloadBytes int64

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writer := &dictionaryPayloadTrackingWriter{allocator: mem}
				lastWrittenDicts := make(map[int64]arrow.Array, tc.numColumns)
				if err := writeDictionaryPayloads(mem, record, false, false, mapper, lastWrittenDicts, writer, encoder); err != nil {
					b.Fatal(err)
				}
				if payloadBytes := writer.peak - base; payloadBytes > peakPayloadBytes {
					peakPayloadBytes = payloadBytes
				}
				for _, dict := range lastWrittenDicts {
					dict.Release()
				}
				if mem.current != base {
					b.Fatalf("allocator bytes changed from %d to %d", base, mem.current)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(peakPayloadBytes), "peak-payload-B")
		})
	}
}

var _ memory.Allocator = (*dictionaryTrackingAllocator)(nil)
