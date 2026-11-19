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

package pqarrow_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func BenchmarkRecordReaderRowRanges(b *testing.B) {
	const (
		groupRows = 64 * 1024
		groups    = 4
		rows      = groupRows * groups
	)
	mem := memory.DefaultAllocator
	tbl := makeRowRangeBenchmarkTable(mem, rows)
	defer tbl.Release()

	for _, indexed := range []bool{true, false} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			var buf bytes.Buffer
			props := parquet.NewWriterProperties(
				parquet.WithPageIndexEnabled(indexed), parquet.WithCompression(compress.Codecs.Snappy),
				parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(128*1024),
			)
			if err := pqarrow.WriteTable(tbl, &buf, groupRows, props, pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}

			for _, percent := range []int64{1, 10, 100} {
				b.Run(fmt.Sprintf("selected=%d%%", percent), func(b *testing.B) {
					var ranges []pqarrow.RowRange
					if percent == 100 {
						ranges = []pqarrow.RowRange{{Start: 0, End: rows}}
					} else {
						for group := range int64(groups) {
							start := group*groupRows + groupRows/2
							ranges = append(ranges, pqarrow.RowRange{Start: start, End: start + groupRows*percent/100})
						}
					}
					var want rowRangeBenchmarkResult
					for _, selected := range ranges {
						for id := selected.Start; id < selected.End; id++ {
							want.rows++
							want.checksum += id + int64(byte(id)) + int64(byte(id>>8))
						}
					}

					for _, method := range []string{"decode-then-select", "seek-then-slice", "row-ranges"} {
						b.Run("method="+method, func(b *testing.B) {
							b.ReportAllocs()
							var sourceBytes int64
							for b.Loop() {
								source := &rowRangeBenchmarkSource{Reader: bytes.NewReader(buf.Bytes())}
								got := readRowRangeBenchmark(b, source, mem, ranges, method)
								if got != want {
									b.Fatalf("read %+v, want %+v", got, want)
								}
								sourceBytes += source.bytesRead
							}
							b.ReportMetric(float64(sourceBytes)/float64(b.N), "read-B/op")
						})
					}
				})
			}
		})
	}
}

type rowRangeBenchmarkSource struct {
	*bytes.Reader
	bytesRead int64
}

func (r *rowRangeBenchmarkSource) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

func (r *rowRangeBenchmarkSource) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.Reader.ReadAt(p, offset)
	r.bytesRead += int64(n)
	return n, err
}

type rowRangeBenchmarkResult struct {
	rows, checksum int64
}

func readRowRangeBenchmark(b *testing.B, source *rowRangeBenchmarkSource, mem memory.Allocator,
	ranges []pqarrow.RowRange, method string) rowRangeBenchmarkResult {
	props := parquet.NewReaderProperties(mem)
	props.BufferedStreamEnabled = true
	pf, err := file.NewParquetReader(source, file.WithReadProps(props))
	if err != nil {
		b.Fatal(err)
	}
	defer pf.Close()
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 4096}, mem)
	if err != nil {
		b.Fatal(err)
	}
	var records pqarrow.RecordReader
	if method == "row-ranges" {
		records, err = reader.GetRecordReaderWithRowRanges(b.Context(), nil, nil, ranges)
	} else {
		records, err = reader.GetRecordReader(b.Context(), nil, nil)
	}
	if err != nil {
		b.Fatal(err)
	}
	defer records.Release()

	var result rowRangeBenchmarkResult
	if method == "seek-then-slice" {
		for _, selected := range ranges {
			if err := records.SeekToRow(selected.Start); err != nil {
				b.Fatal(err)
			}
			for remaining := selected.End - selected.Start; remaining > 0; {
				if !records.Next() {
					b.Fatalf("premature end of range: %v", records.Err())
				}
				batch := records.RecordBatch()
				length := min(remaining, batch.NumRows())
				result.append(batch, 0, length)
				remaining -= length
			}
		}
		return result
	}

	var batchStart int64
	var index int
	for records.Next() {
		batch := records.RecordBatch()
		if method == "row-ranges" {
			result.append(batch, 0, batch.NumRows())
			continue
		}
		batchEnd := batchStart + batch.NumRows()
		for index < len(ranges) && ranges[index].Start < batchEnd {
			selected := ranges[index]
			result.append(batch, max(batchStart, selected.Start)-batchStart, min(batchEnd, selected.End)-batchStart)
			if selected.End > batchEnd {
				break
			}
			index++
		}
		batchStart = batchEnd
	}
	if err := records.Err(); err != nil {
		b.Fatal(err)
	}
	return result
}

func (r *rowRangeBenchmarkResult) append(batch arrow.RecordBatch, start, end int64) {
	ids := batch.Column(0).(*array.Int64)
	payload := batch.Column(1).(*array.FixedSizeBinary)
	for i := start; i < end; i++ {
		value := payload.Value(int(i))
		r.checksum += ids.Value(int(i)) + int64(value[0]) + int64(value[len(value)-1])
	}
	r.rows += end - start
}

func makeRowRangeBenchmarkTable(mem memory.Allocator, rows int) arrow.Table {
	const payloadSize = 128
	payloadType := &arrow.FixedSizeBinaryType{ByteWidth: payloadSize}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "payload", Type: payloadType},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Reserve(rows)
	ids := builder.Field(0).(*array.Int64Builder)
	payload := builder.Field(1).(*array.FixedSizeBinaryBuilder)
	random := rand.NewChaCha8([32]byte{1})
	var value [payloadSize]byte
	for row := range rows {
		ids.Append(int64(row))
		random.Read(value[:])
		value[0] = byte(row)
		value[len(value)-1] = byte(row >> 8)
		payload.Append(value[:])
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	return array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
}
