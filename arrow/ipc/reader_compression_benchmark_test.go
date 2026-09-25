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
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func writeBenchmarkStream(b *testing.B, rec arrow.RecordBatch, numBatches int, opts ...Option) []byte {
	b.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf, append([]Option{WithSchema(rec.Schema())}, opts...)...)
	for range numBatches {
		if err := w.Write(rec); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func writeBenchmarkFile(b *testing.B, rec arrow.RecordBatch, numBatches int, opts ...Option) []byte {
	b.Helper()

	var buf bytes.Buffer
	w, err := NewFileWriter(&buf, append([]Option{WithSchema(rec.Schema())}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	for range numBatches {
		if err := w.Write(rec); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// BenchmarkReaderCompressed measures reading IPC data whose body buffers are
// compressed, for both the stream Reader and the FileReader.
func BenchmarkReaderCompressed(b *testing.B) {
	const numRows = 1024

	codecs := []struct {
		name string
		opt  Option
	}{
		{name: "none"},
		{name: "zstd", opt: WithZstd()},
		{name: "lz4", opt: WithLZ4()},
	}

	for _, codec := range codecs {
		for _, numColumns := range []int{1, 16} {
			for _, numBatches := range []int{1, 64} {
				b.Run(fmt.Sprintf("codec=%s/%dcols/%dbatches", codec.name, numColumns, numBatches), func(b *testing.B) {
					rec := benchmarkRecordBatch(numColumns, numRows)
					defer rec.Release()

					var opts []Option
					if codec.opt != nil {
						opts = append(opts, codec.opt)
					}

					rawBytes := int64(numColumns * numRows * arrow.Int32SizeBytes * numBatches)

					b.Run("stream", func(b *testing.B) {
						data := writeBenchmarkStream(b, rec, numBatches, opts...)

						b.ReportAllocs()
						b.SetBytes(rawBytes)
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							rdr, err := NewReader(bytes.NewReader(data))
							if err != nil {
								b.Fatal(err)
							}
							n := 0
							for rdr.Next() {
								n++
							}
							err = rdr.Err()
							rdr.Release()
							if err != nil {
								b.Fatal(err)
							}
							if n != numBatches {
								b.Fatalf("read %d batches, want %d", n, numBatches)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(len(data))/float64(rawBytes), "size/raw")
					})

					b.Run("file", func(b *testing.B) {
						data := writeBenchmarkFile(b, rec, numBatches, opts...)

						b.ReportAllocs()
						b.SetBytes(rawBytes)
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							rdr, err := NewFileReader(bytes.NewReader(data))
							if err != nil {
								b.Fatal(err)
							}
							n := 0
							for {
								_, err = rdr.Read()
								if err != nil {
									break
								}
								n++
							}
							closeErr := rdr.Close()
							if err != io.EOF {
								b.Fatal(err)
							}
							if closeErr != nil {
								b.Fatal(closeErr)
							}
							if n != numBatches {
								b.Fatalf("read %d batches, want %d", n, numBatches)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(len(data))/float64(rawBytes), "size/raw")
					})
				})
			}
		}
	}
}
