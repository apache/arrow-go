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

package metadata

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type fakeReader struct {
	*bytes.Reader
}

func (f *fakeReader) ReadAt(p []byte, off int64) (int, error) {
	return f.Reader.ReadAt(p, off)
}

func BenchmarkGetColumnBloomFilter_ReproduceProductionLeak(b *testing.B) {
	ctx := context.Background()

	const (
		workersCount        = 65
		iterationsPerWorker = 100
		maxSize             = 4 * 1024 * 1024
	)

	sharedPools := parquet.NewBloomFilterPools(512)

	sharedProps := parquet.NewReaderProperties(memory.NewGoAllocator())
	parquet.WithSharedBloomFilterPools(sharedPools)(sharedProps)

	node, _ := schema.NewPrimitiveNode("test_col", parquet.Repetition(format.FieldRepetitionType_REQUIRED), parquet.Type(format.Type_BYTE_ARRAY), -1, -1)
	rootGroup, _ := schema.NewGroupNode("schema", parquet.Repetition(format.FieldRepetitionType_REPEATED), schema.FieldList{node}, -1)
	sc := schema.NewSchema(rootGroup)

	payload := make([]byte, maxSize)

	b.ResetTimer()
	b.ReportAllocs()

	defer func() {
		close(sharedPools.FilterPool)
		for buf := range sharedPools.FilterPool {
			buf.Release()
		}
	}()

	for n := 0; n < b.N; n++ {
		var wg sync.WaitGroup
		startSignal := make(chan struct{})

		for w := 0; w < workersCount; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				<-startSignal

				r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

				threadRdr := &RowGroupBloomFilterReader{
					input:          &fakeReader{Reader: bytes.NewReader(payload)},
					sourceFileSize: int64(len(payload)),
					props:          sharedProps,
				}

				for i := 0; i < iterationsPerWorker; i++ {
					bloomFilterDataSize := int32(1*1024*1024 + r.Intn(200*1024))
					bloomFilterReadSize := bloomFilterDataSize + 4096

					header := format.BloomFilterHeader{
						NumBytes:    bloomFilterDataSize,
						Algorithm:   &defaultAlgorithm,
						Hash:        &defaultHashStrategy,
						Compression: &defaultCompression,
					}

					serializer := thrift.NewThriftSerializer()
					headerBytes, _ := serializer.Write(ctx, &header)

					var offset int64 = 8
					copy(payload[offset:], headerBytes)

					columnMetaData := format.ColumnMetaData{
						Type:              format.Type_BYTE_ARRAY,
						PathInSchema:      []string{"test_col"},
						Codec:             format.CompressionCodec_UNCOMPRESSED,
						BloomFilterOffset: &offset,
						BloomFilterLength: &bloomFilterReadSize,
					}
					thriftColumnChunk := format.ColumnChunk{MetaData: &columnMetaData}
					thriftRowGroup := format.RowGroup{Columns: []*format.ColumnChunk{&thriftColumnChunk}}
					meta := NewRowGroupMetaData(&thriftRowGroup, sc, nil, nil)

					threadRdr.rgMeta = meta

					bf, err := threadRdr.GetColumnBloomFilter(0)
					if err != nil {
						return
					}

					if closer, ok := bf.(interface{ Close() }); ok {
						closer.Close()
					}
				}
			}(w)
		}

		close(startSignal)
		wg.Wait()
	}
}
