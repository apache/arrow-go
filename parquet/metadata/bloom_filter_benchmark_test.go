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

type testHarness struct {
	payload        []byte
	sourceFileSize int64
	metaList       []*RowGroupMetaData
	metaMask       int
}

func prepareTestHarness(b *testing.B) *testHarness {
	ctx := context.Background()

	node, _ := schema.NewPrimitiveNode("test_col", parquet.Repetition(format.FieldRepetitionType_REQUIRED), parquet.Type(format.Type_BYTE_ARRAY), -1, -1)
	rootGroup, _ := schema.NewGroupNode("schema", parquet.Repetition(format.FieldRepetitionType_REPEATED), schema.FieldList{node}, -1)
	sc := schema.NewSchema(rootGroup)

	const metaCount = 128
	metaList := make([]*RowGroupMetaData, metaCount)

	var payloadBuf bytes.Buffer
	payloadBuf.Write(make([]byte, 8))

	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < metaCount; i++ {
		bloomFilterDataSize := int32(1*1024*1024 + rnd.Intn(200*1024))

		header := format.BloomFilterHeader{
			NumBytes:    bloomFilterDataSize,
			Algorithm:   &defaultAlgorithm,
			Hash:        &defaultHashStrategy,
			Compression: &defaultCompression,
		}

		serializer := thrift.NewThriftSerializer()
		headerBytes, _ := serializer.Write(ctx, &header)

		currentOffset := int64(payloadBuf.Len())
		payloadBuf.Write(headerBytes)

		payloadBuf.Write(make([]byte, bloomFilterDataSize))

		bloomFilterReadSize := int32(len(headerBytes))

		columnMetaData := format.ColumnMetaData{
			Type:              format.Type_BYTE_ARRAY,
			PathInSchema:      []string{"test_col"},
			Codec:             format.CompressionCodec_UNCOMPRESSED,
			BloomFilterOffset: &currentOffset,
			BloomFilterLength: &bloomFilterReadSize,
		}
		thriftColumnChunk := format.ColumnChunk{MetaData: &columnMetaData}
		thriftRowGroup := format.RowGroup{Columns: []*format.ColumnChunk{&thriftColumnChunk}}
		metaList[i] = NewRowGroupMetaData(&thriftRowGroup, sc, nil, nil)
	}

	payload := payloadBuf.Bytes()
	return &testHarness{
		payload:        payload,
		sourceFileSize: int64(len(payload)),
		metaList:       metaList,
		metaMask:       metaCount - 1,
	}
}

func BenchmarkVisitColumnBloomFilter_SyncPool(b *testing.B) {
	h := prepareTestHarness(b)

	originalArrowPool := &sync.Pool{
		New: func() any {
			return memory.NewResizableBuffer(memory.NewGoAllocator())
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		threadRdr := &RowGroupBloomFilterReader{
			input:          &fakeReader{Reader: bytes.NewReader(h.payload)},
			sourceFileSize: h.sourceFileSize,
			bufferPool:     originalArrowPool,
		}

		seq := rand.Intn(h.metaMask)

		for pb.Next() {
			threadRdr.rgMeta = h.metaList[seq&h.metaMask]
			seq++

			err := threadRdr.VisitColumnBloomFilter(0, func(bf BloomFilter) error {
				if bf == nil || bf.Size() <= 0 {
					b.Fatal("bloom filter read path did not run or returned empty bitset")
				}
				_ = bf.Hasher()
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
