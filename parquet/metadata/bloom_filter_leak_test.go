package metadata

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

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

func TestGetColumnBloomFilterFixedChannelsReuse(t *testing.T) {
	ctx := context.Background()
	var bloomFilterDataSize int32 = 1 * 1024 * 1024

	header := format.BloomFilterHeader{
		NumBytes:    bloomFilterDataSize,
		Algorithm:   &defaultAlgorithm,
		Hash:        &defaultHashStrategy,
		Compression: &defaultCompression,
	}

	serializer := thrift.NewThriftSerializer()
	headerBytes, err := serializer.Write(ctx, &header)
	if err != nil {
		t.Fatalf("failed to serialize thrift header: %v", err)
	}

	fileBuffer := bytes.NewBuffer(nil)
	fileBuffer.Write(headerBytes)
	fileBuffer.Write(make([]byte, bloomFilterDataSize))
	fileData := fileBuffer.Bytes()
	totalLength := int32(len(fileData))

	var offset int64 = 0
	columnMetaData := format.ColumnMetaData{
		Type:                  format.Type_BYTE_ARRAY,
		Encodings:             []format.Encoding{format.Encoding_PLAIN},
		PathInSchema:          []string{"test_col"},
		Codec:                 format.CompressionCodec_UNCOMPRESSED,
		NumValues:             100,
		TotalUncompressedSize: 100,
		TotalCompressedSize:   100,
		DataPageOffset:        0,
		BloomFilterOffset:     &offset,
		BloomFilterLength:     &totalLength,
	}

	thriftColumnChunk := format.ColumnChunk{MetaData: &columnMetaData}
	thriftRowGroup := format.RowGroup{
		Columns:       []*format.ColumnChunk{&thriftColumnChunk},
		TotalByteSize: 100,
		NumRows:       100,
	}

	node, err := schema.NewPrimitiveNode("test_col", parquet.Repetition(format.FieldRepetitionType_REQUIRED),
		parquet.Type(format.Type_BYTE_ARRAY), -1, -1)
	if err != nil {
		t.Fatalf("failed to create primitive node: %v", err)
	}

	rootGroup, err := schema.NewGroupNode("schema", parquet.Repetition(format.FieldRepetitionType_REPEATED),
		schema.FieldList{node}, -1)
	if err != nil {
		t.Fatalf("failed to create root group node: %v", err)
	}

	sc := schema.NewSchema(rootGroup)

	realRgMeta := NewRowGroupMetaData(&thriftRowGroup, sc, nil, nil)

	rdr := &RowGroupBloomFilterReader{
		input:          &fakeReader{Reader: bytes.NewReader(fileData)},
		sourceFileSize: int64(len(fileData)),
		rgMeta:         realRgMeta,
	}

	for len(headerPool) > 0 {
		<-headerPool
	}
	for len(filterPool) > 0 {
		<-filterPool
	}

	runtime.GC()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	const (
		goroutines          = 65
		iterationsPerWorker = 100
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startSignal

			for range iterationsPerWorker {
				bf, err := rdr.GetColumnBloomFilter(0)
				if err != nil {
					continue
				}

				if bf != nil {
					bf.Close()
				}

				if rand.Float32() > 0.5 {
					select {
					case tmp := <-headerPool:
						select {
						case headerPool <- tmp:
						default:
						}
					default:
					}
				}
			}
		}()
	}

	startTime := time.Now()
	close(startSignal)
	wg.Wait()
	t.Logf("Fixed channel stress test processed 6500 operations in %v", time.Since(startTime))

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	totalAllocatedMB := float64(memEnd.TotalAlloc-memStart.TotalAlloc) / 1024 / 1024
	heapInuseMB := float64(memEnd.HeapInuse-memStart.HeapInuse) / 1024 / 1024

	fmt.Printf("Cumulative Allocated Memory: %.2f MB\n", totalAllocatedMB)
	fmt.Printf("Active Heap In Use Memory:   %.2f MB\n", heapInuseMB)

	fmt.Printf("Total OS Mallocs Count:      %d\n\n", memEnd.Mallocs-memStart.Mallocs)

	const MaxAllowedAllocMB = 500.0
	if totalAllocatedMB > MaxAllowedAllocMB {
		t.Fatalf("FAIL: Channel reuse failed! Allocated %.2f MB, memory leaks present.", totalAllocatedMB)
	}
}
