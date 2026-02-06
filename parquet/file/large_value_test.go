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

package file_test

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLargeByteArrayValuesDoNotOverflowInt32 tests that writing large byte array
// values that would exceed the 1GB flush threshold does not cause an int32 overflow panic.
// The fix ensures pages are flushed automatically before buffer size exceeds safe limits.
func TestLargeByteArrayValuesDoNotOverflowInt32(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	// Create schema with a single byte array column
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("large_data", parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, -1)),
	}, -1)))

	props := parquet.NewWriterProperties(
		parquet.WithStats(false), // Disable stats to focus on core issue
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false), // Plain encoding
		parquet.WithDataPageSize(1024*1024),  // 1MB page size
	)

	out := &bytes.Buffer{}
	writer := file.NewParquetWriter(out, sc.Root(), file.WithWriterProps(props))
	defer writer.Close()

	rgw := writer.AppendRowGroup()
	colWriter, _ := rgw.NextColumn()

	// Create 700 values of 1.5MB each (1.05GB total)
	// This exceeds the 1GB flush threshold, triggering automatic page flushes
	// Uses minimal memory (single 1.5MB buffer reused) while testing loop logic thoroughly
	const valueSize = 1.5 * 1024 * 1024 // 1.5MB per value (>= 1MB threshold for large value handling)
	const numValues = 700               // 700 values = 1.05GB total

	// Create a single 1.5MB buffer and reuse it (only allocates 1.5MB!)
	largeValue := make([]byte, valueSize)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	values := make([]parquet.ByteArray, numValues)
	for i := range values {
		values[i] = largeValue // Reuse same buffer (memory efficient: 2MB total, writes 1.1GB)
	}

	// This should NOT panic with int32 overflow
	// Expected behavior: automatically flush pages at 1GB threshold
	byteArrayWriter := colWriter.(*file.ByteArrayColumnChunkWriter)
	_, err := byteArrayWriter.WriteBatch(values, nil, nil)

	// Should succeed without panic
	assert.NoError(t, err)

	err = colWriter.Close()
	assert.NoError(t, err)

	err = rgw.Close()
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	// Verify we wrote data successfully
	assert.Greater(t, out.Len(), 0, "should have written data to buffer")
}

// TestLargeStringArrayWithArrow tests the same issue using Arrow arrays
// This tests the pqarrow integration path which is commonly used.
// Uses LARGE_STRING type (int64 offsets) to handle >1GB of string data without overflow.
func TestLargeStringArrayWithArrow(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Skipping test on 32-bit architecture")
	}

	mem := memory.NewGoAllocator()

	// Create Arrow schema with LARGE_STRING field (uses int64 offsets, can handle >2GB)
	field := arrow.Field{Name: "large_strings", Type: arrow.BinaryTypes.LargeString, Nullable: true}
	arrowSchema := arrow.NewSchema([]arrow.Field{field}, nil)

	// Write to Parquet
	out := &bytes.Buffer{}
	props := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024),
	)

	pqw, err := pqarrow.NewFileWriter(arrowSchema, out, props, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)

	// Write in multiple batches to reduce memory usage
	// Each batch: 10 values × 10MB = 100MB
	// Total: 11 batches = 1.1GB written (only 100MB memory at a time!)
	const valueSize = 10 * 1024 * 1024 // 10MB per string (realistic large blob)
	const valuesPerBatch = 10           // 10 values per batch
	const numBatches = 11               // 11 batches = 1.1GB total

	largeStr := string(make([]byte, valueSize))

	for batchNum := 0; batchNum < numBatches; batchNum++ {
		// Build a small batch
		builder := array.NewLargeStringBuilder(mem)
		for i := 0; i < valuesPerBatch; i++ {
			builder.Append(largeStr)
		}
		arr := builder.NewArray()

		rec := array.NewRecord(arrowSchema, []arrow.Array{arr}, int64(valuesPerBatch))

		// Write batch - this should NOT panic with int32 overflow
		err = pqw.Write(rec)

		// Clean up batch resources
		rec.Release()
		arr.Release()
		builder.Release()

		assert.NoError(t, err)
	}

	err = pqw.Close()
	assert.NoError(t, err)

	// Verify we wrote data successfully
	assert.Greater(t, out.Len(), 0, "should have written data to buffer")
}
