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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package parquet_test

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// In a real project, this should be tuned based on memory usage and performance needs
const batchSize = 2

// List of fields to read
var colNames = []string{"intField", "stringField", "listField"}

func Example_writeReadParquet() {
	// --- Phase 1: Writing parquet file ---

	// Create an in-memory buffer to simulate a file
	// For writing real file to disk, use os.Create instead
	buffer := &bytes.Buffer{}

	// Create a schema with three fields
	fields := []arrow.Field{
		{Name: "intField", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "stringField", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "listField", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
	}

	schema := arrow.NewSchema(fields, nil)

	// Create parquet writer props with snappy compression
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)

	// Create arrow writer props to store the schema in the parquet file
	arrowWriterProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	// Create a parquet writer
	writer, err := pqarrow.NewFileWriter(schema, buffer, writerProps, arrowWriterProps)
	if err != nil {
		log.Fatalf("Failed create parquet writer: %v", err)
	}

	// Create a record builder
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	// Create a builder for each field
	intFieldIdx := schema.FieldIndices("intField")[0]
	stringFieldIdx := schema.FieldIndices("stringField")[0]
	listFieldIdx := schema.FieldIndices("listField")[0]

	intFieldBuilder := recordBuilder.Field(intFieldIdx).(*array.Int32Builder)
	stringFieldBuilder := recordBuilder.Field(stringFieldIdx).(*array.StringBuilder)
	listFieldBuilder := recordBuilder.Field(listFieldIdx).(*array.ListBuilder)

	// Get the builder for the list's values (Float32)
	fl32Builder := listFieldBuilder.ValueBuilder().(*array.Float32Builder)

	// Append values for each field
	intFieldBuilder.AppendValues([]int32{38, 13, 53, 93, 66}, nil)
	stringFieldBuilder.AppendValues([]string{"val1", "val2", "val3", "val4", "val5"}, nil)

	// Append five lists, each containing the same float32 values
	for i := 0; i < 5; i++ {
		listFieldBuilder.Append(true)
		fl32Builder.AppendValues([]float32{1.0, 2.0, 4.0, 8.0}, nil)
	}

	// Create a record
	record := recordBuilder.NewRecord()
	if err := writer.Write(record); err != nil {
		log.Fatalf("Failed to write record: %v", err)

	}

	record.Release()
	recordBuilder.Release()

	// IMPORTANT: Close the writer to finalize the file
	if err := writer.Close(); err != nil {
		log.Printf("Failed to close parquet writer: %v", err)
	}

	// --- Phase 2: Reading parquet file ---

	// Create a Parquet reader from the in-memory buffer
	// For reading real file from disk, use file.OpenParquetFile() instead
	fileReader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		log.Fatalf("Failed to create parquet reader: %v", err)
	}
	defer func() {
		if err := fileReader.Close(); err != nil {
			log.Printf("Failed to close file reader: %v", err)
		}
	}()

	// Create arrow read props, specifying the batch size
	arrowReadProps := pqarrow.ArrowReadProperties{BatchSize: batchSize}

	// Create an arrow reader for the parquet file
	arrowReader, err := pqarrow.NewFileReader(fileReader, arrowReadProps, memory.DefaultAllocator)
	if err != nil {
		log.Fatalf("Failed to create arrow reader: %v", err)
	}

	// Get the arrow schema from the file reader
	schema, err = arrowReader.Schema()
	if err != nil {
		log.Fatalf("Failed to get schema: %v", err)
	}

	// colIndices can be nil to read all columns. Here, we specify which columns to read
	colIndices := make([]int, len(colNames))
	for idx := range colNames {
		colIndices[idx] = schema.FieldIndices(colNames[idx])[0]
	}

	// Get the current record from the reader
	recordReader, err := arrowReader.GetRecordReader(context.TODO(), colIndices, nil)
	if err != nil {
		log.Fatalf("Failed to get record reader: %v", err)
	}
	defer recordReader.Release()

	for recordReader.Next() {
		// Create a record
		record := recordReader.Record()
		record.Retain()

		// Get columns
		intCol := record.Column(colIndices[0]).(*array.Int32)
		stringCol := record.Column(colIndices[1]).(*array.String)
		listCol := record.Column(colIndices[2]).(*array.List)
		listValueCol := listCol.ListValues().(*array.Float32)

		// Iterate over the rows within the current record
		for idx := range int(record.NumRows()) {
			// For the list column, get the start and end offsets for the current row
			start, end := listCol.ValueOffsets(idx)

			fmt.Printf("%d  %s  %v\n", intCol.Value(idx), stringCol.Value(idx), listValueCol.Float32Values()[start:end])
		}

		record.Release()
	}

	// Output:
	// 38  val1  [1 2 4 8]
	// 13  val2  [1 2 4 8]
	// 53  val3  [1 2 4 8]
	// 93  val4  [1 2 4 8]
	// 66  val5  [1 2 4 8]
}
