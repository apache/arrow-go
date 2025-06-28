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

package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	dataDir  = "parquet/examples/data"
	fileName = "test-file.parquet"
)

func main() {
	filePath := filepath.Join(dataDir, fileName)

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

	// We don't need to close the file manually. The pqarrow.FileWriter takes ownership
	// of the file writer and will close it when its own Close method is called
	// Calling file.Close() here would lead to a "double close" error
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}

	// Create a parquet writer
	writer, err := pqarrow.NewFileWriter(schema, file, writerProps, arrowWriterProps)
	if err != nil {
		log.Fatalf("Failed create parquet writer: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close parquet writer: %v", err)
		}
	}()

	// Create a record builder
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer recordBuilder.Release()

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
}
