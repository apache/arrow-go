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
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	dataDir  = "parquet/examples/data"
	fileName = "test-file.parquet"

	// In a real project, this should be tuned based on memory usage and performance needs
	batchSize = 2
)

// List of fields to read
var colNames = []string{"intField", "stringField", "listField"}

func main() {
	filePath := filepath.Join(dataDir, fileName)

	// Open parquet file
	fileReader, err := file.OpenParquetFile(filePath, true)
	if err != nil {
		log.Fatalf("Failed to open parquet file: %v", err)
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
	schema, err := arrowReader.Schema()
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
}
