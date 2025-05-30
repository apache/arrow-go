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

package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
)

func main() {
	filePath := "../../../arrow-testing/data/csv/aggregate_test_100.csv" // Test csv file
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Schema defined in the csv file
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c3", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c4", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c5", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c6", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c7", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c9", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c10", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c11", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "c12", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "c13", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	reader := arrowcsv.NewReader(f, schema, arrowcsv.WithHeader(true), arrowcsv.WithChunk(-1))
	defer reader.Release()

	ok := reader.Next()
	if !ok {
		if err := reader.Err(); err != nil {
			fmt.Printf("Error reading CSV: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("No records found")
		os.Exit(0)
	}

	record := reader.Record()
	defer record.Release()

	fmt.Printf("Number of rows: %d\n", record.NumRows())
	fmt.Printf("Number of columns: %d\n", record.NumCols())

	fmt.Println("\nBasic statistics for numeric columns:")
	for i := 1; i < 10; i++ { // colss c2 through c10 are Int64
		col := record.Column(i).(*array.Int64)
		var sum int64
		for j := 0; j < col.Len(); j++ {
			sum += col.Value(j)
		}
		avg := float64(sum) / float64(col.Len())
		fmt.Printf("Column c%d: Average = %.2f\n", i+1, avg)
	}

	for i := 10; i < 12; i++ { // cols c11 and c12 are Float64
		col := record.Column(i).(*array.Float64)
		var sum float64
		for j := 0; j < col.Len(); j++ {
			sum += col.Value(j)
		}
		avg := sum / float64(col.Len())
		fmt.Printf("Column c%d: Average = %.4f\n", i+1, avg)
	}
}
