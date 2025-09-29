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

package arrow_test

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/math"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_tableCreation() {
	// Create a schema with three fields
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "intField", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "stringField", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "floatField", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	// Create a record builder
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	// Append values to each field
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 0, 3, 0, 5}, []bool{true, false, true, false, true})

	// Create a record
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create a table from the record
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	// Calculate sum of floatField
	sum := math.Float64.Sum(tbl.Column(2).Data().Chunk(0).(*array.Float64))
	fmt.Printf("Sum of floatField: %v\n", sum)

	// Print the table contents
	fmt.Println("\nTable contents:")
	fmt.Printf("Number of rows: %d\n", tbl.NumRows())
	fmt.Printf("Number of columns: %d\n", tbl.NumCols())
	fmt.Println("\nColumn names:")
	for i := 0; i < int(tbl.NumCols()); i++ {
		fmt.Printf("  %s\n", tbl.Column(i).Name())
	}

	// Output:
	// Sum of floatField: 9
	//
	// Table contents:
	// Number of rows: 5
	// Number of columns: 3
	//
	// Column names:
	//   intField
	//   stringField
	//   floatField
}
