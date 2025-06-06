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

package array_test

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_typeConversion() {
	pool := memory.NewGoAllocator()

	// 1. Basic type conversion (Int32 to Int64)
	fmt.Println("Example 1: Converting Int32 to Int64")
	int32Builder := array.NewInt32Builder(pool)
	defer int32Builder.Release()

	int32Builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	int32Array := int32Builder.NewInt32Array()
	defer int32Array.Release()

	// Convert to Int64
	int64Builder := array.NewInt64Builder(pool)
	defer int64Builder.Release()

	for i := 0; i < int32Array.Len(); i++ {
		int64Builder.Append(int64(int32Array.Value(i)))
	}
	int64Array := int64Builder.NewInt64Array()
	defer int64Array.Release()

	fmt.Printf("Original Int32 values: %v\n", int32Array.Int32Values())
	fmt.Printf("Converted Int64 values: %v\n", int64Array.Int64Values())

	// 2. Handling nullable fields
	fmt.Println("\nExample 2: Working with nullable fields")
	float64Builder := array.NewFloat64Builder(pool)
	defer float64Builder.Release()

	values := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	valid := []bool{true, true, false, true, false}
	float64Builder.AppendValues(values, valid)
	float64Array := float64Builder.NewFloat64Array()
	defer float64Array.Release()

	stringBuilder := array.NewStringBuilder(pool)
	defer stringBuilder.Release()

	for i := 0; i < float64Array.Len(); i++ {
		if float64Array.IsNull(i) {
			stringBuilder.AppendNull()
		} else {
			stringBuilder.Append(fmt.Sprintf("%.2f", float64Array.Value(i)))
		}
	}
	stringArray := stringBuilder.NewStringArray()
	defer stringArray.Release()

	fmt.Println("Original Float64 values (with nulls):")
	for i := 0; i < float64Array.Len(); i++ {
		if float64Array.IsNull(i) {
			fmt.Printf("  [%d]: null\n", i)
		} else {
			fmt.Printf("  [%d]: %.2f\n", i, float64Array.Value(i))
		}
	}

	fmt.Println("\nConverted String values (with nulls):")
	for i := 0; i < stringArray.Len(); i++ {
		if stringArray.IsNull(i) {
			fmt.Printf("  [%d]: null\n", i)
		} else {
			fmt.Printf("  [%d]: %s\n", i, stringArray.Value(i))
		}
	}

	// 3. Working with nested types (List)
	fmt.Println("\nExample 3: Working with nested types (List)")
	listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer listBuilder.Release()

	valueBuilder := listBuilder.ValueBuilder().(*array.Int32Builder)

	listBuilder.Append(true)
	valueBuilder.AppendValues([]int32{1, 2}, nil)

	listBuilder.Append(true)
	valueBuilder.AppendValues([]int32{3, 4, 5}, nil)

	listBuilder.Append(true)
	valueBuilder.AppendValues([]int32{6}, nil)

	listArray := listBuilder.NewListArray()
	defer listArray.Release()

	// Convert list to string representation
	fmt.Println("List of lists:")
	for i := 0; i < listArray.Len(); i++ {
		values := listArray.ListValues().(*array.Int32).Int32Values()
		offset := listArray.Offsets()[i]
		length := listArray.Offsets()[i+1] - offset
		fmt.Printf("  List %d: %v\n", i, values[offset:offset+length])
	}

	// Output:
	// Example 1: Converting Int32 to Int64
	// Original Int32 values: [1 2 3 4 5]
	// Converted Int64 values: [1 2 3 4 5]
	//
	// Example 2: Working with nullable fields
	// Original Float64 values (with nulls):
	//   [0]: 1.10
	//   [1]: 2.20
	//   [2]: null
	//   [3]: 4.40
	//   [4]: null
	//
	// Converted String values (with nulls):
	//   [0]: 1.10
	//   [1]: 2.20
	//   [2]: null
	//   [3]: 4.40
	//   [4]: null
	//
	// Example 3: Working with nested types (List)
	// List of lists:
	//   List 0: [1 2]
	//   List 1: [3 4 5]
	//   List 2: [6]
}
