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
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
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

	// 4. Type validation and error handling
	fmt.Println("\nExample 4: Type validation and error handling")

	safeConvert := func(arr arrow.Array, targetType arrow.DataType) (arrow.Array, error) {
		switch targetType.ID() {
		case arrow.INT64:
			if arr.DataType().ID() != arrow.INT32 {
				return nil, fmt.Errorf("cannot convert %s to INT64", arr.DataType())
			}
			int32Arr := arr.(*array.Int32)
			builder := array.NewInt64Builder(pool)
			defer builder.Release()

			for i := 0; i < int32Arr.Len(); i++ {
				if int32Arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(int64(int32Arr.Value(i)))
				}
			}
			return builder.NewInt64Array(), nil

		case arrow.STRING:
			if arr.DataType().ID() != arrow.FLOAT64 {
				return nil, fmt.Errorf("cannot convert %s to STRING", arr.DataType())
			}
			floatArr := arr.(*array.Float64)
			builder := array.NewStringBuilder(pool)
			defer builder.Release()

			for i := 0; i < floatArr.Len(); i++ {
				if floatArr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(fmt.Sprintf("%.2f", floatArr.Value(i)))
				}
			}
			return builder.NewStringArray(), nil

		default:
			return nil, fmt.Errorf("unsupported target type: %s", targetType)
		}
	}

	fmt.Println("Testing safe type conversion:")

	// Convert Int32 to Int64
	result, err := safeConvert(int32Array, arrow.PrimitiveTypes.Int64)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
	fmt.Printf("  Int32 to Int64 conversion successful: %v\n", result.(*array.Int64).Int64Values())

	// Invalid conversion handling
	_, err = safeConvert(int32Array, arrow.BinaryTypes.String)
	if err != nil {
		fmt.Printf("  Expected error for invalid conversion: %v\n", err)
	}
}
