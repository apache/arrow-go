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

package arreflect_test

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func ExampleFromSlice() {
	mem := memory.NewGoAllocator()

	arr, err := arreflect.FromSlice([]int32{10, 20, 30}, mem)
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	fmt.Println("Type:", arr.DataType())
	fmt.Println("Len:", arr.Len())
	for i := 0; i < arr.Len(); i++ {
		fmt.Println(arr.(*array.Int32).Value(i))
	}
	// Output:
	// Type: int32
	// Len: 3
	// 10
	// 20
	// 30
}

func ExampleFromSlice_structSlice() {
	mem := memory.NewGoAllocator()

	type Row struct {
		Name  string  `arrow:"name"`
		Score float64 `arrow:"score"`
	}

	arr, err := arreflect.FromSlice([]Row{
		{"alice", 9.5},
		{"bob", 7.0},
	}, mem)
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	sa := arr.(*array.Struct)
	fmt.Println("Type:", sa.DataType())
	fmt.Println("Names:", sa.Field(0))
	fmt.Println("Scores:", sa.Field(1))
	// Output:
	// Type: struct<name: utf8, score: float64>
	// Names: ["alice" "bob"]
	// Scores: [9.5 7]
}

func ExampleFromSlice_withDecimal() {
	mem := memory.NewGoAllocator()

	vals := []decimal128.Num{
		decimal128.FromI64(12345),
		decimal128.FromI64(-67890),
	}
	arr, err := arreflect.FromSlice(vals, mem, arreflect.WithDecimal(10, 2))
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	fmt.Println("Type:", arr.DataType())
	fmt.Println("Len:", arr.Len())
	// Output:
	// Type: decimal(10, 2)
	// Len: 2
}

func ExampleToSlice() {
	mem := memory.NewGoAllocator()

	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(1.1)
	b.Append(2.2)
	b.Append(3.3)
	arr := b.NewArray()
	defer arr.Release()

	vals, err := arreflect.ToSlice[float64](arr)
	if err != nil {
		panic(err)
	}
	fmt.Println(vals)
	// Output:
	// [1.1 2.2 3.3]
}

type Measurement struct {
	Sensor string  `arrow:"sensor"`
	Value  float64 `arrow:"value"`
}

func ExampleRecordFromSlice() {
	mem := memory.NewGoAllocator()

	rows := []Measurement{
		{"temp-1", 23.5},
		{"temp-2", 19.8},
	}
	rec, err := arreflect.RecordFromSlice(rows, mem)
	if err != nil {
		panic(err)
	}
	defer rec.Release()

	fmt.Println("Schema:", rec.Schema())
	fmt.Println("Rows:", rec.NumRows())
	fmt.Println("Col 0:", rec.Column(0))
	fmt.Println("Col 1:", rec.Column(1))
	// Output:
	// Schema: schema:
	//   fields: 2
	//     - sensor: type=utf8
	//     - value: type=float64
	// Rows: 2
	// Col 0: ["temp-1" "temp-2"]
	// Col 1: [23.5 19.8]
}

func ExampleRecordToSlice() {
	mem := memory.NewGoAllocator()

	rows := []Measurement{
		{"temp-1", 23.5},
		{"temp-2", 19.8},
	}
	rec, err := arreflect.RecordFromSlice(rows, mem)
	if err != nil {
		panic(err)
	}
	defer rec.Release()

	got, err := arreflect.RecordToSlice[Measurement](rec)
	if err != nil {
		panic(err)
	}
	for _, m := range got {
		fmt.Printf("%s: %.1f\n", m.Sensor, m.Value)
	}
	// Output:
	// temp-1: 23.5
	// temp-2: 19.8
}

func ExampleAt() {
	mem := memory.NewGoAllocator()

	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append("alpha")
	b.Append("beta")
	b.Append("gamma")
	arr := b.NewArray()
	defer arr.Release()

	val, err := arreflect.At[string](arr, 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(val)
	// Output:
	// beta
}

func ExampleInferSchema() {
	type Event struct {
		ID      int64   `arrow:"id"`
		Name    string  `arrow:"name"`
		Score   float64 `arrow:"score"`
		Comment *string `arrow:"comment"`
	}

	schema, err := arreflect.InferSchema[Event]()
	if err != nil {
		panic(err)
	}
	fmt.Println(schema)
	// Output:
	// schema:
	//   fields: 4
	//     - id: type=int64
	//     - name: type=utf8
	//     - score: type=float64
	//     - comment: type=utf8, nullable
}

func ExampleFromSlice_withDict() {
	mem := memory.NewGoAllocator()

	arr, err := arreflect.FromSlice(
		[]string{"red", "green", "red", "blue", "green"},
		mem,
		arreflect.WithDict(),
	)
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	fmt.Println("Type:", arr.DataType())
	dict := arr.(*array.Dictionary)
	fmt.Println("Indices:", dict.Indices())
	fmt.Println("Dictionary:", dict.Dictionary())
	// Output:
	// Type: dictionary<values=utf8, indices=int32, ordered=false>
	// Indices: [0 1 0 2 1]
	// Dictionary: ["red" "green" "blue"]
}

func ExampleToAnySlice() {
	mem := memory.NewGoAllocator()

	st := arrow.StructOf(
		arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "pop", Type: arrow.PrimitiveTypes.Int64},
	)
	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()

	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("Tokyo")
	sb.FieldBuilder(1).(*array.Int64Builder).Append(14000000)

	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("Paris")
	sb.FieldBuilder(1).(*array.Int64Builder).Append(2200000)

	arr := sb.NewArray()
	defer arr.Release()

	rows, err := arreflect.ToAnySlice(arr)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		fmt.Println(row)
	}
	// Output:
	// {Tokyo 14000000}
	// {Paris 2200000}
}

func ExampleToAnySlice_nullableFields() {
	mem := memory.NewGoAllocator()

	st := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	)
	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()

	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("alice")
	sb.FieldBuilder(1).(*array.Float64Builder).Append(9.5)

	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("bob")
	sb.FieldBuilder(1).(*array.Float64Builder).AppendNull()

	arr := sb.NewArray()
	defer arr.Release()

	rows, err := arreflect.ToAnySlice(arr)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		v := reflect.ValueOf(row)
		var name string
		var scoreField reflect.Value
		for i := 0; i < v.NumField(); i++ {
			switch v.Type().Field(i).Tag.Get("arrow") {
			case "name":
				name = v.Field(i).String()
			case "score":
				scoreField = v.Field(i)
			}
		}
		if scoreField.IsNil() {
			fmt.Printf("%s: <null>\n", name)
		} else {
			fmt.Printf("%s: %.1f\n", name, scoreField.Elem().Float())
		}
	}
	// Output:
	// alice: 9.5
	// bob: <null>
}

func ExampleWithLarge() {
	mem := memory.NewGoAllocator()

	arr, err := arreflect.FromSlice([]string{"hello", "world"}, mem, arreflect.WithLarge())
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	fmt.Println("Type:", arr.DataType())
	fmt.Println("Len:", arr.Len())
	// Output:
	// Type: large_utf8
	// Len: 2
}

func ExampleFromSlice_largeStruct() {
	type Event struct {
		Name string `arrow:"name,large"`
		Code int32  `arrow:"code"`
	}

	schema, err := arreflect.InferSchema[Event]()
	if err != nil {
		panic(err)
	}
	fmt.Println("Schema:", schema)

	mem := memory.NewGoAllocator()
	arr, err := arreflect.FromSlice([]Event{{"click", 1}, {"view", 2}}, mem)
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	sa := arr.(*array.Struct)
	fmt.Println("Name type:", sa.Field(0).DataType())
	fmt.Println("Code type:", sa.Field(1).DataType())
	// Output:
	// Schema: schema:
	//   fields: 2
	//     - name: type=large_utf8
	//     - code: type=int32
	// Name type: large_utf8
	// Code type: int32
}

func ExampleWithView() {
	mem := memory.NewGoAllocator()

	arr, err := arreflect.FromSlice([]string{"hello", "world"}, mem, arreflect.WithView())
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	fmt.Println("Type:", arr.DataType())
	fmt.Println("Len:", arr.Len())
	// Output:
	// Type: string_view
	// Len: 2
}

func ExampleFromSlice_viewStruct() {
	type Event struct {
		Name string `arrow:"name,view"`
		Code int32  `arrow:"code"`
	}

	schema, err := arreflect.InferSchema[Event]()
	if err != nil {
		panic(err)
	}
	fmt.Println("Schema:", schema)
	// Output:
	// Schema: schema:
	//   fields: 2
	//     - name: type=string_view
	//     - code: type=int32
}
