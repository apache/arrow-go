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

package arrgen_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"time"

	"github.com/apache/arrow-go/arrgen"
	"github.com/apache/arrow-go/arrgen/internal/gentypes"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The examples below are driven by internal/gentypes.Metric, this module's
// checked-in fixture:
//
//	type Metric struct {
//		Day    time.Time `arrow:"day,date32"`
//		Host   string    `arrow:"host"`
//		CPU    float64   `arrow:"cpu"`
//		Value  *float64  `arrow:"value"` // nullable: a nil pointer appends null
//		Secret string    `arrow:"-"`     // never leaves the process
//	}
//
// Running arrgen over it emits MetricSchema, NewMetricAppender and
// MetricRecordBatch. In your own package the names are derived from your own
// struct the same way.

// ExampleGenerate runs the generator the way the arrgen command does and lists
// the API it emitted. Generate returns formatted source and writes nothing, so
// a caller can diff or inspect the result before it lands on disk.
func ExampleGenerate() {
	src, err := arrgen.Generate(arrgen.Config{
		Dir:   "testdata/basic",
		Types: []string{"Metric"},
	})
	if err != nil {
		fmt.Println("generate:", err)
		return
	}

	file, err := parser.ParseFile(token.NewFileSet(), "metric_arrow.go", src, 0)
	if err != nil {
		fmt.Println("parse:", err)
		return
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				fmt.Println("func", d.Name.Name)
			}
		case *ast.GenDecl:
			if d.Tok == token.TYPE {
				for _, spec := range d.Specs {
					fmt.Println("type", spec.(*ast.TypeSpec).Name.Name)
				}
			}
		}
	}
	// Output:
	// func MetricSchema
	// type MetricAppender
	// func NewMetricAppender
	// func MetricRecordBatch
}

// Example encodes a slice of rows in one call, replacing
// arreflect.RecordFromSlice[Metric](metrics, mem).
func Example() {
	cpuLoad := 0.75
	day := time.Date(2024, time.March, 17, 0, 0, 0, 0, time.UTC)
	metrics := []gentypes.Metric{
		{Day: day, Host: "web-1", CPU: 0.5, Value: &cpuLoad},
		{Day: day.AddDate(0, 0, 1), Host: "web-2", CPU: 1.5}, // Value is nil: a null
	}

	rec, err := gentypes.MetricRecordBatch(memory.DefaultAllocator, metrics)
	if err != nil {
		fmt.Println("encode:", err)
		return
	}
	defer rec.Release()

	fmt.Println("rows:", rec.NumRows())
	for i, col := range rec.Columns() {
		fmt.Printf("%s: %v\n", rec.Schema().Field(i).Name, col)
	}
	// Output:
	// rows: 2
	// day: [19799 19800]
	// host: ["web-1" "web-2"]
	// cpu: [0.5 1.5]
	// value: [0.75 (null)]
}

// Example_streamingAppend streams rows in one at a time and cuts a batch at a
// size boundary. The appender never retains a row, so the loop reuses a single
// variable and allocates nothing.
func Example_streamingAppend() {
	const batchSize = 2
	day := time.Date(2024, time.March, 17, 0, 0, 0, 0, time.UTC)

	a := gentypes.NewMetricAppender(memory.DefaultAllocator)
	defer a.Release()
	a.Reserve(batchSize)

	var row gentypes.Metric
	for i := 0; i < 5; i++ {
		row = gentypes.Metric{
			Day:  day.AddDate(0, 0, i),
			Host: fmt.Sprintf("web-%d", i),
			CPU:  float64(i),
		}
		a.Append(&row)

		if a.Len() == batchSize {
			rec := a.NewRecordBatch()
			fmt.Println("batch of", rec.NumRows())
			rec.Release()
			a.Reserve(batchSize)
		}
	}
	if a.Len() > 0 {
		rec := a.NewRecordBatch()
		fmt.Println("final batch of", rec.NumRows())
		rec.Release()
	}
	if err := a.Err(); err != nil {
		fmt.Println("append:", err)
	}
	// Output:
	// batch of 2
	// batch of 2
	// final batch of 1
}

// Example_schema shows the schema the generator resolved from the tags. It is
// a package-level value in the generated file, so reading it costs nothing.
func Example_schema() {
	for _, f := range gentypes.MetricSchema().Fields() {
		fmt.Printf("%s %s nullable=%t\n", f.Name, f.Type, f.Nullable)
	}
	// Output:
	// day date32 nullable=false
	// host utf8 nullable=false
	// cpu float64 nullable=false
	// value float64 nullable=true
}
