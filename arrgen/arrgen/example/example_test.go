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

package example_test

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/arrgen/example"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Example encodes a slice of rows in one call, replacing
// arreflect.RecordFromSlice[Metric](metrics, mem).
func Example() {
	cpuLoad := 0.75
	metrics := []example.Metric{
		{Time: time.Unix(0, 0).UTC(), Host: "web-1", CPU: 0.5, Value: &cpuLoad},
		{Time: time.Unix(1, 0).UTC(), Host: "web-2", CPU: 1.5}, // Value is nil: a null
	}

	rec, err := example.MetricRecordBatch(memory.DefaultAllocator, metrics)
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
	// ts: [0 1000000000]
	// host: ["web-1" "web-2"]
	// cpu: [0.5 1.5]
	// value: [0.75 (null)]
}

// ExampleMetricAppender streams rows in one at a time and cuts a batch at a
// size boundary. The appender never retains a row, so the loop reuses a single
// variable and allocates nothing.
func ExampleMetricAppender() {
	const batchSize = 2

	a := example.NewMetricAppender(memory.DefaultAllocator)
	defer a.Release()
	a.Reserve(batchSize)

	var row example.Metric
	for i := 0; i < 5; i++ {
		row = example.Metric{
			Time: time.Unix(int64(i), 0).UTC(),
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

// ExampleMetricSchema shows the schema the generator resolved from the tags.
func ExampleMetricSchema() {
	for _, f := range example.MetricSchema().Fields() {
		fmt.Printf("%s %s nullable=%t\n", f.Name, f.Type, f.Nullable)
	}
	// Output:
	// ts timestamp[ns, tz=UTC] nullable=false
	// host utf8 nullable=false
	// cpu float64 nullable=false
	// value float64 nullable=true
}
