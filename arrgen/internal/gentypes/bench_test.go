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

package gentypes_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/arrgen/internal/gentypes"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// benchRows is the batch size the encoder benchmarks build. It is large enough
// that per-batch setup does not dominate and small enough to stay in cache.
const benchRows = 1024

func makeMetrics(n int) []gentypes.Metric {
	metrics := make([]gentypes.Metric, n)
	v := 42.5
	for i := range metrics {
		metrics[i] = gentypes.Metric{
			Day:  base.AddDate(0, 0, i),
			Host: fmt.Sprintf("host-%d", i%16),
			CPU:  float64(i) / 100,
		}
		if i%4 != 0 {
			metrics[i].Value = &v
		}
	}
	return metrics
}

func makeFixed(n int) []gentypes.Fixed {
	rows := make([]gentypes.Fixed, n)
	v := 1.5
	for i := range rows {
		rows[i] = gentypes.Fixed{
			Day:   base.AddDate(0, 0, i),
			ID:    int64(i),
			Value: float64(i) * 1.5,
			OK:    i%2 == 0,
		}
		if i%3 != 0 {
			rows[i].Optional = &v
		}
	}
	return rows
}

// BenchmarkMetricBatch is the headline comparison: the same slice of rows
// encoded into a record batch by the reflection path and by the generated one.
func BenchmarkMetricBatch(b *testing.B) {
	metrics := makeMetrics(benchRows)
	mem := memory.DefaultAllocator

	b.Run("arreflect", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(metrics, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})

	b.Run("arrgen", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := gentypes.MetricRecordBatch(mem, metrics)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})
}

// BenchmarkRowBatch runs the same comparison over the wide fixture, where a
// row costs 45 columns of reflection instead of four.
func BenchmarkRowBatch(b *testing.B) {
	rows := makeRows(benchRows)
	mem := memory.DefaultAllocator

	b.Run("arreflect", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(rows, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})

	b.Run("arrgen", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := gentypes.RowRecordBatch(mem, rows)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})
}

// BenchmarkFixedBatch isolates the encoder from Arrow's variable-width buffers:
// every column is fixed width, so what is left in the allocation column is the
// cost of the encoding strategy itself rather than of growing a data buffer.
func BenchmarkFixedBatch(b *testing.B) {
	rows := makeFixed(benchRows)
	mem := memory.DefaultAllocator

	b.Run("arreflect", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(rows, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})

	b.Run("arrgen", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rec, err := gentypes.FixedRecordBatch(mem, rows)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.ReportMetric(float64(benchRows)*float64(b.N)/b.Elapsed().Seconds()/1e6, "Mrows/s")
	})
}

// BenchmarkStreamAppend measures the per-row cost of the streaming entry point,
// where a caller feeds rows in one at a time from a reused variable. The
// reflection path has no single-row API, so its column is the amortized cost of
// encoding a batch of the same rows.
func BenchmarkStreamAppend(b *testing.B) {
	rows := makeFixed(benchRows)
	mem := memory.DefaultAllocator

	b.Run("arreflect/batched", func(b *testing.B) {
		b.ReportAllocs()
		n := 0
		for b.Loop() {
			rec, err := arreflect.RecordFromSlice(rows, mem)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
			n += benchRows
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/row")
	})

	b.Run("arrgen/streamed", func(b *testing.B) {
		a := gentypes.NewFixedAppender(mem)
		defer a.Release()
		b.ReportAllocs()
		var row gentypes.Fixed
		i := 0
		for b.Loop() {
			row = rows[i%benchRows]
			i++
			if i%benchRows == 0 {
				a.NewRecordBatch().Release()
				a.Reserve(benchRows)
			}
			a.Append(&row)
		}
		a.NewRecordBatch().Release()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/row")
	})
}

// BenchmarkSchema shows the other cost code generation removes: arreflect walks
// the struct to infer a schema, while the generated schema is a package-level
// variable built once at init.
func BenchmarkSchema(b *testing.B) {
	b.Run("arreflect", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := arreflect.InferSchema[gentypes.Row](); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("arrgen", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = gentypes.RowSchema()
		}
	})
}
