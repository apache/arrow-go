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
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/arrgen/internal/gentypes"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/array/arreflect"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// compareColumns asserts that got and want hold the same columns.
//
// It walks the columns rather than calling array.RecordEqual so a failure names
// the column that differs instead of dumping two whole batches.
//
// dictsByValue relaxes dictionary columns to a comparison of the values they
// decode to. An appender reused across batches carries its memo table forward,
// so its second batch encodes the same values against a longer dictionary than
// a freshly built one - a different encoding of identical data, and the reason
// Arrow has delta dictionaries at all.
func compareColumns(t *testing.T, label string, got, want arrow.RecordBatch, dictsByValue bool) {
	t.Helper()
	if got.NumRows() != want.NumRows() {
		t.Fatalf("%s: NumRows = %d, want %d", label, got.NumRows(), want.NumRows())
	}
	if got.NumCols() != want.NumCols() {
		t.Fatalf("%s: NumCols = %d, want %d", label, got.NumCols(), want.NumCols())
	}
	for i := 0; i < int(got.NumCols()); i++ {
		name := got.Schema().Field(i).Name
		gc, wc := got.Column(i), want.Column(i)
		if dictsByValue && gc.DataType().ID() == arrow.DICTIONARY {
			compareByValueStr(t, label, name, gc, wc)
			continue
		}
		if !array.Equal(gc, wc) {
			t.Errorf("%s: column %q differs\n got: %s\nwant: %s", label, name, gc, wc)
		}
	}
}

// compareByValueStr compares two arrays by the value each row decodes to,
// ignoring how that value is physically encoded.
func compareByValueStr(t *testing.T, label, name string, got, want arrow.Array) {
	t.Helper()
	if got.Len() != want.Len() {
		t.Errorf("%s: column %q: len = %d, want %d", label, name, got.Len(), want.Len())
		return
	}
	for i := 0; i < got.Len(); i++ {
		if got.IsNull(i) != want.IsNull(i) {
			t.Errorf("%s: column %q row %d: null = %t, want %t", label, name, i, got.IsNull(i), want.IsNull(i))
			return
		}
		if got.IsNull(i) {
			continue
		}
		if got.ValueStr(i) != want.ValueStr(i) {
			t.Errorf("%s: column %q row %d = %s, want %s", label, name, i, got.ValueStr(i), want.ValueStr(i))
			return
		}
	}
}

// base is a fixed instant with a non-zero time of day and sub-second part, so
// the date32/date64/time32/time64 columns each exercise a different truncation.
var base = time.Date(2024, time.March, 17, 13, 45, 12, 123456789, time.UTC)

// makeRows builds n deterministic rows. Every third row nulls its pointer
// fields and empties its variable-width ones, so the comparison covers null
// handling and not just the happy path.
func makeRows(n int) []gentypes.Row {
	rows := make([]gentypes.Row, n)
	for i := range rows {
		null := i%3 == 0
		r := gentypes.Row{
			Bool:     i%2 == 0,
			Int8:     int8(i),
			Int16:    int16(i * 3),
			Int32:    int32(i * 7),
			Int64:    int64(i) * 1e6,
			Int:      i,
			Uint8:    uint8(i),
			Uint16:   uint16(i * 5),
			Uint32:   uint32(i * 11),
			Uint64:   uint64(i) * 1e9,
			Uint:     uint(i),
			Float32:  float32(i) + 0.25,
			Float64:  float64(i) * math.Pi,
			Str:      fmt.Sprintf("host-%d", i%7),
			Bin:      []byte{byte(i), byte(i >> 8)},
			Date32:   base.AddDate(0, 0, i),
			Date64:   base.AddDate(0, 0, i),
			Time32:   base.Add(time.Duration(i) * time.Minute),
			Time64:   base.Add(time.Duration(i) * time.Minute),
			Duration: time.Duration(i) * time.Millisecond,
			Dec32:    decimal.Decimal32(i * 100),
			Dec64:    decimal.Decimal64(i * 10000),
			Dec128:   decimal128.FromU64(uint64(i) * 1000),
			Dec256:   decimal256.FromU64(uint64(i) * 2000),
			LargeStr: fmt.Sprintf("large-%d", i),
			ViewStr:  fmt.Sprintf("view-%d", i),
			LargeBin: []byte(fmt.Sprintf("lb-%d", i)),
			ViewBin:  []byte(fmt.Sprintf("vb-%d", i)),
			DictStr:  fmt.Sprintf("region-%d", i%3),
			DictBin:  []byte(fmt.Sprintf("k%d", i%4)),
			DictInt:  int32(i % 5),
			DictF64:  float64(i % 6),
			Untagged: int64(i),
			Secret:   "never encoded",
		}
		if null {
			// Leave every pointer nil, and null out the two shapes that map to
			// null without a pointer: a nil []byte column.
			r.Bin = nil
			r.LargeBin = nil
			r.ViewBin = nil
			r.DictBin = nil
			rows[i] = r
			continue
		}
		b := i%2 == 1
		i64 := int64(i) * 3
		f64 := float64(i) / 3
		s := fmt.Sprintf("p-%d", i)
		bin := []byte(fmt.Sprintf("pb-%d", i))
		ts := base.Add(time.Duration(i) * time.Hour)
		dur := time.Duration(i) * time.Microsecond
		dec := decimal.Decimal32(i)
		ds := fmt.Sprintf("pd-%d", i%2)
		r.PBool, r.PInt64, r.PF64, r.PStr, r.PBin = &b, &i64, &f64, &s, &bin
		dec128 := decimal128.FromU64(uint64(i))
		r.PDate32, r.PDate64, r.PTime64, r.PDur, r.PDec32, r.PDictS = &ts, &ts, &ts, &dur, &dec, &ds
		r.PDec128 = &dec128

		// Multi-level pointers. Every fourth row nils the inner level while the
		// outer one stays set, so each per-level nil check is covered instead of
		// only ever failing at the first.
		pi64, pf64, pstr := &i64, &f64, &s
		ppf64 := &pf64
		r.PPInt64, r.PPStr, r.PPPF64 = &pi64, &pstr, &ppf64
		ppbin := &bin
		if i%4 == 1 {
			ppbin = nil // a nil inner pointer behind a set outer one
		} else if i%4 == 2 {
			var nilBin []byte
			ppbin = &nilBin // a set pointer to a nil []byte
		}
		r.PPBin = &ppbin
		rows[i] = r
	}
	return rows
}

// TestRowSchemaMatchesArreflect is the cheaper half of the equivalence
// guarantee: the generated schema is what arreflect would infer, column for
// column, including names, types and nullability.
func TestRowSchemaMatchesArreflect(t *testing.T) {
	want, err := arreflect.InferSchema[gentypes.Row]()
	if err != nil {
		t.Fatalf("InferSchema: %v", err)
	}
	compareSchemas(t, gentypes.RowSchema(), want)
}

func TestMetricSchemaMatchesArreflect(t *testing.T) {
	want, err := arreflect.InferSchema[gentypes.Metric]()
	if err != nil {
		t.Fatalf("InferSchema: %v", err)
	}
	compareSchemas(t, gentypes.MetricSchema(), want)
}

func TestFixedSchemaMatchesArreflect(t *testing.T) {
	want, err := arreflect.InferSchema[gentypes.Fixed]()
	if err != nil {
		t.Fatalf("InferSchema: %v", err)
	}
	compareSchemas(t, gentypes.FixedSchema(), want)
}

// compareSchemas asserts got and want are the same schema, field for field.
// Schema.Equal would answer the same question in one call, but a mismatch on a
// 49-column fixture is only actionable if the failure names the column.
func compareSchemas(t *testing.T, got, want *arrow.Schema) {
	t.Helper()
	if got.NumFields() != want.NumFields() {
		t.Fatalf("field count = %d, want %d\n got: %s\nwant: %s", got.NumFields(), want.NumFields(), got, want)
	}
	for i := 0; i < got.NumFields(); i++ {
		gf, wf := got.Field(i), want.Field(i)
		if gf.Name != wf.Name {
			t.Errorf("field %d: name = %q, want %q", i, gf.Name, wf.Name)
			continue
		}
		if !arrow.TypeEqual(gf.Type, wf.Type) || gf.Nullable != wf.Nullable {
			t.Errorf("field %q: got %s nullable=%t, want %s nullable=%t", gf.Name, gf.Type, gf.Nullable, wf.Type, wf.Nullable)
		}
	}
	if !got.Equal(want) {
		t.Errorf("schemas differ beyond their fields\n got: %s\nwant: %s", got, want)
	}
}

// TestRowRecordMatchesArreflect is the guarantee that matters: for the same
// input, the generated encoder and the reflection encoder produce equal
// columns. Everything else in this package - the benchmarks, the allocation
// assertions - is only interesting because this holds.
//
// Every column is compared. arrgen rejects at generate time any field it cannot
// map the way arreflect would, so nothing needs skipping here.
func TestRowRecordMatchesArreflect(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 64} {
		t.Run(fmt.Sprintf("rows=%d", n), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			rows := makeRows(n)

			want, err := arreflect.RecordFromSlice(rows, mem)
			if err != nil {
				t.Fatalf("arreflect.RecordFromSlice: %v", err)
			}
			defer want.Release()

			got, err := gentypes.RowRecordBatch(mem, rows)
			if err != nil {
				t.Fatalf("RowRecordBatch: %v", err)
			}
			defer got.Release()

			compareColumns(t, fmt.Sprintf("rows=%d", n), got, want, false)
		})
	}
}

func TestMetricRecordMatchesArreflect(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	v := 3.5
	metrics := []gentypes.Metric{
		{Day: base, Host: "a", CPU: 0.5, Value: &v, Secret: "hidden"},
		{Day: base.AddDate(0, 0, 1), Host: "b", CPU: 1.5, Value: nil},
	}

	want, err := arreflect.RecordFromSlice(metrics, mem)
	if err != nil {
		t.Fatalf("arreflect.RecordFromSlice: %v", err)
	}
	defer want.Release()

	got, err := gentypes.MetricRecordBatch(mem, metrics)
	if err != nil {
		t.Fatalf("MetricRecordBatch: %v", err)
	}
	defer got.Release()

	compareColumns(t, "Metric", got, want, false)
	if secrets := got.Schema().FieldIndices("Secret"); len(secrets) != 0 {
		t.Errorf(`a field tagged arrow:"-" was encoded as column %v`, secrets)
	}
}

// TestAppendMatchesAppendSlice checks the streaming entry point against the
// bulk one: a caller feeding rows one at a time must land in the same place as
// a caller handing over a slice.
func TestAppendMatchesAppendSlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rows := makeRows(16)

	bulk := gentypes.NewRowAppender(mem)
	defer bulk.Release()
	bulk.AppendSlice(rows)
	bulkRec := bulk.NewRecordBatch()
	defer bulkRec.Release()

	streamed := gentypes.NewRowAppender(mem)
	defer streamed.Release()
	streamed.Reserve(len(rows))
	var row gentypes.Row
	for i := range rows {
		row = rows[i] // a caller reusing one row variable must be safe
		streamed.Append(&row)
	}
	if got, want := streamed.Len(), len(rows); got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
	streamedRec := streamed.NewRecordBatch()
	defer streamedRec.Release()

	if !array.RecordEqual(streamedRec, bulkRec) {
		t.Errorf("streamed batch differs from bulk batch\n got: %s\nwant: %s", streamedRec, bulkRec)
	}
	if err := streamed.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}

// TestAppenderReusableAcrossBatches covers the roll boundary: NewRecordBatch
// hands over the rows so far and leaves the appender ready for the next batch.
func TestAppenderReusableAcrossBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rows := makeRows(9)
	a := gentypes.NewRowAppender(mem)
	defer a.Release()

	for i := 0; i < len(rows); i += 3 {
		a.AppendSlice(rows[i : i+3])
		rec := a.NewRecordBatch()
		if got := rec.NumRows(); got != 3 {
			t.Errorf("batch %d: NumRows() = %d, want 3", i/3, got)
		}
		want, err := arreflect.RecordFromSlice(rows[i:i+3], mem)
		if err != nil {
			t.Fatalf("arreflect.RecordFromSlice: %v", err)
		}
		compareColumns(t, fmt.Sprintf("batch %d", i/3), rec, want, true)
		want.Release()
		rec.Release()
		if got := a.Len(); got != 0 {
			t.Errorf("batch %d: Len() after NewRecordBatch = %d, want 0", i/3, got)
		}
	}
}

// TestNilAllocatorUsesDefault documents the constructor's contract rather than
// leaving callers to discover it by crashing.
func TestNilAllocatorUsesDefault(t *testing.T) {
	a := gentypes.NewRowAppender(nil)
	defer a.Release()
	a.AppendSlice(makeRows(2))
	rec := a.NewRecordBatch()
	defer rec.Release()
	if got := rec.NumRows(); got != 2 {
		t.Errorf("NumRows() = %d, want 2", got)
	}
}
