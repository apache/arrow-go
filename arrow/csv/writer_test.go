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

package csv_test

import (
	"bufio"
	"bytes"
	"context"
	ecsv "encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	separator = ';'
	nullVal   = "null"
)

func Example_writer() {
	f := new(bytes.Buffer)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2", "str-3", "str-4", "str-5", "str-6", "str-7", "str-8", "str-9"}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	w := csv.NewWriter(f, schema, csv.WithComma(';'))
	err := w.Write(rec)
	if err != nil {
		log.Fatal(err)
	}

	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}

	err = w.Error()
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(f, schema, csv.WithComment('#'), csv.WithComma(';'))
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.RecordBatch()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

	// check for reader errors indicating issues converting csv values
	// to the arrow schema types
	err = r.Err()
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// rec[0]["i64"]: [0]
	// rec[0]["f64"]: [0]
	// rec[0]["str"]: ["str-0"]
	// rec[1]["i64"]: [1]
	// rec[1]["f64"]: [1]
	// rec[1]["str"]: ["str-1"]
	// rec[2]["i64"]: [2]
	// rec[2]["f64"]: [2]
	// rec[2]["str"]: ["str-2"]
	// rec[3]["i64"]: [3]
	// rec[3]["f64"]: [3]
	// rec[3]["str"]: ["str-3"]
	// rec[4]["i64"]: [4]
	// rec[4]["f64"]: [4]
	// rec[4]["str"]: ["str-4"]
	// rec[5]["i64"]: [5]
	// rec[5]["f64"]: [5]
	// rec[5]["str"]: ["str-5"]
	// rec[6]["i64"]: [6]
	// rec[6]["f64"]: [6]
	// rec[6]["str"]: ["str-6"]
	// rec[7]["i64"]: [7]
	// rec[7]["f64"]: [7]
	// rec[7]["str"]: ["str-7"]
	// rec[8]["i64"]: [8]
	// rec[8]["f64"]: [8]
	// rec[8]["str"]: ["str-8"]
	// rec[9]["i64"]: [9]
	// rec[9]["f64"]: [9]
	// rec[9]["str"]: ["str-9"]
}

var (
	fullData = [][]string{
		{"bool", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f16", "f32", "f64", "str", "large_str", "ts_s", "d32", "d64", "dec128", "dec256", "list(i64)", "large_list(i64)", "fixed_size_list(i64)", "binary", "large_binary", "fixed_size_binary", "uuid", "null"},
		{"true", "-1", "-1", "-1", "-1", "0", "0", "0", "0", "0", "0", "0", "str-0", "str-0", "2014-07-28 15:04:05", "2017-05-18", "2028-04-26", "-123.45", "-123.45", "{1,2,3}", "{1,2,3}", "{1,2,3}", "AAEC", "AAEC", "AAEC", "00000000-0000-0000-0000-000000000001", nullVal},
		{"false", "0", "0", "0", "0", "1", "1", "1", "1", "0.099975586", "0.1", "0.1", "str-1", "str-1", "2016-09-08 15:04:05", "2022-11-08", "2031-06-28", "0", "0", "{4,5,6}", "{4,5,6}", "{4,5,6}", "AwQF", "AwQF", "AwQF", "00000000-0000-0000-0000-000000000002", nullVal},
		{"true", "1", "1", "1", "1", "2", "2", "2", "2", "0.19995117", "0.2", "0.2", "str-2", "str-2", "2021-09-18 15:04:05", "2025-08-04", "2034-08-28", "123.45", "123.45", "{7,8,9}", "{7,8,9}", "{7,8,9}", "", "", "AAAA", "00000000-0000-0000-0000-000000000003", nullVal},
		{nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal},
	}
	bananaData = [][]string{
		{"bool", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f16", "f32", "f64", "str", "large_str", "ts_s", "d32", "d64", "dec128", "dec256", "list(i64)", "large_list(i64)", "fixed_size_list(i64)", "binary", "large_binary", "fixed_size_binary", "uuid", "null"},
		{"BANANA", "-1", "-1", "-1", "-1", "0", "0", "0", "0", "0", "0", "0", "str-0", "str-0", "2014-07-28 15:04:05", "2017-05-18", "2028-04-26", "-123.45", "-123.45", "{1,2,3}", "{1,2,3}", "{1,2,3}", "AAEC", "AAEC", "AAEC", "00000000-0000-0000-0000-000000000001", nullVal},
		{"MANGO", "0", "0", "0", "0", "1", "1", "1", "1", "0.099975586", "0.1", "0.1", "str-1", "str-1", "2016-09-08 15:04:05", "2022-11-08", "2031-06-28", "0", "0", "{4,5,6}", "{4,5,6}", "{4,5,6}", "AwQF", "AwQF", "AwQF", "00000000-0000-0000-0000-000000000002", nullVal},
		{"BANANA", "1", "1", "1", "1", "2", "2", "2", "2", "0.19995117", "0.2", "0.2", "str-2", "str-2", "2021-09-18 15:04:05", "2025-08-04", "2034-08-28", "123.45", "123.45", "{7,8,9}", "{7,8,9}", "{7,8,9}", "", "", "AAAA", "00000000-0000-0000-0000-000000000003", nullVal},
		{nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal},
	}
)

func TestCSVWriter(t *testing.T) {
	tests := []struct {
		name       string
		header     bool
		boolFormat func(bool) string
		data       [][]string
	}{
		{
			name:   "Noheader",
			header: false,
			data:   fullData[1:],
		},
		{
			name:   "header",
			header: true,
			data:   fullData,
		},
		{
			name:   "Header with bool fmt",
			header: true,
			boolFormat: func(b bool) string {
				if b {
					return "BANANA"
				}
				return "MANGO"
			},
			data: bananaData,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCSVWriter(t, test.data, test.header, test.boolFormat)
		})
	}
}

func genTimestamps(unit arrow.TimeUnit) []arrow.Timestamp {
	out := []arrow.Timestamp{}
	for _, input := range []string{"2014-07-28 15:04:05", "2016-09-08 15:04:05", "2021-09-18 15:04:05"} {
		ts, err := arrow.TimestampFromString(input, unit)
		if err != nil {
			panic(fmt.Errorf("could not convert %s to arrow.Timestamp err=%s", input, err))
		}
		out = append(out, ts)
	}
	return out
}

func testCSVWriter(t *testing.T, data [][]string, writeHeader bool, fmtr func(bool) string) {
	f := new(bytes.Buffer)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f16", Type: arrow.FixedWidthTypes.Float16},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "large_str", Type: arrow.BinaryTypes.LargeString},
			{Name: "ts_s", Type: arrow.FixedWidthTypes.Timestamp_s},
			{Name: "d32", Type: arrow.FixedWidthTypes.Date32},
			{Name: "d64", Type: arrow.FixedWidthTypes.Date64},
			{Name: "dec128", Type: &arrow.Decimal128Type{Precision: 5, Scale: 2}},
			{Name: "dec256", Type: &arrow.Decimal256Type{Precision: 5, Scale: 2}},
			{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "large_list(i64)", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "fixed_size_list(i64)", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int64)},
			{Name: "binary", Type: arrow.BinaryTypes.Binary},
			{Name: "large_binary", Type: arrow.BinaryTypes.LargeBinary},
			{Name: "fixed_size_binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}},
			{Name: "uuid", Type: extensions.NewUUIDType()},
			{Name: "null", Type: arrow.Null},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	b.Field(1).(*array.Int8Builder).AppendValues([]int8{-1, 0, 1}, nil)
	b.Field(2).(*array.Int16Builder).AppendValues([]int16{-1, 0, 1}, nil)
	b.Field(3).(*array.Int32Builder).AppendValues([]int32{-1, 0, 1}, nil)
	b.Field(4).(*array.Int64Builder).AppendValues([]int64{-1, 0, 1}, nil)
	b.Field(5).(*array.Uint8Builder).AppendValues([]uint8{0, 1, 2}, nil)
	b.Field(6).(*array.Uint16Builder).AppendValues([]uint16{0, 1, 2}, nil)
	b.Field(7).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
	b.Field(8).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2}, nil)
	b.Field(9).(*array.Float16Builder).AppendValues([]float16.Num{float16.New(0.0), float16.New(0.1), float16.New(0.2)}, nil)
	b.Field(10).(*array.Float32Builder).AppendValues([]float32{0.0, 0.1, 0.2}, nil)
	b.Field(11).(*array.Float64Builder).AppendValues([]float64{0.0, 0.1, 0.2}, nil)
	b.Field(12).(*array.StringBuilder).AppendValues([]string{"str_0", "str-1", "str-2"}, nil)
	b.Field(13).(*array.LargeStringBuilder).AppendValues([]string{"str_0", "str-1", "str-2"}, nil)
	b.Field(14).(*array.TimestampBuilder).AppendValues(genTimestamps(arrow.Second), nil)
	b.Field(15).(*array.Date32Builder).AppendValues([]arrow.Date32{17304, 19304, 20304}, nil)
	b.Field(16).(*array.Date64Builder).AppendValues([]arrow.Date64{1840400000000, 1940400000000, 2040400000000}, nil)
	b.Field(17).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-12345), decimal128.FromI64(0), decimal128.FromI64(12345)}, nil)
	b.Field(18).(*array.Decimal256Builder).AppendValues([]decimal256.Num{decimal256.FromI64(-12345), decimal256.FromI64(0), decimal256.FromI64(12345)}, nil)
	listBuilder := b.Field(19).(*array.ListBuilder)
	listBuilderInt64 := listBuilder.ValueBuilder().(*array.Int64Builder)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{1, 2, 3}, nil)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{4, 5, 6}, nil)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{7, 8, 9}, nil)
	largeListBuilder := b.Field(20).(*array.LargeListBuilder)
	largeListBuilderInt64 := largeListBuilder.ValueBuilder().(*array.Int64Builder)
	largeListBuilder.Append(true)
	largeListBuilderInt64.AppendValues([]int64{1, 2, 3}, nil)
	largeListBuilder.Append(true)
	largeListBuilderInt64.AppendValues([]int64{4, 5, 6}, nil)
	largeListBuilder.Append(true)
	largeListBuilderInt64.AppendValues([]int64{7, 8, 9}, nil)
	fixedSizeListBuilder := b.Field(21).(*array.FixedSizeListBuilder)
	fixedSizeListBuilderInt64 := fixedSizeListBuilder.ValueBuilder().(*array.Int64Builder)
	fixedSizeListBuilder.Append(true)
	fixedSizeListBuilderInt64.AppendValues([]int64{1, 2, 3}, nil)
	fixedSizeListBuilder.Append(true)
	fixedSizeListBuilderInt64.AppendValues([]int64{4, 5, 6}, nil)
	fixedSizeListBuilder.Append(true)
	fixedSizeListBuilderInt64.AppendValues([]int64{7, 8, 9}, nil)
	b.Field(22).(*array.BinaryBuilder).AppendValues([][]byte{{0, 1, 2}, {3, 4, 5}, {}}, nil)
	b.Field(23).(*array.BinaryBuilder).AppendValues([][]byte{{0, 1, 2}, {3, 4, 5}, {}}, nil)
	b.Field(24).(*array.FixedSizeBinaryBuilder).AppendValues([][]byte{{0, 1, 2}, {3, 4, 5}, {}}, nil)
	b.Field(25).(*extensions.UUIDBuilder).AppendValues([]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002"), uuid.MustParse("00000000-0000-0000-0000-000000000003")}, nil)
	b.Field(26).(*array.NullBuilder).AppendEmptyValues(3)

	for _, field := range b.Fields() {
		field.AppendNull()
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	w := csv.NewWriter(f, schema,
		csv.WithComma(separator),
		csv.WithCRLF(false),
		csv.WithHeader(writeHeader),
		csv.WithNullWriter(nullVal),
		csv.WithBoolWriter(fmtr),
		csv.WithStringsReplacer(strings.NewReplacer("_", "-")),
	)
	err := w.Write(rec)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Flush()
	if err != nil {
		t.Fatal(err)
	}

	err = w.Error()
	if err != nil {
		t.Fatal(err)
	}

	bdata, err := expectedOutput(data)
	if err != nil {
		t.Fatal(err)
	}

	if err = matchCSV(bdata.Bytes(), f.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func expectedOutput(data [][]string) (*bytes.Buffer, error) {
	b := bytes.NewBuffer(nil)
	w := ecsv.NewWriter(b)
	w.Comma = separator
	w.UseCRLF = false
	return b, w.WriteAll(data)
}

func matchCSV(expected, test []byte) error {
	expectedScanner := bufio.NewScanner(bytes.NewReader(expected))
	testScanner := bufio.NewScanner(bytes.NewReader(test))
	line := 0
	for expectedScanner.Scan() && testScanner.Scan() {
		if expectedScanner.Text() != testScanner.Text() {
			return fmt.Errorf("expected=%s != test=%s line=%d", expectedScanner.Text(), testScanner.Text(), line)
		}
		line++
	}

	if expectedScanner.Scan() {
		return fmt.Errorf("expected unprocessed:%s", expectedScanner.Text())
	}

	if testScanner.Scan() {
		return fmt.Errorf("test unprocessed:%s", testScanner.Text())
	}

	if err := expectedScanner.Err(); err != nil {
		return err
	}

	return testScanner.Err()
}

func BenchmarkWrite(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f16", Type: arrow.FixedWidthTypes.Float16},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "large_str", Type: arrow.BinaryTypes.LargeString},
			{Name: "dec128", Type: &arrow.Decimal128Type{Precision: 4, Scale: 3}},
			{Name: "dec128", Type: &arrow.Decimal256Type{Precision: 4, Scale: 3}},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	const N = 1000
	for i := 0; i < N; i++ {
		bldr.Field(0).(*array.BooleanBuilder).Append(i%10 == 0)
		bldr.Field(1).(*array.Int8Builder).Append(int8(i))
		bldr.Field(2).(*array.Int16Builder).Append(int16(i))
		bldr.Field(3).(*array.Int32Builder).Append(int32(i))
		bldr.Field(4).(*array.Int64Builder).Append(int64(i))
		bldr.Field(5).(*array.Uint8Builder).Append(uint8(i))
		bldr.Field(6).(*array.Uint16Builder).Append(uint16(i))
		bldr.Field(7).(*array.Uint32Builder).Append(uint32(i))
		bldr.Field(8).(*array.Uint64Builder).Append(uint64(i))
		bldr.Field(9).(*array.Float16Builder).Append(float16.New(float32(i)))
		bldr.Field(10).(*array.Float32Builder).Append(float32(i))
		bldr.Field(11).(*array.Float64Builder).Append(float64(i))
		bldr.Field(12).(*array.StringBuilder).Append(fmt.Sprintf("str-%d", i))
		bldr.Field(13).(*array.LargeStringBuilder).Append(fmt.Sprintf("str-%d", i))
		bldr.Field(14).(*array.Decimal128Builder).Append(decimal128.FromI64(int64(i)))
		bldr.Field(15).(*array.Decimal256Builder).Append(decimal256.FromI64(int64(i)))
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	w := csv.NewWriter(io.Discard, schema, csv.WithComma(';'), csv.WithCRLF(false))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := w.Write(rec)
		if err != nil {
			b.Fatal(err)
		}
		err = w.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestParquetTestingCSVWriter tests that the CSV writer successfully convert arrow/parquet-testing files to CSV
func TestParquetTestingCSVWriter(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	assert.DirExists(t, dir)

	t.Run("alltypes_plain.parquet", func(t *testing.T) {
		testFile, err := os.Open(path.Join(dir, "alltypes_plain.parquet"))
		require.NoError(t, err)
		defer testFile.Close()

		r, err := file.NewParquetReader(testFile)
		require.NoError(t, err)
		defer r.Close()

		alloc := memory.NewGoAllocator()

		arrowReader, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{BatchSize: 1024}, alloc)
		require.NoError(t, err)

		schema, err := arrowReader.Schema()
		require.NoError(t, err)

		buf := &bytes.Buffer{}
		csvWriter := csv.NewWriter(buf, schema, csv.WithHeader(true))

		recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
		require.NoError(t, err)

		for recordReader.Next() {
			rec := recordReader.RecordBatch()
			err := csvWriter.Write(rec)
			require.NoError(t, err)
		}
		require.NoError(t, csvWriter.Error())
		require.NoError(t, csvWriter.Flush())

		expected := `id,bool_col,tinyint_col,smallint_col,int_col,bigint_col,float_col,double_col,date_string_col,string_col,timestamp_col
4,true,0,0,0,0,0,0,MDMvMDEvMDk=,MA==,2009-03-01 00:00:00
5,false,1,1,1,10,1.1,10.1,MDMvMDEvMDk=,MQ==,2009-03-01 00:01:00
6,true,0,0,0,0,0,0,MDQvMDEvMDk=,MA==,2009-04-01 00:00:00
7,false,1,1,1,10,1.1,10.1,MDQvMDEvMDk=,MQ==,2009-04-01 00:01:00
2,true,0,0,0,0,0,0,MDIvMDEvMDk=,MA==,2009-02-01 00:00:00
3,false,1,1,1,10,1.1,10.1,MDIvMDEvMDk=,MQ==,2009-02-01 00:01:00
0,true,0,0,0,0,0,0,MDEvMDEvMDk=,MA==,2009-01-01 00:00:00
1,false,1,1,1,10,1.1,10.1,MDEvMDEvMDk=,MQ==,2009-01-01 00:01:00
`

		require.Equal(t, expected, buf.String())
	})
	t.Run("delta_byte_array.parquet", func(t *testing.T) {
		testFile, err := os.Open(path.Join(dir, "delta_byte_array.parquet"))
		require.NoError(t, err)
		defer testFile.Close()

		r, err := file.NewParquetReader(testFile)
		require.NoError(t, err)
		defer r.Close()

		alloc := memory.NewGoAllocator()

		arrowReader, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{BatchSize: 1024}, alloc)
		require.NoError(t, err)

		schema, err := arrowReader.Schema()
		require.NoError(t, err)

		buf := &bytes.Buffer{}
		csvWriter := csv.NewWriter(
			buf,
			schema,
			csv.WithHeader(true),
			csv.WithNullWriter(""),
		)

		recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
		require.NoError(t, err)

		for recordReader.Next() {
			rec := recordReader.RecordBatch()
			err := csvWriter.Write(rec)
			require.NoError(t, err)
		}
		require.NoError(t, csvWriter.Error())
		require.NoError(t, csvWriter.Flush())

		expected, err := os.Open(path.Join(dir, "delta_byte_array_expect.csv"))
		require.NoError(t, err)
		defer expected.Close()

		// parse expected as CSV
		expectedScanner := ecsv.NewReader(expected)
		expectedLines, err := expectedScanner.ReadAll()
		require.NoError(t, err)

		// parse buf as CSV
		bufLines, err := ecsv.NewReader(buf).ReadAll()
		require.NoError(t, err)

		// compare line by line
		require.Equal(t, len(bufLines), len(expectedLines))

		for i, line := range bufLines {
			require.Equal(t, expectedLines[i], line)
		}
	})
}

// TestParquetTestingCSVWriter tests that the CSV writer successfully convert arrow/parquet-testing files to CSV
func TestCustomTypeConversion(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	assert.DirExists(t, dir)

	testFile, err := os.Open(path.Join(dir, "alltypes_plain.parquet"))
	require.NoError(t, err)
	defer testFile.Close()

	r, err := file.NewParquetReader(testFile)
	require.NoError(t, err)
	defer r.Close()

	alloc := memory.NewGoAllocator()

	arrowReader, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{BatchSize: 1024}, alloc)
	require.NoError(t, err)

	schema, err := arrowReader.Schema()
	require.NoError(t, err)

	buf := &bytes.Buffer{}
	csvWriter := csv.NewWriter(
		buf,
		schema,
		csv.WithHeader(true),
		csv.WithCustomTypeConverter(func(typ arrow.DataType, col arrow.Array) (result []string, handled bool) {
			if typ.ID() == arrow.BINARY {
				result = make([]string, col.Len())
				arr := col.(*array.Binary)
				for i := 0; i < arr.Len(); i++ {
					if !arr.IsValid(i) {
						result[i] = "NULL"
						continue
					}
					result[i] = fmt.Sprintf("\\x%x", arr.Value(i))
				}
				return result, true
			}
			if typ.ID() == arrow.TIMESTAMP {
				result = make([]string, col.Len())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					if !arr.IsValid(i) {
						result[i] = "NULL"
						continue
					}
					fn, err := typ.(*arrow.TimestampType).GetToTimeFunc()
					if err != nil {
						result[i] = "NULL"
						continue
					}

					result[i] = fn(arr.Value(i)).Format(time.RFC3339)
				}
				return result, true
			}
			return nil, false
		}),
	)

	recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)

	for recordReader.Next() {
		rec := recordReader.RecordBatch()
		err := csvWriter.Write(rec)
		require.NoError(t, err)
	}
	require.NoError(t, csvWriter.Error())
	require.NoError(t, csvWriter.Flush())

	expected := `id,bool_col,tinyint_col,smallint_col,int_col,bigint_col,float_col,double_col,date_string_col,string_col,timestamp_col
4,true,0,0,0,0,0,0,\x30332f30312f3039,\x30,2009-03-01T00:00:00Z
5,false,1,1,1,10,1.1,10.1,\x30332f30312f3039,\x31,2009-03-01T00:01:00Z
6,true,0,0,0,0,0,0,\x30342f30312f3039,\x30,2009-04-01T00:00:00Z
7,false,1,1,1,10,1.1,10.1,\x30342f30312f3039,\x31,2009-04-01T00:01:00Z
2,true,0,0,0,0,0,0,\x30322f30312f3039,\x30,2009-02-01T00:00:00Z
3,false,1,1,1,10,1.1,10.1,\x30322f30312f3039,\x31,2009-02-01T00:01:00Z
0,true,0,0,0,0,0,0,\x30312f30312f3039,\x30,2009-01-01T00:00:00Z
1,false,1,1,1,10,1.1,10.1,\x30312f30312f3039,\x31,2009-01-01T00:01:00Z
`

	require.Equal(t, expected, buf.String())

}
