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

// Package gentypes holds the fixtures the arrgen test suite generates from.
//
// Row is deliberately exhaustive: it carries one field for every column shape
// arrgen claims to support, so the equivalence tests in this package compare
// the generated encoder against arreflect over the whole supported surface
// rather than over a convenient subset.
package gentypes

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

// Row covers every supported field type, tag option and nullable variant.
type Row struct {
	Bool    bool    `arrow:"b"`
	Int8    int8    `arrow:"i8"`
	Int16   int16   `arrow:"i16"`
	Int32   int32   `arrow:"i32"`
	Int64   int64   `arrow:"i64"`
	Int     int     `arrow:"i"`
	Uint8   uint8   `arrow:"u8"`
	Uint16  uint16  `arrow:"u16"`
	Uint32  uint32  `arrow:"u32"`
	Uint64  uint64  `arrow:"u64"`
	Uint    uint    `arrow:"u"`
	Float32 float32 `arrow:"f32"`
	Float64 float64 `arrow:"f64"`
	Str     string  `arrow:"s"`
	Bin     []byte  `arrow:"bin"`

	Date32   time.Time     `arrow:"d32,date32"`
	Date64   time.Time     `arrow:"d64,date64"`
	Time32   time.Time     `arrow:"t32,time32"`
	Time64   time.Time     `arrow:"t64,time64"`
	Duration time.Duration `arrow:"dur"`

	Dec32  decimal.Decimal32 `arrow:"dec32"`
	Dec64  decimal.Decimal64 `arrow:"dec64,decimal(18,4)"`
	Dec128 decimal128.Num    `arrow:"dec128,decimal(20,3)"`
	Dec256 decimal256.Num    `arrow:"dec256,decimal(40,5)"`

	LargeStr string `arrow:"ls,large"`
	ViewStr  string `arrow:"vs,view"`
	LargeBin []byte `arrow:"lb,large"`
	ViewBin  []byte `arrow:"vb,view"`

	DictStr  string  `arrow:"ds,dict"`
	DictBin  []byte  `arrow:"db,dict"`
	DictInt  int32   `arrow:"di,dict"`
	DictF64  float64 `arrow:"df,dict"`
	Untagged int64   // no tag: the column takes the Go field name

	PBool   *bool              `arrow:"pb"`
	PInt64  *int64             `arrow:"pi64"`
	PF64    *float64           `arrow:"pf64"`
	PStr    *string            `arrow:"ps"`
	PBin    *[]byte            `arrow:"pbin"`
	PDate32 *time.Time         `arrow:"pd32,date32"`
	PDate64 *time.Time         `arrow:"pd64,date64"`
	PTime64 *time.Time         `arrow:"pt64,time64"`
	PDur    *time.Duration     `arrow:"pdur"`
	PDec32  *decimal.Decimal32 `arrow:"pdec32"`
	PDec128 *decimal128.Num    `arrow:"pdec128,decimal(20,3)"`
	PDictS  *string            `arrow:"pds,dict"`

	Secret string `arrow:"-"` // excluded from Arrow entirely
}

// Fixed holds only fixed-width columns, so appending a row never has to grow a
// variable-length data buffer. It is what the zero-allocation assertion in
// alloc_test.go measures: with space reserved up front, Append does no
// allocating at all, which is not something a string or []byte column can
// promise once its data buffer needs to double.
type Fixed struct {
	Day      time.Time `arrow:"day,date32"`
	ID       int64     `arrow:"id"`
	Value    float64   `arrow:"val"`
	OK       bool      `arrow:"ok"`
	Optional *float64  `arrow:"opt"`
}

//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen -type Row,Fixed -header ../../license_header.txt -output row_arrow.go
