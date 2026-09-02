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

// Package errors holds one struct per rejection the generator makes, so the
// error messages stay covered and stay readable.
package errors

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

// Inner is a nested struct used by Nested and Embedded.
type Inner struct {
	A int64
}

// ID is a defined scalar type, which arreflect matches by exact type and so
// cannot map either.
type ID int64

type Embedded struct {
	Inner
	B int64
}

type Nested struct {
	Field Inner
}

type SliceField struct {
	Field []int64
}

type ArrayField struct {
	Field [3]int64
}

type MapField struct {
	Field map[string]int64
}

type NamedScalar struct {
	Field ID
}

type UnknownOption struct {
	Field int64 `arrow:"f,nope"`
}

type BadDecimalTag struct {
	Field int64 `arrow:"f,decimal(a,2)"`
}

type ShortDecimalTag struct {
	Field int64 `arrow:"f,decimal(4)"`
}

type DuplicateNames struct {
	A int64 `arrow:"same"`
	B int64 `arrow:"same"`
}

type TemporalOnInt struct {
	Field int64 `arrow:"f,date32"`
}

type DecimalOnInt struct {
	Field int64 `arrow:"f,decimal(10,2)"`
}

type DictOnBool struct {
	Field bool `arrow:"f,dict"`
}

type ViewOnInt struct {
	Field int64 `arrow:"f,view"`
}

type LargeOnInt struct {
	Field int64 `arrow:"f,large"`
}

type RunEndEncoded struct {
	Field string `arrow:"f,ree"`
}

type DictAndView struct {
	Field string `arrow:"f,dict,view"`
}

type DictAndLarge struct {
	Field string `arrow:"f,dict,large"`
}

type NoColumns struct {
	Field  string `arrow:"-"`
	hidden int64
}

type DoublePointer struct {
	Field **int64
}

type TimeSlice struct {
	Field []time.Time
}

// NotAStruct is a named type whose underlying type is not a struct.
type NotAStruct int64

// BareTime, TimestampTime and the two bare decimals below are the spellings
// arreflect cannot infer as a struct field, so arrgen rejects them rather than
// generating a column the reflection path would not agree with.
type BareTime struct {
	Field time.Time
}

type TimestampTime struct {
	Field time.Time `arrow:"f,timestamp"`
}

type BareDecimal128 struct {
	Field decimal128.Num
}

type BareDecimal256 struct {
	Field decimal256.Num
}

type Collide struct {
	Field int64
}

type collide struct {
	Field int64
}
