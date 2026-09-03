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

// Package arrgen generates zero-reflection Arrow encoders for Go struct types.
//
// It is the code-generation counterpart to
// [github.com/apache/arrow-go/v18/arrow/array/arreflect]: arreflect interprets
// a struct's `arrow:"..."` tags with reflection on every value, while arrgen
// reads exactly the same tags once, at generate time, and emits typed Go source
// that appends struct fields straight into typed Arrow builders.
//
// The two paths are interchangeable. For every struct arrgen accepts, the
// generated schema equals arreflect.InferSchema and the generated record batch
// equals the one arreflect.RecordFromSlice builds, so adopting the generated
// code is a call-site change and nothing else.
//
// # Usage
//
// Add the generator to your module as a tool dependency, then put a go:generate
// directive next to the struct:
//
//	go get -tool github.com/apache/arrow-go/arrgen/cmd/arrgen
//
//	//go:generate go tool arrgen -type Metric
//
// then run `go generate ./...`, which writes metric_arrow.go next to the type.
// Nothing in the arrow-go module depends on arrgen, and the code it emits
// imports only arrow, arrow/array and arrow/memory, so the generator itself is
// a build-time-only dependency. The unversioned `go run <pkg>` spelling this
// module's own directives use resolves only inside a module that already
// requires arrgen; see the README for the alternatives.
//
// # Supported field types
//
// bool, int8/16/32/64, int, uint8/16/32/64, uint, float32/64, string, []byte,
// time.Time, time.Duration, decimal.Decimal32, decimal.Decimal64,
// decimal128.Num and decimal256.Num, plus one or more pointers to any of those
// for a nullable column - a nil at any level is a null, as it is in arreflect.
//
// Tag options mirror arreflect: a leading name, "-" to skip a field, the
// temporal overrides date32, date64, time32 and time64, dict, view, large, and
// decimal(precision,scale).
//
// A time.Time field must carry one of the four temporal tags, and a
// decimal128.Num or decimal256.Num field must carry a decimal(precision,scale)
// tag. Untagged, all three are Go structs that arreflect's inferArrowType
// resolves through inferStructType - it switches on reflect.Kind before it
// reaches the types it matches by identity - so it infers an empty struct<> and
// drops the value. Generating the column Arrow means here would put the two
// paths out of step, so arrgen rejects those spellings instead. One consequence
// is that arrgen cannot emit a TIMESTAMP column at all: ",timestamp" is
// rejected along with the untagged spelling.
//
// Anything arrgen cannot map exactly the way arreflect would - nested structs,
// slices other than []byte, arrays, maps, embedded fields, named scalar types -
// is a generate-time error naming the field, never a silently dropped column.
// Those structs still work with arreflect at runtime.
package arrgen
