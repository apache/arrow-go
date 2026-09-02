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
// reads the same tags once, at generate time, and emits typed Go source that
// appends struct fields into typed Arrow builders. For every struct arrgen
// accepts, the generated schema equals arreflect.InferSchema and the generated
// record batch equals the one arreflect.RecordFromSlice builds.
//
// # Usage
//
// Put a go:generate directive next to the struct:
//
//	//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen -type Metric
//
// then run `go generate ./...`, which writes metric_arrow.go next to the type.
// The emitted code imports only arrow, arrow/array and arrow/memory, never
// arrgen itself.
//
// # Supported field types
//
// bool, int8/16/32/64, int, uint8/16/32/64, uint, float32/64, string, []byte,
// time.Time, time.Duration, decimal.Decimal32, decimal.Decimal64,
// decimal128.Num and decimal256.Num, plus a pointer to any of those for a
// nullable column.
//
// Tag options mirror arreflect: a leading name, "-" to skip a field, the
// temporal overrides date32, date64, time32, time64 and timestamp, dict, view,
// large, and decimal(precision,scale).
//
// Anything else - nested structs, slices other than []byte, arrays, maps,
// embedded fields, named scalar types - is a generate-time error naming the
// field, never a silently dropped column.
package arrgen
