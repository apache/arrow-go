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

// Package pregen is the state a package is in the first time the generator runs
// against it: the call sites exist, the generated file does not, and so the
// package does not type-check.
package pregen

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Sample struct {
	At    time.Time `arrow:"at"`
	Value float64   `arrow:"value"`
}

// Encode calls into the file arrgen has not written yet.
func Encode(mem memory.Allocator, samples []Sample) (arrow.RecordBatch, error) {
	return SampleRecordBatch(mem, samples)
}
