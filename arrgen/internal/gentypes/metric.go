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

package gentypes

import "time"

// Metric is the small, readable fixture behind arrgen's package examples: a
// date column, two scalar columns, a nullable pointer column, and a field kept
// out of Arrow entirely. metric_arrow.go next to it is what arrgen emitted from
// these tags, committed as stringer or easyjson output would be.
type Metric struct {
	Day    time.Time `arrow:"day,date32"`
	Host   string    `arrow:"host"`
	CPU    float64   `arrow:"cpu"`
	Value  *float64  `arrow:"value"` // nullable: a nil pointer appends null
	Secret string    `arrow:"-"`     // never leaves the process
}

//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen -type Metric -header ../../license_header.txt
