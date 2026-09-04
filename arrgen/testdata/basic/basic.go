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

// Package basic is the fixture behind arrgen's golden test. It is under
// testdata so the go tool does not build it as part of the module; the
// generator loads it by directory instead.
package basic

import "time"

// Metric is an exported type, so its generated API is exported too.
type Metric struct {
	Day   time.Time `arrow:"day,date64"`
	Host  string    `arrow:"host,dict"`
	Count *int64    `arrow:"count"`
	Blob  []byte
	Local string `arrow:"-"`
}

// reading is unexported, so the generated appender is unexported as well.
type reading struct {
	At    time.Time `arrow:"at,date32"`
	Value float64   `arrow:"value"`
}
