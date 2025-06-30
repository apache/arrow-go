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

package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestCommonTemporal(t *testing.T) {
	tests := []struct {
		name     string
		dtList   []arrow.DataType
		expected arrow.DataType
	}{
		{"no input", []arrow.DataType{}, nil},
		{"finest unit time32", []arrow.DataType{
			arrow.FixedWidthTypes.Time32ms,
			arrow.FixedWidthTypes.Time32s,
		}, arrow.FixedWidthTypes.Time32ms},
		{"time32 -> time64", []arrow.DataType{
			arrow.FixedWidthTypes.Time32s,
			arrow.FixedWidthTypes.Time64us,
			arrow.FixedWidthTypes.Time32ms,
		}, arrow.FixedWidthTypes.Time64us},
		{"timestamp units", []arrow.DataType{
			arrow.FixedWidthTypes.Date32,
			arrow.FixedWidthTypes.Timestamp_ms,
		}, arrow.FixedWidthTypes.Timestamp_ms},
		{"duration units", []arrow.DataType{
			arrow.FixedWidthTypes.Duration_s,
			arrow.FixedWidthTypes.Duration_ns,
			arrow.FixedWidthTypes.Duration_us,
		}, arrow.FixedWidthTypes.Duration_ns},
		{"date32 only", []arrow.DataType{
			arrow.FixedWidthTypes.Date32,
			arrow.FixedWidthTypes.Date32,
		}, arrow.FixedWidthTypes.Date32},
		{"date64", []arrow.DataType{
			arrow.FixedWidthTypes.Date32,
			arrow.FixedWidthTypes.Date64,
		}, arrow.FixedWidthTypes.Date64},
		{"ts, date, time", []arrow.DataType{
			arrow.FixedWidthTypes.Timestamp_s,
			arrow.FixedWidthTypes.Date32,
			arrow.FixedWidthTypes.Time64ns,
		}, nil},
		{"date64, duration", []arrow.DataType{
			arrow.FixedWidthTypes.Date64,
			arrow.FixedWidthTypes.Duration_ms,
		}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := commonTemporal(tt.dtList...)
			assert.Truef(t, arrow.TypeEqual(tt.expected, actual),
				"got: %s, expected: %s", actual, tt.expected)
		})
	}
}
