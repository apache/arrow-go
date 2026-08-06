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

//go:build go1.18

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemporalOddMultipleRounding(t *testing.T) {
	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	defer builder.Release()
	builder.AppendValues([]arrow.Timestamp{1, 2, -1, -2}, nil)

	input := builder.NewArray()
	defer input.Release()

	result, err := compute.RoundTemporal(context.Background(), compute.RoundTemporalOptions{
		Multiple: 3,
		Unit:     compute.RoundTemporalSecond,
	}, compute.NewDatum(input))
	require.NoError(t, err)
	defer result.Release()

	output := result.(*compute.ArrayDatum).MakeArray().(*array.Timestamp)
	defer output.Release()

	assert.Equal(t, []arrow.Timestamp{0, 3, 0, -3}, output.Values())
}
