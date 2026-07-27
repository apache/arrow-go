// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqarrow

import (
	"errors"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteCoerceTimestampsOverflow(t *testing.T) {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	defer b.Release()
	b.AppendValues([]arrow.Timestamp{arrow.Timestamp(math.MaxInt64/1000 + 1), arrow.Timestamp(math.MinInt64/1000 - 1)}, nil)
	arr := b.NewTimestampArray()
	defer arr.Release()

	out := make([]int64, arr.Len())
	props := NewArrowWriterProperties(WithCoerceTimestamps(arrow.Millisecond))
	err := writeCoerceTimestamps(arr, &props, out)
	require.Error(t, err)
	assert.True(t, errors.Is(err, arrow.ErrInvalid))
}

func TestWriteCoerceTimestampsIgnoresNullOverflow(t *testing.T) {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Second})
	defer b.Release()
	b.AppendValues([]arrow.Timestamp{1, arrow.Timestamp(math.MaxInt64)}, []bool{true, false})
	arr := b.NewTimestampArray()
	defer arr.Release()

	out := make([]int64, arr.Len())
	props := NewArrowWriterProperties(WithCoerceTimestamps(arrow.Millisecond))
	require.NoError(t, writeCoerceTimestamps(arr, &props, out))
	assert.Equal(t, int64(1000), out[0])
}
