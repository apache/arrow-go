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

package array_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderReleaseBuilderAfterPartialReadError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// The first value allocates builder buffers. Rejecting the duplicate key then
	// fails before a record batch is created, leaving cur nil while the builder
	// still owns those buffers.
	rdr := array.NewJSONReader(
		strings.NewReader(`{"value": 1, "value": 2}`),
		schema,
		array.WithAllocator(mem),
	)
	require.False(t, rdr.Next())
	require.Error(t, rdr.Err())

	rdr.Release()
	mem.AssertSize(t, 0)
}
