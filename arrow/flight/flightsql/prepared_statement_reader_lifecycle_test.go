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

package flightsql_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestPreparedStatementReleasesRecordReaderBindingOnce(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	rec, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[{"id": 1}]`))
	require.NoError(t, err)

	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)

	prepared := flightsql.NewPreparedStatement(&flightsql.Client{}, nil)
	prepared.SetRecordReader(rdr)

	// Drop the caller-owned references. Replacing the binding must release the
	// single reference retained by the prepared statement and free the record.
	rdr.Release()
	rec.Release()
	prepared.SetParameters(nil)

	mem.AssertSize(t, 0)
}
