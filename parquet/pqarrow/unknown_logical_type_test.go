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

package pqarrow_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unknown-logical-type.parquet was written with a logical type id no reader knows yet.
// An older reader has to ignore it and read the column as its physical type.
func TestReadUnknownLogicalType(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	require.DirExists(t, dir)

	rdr, err := file.OpenParquetFile(path.Join(dir, "unknown-logical-type.parquet"), false)
	require.NoError(t, err)
	defer rdr.Close()

	sc := rdr.MetaData().Schema
	require.Equal(t, 2, sc.NumColumns())
	assert.Equal(t, parquet.Types.ByteArray, sc.Column(1).PhysicalType())
	assert.True(t, sc.Column(1).LogicalType().IsNone())

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	require.EqualValues(t, 3, tbl.NumRows())
	require.EqualValues(t, 2, tbl.NumCols())

	known := tbl.Column(0).Data().Chunk(0)
	assert.Equal(t, arrow.STRING, known.DataType().ID())
	for i, want := range []string{"known string 1", "known string 2", "known string 3"} {
		assert.Equal(t, want, known.(*array.String).Value(i))
	}

	unknown := tbl.Column(1).Data().Chunk(0)
	assert.Equal(t, arrow.BINARY, unknown.DataType().ID())
	for i, want := range []string{"unknown string 1", "unknown string 2", "unknown string 3"} {
		assert.Equal(t, want, string(unknown.(*array.Binary).Value(i)))
	}
}
