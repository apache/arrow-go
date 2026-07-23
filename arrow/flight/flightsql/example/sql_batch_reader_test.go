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

package example

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

var errRowIteration = errors.New("row iteration failed")
var registerFailingRowsDriver sync.Once

type failingRowsDriver struct{}

func (failingRowsDriver) Open(string) (driver.Conn, error) { return failingRowsConn{}, nil }

type failingRowsConn struct{}

func (failingRowsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (failingRowsConn) Close() error { return nil }

func (failingRowsConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func (failingRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &failingRows{}, nil
}

type failingRows struct {
	returnedRow bool
}

func (*failingRows) Columns() []string { return []string{"value"} }

func (*failingRows) Close() error { return nil }

func (r *failingRows) Next(dest []driver.Value) error {
	if r.returnedRow {
		return errRowIteration
	}
	r.returnedRow = true
	dest[0] = int64(1)
	return nil
}

func TestSqlBatchReaderPropagatesRowsError(t *testing.T) {
	const driverName = "arrow-go-failing-rows"
	registerFailingRowsDriver.Do(func() { sql.Register(driverName, failingRowsDriver{}) })
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT value")
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	r, err := NewSqlBatchReaderWithSchema(mem, schema, rows)
	require.NoError(t, err)
	defer r.Release()

	require.False(t, r.Next())
	require.ErrorIs(t, r.Err(), errRowIteration)
	require.Nil(t, r.RecordBatch())
}
