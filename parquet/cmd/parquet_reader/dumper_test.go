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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
)

type errorColumnReader struct {
	file.ColumnChunkReader
	err error
}

func (r *errorColumnReader) HasNext() bool { return false }
func (r *errorColumnReader) Err() error    { return r.err }

func TestDumperHasNextReportsReaderErrors(t *testing.T) {
	want := errors.New("page read failed")
	dump := &Dumper{reader: &errorColumnReader{err: want}}

	hasNext, err := dump.hasNext()
	require.False(t, hasNext)
	require.ErrorIs(t, err, want)
}
