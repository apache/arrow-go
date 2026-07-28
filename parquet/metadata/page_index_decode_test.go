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

package metadata

import (
	"bytes"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeserializePageIndexReturnsErrors(t *testing.T) {
	descr := schema.NewColumn(schema.NewInt32Node("values", parquet.Repetitions.Required, -1), 0, 0)

	_, err := deserializeColumnIndex(descr, []byte{0xff}, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, arrow.ErrInvalid))

	_, err = deserializeOffsetIndex([]byte{0xff}, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, arrow.ErrInvalid))
}

func TestRowGroupPageIndexReaderReturnsDecodeErrors(t *testing.T) {
	meta := constructFakeMetadata([]PageIndexRanges{{
		ColIndexOffset: 0, ColIndexLen: 1,
		OffsetIndexOffset: 1, OffsetIndexLen: 1,
	}})
	reader := &PageIndexReader{
		Input:        bytes.NewReader([]byte{0xff, 0xff}),
		FileMetadata: meta,
		Props:        parquet.NewReaderProperties(nil),
	}
	rgReader, err := reader.RowGroup(0)
	require.NoError(t, err)
	require.NotNil(t, rgReader)

	_, err = rgReader.GetColumnIndex(0)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assertCausePreserved(t, err)

	_, err = rgReader.GetOffsetIndex(0)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assertCausePreserved(t, err)
}

func assertCausePreserved(t *testing.T, err error) {
	t.Helper()
	unwrapper, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok)
	require.Len(t, unwrapper.Unwrap(), 2)
}

func TestDeserializeColumnIndexRepanicsRuntimeErrors(t *testing.T) {
	var serialized bytes.Buffer
	_, err := thrift.NewThriftSerializer().Serialize(&format.ColumnIndex{}, &serialized, nil)
	require.NoError(t, err)

	assert.Panics(t, func() {
		deserializeColumnIndex(nil, serialized.Bytes(), nil)
	})
}
