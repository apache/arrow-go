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

package metadata_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/stretchr/testify/require"
)

func TestRowGroupColumnIndexBounds(t *testing.T) {
	rowGroup := metadata.NewRowGroupMetaData(
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{{}}},
		nil,
		nil,
		nil,
	)

	_, err := rowGroup.ColumnChunk(-1)
	require.ErrorIs(t, err, arrow.ErrIndex)
	_, err = rowGroup.ColumnChunk(1)
	require.ErrorIs(t, err, arrow.ErrIndex)

	_, ok := rowGroup.ColumnIndexLocation(-1)
	require.False(t, ok)
	_, ok = rowGroup.ColumnIndexLocation(1)
	require.False(t, ok)

	_, ok = rowGroup.OffsetIndexLocation(-1)
	require.False(t, ok)
	_, ok = rowGroup.OffsetIndexLocation(1)
	require.False(t, ok)
}
