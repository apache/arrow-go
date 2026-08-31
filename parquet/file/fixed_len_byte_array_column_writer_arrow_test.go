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

package file

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestFixedLenByteArraySupportsArrowValuesBatchLimit(t *testing.T) {
	for _, tc := range []struct {
		byteWidth int32
		limit     int64
	}{
		{byteWidth: 3, limit: 153391689},
		{byteWidth: 1 << 20, limit: 1023},
		{byteWidth: 1 << 30, limit: 1},
	} {
		byteWidth, limit := tc.byteWidth, tc.limit
		for _, batchSize := range []int64{-1, 0, limit, limit + 1} {
			t.Run(fmt.Sprintf("width-%d/batch-%d", byteWidth, batchSize), func(t *testing.T) {
				node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, -1, byteWidth)
				require.NoError(t, err)
				descr := schema.NewColumn(node, 0, 0)
				enc := encoding.NewEncoder(parquet.Types.FixedLenByteArray, parquet.Encodings.Plain, false, descr, memory.DefaultAllocator)
				defer enc.Release()
				writer := &FixedLenByteArrayColumnChunkWriter{columnWriter: columnWriter{
					descr:          descr,
					props:          parquet.NewWriterProperties(parquet.WithBatchSize(batchSize)),
					currentEncoder: enc,
				}}
				require.Equal(t, batchSize == limit, writer.SupportsArrowValues())
				if batchSize != limit {
					_, err := writer.WriteBatchArrow(nil, nil, nil)
					require.ErrorContains(t, err, "does not support Arrow values")
					_, err = writer.WriteBatchSpacedArrow(nil, nil, nil, nil, 0)
					require.ErrorContains(t, err, "does not support Arrow values")
				}
			})
		}
	}
}
