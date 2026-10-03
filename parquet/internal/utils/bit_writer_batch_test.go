// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestBitWriterWriteValuesMatchesScalar(t *testing.T) {
	const nvalues = 17
	for width := uint(0); width <= 64; width++ {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			mask := uint64(math.MaxUint64)
			if width < 64 {
				mask = (uint64(1) << width) - 1
			}
			values := make([]uint64, nvalues)
			for i := range values {
				values[i] = (uint64(i)*0x9e3779b97f4a7c15 + uint64(i/3)) & mask
			}

			outputSize := int(bitutil.BytesForBits(int64(3+width*nvalues))) + 8
			scalarOutput := make([]byte, outputSize)
			batchOutput := make([]byte, outputSize)
			scalar := utils.NewBitWriter(utils.NewWriterAtBuffer(scalarOutput))
			batch := utils.NewBitWriter(utils.NewWriterAtBuffer(batchOutput))
			require.NoError(t, scalar.WriteValue(5, 3))
			require.NoError(t, batch.WriteValue(5, 3))
			for _, value := range values {
				require.NoError(t, scalar.WriteValue(value, width))
			}
			require.NoError(t, batch.WriteValues(values, width))
			scalar.Flush(false)
			batch.Flush(false)
			require.Equal(t, scalarOutput[:scalar.Written()], batchOutput[:batch.Written()])
		})
	}
}
