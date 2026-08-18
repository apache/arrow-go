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

package encoding

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestDeltaLengthByteArrayEncoderPreservesBatches(t *testing.T) {
	for _, nvalues := range []int{255, 256, 257, 511, 512, 513} {
		nvalues := nvalues
		t.Run(fmt.Sprintf("%d-values", nvalues), func(t *testing.T) {
			values := make([]parquet.ByteArray, nvalues)
			for i := range values {
				values[i] = parquet.ByteArray(fmt.Sprintf("value-%d", i))
			}

			encode := func(batches ...[]parquet.ByteArray) []byte {
				t.Helper()
				enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
				defer enc.Release()
				for _, batch := range batches {
					enc.Put(batch)
				}
				buf, err := enc.FlushValues()
				require.NoError(t, err)
				defer buf.Release()
				return append([]byte(nil), buf.Bytes()...)
			}

			roundTrip := func(data []byte) {
				t.Helper()
				dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray,
					nil, memory.DefaultAllocator).(ByteArrayDecoder)
				require.NoError(t, dec.SetData(nvalues, data))
				out := make([]parquet.ByteArray, nvalues)
				decoded, err := dec.Decode(out)
				require.NoError(t, err)
				require.Equal(t, nvalues, decoded)
				require.Equal(t, values, out)
			}

			want := encode(values)
			splits := []int{1, nvalues / 2, nvalues - 1}
			for _, boundary := range []int{deltaByteArrayBatchSize - 1, deltaByteArrayBatchSize, deltaByteArrayBatchSize + 1} {
				if boundary > 0 && boundary < nvalues {
					splits = append(splits, boundary)
				}
			}
			for _, split := range splits {
				split := split
				t.Run(fmt.Sprintf("split-%d", split), func(t *testing.T) {
					got := encode(values[:split], values[split:])
					require.Equal(t, want, got)
					roundTrip(got)
				})
			}
		})
	}
}
