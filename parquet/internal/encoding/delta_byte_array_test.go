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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeltaByteArrayDecoder_SetData(t *testing.T) {
	tests := []struct {
		name    string
		nvalues int
		data    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "null only page",
			nvalues: 126609,
			data:    []byte{128, 1, 4, 0, 0},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		d := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaLengthByteArray, nil, memory.DefaultAllocator)
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, d.SetData(tt.nvalues, tt.data), fmt.Sprintf("SetData(%v, %v)", tt.nvalues, tt.data))
		})
	}
}

func TestDeltaByteArrayEncoderPreservesLastValueAcrossBatches(t *testing.T) {
	values := make([]parquet.ByteArray, deltaByteArrayBatchSize*2+3)
	for i := range values {
		values[i] = parquet.ByteArray(fmt.Sprintf("partition-%03d/value-%03d", i/7, i%7))
	}
	values[0] = parquet.ByteArray{}
	values[deltaByteArrayBatchSize-1] = parquet.ByteArray("boundary/repeated")
	values[deltaByteArrayBatchSize] = parquet.ByteArray("boundary/repeated")
	values[deltaByteArrayBatchSize+1] = parquet.ByteArray("boundary")
	values[deltaByteArrayBatchSize*2] = parquet.ByteArray("boundary")

	encode := func(batches ...[]parquet.ByteArray) []byte {
		t.Helper()
		enc := NewEncoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, false, nil, memory.DefaultAllocator).(ByteArrayEncoder)
		defer enc.Release()
		for _, batch := range batches {
			enc.Put(batch)
		}
		buf, err := enc.FlushValues()
		require.NoError(t, err)
		defer buf.Release()
		return append([]byte(nil), buf.Bytes()...)
	}

	want := encode(values)
	for _, split := range []int{0, 1, deltaByteArrayBatchSize - 1, deltaByteArrayBatchSize,
		deltaByteArrayBatchSize + 1, deltaByteArrayBatchSize*2 - 1, deltaByteArrayBatchSize * 2,
		len(values) - 1, len(values)} {
		t.Run(fmt.Sprintf("split-%d", split), func(t *testing.T) {
			got := encode(values[:split], values[split:])
			require.Equal(t, want, got)

			dec := NewDecoder(parquet.Types.ByteArray, parquet.Encodings.DeltaByteArray, nil, memory.DefaultAllocator).(ByteArrayDecoder)
			require.NoError(t, dec.SetData(len(values), got))
			out := make([]parquet.ByteArray, len(values))
			decoded, err := dec.Decode(out)
			require.NoError(t, err)
			require.Equal(t, len(values), decoded)
			for i := range values {
				assert.Equal(t, string(values[i]), string(out[i]), "value %d", i)
			}
		})
	}
}
