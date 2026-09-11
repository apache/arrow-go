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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlainDecoderDecodeSpaced(t *testing.T) {
	cases := []struct {
		name   string
		valid  []bool
		offset int64
	}{
		{
			name:  "leading null",
			valid: []bool{false, true, true, true, true, true},
		},
		{
			name:  "internal null",
			valid: []bool{true, true, false, true, true, true},
		},
		{
			name:  "trailing null",
			valid: []bool{true, true, true, true, true, false},
		},
		{
			name:  "clustered nulls",
			valid: []bool{true, true, false, false, false, true, true, true},
		},
		{
			name:  "fragmented validity",
			valid: []bool{true, false, true, false, true, false, true, false, true, false, true, false},
		},
		{
			name:  "highly fragmented validity",
			valid: []bool{true, false, true, false, true, false, true, false, true, false, true, false, true, false, true, false, true, false, true, false},
		},
		{
			name:   "offset",
			valid:  []bool{false, true, true, false, true, true},
			offset: 3,
		},
		{
			name:  "all null",
			valid: []bool{false, false, false, false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validBits := make([]byte, bitutil.BytesForBits(tc.offset+int64(len(tc.valid))))
			physical := make([]int32, 0, len(tc.valid))
			nullCount := 0
			for i, valid := range tc.valid {
				if valid {
					bitutil.SetBit(validBits, int(tc.offset)+i)
					physical = append(physical, int32(i))
				} else {
					nullCount++
				}
			}

			enc := NewEncoder(parquet.Types.Int32, parquet.Encodings.Plain, false, nil, memory.DefaultAllocator).(Int32Encoder)
			enc.Put(physical)
			data, err := enc.FlushValues()
			require.NoError(t, err)
			defer data.Release()

			dec := NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator).(Int32Decoder)
			require.NoError(t, dec.SetData(len(physical), data.Bytes()))

			out := make([]int32, len(tc.valid))
			for i := range out {
				out[i] = -1
			}

			n, err := dec.DecodeSpaced(out, nullCount, validBits, tc.offset)
			require.NoError(t, err)
			assert.Equal(t, len(tc.valid), n)
			for i, valid := range tc.valid {
				if valid {
					assert.Equal(t, int32(i), out[i])
				}
			}
		})
	}
}
