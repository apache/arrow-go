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

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestPlainFixedLenByteArrayEncoder_Put(t *testing.T) {
	sink := NewPooledBufferWriter(0)
	elem := schema.NewFixedLenByteArrayNode("test", parquet.Repetitions.Required, 4, 0)
	descr := schema.NewColumn(elem, 0, 0)
	encoder := &PlainFixedLenByteArrayEncoder{
		encoder: encoder{
			descr: descr,
			sink:  sink,
		},
	}

	tests := []struct {
		name     string
		input    []parquet.FixedLenByteArray
		expected []byte
	}{
		{
			name: "Normal input",
			input: []parquet.FixedLenByteArray{
				[]byte("abcd"),
				[]byte("efgh"),
				[]byte("ijkl"),
			},
			expected: []byte("abcdefghijkl"),
		},
		{
			name: "Input with nil values",
			input: []parquet.FixedLenByteArray{
				[]byte("abcd"),
				nil,
				[]byte("ijkl"),
			},
			expected: []byte("abcd\x00\x00\x00\x00ijkl"), // Nil replaced with zero bytes
		},
		{
			name:     "Empty input",
			input:    []parquet.FixedLenByteArray{},
			expected: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset the sink before each test
			sink.Reset(0)

			// Perform the encoding
			encoder.Put(tt.input)

			// Assert the result
			require.Equal(t, tt.expected, sink.Bytes(), "Encoded bytes should match expected output")
		})
	}
}
