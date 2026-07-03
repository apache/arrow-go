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
	"bytes"
	"encoding/binary"
	"testing"

	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/require"
)

func TestReadLevelDataRLELengthBound(t *testing.T) {
	p := &serializedPageReader{maxDefLevel: 1}

	// a complete 8-byte RLE level region, so the reject below is the bound, not EOF
	data := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(data[:4], 8)

	_, err := p.readLevelData(bytes.NewReader(data), format.Encoding_RLE, format.Encoding_RLE, 4, 8)
	require.Error(t, err)

	out, err := p.readLevelData(bytes.NewReader(data), format.Encoding_RLE, format.Encoding_RLE, 4, 1024)
	require.NoError(t, err)
	require.Len(t, out, 12)
}
