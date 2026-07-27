// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/require"
)

func TestLevelDecoderStopsAtDeclaredRLELength(t *testing.T) {
	// The declared payload contains the run header but not its value byte.
	data := []byte{1, 0, 0, 0, 2, 1}
	var dec LevelDecoder
	_, err := dec.SetData(parquet.Encodings.RLE, 1, 1, data)
	require.NoError(t, err)

	_, _, err = dec.Decode(make([]int16, 1))
	require.Error(t, err)
}

func TestLevelDecoderV2StopsAtDeclaredRLELength(t *testing.T) {
	var dec LevelDecoder
	require.NoError(t, dec.SetDataV2(1, 1, 1, []byte{2, 1}))

	_, _, err := dec.Decode(make([]int16, 1))
	require.Error(t, err)
}

func TestLevelDecoderV2RejectsOversizedDeclaredLength(t *testing.T) {
	var dec LevelDecoder
	require.Error(t, dec.SetDataV2(3, 1, 1, []byte{2, 1}))
}

func TestRLEBooleanDecoderStopsAtDeclaredLength(t *testing.T) {
	dec := NewDecoder(parquet.Types.Boolean, parquet.Encodings.RLE, nil, memory.DefaultAllocator)
	require.NoError(t, dec.SetData(1, []byte{1, 0, 0, 0, 2, 1}))

	_, err := dec.(BooleanDecoder).Decode(make([]bool, 1))
	require.Error(t, err)
}
