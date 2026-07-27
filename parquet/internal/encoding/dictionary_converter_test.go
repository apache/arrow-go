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
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestDictionaryConverterBoundsGrowthByValuesLeft(t *testing.T) {
	decoder := NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
	require.NoError(t, decoder.SetData(1, []byte{42, 0, 0, 0}))
	converter := NewDictConverter[int32](decoder).(*dictConverter[int32])

	require.Zero(t, cap(converter.dict))
	require.Equal(t, 1, converter.dictLen)
	require.Error(t, converter.ensure(utils.IndexType(1<<30)))
	require.Zero(t, cap(converter.dict))

	require.NoError(t, converter.ensure(0))
	require.Equal(t, []int32{42}, converter.dict)
}
