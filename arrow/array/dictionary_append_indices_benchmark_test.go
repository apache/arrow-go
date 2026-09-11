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

package array_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkDictionaryBuilderAppendIndices(b *testing.B) {
	const (
		length      = 1 << 16
		cardinality = 1 << 8
	)

	indices := make([]int, length)
	valid := make([]bool, length)
	for i := range indices {
		indices[i] = i % cardinality
		valid[i] = i%10 != 0
	}

	dictionaryValues := make([]string, cardinality)
	for i := range dictionaryValues {
		dictionaryValues[i] = strconv.Itoa(i)
	}

	indexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint64,
	}

	for _, indexType := range indexTypes {
		indexType := indexType
		b.Run(fmt.Sprintf("%s/non-null", indexType), func(b *testing.B) {
			benchmarkDictionaryBuilderAppendIndices(b, indexType, indices, nil, dictionaryValues)
		})
		b.Run(fmt.Sprintf("%s/nullable", indexType), func(b *testing.B) {
			benchmarkDictionaryBuilderAppendIndices(b, indexType, indices, valid, dictionaryValues)
		})
	}
}

func benchmarkDictionaryBuilderAppendIndices(b *testing.B, indexType arrow.DataType, indices []int, valid []bool, dictionaryValues []string) {
	mem := memory.NewGoAllocator()
	dictBuilder := array.NewStringBuilder(mem)
	dictBuilder.AppendValues(dictionaryValues, nil)
	dictionary := dictBuilder.NewStringArray()
	dictBuilder.Release()
	defer dictionary.Release()

	builder := array.NewDictionaryBuilderWithDict(mem, &arrow.DictionaryType{
		IndexType: indexType,
		ValueType: arrow.BinaryTypes.String,
	}, dictionary)
	defer builder.Release()

	b.ReportAllocs()
	b.SetBytes(int64(len(indices) * indexType.(arrow.FixedWidthDataType).Bytes()))
	b.ResetTimer()
	for b.Loop() {
		builder.AppendIndices(indices, valid)
		arr := builder.NewDictionaryArray()
		arr.Release()
	}
}
