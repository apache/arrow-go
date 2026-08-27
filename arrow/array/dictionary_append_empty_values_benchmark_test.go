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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkDictionaryBuilderAppendEmptyValues(b *testing.B) {
	valueTypes := []struct {
		name string
		typ  arrow.DataType
	}{
		{"int32", arrow.PrimitiveTypes.Int32},
		{"string", arrow.BinaryTypes.String},
	}

	for _, valueType := range valueTypes {
		valueType := valueType
		b.Run(valueType.name, func(b *testing.B) {
			for _, count := range []int{1, 64, 4096, 65536} {
				count := count
				b.Run(fmt.Sprintf("count-%d", count), func(b *testing.B) {
					mem := memory.NewGoAllocator()
					bldr := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{
						IndexType: arrow.PrimitiveTypes.Int32,
						ValueType: valueType.typ,
					})
					defer bldr.Release()

					bldr.AppendEmptyValues(count)
					bldr.Resize(0)

					b.ReportAllocs()
					b.SetBytes(int64(count))
					b.ResetTimer()
					for b.Loop() {
						bldr.Resize(0)
						bldr.AppendEmptyValues(count)
					}
				})
			}
		})
	}
}
