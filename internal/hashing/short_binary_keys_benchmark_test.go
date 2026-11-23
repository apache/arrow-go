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

package hashing_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/hashing"
)

func BenchmarkBinaryMemoTableShortKeys(b *testing.B) {
	const batchSize = 1 << 16
	const distinct = 256
	keys := make([][4]byte, distinct)
	table := hashing.NewBinaryMemoTable(distinct, distinct*4,
		array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary))
	defer table.Release()
	for i := range keys {
		keys[i] = [4]byte{'k', '0' + byte(i/100), '0' + byte(i/10%10), '0' + byte(i%10)}
		if _, _, err := table.InsertOrGet(keys[i][:]); err != nil {
			b.Fatal(err)
		}
	}
	indices := make([]byte, batchSize)
	state := uint32(1)
	for i := range indices {
		state = state*1664525 + 1013904223
		indices[i] = byte(state >> 24)
	}
	b.ReportAllocs()
	b.SetBytes(batchSize * 4)
	for b.Loop() {
		for _, index := range indices {
			_, _, _ = table.InsertOrGet(keys[index][:])
		}
	}
}
