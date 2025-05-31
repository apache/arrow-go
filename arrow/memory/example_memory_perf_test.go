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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package memory_test

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_memoryPerf() {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a large Arrow array
	fmt.Println("Creating a large Arrow Int64 array...")
	const N = 10_000_000
	builder := array.NewInt64Builder(pool)
	defer builder.Release()

	for i := 0; i < N; i++ {
		builder.Append(int64(i))
	}
	arr := builder.NewInt64Array()
	defer arr.Release()

	fmt.Printf("Built array of %d elements\n", N)

	// Batch processing
	fmt.Println("\nBatch processing large data in chunks...")
	batchSize := 1_000_000
	for batch := 0; batch < N; batch += batchSize {
		end := batch + batchSize
		if end > N {
			end = N
		}
		b := array.NewInt64Builder(pool)
		for i := batch; i < end; i++ {
			b.Append(int64(i))
		}
		chunk := b.NewInt64Array()
		// Simulate processing
		_ = chunk.Value(0)
		chunk.Release()
		b.Release()
	}
	fmt.Printf("Processed %d elements in batches of %d\n", N, batchSize)

	// Output:
	// Creating a large Arrow Int64 array...
	// Built array of 10000000 elements
	//
	// Batch processing large data in chunks...
	// Processed 10000000 elements in batches of 1000000
}
