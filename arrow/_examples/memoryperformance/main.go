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

package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// printMem prints current memory usage
func printMem(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("%s: Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB, NumGC = %v\n",
		label, m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
}

func main() {
	pool := memory.NewGoAllocator()
	printMem("Start")

	// Create a large Arrow array
	fmt.Println("\nCreating a large Arrow Int64 array...")
	const N = 10_000_000
	builder := array.NewInt64Builder(pool)
	start := time.Now()
	for i := 0; i < N; i++ {
		builder.Append(int64(i))
	}
	arr := builder.NewInt64Array()
	builder.Release()
	fmt.Printf("Built array of %d elements in %v\n", N, time.Since(start))
	printMem("After array creation")

	// Release memory
	fmt.Println("\nReleasing array memory...")
	arr.Release()
	runtime.GC()
	printMem("After array release and GC")

	// Batch processing
	fmt.Println("\nBatch processing large data in chunks...")
	batchSize := 1_000_000
	start = time.Now()
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
		b.Release()
		// Simulate processing
		_ = chunk.Value(0)
		chunk.Release()
	}
	fmt.Printf("Processed %d elements in batches of %d in %v\n", N, batchSize, time.Since(start))
	printMem("After batch processing")
}
