package memory_test

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func ExampleMemoryPerformance() {
	pool := memory.NewGoAllocator()

	// Create a large Arrow array
	fmt.Println("Creating a large Arrow Int64 array...")
	const N = 10_000_000
	builder := array.NewInt64Builder(pool)
	for i := 0; i < N; i++ {
		builder.Append(int64(i))
	}
	arr := builder.NewInt64Array()
	builder.Release()
	fmt.Printf("Built array of %d elements\n", N)

	// Release memory
	fmt.Println("\nReleasing array memory...")
	arr.Release()
	runtime.GC()

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
		b.Release()
		// Simulate processing
		_ = chunk.Value(0)
		chunk.Release()
	}
	fmt.Printf("Processed %d elements in batches of %d\n", N, batchSize)

	// Output:
	// Creating a large Arrow Int64 array...
	// Built array of 10000000 elements
	//
	// Releasing array memory...
	//
	// Batch processing large data in chunks...
	// Processed 10000000 elements in batches of 1000000
}
