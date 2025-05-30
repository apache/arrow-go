package csv_test

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
)

func ExampleReader() {
	filePath := "../../arrow-testing/data/csv/aggregate_test_100.csv" // Test csv file
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer f.Close()

	// Schema defined in the csv file
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c3", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c4", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c5", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c6", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c7", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c9", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c10", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "c11", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "c12", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "c13", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	reader := arrowcsv.NewReader(f, schema, arrowcsv.WithHeader(true), arrowcsv.WithChunk(-1))
	defer reader.Release()

	ok := reader.Next()
	if !ok {
		if err := reader.Err(); err != nil {
			fmt.Printf("Error reading CSV: %v\n", err)
			return
		}
		fmt.Println("No records found")
		return
	}

	record := reader.Record()
	defer record.Release()

	fmt.Printf("Number of rows: %d\n", record.NumRows())
	fmt.Printf("Number of columns: %d\n", record.NumCols())
	fmt.Println()

	fmt.Println("Basic statistics for numeric columns:")
	for i := 1; i < 10; i++ { // cols c2 through c10 are Int64
		col := record.Column(i).(*array.Int64)
		var sum int64
		for j := 0; j < col.Len(); j++ {
			sum += col.Value(j)
		}
		avg := float64(sum) / float64(col.Len())
		fmt.Printf("Column c%d: Average = %.2f\n", i+1, avg)
	}

	for i := 10; i < 12; i++ { // cols c11 and c12 are Float64
		col := record.Column(i).(*array.Float64)
		var sum float64
		for j := 0; j < col.Len(); j++ {
			sum += col.Value(j)
		}
		avg := sum / float64(col.Len())
		fmt.Printf("Column c%d: Average = %.4f\n", i+1, avg)
	}

	// Output:
	// Number of rows: 100
	// Number of columns: 13
	//
	// Basic statistics for numeric columns:
	// Column c2: Average = 2.85
	// Column c3: Average = 7.81
	// Column c4: Average = 2319.97
	// Column c5: Average = 158626279.61
	// Column c6: Average = 59276376114661656.00
	// Column c7: Average = 130.60
	// Column c8: Average = 30176.41
	// Column c9: Average = 2220897700.60
	// Column c10: Average = -86834033398685392.00
	// Column c11: Average = 0.4793
	// Column c12: Average = 0.5090
}
