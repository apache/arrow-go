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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/stretchr/testify/assert"
)

const jsondata = `
	{"region": "NY", "model": "3", "sales": 742.0}
	{"region": "NY", "model": "S", "sales": 304.125}
	{"region": "NY", "model": "X", "sales": 136.25}
	{"region": "NY", "model": "Y", "sales": 27.5}
	{"region": "CA", "model": "3", "sales": 512}
	{"region": "CA", "model": "S", "sales": 978}
	{"region": "CA", "model": "X", "sales": 1.0}
	{"region": "CA", "model": "Y", "sales": 69}
	{"region": "QC", "model": "3", "sales": 273.5}
	{"region": "QC", "model": "S", "sales": 13}
	{"region": "QC", "model": "X", "sales": 54}
	{"region": "QC", "model": "Y", "sales": 21}
	{"region": "QC", "model": "3", "sales": 152.25}
	{"region": "QC", "model": "S", "sales": 10}
	{"region": "QC", "model": "X", "sales": 42}
	{"region": "QC", "model": "Y", "sales": 37}`

func TestJSONReader(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema)
	defer rdr.Release()

	n := 0
	for rdr.Next() {
		n++
		rec := rdr.RecordBatch()
		assert.NotNil(t, rec)
		assert.EqualValues(t, 1, rec.NumRows())
		assert.EqualValues(t, 3, rec.NumCols())
	}

	assert.NoError(t, rdr.Err())
	assert.Equal(t, 16, n)
}

func TestJSONReaderAll(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema, array.WithAllocator(mem), array.WithChunk(-1))
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	assert.NotNil(t, rec)
	assert.NoError(t, rdr.Err())

	assert.EqualValues(t, 16, rec.NumRows())
	assert.EqualValues(t, 3, rec.NumCols())
	assert.False(t, rdr.Next())
}

func TestJSONReaderChunked(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema, array.WithAllocator(mem), array.WithChunk(4))
	defer rdr.Release()

	n := 0
	for rdr.Next() {
		n++
		rec := rdr.RecordBatch()
		assert.NotNil(t, rec)
		assert.NoError(t, rdr.Err())
		assert.EqualValues(t, 4, rec.NumRows())
	}

	assert.Equal(t, 4, n)
	assert.NoError(t, rdr.Err())
}

func TestUnmarshalJSON(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	recordBuilder := array.NewRecordBuilder(mem, schema)
	defer recordBuilder.Release()

	jsondata := `{"region": "NY", "model": "3", "sales": 742.0, "extra": 1234}`

	err := recordBuilder.UnmarshalJSON([]byte(jsondata))
	assert.NoError(t, err)

	record := recordBuilder.NewRecordBatch()
	defer record.Release()

	assert.NotNil(t, record)
}

func TestJSONReaderExponentialNotation(t *testing.T) {
	tests := []struct {
		name     string
		dataType arrow.DataType
		input    string
		expected interface{}
	}{
		{
			name:     "int64 exponential notation",
			dataType: arrow.PrimitiveTypes.Int64,
			input:    `{"value":"6.6999677E+8"}`,
			expected: int64(669996770),
		},
		{
			name:     "uint64 exponential notation",
			dataType: arrow.PrimitiveTypes.Uint64,
			input:    `{"value":"6.6999677E+8"}`,
			expected: uint64(669996770),
		},
		{
			name:     "int64 lowercase exponential",
			dataType: arrow.PrimitiveTypes.Int64,
			input:    `{"value":"1.5e+3"}`,
			expected: int64(1500),
		},
		{
			name:     "uint64 negative exponent",
			dataType: arrow.PrimitiveTypes.Uint64,
			input:    `{"value":"1.5e+3"}`,
			expected: uint64(1500),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "value", Type: tt.dataType, Nullable: true},
			}, nil)

			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			recordBuilder := array.NewRecordBuilder(mem, schema)
			defer recordBuilder.Release()

			err := recordBuilder.UnmarshalJSON([]byte(tt.input))
			if !assert.NoError(t, err, "should parse exponential notation") {
				return
			}

			record := recordBuilder.NewRecordBatch()
			defer record.Release()

			if !assert.Equal(t, int64(1), record.NumRows()) {
				return
			}
			col := record.Column(0)

			switch v := tt.expected.(type) {
			case int64:
				intCol, ok := col.(*array.Int64)
				assert.True(t, ok)
				assert.Equal(t, v, intCol.Value(0))
			case uint64:
				uintCol, ok := col.(*array.Uint64)
				assert.True(t, ok)
				assert.Equal(t, v, uintCol.Value(0))
			}
		})
	}
}

func generateJSONData(n int) []byte {
	records := make([]map[string]any, n)
	for i := range n {
		records[i] = map[string]any{
			"id":       i,
			"name":     fmt.Sprintf("record_%d", i),
			"value":    float64(i) * 1.5,
			"active":   i%2 == 0,
			"metadata": fmt.Sprintf("metadata_%d_%s", i, make([]byte, 500)),
		}
	}

	data, _ := json.Marshal(records)
	return data
}

func jsonArrayToNDJSON(data []byte) ([]byte, error) {
	var records []json.RawMessage
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}

	var ndjson bytes.Buffer
	for _, record := range records {
		ndjson.Write(record)
		ndjson.WriteString("\n")
	}

	return ndjson.Bytes(), nil
}

func BenchmarkRecordFromJSON(b *testing.B) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "metadata", Type: arrow.BinaryTypes.String},
	}, nil)

	testSizes := []int64{1000, 5000, 10000}

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := generateJSONData(int(size))
			pool := memory.NewGoAllocator()

			var rdr bytes.Reader
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				rdr.Reset(data)

				record, _, err := array.RecordFromJSON(pool, schema, &rdr)
				if err != nil {
					b.Error(err)
				}

				if record.NumRows() != size {
					b.Errorf("expected %d rows, got %d", size, record.NumRows())
				}
				record.Release()
			}
		})
	}
}

func BenchmarkJSONReader(b *testing.B) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "metadata", Type: arrow.BinaryTypes.String},
	}, nil)

	testSizes := []int64{1000, 5000, 10000}

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := generateJSONData(int(size))
			data, err := jsonArrayToNDJSON(data)
			if err != nil {
				b.Fatalf("failed to convert JSON to NDJSON: %v", err)
			}

			var rdr bytes.Reader
			for _, chkSize := range []int{-1, int(size / 2), int(size)} {
				b.Run(fmt.Sprintf("ChunkSize_%d", chkSize), func(b *testing.B) {
					pool := memory.NewGoAllocator()
					b.SetBytes(int64(len(data)))
					b.ResetTimer()
					for range b.N {
						rdr.Reset(data)

						jsonRdr := array.NewJSONReader(&rdr, schema, array.WithAllocator(pool),
							array.WithChunk(chkSize))

						var totalRows int64
						for jsonRdr.Next() {
							rec := jsonRdr.RecordBatch()
							totalRows += rec.NumRows()
						}

						if err := jsonRdr.Err(); err != nil {
							b.Errorf("error reading JSON: %v", err)
						}
						jsonRdr.Release()

						if totalRows != size {
							b.Errorf("expected %d rows, got %d", size, totalRows)
						}
					}
				})
			}
		})
	}
}
