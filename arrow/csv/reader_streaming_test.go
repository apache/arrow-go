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

package csv_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestCSVReadAllStreaming(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	input := "id,name,value\n1,\"hello, world\",7\n2,\"line\nbreak\",NULL\n3,third,9\n"
	r := csv.NewReader(strings.NewReader(input), schema,
		csv.WithHeader(true), csv.WithChunk(-1), csv.WithNullReader(true, "NULL"))
	defer r.Release()

	require.True(t, r.Next())
	require.NoError(t, r.Err())
	batch := r.RecordBatch()
	require.Equal(t, int64(3), batch.NumRows())

	ids := batch.Column(0).(*array.Int64)
	require.Equal(t, int64(1), ids.Value(0))
	require.Equal(t, int64(3), ids.Value(2))
	names := batch.Column(1).(*array.String)
	require.Equal(t, "hello, world", names.Value(0))
	require.Equal(t, "line\nbreak", names.Value(1))
	values := batch.Column(2).(*array.Int64)
	require.True(t, values.IsNull(1))
	require.Equal(t, int64(9), values.Value(2))

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestCSVReadAllStreamingHeaderlessInference(t *testing.T) {
	r := csv.NewInferringReader(strings.NewReader("4,alpha\n5,beta\n"),
		csv.WithHeader(false), csv.WithChunk(-1))
	defer r.Release()

	require.True(t, r.Next())
	require.NoError(t, r.Err())
	require.Equal(t, "f0", r.Schema().Field(0).Name)
	require.Equal(t, "f1", r.Schema().Field(1).Name)
	require.Equal(t, int64(2), r.RecordBatch().NumRows())
	require.Equal(t, int64(4), r.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.Equal(t, "beta", r.RecordBatch().Column(1).(*array.String).Value(1))
	require.False(t, r.Next())
}

func TestCSVReadAllStreamingEmpty(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	r := csv.NewReader(strings.NewReader(""), schema, csv.WithChunk(-1))
	defer r.Release()

	require.True(t, r.Next())
	require.NoError(t, r.Err())
	require.Equal(t, int64(0), r.RecordBatch().NumRows())
	require.False(t, r.Next())
}

func TestCSVReadAllStreamingMalformedRow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	r := csv.NewReader(strings.NewReader("1,ok\n2,\"unterminated\n"), schema,
		csv.WithChunk(-1), csv.WithAllocator(mem))
	defer func() {
		r.Release()
		mem.AssertSize(t, 0)
	}()

	require.False(t, r.Next())
	require.Error(t, r.Err())
	require.Nil(t, r.RecordBatch())
	require.False(t, r.Next())
}

func TestCSVReadAllStreamingMalformedInferredRow(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	r := csv.NewInferringReader(strings.NewReader("1,ok\n2,\"unterminated\n"),
		csv.WithHeader(false), csv.WithChunk(-1), csv.WithAllocator(mem))
	defer func() {
		r.Release()
		mem.AssertSize(t, 0)
	}()

	require.False(t, r.Next())
	require.Error(t, r.Err())
	require.Nil(t, r.RecordBatch())
	require.False(t, r.Next())
}
func TestCSVReadAllStreamingConversionError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	r := csv.NewReader(strings.NewReader("1,first\ninvalid,second\n3,third\n"), schema, csv.WithChunk(-1))
	defer r.Release()

	require.True(t, r.Next())
	require.Error(t, r.Err())
	batch := r.RecordBatch()
	require.Equal(t, int64(3), batch.NumRows())
	require.True(t, batch.Column(0).IsNull(1))
	require.Equal(t, int64(3), batch.Column(0).(*array.Int64).Value(2))
	require.Equal(t, "second", batch.Column(1).(*array.String).Value(1))
	require.False(t, r.Next())
}

func BenchmarkCSVReadAllStreaming(b *testing.B) {
	const rows = 100000
	var input strings.Builder
	for i := 0; i < rows; i++ {
		input.WriteString(strconv.Itoa(i))
		input.WriteString(",value,")
		input.WriteString(strconv.Itoa(i))
		input.WriteByte('\n')
	}
	data := input.String()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		r := csv.NewReader(strings.NewReader(data), schema, csv.WithChunk(-1))
		if !r.Next() {
			err := r.Err()
			r.Release()
			b.Fatalf("read CSV: %v", err)
		}
		if got := r.RecordBatch().NumRows(); got != rows {
			r.Release()
			b.Fatalf("expected %d rows, got %d", rows, got)
		}
		r.Release()
	}
}
