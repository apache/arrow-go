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

package pqarrow

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowTimestampToImpalaTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		unit  arrow.TimeUnit
		value arrow.Timestamp
	}{
		{"seconds after epoch", arrow.Second, 946_684_801},
		{"seconds before epoch", arrow.Second, -1},
		{"milliseconds after epoch", arrow.Millisecond, 946_684_800_001},
		{"milliseconds before epoch", arrow.Millisecond, -1},
		{"microseconds after epoch", arrow.Microsecond, 946_684_800_000_001},
		{"microseconds before epoch", arrow.Microsecond, -1},
		{"nanoseconds after epoch", arrow.Nanosecond, 946_684_800_000_000_001},
		{"nanoseconds before epoch", arrow.Nanosecond, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got parquet.Int96
			arrowTimestampToImpalaTimestamp(tt.unit, int64(tt.value), &got)

			assert.Equal(t, tt.value.ToTime(tt.unit), got.ToTime())
		})
	}
}

func TestReadInt96RejectsInvalidTimestamp(t *testing.T) {
	nanosPerDay := uint64(24 * time.Hour)
	tests := []struct {
		name    string
		corrupt parquet.Int96
	}{
		{
			name:    "zero value",
			corrupt: parquet.NewInt96([3]uint32{0, 0, 0}),
		},
		{
			name:    "julian day out of range",
			corrupt: parquet.NewInt96([3]uint32{0, 0, ^uint32(0)}),
		},
		{
			name: "nanoseconds at end of day",
			corrupt: parquet.NewInt96([3]uint32{
				uint32(nanosPerDay),
				uint32(nanosPerDay >> 32),
				0,
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := readCorruptInt96(t, tt.corrupt)
			require.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func readCorruptInt96(t *testing.T, corrupt parquet.Int96) error {
	t.Helper()
	mem := memory.NewGoAllocator()
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	sc := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: timestampType}}, nil)
	builder := array.NewTimestampBuilder(mem, timestampType)
	builder.Append(0)
	record := array.NewRecordBatch(sc, []arrow.Array{builder.NewArray()}, 1)
	builder.Release()
	defer record.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(
		sc,
		&buf,
		parquet.NewWriterProperties(
			parquet.WithDictionaryDefault(false),
			parquet.WithEncodingFor("ts", parquet.Encodings.Plain),
		),
		NewArrowWriterProperties(WithDeprecatedInt96Timestamps(true)),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	var valid parquet.Int96
	arrowTimestampToImpalaTimestamp(arrow.Nanosecond, 0, &valid)
	encoded := buf.Bytes()
	idx := bytes.Index(encoded, valid[:])
	require.GreaterOrEqual(t, idx, 0)
	copy(encoded[idx:idx+parquet.Int96SizeBytes], corrupt[:])

	fileReader, err := file.NewParquetReader(bytes.NewReader(encoded))
	require.NoError(t, err)
	defer fileReader.Close()

	arrowReader, err := NewFileReader(fileReader, ArrowReadProperties{}, mem)
	require.NoError(t, err)
	columnReader, err := arrowReader.GetColumn(context.Background(), 0)
	require.NoError(t, err)
	defer columnReader.Release()

	_, err = columnReader.NextBatch(1)
	return err
}

func TestReadInt96SkipsNullPhysicalValues(t *testing.T) {
	mem := memory.NewGoAllocator()
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	sc := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: timestampType, Nullable: true}}, nil)
	builder := array.NewTimestampBuilder(mem, timestampType)
	builder.AppendNull()
	builder.Append(0)
	record := array.NewRecordBatch(sc, []arrow.Array{builder.NewArray()}, 2)
	builder.Release()
	defer record.Release()

	var buf bytes.Buffer
	writer, err := NewFileWriter(
		sc,
		&buf,
		parquet.NewWriterProperties(
			parquet.WithDictionaryDefault(false),
			parquet.WithEncodingFor("ts", parquet.Encodings.Plain),
		),
		NewArrowWriterProperties(WithDeprecatedInt96Timestamps(true)),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	fileReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer fileReader.Close()

	arrowReader, err := NewFileReader(fileReader, ArrowReadProperties{}, mem)
	require.NoError(t, err)
	columnReader, err := arrowReader.GetColumn(context.Background(), 0)
	require.NoError(t, err)
	defer columnReader.Release()

	chunked, err := columnReader.NextBatch(2)
	require.NoError(t, err)
	defer chunked.Release()

	values := chunked.Chunk(0).(*array.Timestamp)
	assert.True(t, values.IsNull(0))
	assert.Equal(t, arrow.Timestamp(0), values.Value(1))
}
