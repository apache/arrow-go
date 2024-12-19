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

package metadata

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type PageIndexRanges struct {
	ColIndexOffset, OffsetIndexOffset int64
	ColIndexLen, OffsetIndexLen       int32
}

func constructFakeMetadata(rowGroupRanges []PageIndexRanges) *FileMetaData {
	var rg format.RowGroup
	for _, r := range rowGroupRanges {
		var colChunk format.ColumnChunk
		colChunk.MetaData = &format.ColumnMetaData{}
		if r.ColIndexOffset != -1 {
			colChunk.ColumnIndexOffset = &r.ColIndexOffset
		}
		if r.ColIndexLen != -1 {
			colChunk.ColumnIndexLength = &r.ColIndexLen
		}
		if r.OffsetIndexOffset != -1 {
			colChunk.OffsetIndexOffset = &r.OffsetIndexOffset
		}
		if r.OffsetIndexLen != -1 {
			colChunk.OffsetIndexLength = &r.OffsetIndexLen
		}
		rg.Columns = append(rg.Columns, &colChunk)
	}

	var fileMeta format.FileMetaData
	fileMeta.RowGroups = append(fileMeta.RowGroups, &rg)

	fields := make(schema.FieldList, 0)
	for i := range rowGroupRanges {
		fields = append(fields, schema.NewInt64Node(strconv.Itoa(i), parquet.Repetitions.Optional, -1))
	}

	grpNode, _ := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	fileMeta.Schema = schema.ToThrift(grpNode)

	var buf bytes.Buffer
	thrift.NewThriftSerializer().Serialize(&fileMeta, &buf, nil)

	meta, _ := NewFileMetaData(buf.Bytes(), nil)
	return meta
}

// validates that determinePagteIndexRangesInRowGroup selects the expected
// file offsets and sizes or returns false when the row group doesn't have
// a page index
func validatePageIndexRange(t *testing.T, ranges []PageIndexRanges, colIndices []int32, expectedHasColIndex, expectedHasOffsetIndex bool, expCIStart, expCISize, expOIStart, expOISize int) {
	fileMeta := constructFakeMetadata(ranges)
	readRange, err := determinePageIndexRangesInRowGroup(fileMeta.RowGroup(0), colIndices)
	require.NoError(t, err)

	if expectedHasColIndex {
		assert.NotNil(t, readRange.ColIndex)
		assert.EqualValues(t, expCIStart, readRange.ColIndex.Offset)
		assert.EqualValues(t, expCISize, readRange.ColIndex.Length)
	} else {
		assert.Nil(t, readRange.ColIndex)
	}

	if expectedHasOffsetIndex {
		assert.NotNil(t, readRange.OffsetIndex)
		assert.EqualValues(t, expOIStart, readRange.OffsetIndex.Offset)
		assert.EqualValues(t, expOISize, readRange.OffsetIndex.Length)
	} else {
		assert.Nil(t, readRange.OffsetIndex)
	}
}

// construct artificial row groups with page index offsets in them.
// then validate if determinePageIndexRangesInRowGroup properly
// computes the file range that contains the whole page index
func TestDeterminePageIndexRangesInRowGroup(t *testing.T) {
	tests := []struct {
		name                          string
		ranges                        []PageIndexRanges
		cols                          []int32
		expectHasCol, expectHasOffset bool
		expectCIStart, expectCISize   int
		expectOIStart, expectOISize   int
	}{
		{"no col chunks", nil, nil, false, false, -1, -1, -1, -1},
		{"no page index", []PageIndexRanges{{-1, -1, -1, -1}}, nil, false, false, -1, -1, -1, -1},
		{"page index for single col", []PageIndexRanges{{10, 15, 5, 5}}, nil, true, true, 10, 5, 15, 5},
		{"page index for two columns", []PageIndexRanges{{10, 30, 5, 25}, {15, 50, 15, 20}},
			nil, true, true, 10, 20, 30, 40},
		{"page index for second column", []PageIndexRanges{{-1, -1, -1, -1}, {20, 30, 10, 25}}, nil,
			true, true, 20, 10, 30, 25},
		{"page index for first column", []PageIndexRanges{{10, 15, 5, 5}, {-1, -1, -1, -1}}, nil,
			true, true, 10, 5, 15, 5},
		{"missing offset index for first column, gap in col index",
			[]PageIndexRanges{{10, -1, 5, -1}, {20, 30, 10, 25}}, nil, true, true, 10, 20, 30, 25},
		{"missing offset index for second column", []PageIndexRanges{{10, 25, 5, 5}, {20, -1, 10, -1}},
			nil, true, true, 10, 20, 25, 5},
		{"four col chunks",
			[]PageIndexRanges{{100, 220, 10, 30}, {110, 250, 25, 10}, {140, 260, 30, 40}, {200, 300, 10, 100}}, nil,
			true, true, 100, 110, 220, 180},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validatePageIndexRange(t, tt.ranges, tt.cols, tt.expectHasCol, tt.expectHasOffset,
				tt.expectCIStart, tt.expectCISize, tt.expectOIStart, tt.expectOISize)
		})
	}
}

// properly compute file range that contains the page index of selected columns
func TestDeterminePageIndexRangesInRowGroupWithPartialColumnsSelected(t *testing.T) {
	tests := []struct {
		name                          string
		ranges                        []PageIndexRanges
		cols                          []int32
		expectHasCol, expectHasOffset bool
		expectCIStart, expectCISize   int
		expectOIStart, expectOISize   int
	}{
		{"no page index", []PageIndexRanges{{-1, -1, -1, -1}}, []int32{0}, false, false, -1, -1, -1, -1},
		{"page index for single col", []PageIndexRanges{{10, 15, 5, 5}}, []int32{0}, true, true, 10, 5, 15, 5},
		{"page index for the 1st col", []PageIndexRanges{{10, 30, 5, 25}, {15, 50, 15, 20}}, []int32{0},
			true, true, 10, 5, 30, 25},
		{"page index for the 2nd col", []PageIndexRanges{{10, 30, 5, 25}, {15, 50, 15, 20}}, []int32{0},
			true, true, 10, 5, 30, 25},
		{"only 2nd col is selected from 4 chunks",
			[]PageIndexRanges{{100, 220, 10, 30}, {110, 250, 25, 10}, {140, 260, 30, 40}, {200, 300, 10, 100}},
			[]int32{1}, true, true, 110, 25, 250, 10},
		{"2nd and 3rd cols selected", []PageIndexRanges{{100, 220, 10, 30}, {110, 250, 25, 10}, {140, 260, 30, 40}, {200, 300, 10, 100}},
			[]int32{1, 2}, true, true, 110, 60, 250, 50},
		{"2nd and 4th cols selected", []PageIndexRanges{{100, 220, 10, 30}, {110, 250, 25, 10}, {140, 260, 30, 40}, {200, 300, 10, 100}},
			[]int32{1, 3}, true, true, 110, 100, 250, 150},
		{"1st, 2nd, and 4th cols selected", []PageIndexRanges{{100, 220, 10, 30}, {110, 250, 25, 10}, {140, 260, 30, 40}, {200, 300, 10, 100}},
			[]int32{0, 1, 3}, true, true, 100, 110, 220, 180},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validatePageIndexRange(t, tt.ranges, tt.cols, tt.expectHasCol, tt.expectHasOffset,
				tt.expectCIStart, tt.expectCISize, tt.expectOIStart, tt.expectOISize)
		})
	}
}

// validate that a column index or offset index is missing
func TestDeterminePageIndexRangesInRowGroupWithMissingPageIndex(t *testing.T) {
	tests := []struct {
		name                          string
		ranges                        []PageIndexRanges
		cols                          []int32
		expectHasCol, expectHasOffset bool
		expectCIStart, expectCISize   int
		expectOIStart, expectOISize   int
	}{
		{"no page index", []PageIndexRanges{{-1, 15, -1, 5}}, nil, false, true, -1, -1, 15, 5},
		{"no offset index", []PageIndexRanges{{10, -1, 5, -1}}, nil, true, false, 10, 5, -1, -1},
		{"no column index at all among two cols", []PageIndexRanges{{-1, 30, -1, 25}, {-1, 50, -1, 20}}, nil,
			false, true, -1, -1, 30, 40},
		{"no offset index at all among two cols", []PageIndexRanges{{10, -1, 5, -1}, {15, -1, 15, -1}}, nil,
			true, false, 10, 20, -1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validatePageIndexRange(t, tt.ranges, tt.cols, tt.expectHasCol, tt.expectHasOffset,
				tt.expectCIStart, tt.expectCISize, tt.expectOIStart, tt.expectOISize)
		})
	}
}
