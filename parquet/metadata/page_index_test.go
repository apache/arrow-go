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

package metadata_test

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/utils"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestReadOffsetIndex(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	path := filepath.Join(dir, "alltypes_tiny_pages.parquet")
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()

	const (
		rgID, colID int = 0, 0
	)

	fileMetadata := rdr.MetaData()
	colMeta, err := fileMetadata.RowGroup(rgID).ColumnChunk(colID)
	require.NoError(t, err)
	indexLoc := colMeta.GetOffsetIndexLocation()
	require.NotNil(t, indexLoc)

	// read serialized offset index from file
	src, err := os.Open(path)
	require.NoError(t, err)
	buf := make([]byte, indexLoc.Length)
	n, err := src.ReadAt(buf, indexLoc.Offset)
	require.NoError(t, err)
	assert.EqualValues(t, indexLoc.Length, n)
	require.NoError(t, src.Close())

	// deserialize offset index
	offsetIdx := metadata.NewOffsetIndex(buf, nil, nil)

	// verify only partial data as it contains 325 pages in total
	const numPages = 325
	var (
		pageIndices   = [...]uint64{0, 100, 200, 300}
		pageLocations = []metadata.PageLocation{
			{Offset: 4, CompressedPageSize: 109, FirstRowIndex: 0},
			{Offset: 11480, CompressedPageSize: 133, FirstRowIndex: 2244},
			{Offset: 22980, CompressedPageSize: 133, FirstRowIndex: 4494},
			{Offset: 34480, CompressedPageSize: 133, FirstRowIndex: 6744},
		}
	)

	offsetPageLocs := offsetIdx.GetPageLocations()
	assert.Len(t, offsetPageLocs, numPages)

	for i, pageID := range pageIndices {
		readPageLocation := offsetPageLocs[pageID]
		expectedPageLoc := pageLocations[i]

		assert.Equal(t, expectedPageLoc, *readPageLocation)
	}
}

func ulps64(actual, expected float64) int64 {
	ulp := math.Nextafter(actual, math.Inf(1)) - actual
	return int64(math.Abs((expected - actual) / ulp))
}

func ulps32(actual, expected float32) int64 {
	ulp := math.Nextafter32(actual, float32(math.Inf(1))) - actual
	return int64(math.Abs(float64((expected - actual) / ulp)))
}

func assertFloat32Approx(t assert.TestingT, expected, actual any, _ ...any) bool {
	const maxulps int64 = 4
	ulps := ulps32(actual.(float32), expected.(float32))
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", actual, expected, ulps)
}

func assertFloat64Approx(t assert.TestingT, expected, actual any, _ ...any) bool {
	const maxulps int64 = 4
	ulps := ulps64(actual.(float64), expected.(float64))
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", actual, expected, ulps)
}

type testcase struct {
	fileName      string
	colID         int
	numPages      uint64
	boundaryOrder metadata.BoundaryOrder
	pageIndices   []uint64
	nullPages     []bool
	hasNullCounts bool
	nullCounts    []int64
}

func testReadTypedColIndex[T parquet.ColumnTypes](t *testing.T, tc testcase, minValues, maxValues []T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	path := filepath.Join(dir, tc.fileName)
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()

	fileMetadata := rdr.MetaData()

	// Get column index location to a specific column chunk
	const rgID = 0
	colMeta, err := fileMetadata.RowGroup(rgID).ColumnChunk(tc.colID)
	require.NoError(t, err)

	indexLoc := colMeta.GetColumnIndexLocation()
	require.NotNil(t, indexLoc)

	// read serialized column index from the file
	src, err := os.Open(path)
	require.NoError(t, err)
	buf := make([]byte, indexLoc.Length)
	n, err := src.ReadAt(buf, indexLoc.Offset)
	require.NoError(t, err)
	assert.EqualValues(t, indexLoc.Length, n)
	require.NoError(t, src.Close())

	descr := fileMetadata.Schema.Column(tc.colID)
	colIndex := metadata.NewColumnIndex(descr, buf, nil, nil)
	assert.IsType(t, (*metadata.TypedColumnIndex[T])(nil), colIndex)

	typedColIndex := colIndex.(*metadata.TypedColumnIndex[T])
	// verify only partial data as there are too many pages
	assert.Len(t, typedColIndex.GetNullPages(), int(tc.numPages))
	assert.Equal(t, tc.hasNullCounts, typedColIndex.IsSetNullCounts())
	assert.Equal(t, tc.boundaryOrder, typedColIndex.GetBoundaryOrder())

	var cmp assert.ComparisonAssertionFunc
	switch any(minValues).(type) {
	case []float64:
		cmp = assertFloat64Approx
	case []float32:
		cmp = assertFloat32Approx
	default:
		cmp = assert.Equal
	}

	for i, pageID := range tc.pageIndices {
		assert.Equal(t, tc.nullPages[i], typedColIndex.GetNullPages()[pageID])
		if tc.hasNullCounts {
			assert.Equal(t, tc.nullCounts[i], typedColIndex.GetNullCounts()[pageID])
		}

		// min/max values are only meaningful for non-null pages
		if !tc.nullPages[i] {
			cmp(t, minValues[i], typedColIndex.MinValues()[pageID])
			cmp(t, maxValues[i], typedColIndex.MaxValues()[pageID])
		}
	}
}

func TestReadTypedColumnIndex(t *testing.T) {
	tests := []struct {
		tc               testcase
		minVals, maxVals any
	}{
		{testcase{"alltypes_tiny_pages.parquet", 5, 528, metadata.Unordered,
			[]uint64{0, 99, 426, 520}, []bool{false, false, false, false}, true, []int64{0, 0, 0, 0}},
			[]int64{0, 10, 0, 0}, []int64{90, 90, 80, 70}},
		{testcase{"alltypes_tiny_pages.parquet", 7, 528, metadata.Unordered,
			[]uint64{0, 51, 212, 527}, []bool{false, false, false, false}, true, []int64{0, 0, 0, 0}},
			[]float64{-0, 30.3, 10.1, 40.4}, []float64{90.9, 90.9, 90.9, 60.6}},
		{testcase{"alltypes_tiny_pages.parquet", 9, 352, metadata.Ascending,
			[]uint64{0, 128, 256}, []bool{false, false, false}, true, []int64{0, 0, 0}},
			[]parquet.ByteArray{{'0'}, {'0'}, {'0'}}, []parquet.ByteArray{{'9'}, {'9'}, {'9'}}},
		{testcase{"alltypes_tiny_pages.parquet", 1, 82, metadata.Ascending,
			[]uint64{0, 16, 64}, []bool{false, false, false}, true, []int64{0, 0, 0}},
			[]bool{false, false, false}, []bool{true, true, true}},
		{testcase{"fixed_length_byte_array.parquet", 0, 10, metadata.Descending,
			[]uint64{0, 4, 8}, []bool{false, false, false}, true, []int64{9, 13, 9}},
			[]parquet.FixedLenByteArray{{0x00, 0x00, 0x03, 0x85}, {0x00, 0x00, 0x01, 0xF5}, {0x00, 0x00, 0x00, 0x65}},
			[]parquet.FixedLenByteArray{{0x00, 0x00, 0x03, 0xE8}, {0x00, 0x00, 0x02, 0x58}, {0x00, 0x00, 0x00, 0xC8}}},
		{testcase{"int32_with_null_pages.parquet", 0, 10, metadata.Unordered,
			[]uint64{2, 4, 8}, []bool{true, false, false}, true, []int64{100, 16, 8}},
			[]int32{0, -2048691758, -2046900272}, []int32{0, 2143189382, 2087168549}},
	}

	for _, tt := range tests {
		t.Run(tt.tc.fileName+" "+strconv.Itoa(tt.tc.colID), func(t *testing.T) {
			switch mv := tt.minVals.(type) {
			case []bool:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]bool))
			case []int32:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]int32))
			case []int64:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]int64))
			case []float64:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]float64))
			case []parquet.ByteArray:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]parquet.ByteArray))
			case []parquet.FixedLenByteArray:
				testReadTypedColIndex(t, tt.tc, mv, tt.maxVals.([]parquet.FixedLenByteArray))
			}
		})
	}
}

func TestWriteOffsetIndex(t *testing.T) {
	var bldr metadata.OffsetIndexBuilder

	const (
		numPages      = 5
		finalPosition = 4096
	)
	var (
		offsets         = []int64{100, 200, 300, 400, 500}
		pageSizes       = []int32{1024, 2048, 3072, 4096, 8192}
		firstRowIndices = []int64{0, 10000, 20000, 30000, 40000}
	)

	for i := 0; i < numPages; i++ {
		bldr.AddPage(offsets[i], firstRowIndices[i], pageSizes[i])
	}
	bldr.Finish(finalPosition)

	offsetIndexes := make([]metadata.OffsetIndex, 0)
	// 1st element is the offset index just built
	offsetIndexes = append(offsetIndexes, bldr.Build())
	// 2nd element is the offset index restored by serialize-then-deserialize round trip
	var buf bytes.Buffer
	bldr.WriteTo(&buf, nil)

	offsetIndexes = append(offsetIndexes, metadata.NewOffsetIndex(buf.Bytes(), nil, nil))

	// verify the data in the offset index
	for _, offsetIndex := range offsetIndexes {
		pageLocations := offsetIndex.GetPageLocations()
		assert.Len(t, pageLocations, numPages)

		for i := 0; i < numPages; i++ {
			pageLoc := pageLocations[i]
			assert.Equal(t, offsets[i]+finalPosition, pageLoc.Offset)
			assert.Equal(t, pageSizes[i], pageLoc.CompressedPageSize)
			assert.Equal(t, firstRowIndices[i], pageLoc.FirstRowIndex)
		}
	}
}

func simpleEncode[T int32 | int64 | float32 | float64](val T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&val)), unsafe.Sizeof(val))
}

func TestWriteTypedColumnIndex(t *testing.T) {
	tests := []struct {
		name          string
		n             *schema.PrimitiveNode
		pageStats     []*metadata.EncodedStatistics
		boundaryOrder metadata.BoundaryOrder
		hasNullCounts bool
	}{
		{"write_int32", schema.NewInt32Node("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetNullCount(1).SetMin(simpleEncode(int32(1))).SetMax(simpleEncode(int32(2))),
				(&metadata.EncodedStatistics{}).SetNullCount(2).SetMin(simpleEncode(int32(2))).SetMax(simpleEncode(int32(3))),
				(&metadata.EncodedStatistics{}).SetNullCount(3).SetMin(simpleEncode(int32(3))).SetMax(simpleEncode(int32(4))),
			}, metadata.Ascending, true},
		{"write_int64", schema.NewInt64Node("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetNullCount(4).SetMin(simpleEncode(int64(-1))).SetMax(simpleEncode(int64(-2))),
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin(simpleEncode(int64(-2))).SetMax(simpleEncode(int64(-3))),
				(&metadata.EncodedStatistics{}).SetNullCount(4).SetMin(simpleEncode(int64(-3))).SetMax(simpleEncode(int64(-4))),
			}, metadata.Descending, true},
		{"write_float32", schema.NewFloat32Node("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin(simpleEncode(float32(2.2))).SetMax(simpleEncode(float32(4.4))),
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin(simpleEncode(float32(1.1))).SetMax(simpleEncode(float32(5.5))),
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin(simpleEncode(float32(3.3))).SetMax(simpleEncode(float32(6.6))),
			}, metadata.Unordered, true},
		{"write_float64", schema.NewFloat64Node("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(float64(1.2))).SetMax(simpleEncode(float64(4.4))),
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(float64(2.2))).SetMax(simpleEncode(float64(5.5))),
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(float64(3.3))).SetMax(simpleEncode(float64(-6.6))),
			}, metadata.Unordered, false},
		{"write_byte_array", schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetMin([]byte("bar")).SetMax([]byte("foo")),
				(&metadata.EncodedStatistics{}).SetMin([]byte("bar")).SetMax([]byte("foo")),
				(&metadata.EncodedStatistics{}).SetMin([]byte("bar")).SetMax([]byte("foo")),
			}, metadata.Ascending, false},
		{"write_fixed_len_byte_array", schema.NewFixedLenByteArrayNode("c1", parquet.Repetitions.Optional, 3, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetMin([]byte("abc")).SetMax([]byte("ABC")),
				(&metadata.EncodedStatistics{AllNullValue: true, Min: []byte{}, Max: []byte{}}),
				(&metadata.EncodedStatistics{}).SetMin([]byte("foo")).SetMax([]byte("FOO")),
				(&metadata.EncodedStatistics{AllNullValue: true, Min: []byte{}, Max: []byte{}}),
				(&metadata.EncodedStatistics{}).SetMin([]byte("xyz")).SetMax([]byte("XYZ")),
			}, metadata.Ascending, false},
		{"write_float16", schema.MustPrimitive(
			schema.NewPrimitiveNodeLogical("c1", parquet.Repetitions.Optional,
				schema.Float16LogicalType{}, parquet.Types.FixedLenByteArray, 2, -1)),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetMin(float16.New(-1.3).ToLEBytes()).SetMax(float16.New(3.6).ToLEBytes()),
				(&metadata.EncodedStatistics{}).SetMin(float16.New(-0.2).ToLEBytes()).SetMax(float16.New(4.5).ToLEBytes()),
				(&metadata.EncodedStatistics{}).SetMin(float16.New(1.1).ToLEBytes()).SetMax(float16.New(5.4).ToLEBytes()),
				(&metadata.EncodedStatistics{}).SetMin(float16.New(2.0).ToLEBytes()).SetMax(float16.New(6.3).ToLEBytes()),
			}, metadata.Ascending, false},
		{"write_all_nulls", schema.NewInt32Node("c1", parquet.Repetitions.Optional, -1),
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{AllNullValue: true, Min: []byte{}, Max: []byte{}}).SetNullCount(100),
				(&metadata.EncodedStatistics{AllNullValue: true, Min: []byte{}, Max: []byte{}}).SetNullCount(100),
				(&metadata.EncodedStatistics{AllNullValue: true, Min: []byte{}, Max: []byte{}}).SetNullCount(100),
			}, metadata.Unordered, true},
		{"invalid_null_counts", schema.NewInt32Node("c1", parquet.Repetitions.Optional, -1),
			// one page doesn't provide null count
			[]*metadata.EncodedStatistics{
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(int32(1))).SetMax(simpleEncode(int32(2))).SetNullCount(0),
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(int32(1))).SetMax(simpleEncode(int32(3))),
				(&metadata.EncodedStatistics{}).SetMin(simpleEncode(int32(2))).SetMax(simpleEncode(int32(3))).SetNullCount(0),
			}, metadata.Ascending, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descr := schema.NewColumn(tt.n, 1, 0)

			bldr := metadata.NewColumnIndexBuilder(descr)
			for _, stats := range tt.pageStats {
				bldr.AddPage(stats)
			}
			require.NoError(t, bldr.Finish())

			colIndexes := make([]metadata.ColumnIndex, 0)
			// 1st element is the column index just built
			colIndexes = append(colIndexes, bldr.Build())
			// 2nd element is the column index restored by serialize-then-deserialize round trip
			var buf bytes.Buffer
			bldr.WriteTo(&buf, nil)
			colIndexes = append(colIndexes, metadata.NewColumnIndex(descr, buf.Bytes(), nil, nil))

			for _, colIndex := range colIndexes {
				assert.Equal(t, tt.hasNullCounts, colIndex.IsSetNullCounts())
				assert.Equal(t, tt.boundaryOrder, colIndex.GetBoundaryOrder())
				numPages := len(colIndex.GetNullPages())
				for i := 0; i < numPages; i++ {
					assert.Equal(t, tt.pageStats[i].AllNullValue, colIndex.GetNullPages()[i])
					assert.Equalf(t, tt.pageStats[i].Min, colIndex.GetMinValues()[i], "colIndex: %d, page: %d", colIndex, i)
					assert.Equalf(t, tt.pageStats[i].Max, colIndex.GetMaxValues()[i], "colIndex: %d, page: %d", colIndex, i)
					if tt.hasNullCounts {
						assert.Equal(t, tt.pageStats[i].NullCount, colIndex.GetNullCounts()[i])
					}
				}
			}
		})
	}
}

func TestWriteColumnIndexCorruptedStats(t *testing.T) {
	pageStats := []*metadata.EncodedStatistics{
		(&metadata.EncodedStatistics{}).SetMin(simpleEncode(int32(1))).SetMax(simpleEncode(int32(2))),
		(&metadata.EncodedStatistics{}),
		(&metadata.EncodedStatistics{}).SetMin(simpleEncode(int32(3))).SetMax(simpleEncode(int32(4))),
	}

	descr := schema.NewColumn(schema.NewInt32Node("c1", parquet.Repetitions.Optional, -1), 1, 0)
	bldr := metadata.NewColumnIndexBuilder(descr)
	for _, stats := range pageStats {
		bldr.AddPage(stats)
	}
	require.NoError(t, bldr.Finish())
	assert.Nil(t, bldr.Build())

	var buf bytes.Buffer
	bldr.WriteTo(&buf, nil)
	assert.Zero(t, buf.Len())
}

func TestPageIndexBuilderWithZeroRowGroup(t *testing.T) {
	fields := schema.FieldList{
		schema.NewInt32Node("c1", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("c2", parquet.Repetitions.Optional, -1),
	}

	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1))
	sc := schema.NewSchema(root)

	bldr := metadata.PageIndexBuilder{Schema: sc}

	// AppendRowGroup not called
	cb, err := bldr.GetColumnIndexBuilder(0)
	assert.Error(t, err)
	assert.Nil(t, cb)
	ob, err := bldr.GetOffsetIndexBuilder(0)
	assert.Error(t, err)
	assert.Nil(t, ob)
	bldr.Finish()

	var (
		buf bytes.Buffer
		loc metadata.PageIndexLocation
	)

	assert.NoError(t, bldr.WriteTo(&utils.TellWrapper{Writer: &buf}, &loc))
	assert.Zero(t, buf.Len())
	assert.Len(t, loc.ColIndexLocation, 0)
	assert.Len(t, loc.OffsetIndexLocation, 0)
}

type PageIndexBuilderSuite struct {
	suite.Suite

	sc           *schema.Schema
	pageIndexLoc metadata.PageIndexLocation
	buf          bytes.Buffer
}

func (s *PageIndexBuilderSuite) SetupTest() {
	s.buf.Reset()
}

func (s *PageIndexBuilderSuite) writePageIndexes(numRGs, numCols int, pageStats [][]*metadata.EncodedStatistics, pageLocs [][]metadata.PageLocation, finalPos int) {
	bldr := metadata.PageIndexBuilder{Schema: s.sc}
	for rg := range numRGs {
		s.Require().NoError(bldr.AppendRowGroup())

		for col := range numCols {
			if col < len(pageStats[rg]) {
				colIdxBldr, err := bldr.GetColumnIndexBuilder(col)
				s.Require().NoError(err)
				s.Require().NoError(colIdxBldr.AddPage(pageStats[rg][col]))
				s.Require().NoError(colIdxBldr.Finish())
			}

			if col < len(pageLocs[rg]) {
				offsetIdxBldr, err := bldr.GetOffsetIndexBuilder(col)
				s.Require().NoError(err)
				s.Require().NoError(offsetIdxBldr.AddPageLoc(pageLocs[rg][col]))
				s.Require().NoError(offsetIdxBldr.Finish(int64(finalPos)))
			}
		}
	}
	bldr.Finish()

	s.Require().NoError(bldr.WriteTo(&utils.TellWrapper{Writer: &s.buf}, &s.pageIndexLoc))
	s.Len(s.pageIndexLoc.ColIndexLocation, numRGs)
	s.Len(s.pageIndexLoc.OffsetIndexLocation, numRGs)
	for rg := range numRGs {
		s.Len(s.pageIndexLoc.ColIndexLocation[uint64(rg)], numCols)
		s.Len(s.pageIndexLoc.OffsetIndexLocation[uint64(rg)], numCols)
	}
}

func (s *PageIndexBuilderSuite) checkColumnIndex(rg, col int, stats *metadata.EncodedStatistics) {
	colIndex := s.readColumnIndex(rg, col)
	s.Require().NotNil(colIndex)
	s.Len(colIndex.GetNullPages(), 1)
	s.Equal(stats.AllNullValue, colIndex.GetNullPages()[0])
	s.Equal(stats.Min, colIndex.GetMinValues()[0])
	s.Equal(stats.Max, colIndex.GetMaxValues()[0])
	s.Equal(stats.HasNullCount, colIndex.IsSetNullCounts())
	if stats.HasNullCount {
		s.Equal(stats.NullCount, colIndex.GetNullCounts()[0])
	}
}

func (s *PageIndexBuilderSuite) checkOffsetIndex(rg, col int, loc metadata.PageLocation, finalLoc int64) {
	offsetIndex := s.readOffsetIndex(rg, col)
	s.Require().NotNil(offsetIndex)
	s.Len(offsetIndex.GetPageLocations(), 1)
	location := offsetIndex.GetPageLocations()[0]
	s.Equal(loc.Offset+finalLoc, location.Offset)
	s.Equal(loc.CompressedPageSize, location.CompressedPageSize)
	s.Equal(loc.FirstRowIndex, location.FirstRowIndex)
}

func (s *PageIndexBuilderSuite) readColumnIndex(rg, col int) metadata.ColumnIndex {
	location := s.pageIndexLoc.ColIndexLocation[uint64(rg)][col]
	if location == nil {
		return nil
	}

	start, end := location.Offset, location.Offset+int64(location.Length)
	return metadata.NewColumnIndex(s.sc.Column(col), s.buf.Bytes()[start:end], nil, nil)
}

func (s *PageIndexBuilderSuite) readOffsetIndex(rg, col int) metadata.OffsetIndex {
	location := s.pageIndexLoc.OffsetIndexLocation[uint64(rg)][col]
	if location == nil {
		return nil
	}

	start, end := location.Offset, location.Offset+int64(location.Length)
	return metadata.NewOffsetIndex(s.buf.Bytes()[start:end], nil, nil)
}

func (s *PageIndexBuilderSuite) TestSingleRowGroup() {
	s.sc = schema.NewSchema(schema.MustGroup(
		schema.NewGroupNode("schema", parquet.Repetitions.Repeated,
			schema.FieldList{
				schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c2", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c3", parquet.Repetitions.Optional, -1),
			}, -1)))

	// prepare page stats and page locations
	// note the 3rd column doesnt have any stats
	const (
		numRowGroups = 1
		numCols      = 3
		finalPos     = 200
	)

	var (
		pageStats = [][]*metadata.EncodedStatistics{
			// row group id = 0
			{
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin([]byte("a")).SetMax([]byte("b")),
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin([]byte("A")).SetMax([]byte("B")),
			},
		}

		pageLocations = [][]metadata.PageLocation{
			// row group id = 0
			{
				{Offset: 128, CompressedPageSize: 512, FirstRowIndex: 0},
				{Offset: 1024, CompressedPageSize: 512, FirstRowIndex: 0},
			},
		}
	)

	s.writePageIndexes(numRowGroups, numCols, pageStats, pageLocations, finalPos)

	// verify the column index and offset index
	for col := 0; col < 2; col++ {
		s.checkColumnIndex(0, col, pageStats[0][col])
		s.checkOffsetIndex(0, col, pageLocations[0][col], finalPos)
	}

	// 3rd column should not have any column index
	s.Nil(s.readColumnIndex(0, 2))
	s.Nil(s.readOffsetIndex(0, 2))
}

func (s *PageIndexBuilderSuite) TestTwoRowGroups() {
	s.sc = schema.NewSchema(schema.MustGroup(
		schema.NewGroupNode("schema", parquet.Repetitions.Repeated,
			schema.FieldList{
				schema.NewByteArrayNode("c1", parquet.Repetitions.Optional, -1),
				schema.NewByteArrayNode("c2", parquet.Repetitions.Optional, -1),
			}, -1)))

	// prepare page stats and page locations
	// note the 3rd column doesnt have any stats
	const (
		numRowGroups = 2
		numCols      = 2
		finalPos     = 200
	)

	var (
		pageStats = [][]*metadata.EncodedStatistics{
			{ // row group id = 0
				(&metadata.EncodedStatistics{}).SetMin([]byte("a")).SetMax([]byte("b")),
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin([]byte("A")).SetMax([]byte("B")),
			},
			{ // row group id = 1
				(&metadata.EncodedStatistics{}), // corrupted stats
				(&metadata.EncodedStatistics{}).SetNullCount(0).SetMin([]byte("bar")).SetMax([]byte("foo")),
			},
		}

		pageLocations = [][]metadata.PageLocation{
			{ // row group id = 0
				{Offset: 128, CompressedPageSize: 512, FirstRowIndex: 0},
				{Offset: 1024, CompressedPageSize: 512, FirstRowIndex: 0},
			},
			{ // row group id = 1
				{Offset: 128, CompressedPageSize: 512, FirstRowIndex: 0},
				{Offset: 1024, CompressedPageSize: 512, FirstRowIndex: 0},
			},
		}
	)

	s.writePageIndexes(numRowGroups, numCols, pageStats, pageLocations, finalPos)

	// verify that all columns have good column indexes except the 2nd column
	// in the 2nd row group
	s.checkColumnIndex(0, 0, pageStats[0][0])
	s.checkColumnIndex(0, 1, pageStats[0][1])
	s.checkColumnIndex(1, 1, pageStats[1][1])
	s.Nil(s.readColumnIndex(1, 0))

	// verify that two columns have good offset indexes
	s.checkOffsetIndex(0, 0, pageLocations[0][0], finalPos)
	s.checkOffsetIndex(0, 1, pageLocations[0][1], finalPos)
	s.checkOffsetIndex(1, 0, pageLocations[1][0], finalPos)
	s.checkOffsetIndex(1, 1, pageLocations[1][1], finalPos)
}

func TestPageIndexBuilder(t *testing.T) {
	suite.Run(t, new(PageIndexBuilderSuite))
}

func TestPageIndexReader(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	path := filepath.Join(dir, "alltypes_tiny_pages.parquet")
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()

	fileMeta := rdr.MetaData()
	assert.EqualValues(t, 1, fileMeta.NumRowGroups())
	assert.EqualValues(t, 13, fileMeta.Schema.NumColumns())

	pageIdxReader := rdr.GetPageIndexReader()
	require.NotNil(t, pageIdxReader)

	idxSelectionTests := []metadata.PageIndexSelection{
		{ColumnIndex: true, OffsetIndex: true},
		{ColumnIndex: true, OffsetIndex: false},
		{ColumnIndex: false, OffsetIndex: true},
		{ColumnIndex: false, OffsetIndex: false},
	}

	tests := []struct {
		rowGroupIndices []int32
		colIndices      []int32
	}{
		{[]int32{}, []int32{}},
		{[]int32{0}, []int32{}},
		{[]int32{0}, []int32{0}},
		{[]int32{0}, []int32{5}},
		{[]int32{0}, []int32{0, 5}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("rowGroupIndices=%v,colIndices=%v", tt.rowGroupIndices, tt.colIndices), func(t *testing.T) {
			for _, idxSelection := range idxSelectionTests {
				t.Run(fmt.Sprintf("indexSelection=%v", idxSelection), func(t *testing.T) {
					callWillNeed := len(tt.rowGroupIndices) > 0
					if callWillNeed {
						pageIdxReader.WillNeed(tt.rowGroupIndices, tt.colIndices, idxSelection)
					}

					rgIdxReader, err := pageIdxReader.RowGroup(0)
					assert.NoError(t, err)
					if !callWillNeed || idxSelection.OffsetIndex || idxSelection.ColumnIndex {
						assert.NotNil(t, rgIdxReader)
					} else {
						assert.Nil(t, rgIdxReader)
						return
					}

					colIdxRequested := func(colID int32) bool {
						return !callWillNeed || (idxSelection.ColumnIndex &&
							(len(tt.colIndices) == 0 || slices.Contains(tt.colIndices, colID)))
					}

					offsetIdxRequested := func(colID int32) bool {
						return !callWillNeed || (idxSelection.OffsetIndex &&
							(len(tt.colIndices) == 0 || slices.Contains(tt.colIndices, colID)))
					}

					if offsetIdxRequested(0) {
						// verify offset index of column 0 and only partial data as it contains 325 pages
						const numPages = 325
						var (
							pageIndices   = []uint64{0, 100, 200, 300}
							pageLocations = []metadata.PageLocation{
								{Offset: 4, CompressedPageSize: 109, FirstRowIndex: 0},
								{Offset: 11480, CompressedPageSize: 133, FirstRowIndex: 2244},
								{Offset: 22980, CompressedPageSize: 133, FirstRowIndex: 4494},
								{Offset: 34480, CompressedPageSize: 133, FirstRowIndex: 6744},
							}
						)

						offsetIdx, err := rgIdxReader.GetOffsetIndex(0)
						require.NoError(t, err)
						assert.NotNil(t, offsetIdx)

						assert.Len(t, offsetIdx.GetPageLocations(), numPages)
						for i, pageID := range pageIndices {
							readPageLoc, expectedPageLoc := offsetIdx.GetPageLocations()[pageID], pageLocations[i]
							assert.EqualValues(t, expectedPageLoc.Offset, readPageLoc.Offset)
							assert.EqualValues(t, expectedPageLoc.CompressedPageSize, readPageLoc.CompressedPageSize)
							assert.EqualValues(t, expectedPageLoc.FirstRowIndex, readPageLoc.FirstRowIndex)
						}
					} else {
						_, err := rgIdxReader.GetOffsetIndex(0)
						assert.Error(t, err)
					}

					if colIdxRequested(5) {
						// verify column index of column 5 and only partial data as it contains 528 pages
						const (
							numPages      = 528
							boundaryOrder = metadata.Unordered
							hasNullCounts = true
						)

						var (
							pageIndices = []uint64{0, 99, 426, 520}
							nullPages   = []bool{false, false, false, false}
							nullCounts  = []int64{0, 0, 0, 0}
							minValues   = []int64{0, 10, 0, 0}
							maxValues   = []int64{90, 90, 80, 70}
						)

						colIdx, err := rgIdxReader.GetColumnIndex(5)
						require.NoError(t, err)
						assert.NotNil(t, colIdx)

						assert.IsType(t, (*metadata.TypedColumnIndex[int64])(nil), colIdx)
						typedColIdx := colIdx.(*metadata.TypedColumnIndex[int64])

						assert.Len(t, colIdx.GetNullPages(), numPages)
						assert.Equal(t, hasNullCounts, colIdx.IsSetNullCounts())
						assert.Equal(t, boundaryOrder, colIdx.GetBoundaryOrder())
						for i, pageID := range pageIndices {
							assert.Equal(t, nullPages[i], colIdx.GetNullPages()[pageID])
							if hasNullCounts {
								assert.Equal(t, nullCounts[i], colIdx.GetNullCounts()[pageID])
							}
							if !nullPages[i] {
								assert.Equal(t, minValues[i], typedColIdx.MinValues()[pageID])
								assert.Equal(t, maxValues[i], typedColIdx.MaxValues()[pageID])
							}
						}
					} else {
						_, err := rgIdxReader.GetColumnIndex(5)
						assert.Error(t, err)
					}

					// verify null is returned if column index does not exist
					colIdx, err := rgIdxReader.GetColumnIndex(10)
					assert.NoError(t, err)
					assert.Nil(t, colIdx)
				})
			}
		})
	}
}

func TestReadFileWithoutPageIndex(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		t.Skip("PARQUET_TEST_DATA not set")
	}
	require.DirExists(t, dir)

	path := filepath.Join(dir, "int32_decimal.parquet")
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()

	fileMeta := rdr.MetaData()
	assert.EqualValues(t, 1, fileMeta.NumRowGroups())
	pageIdxReader := rdr.GetPageIndexReader()
	assert.NotNil(t, pageIdxReader)
	rgIdxReader, err := pageIdxReader.RowGroup(0)
	assert.NoError(t, err)
	assert.Nil(t, rgIdxReader)
}
