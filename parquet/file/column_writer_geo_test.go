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

package file_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func geoPoint(x, y float64) parquet.ByteArray {
	b := make([]byte, 21)
	b[0] = 1
	binary.LittleEndian.PutUint32(b[1:], 1)
	binary.LittleEndian.PutUint64(b[5:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[13:], math.Float64bits(y))
	return b
}

func writeGeoFile(t *testing.T, props *parquet.WriterProperties, write func(*file.ByteArrayColumnChunkWriter)) *file.Reader {
	t.Helper()
	geom, err := schema.NewPrimitiveNodeLogical("geom", parquet.Repetitions.Optional,
		schema.GeometryLogicalType{}, parquet.Types.ByteArray, -1, -1)
	require.NoError(t, err)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{geom}, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	rg := w.AppendRowGroup()
	cw, err := rg.NextColumn()
	require.NoError(t, err)
	write(cw.(*file.ByteArrayColumnChunkWriter))
	require.NoError(t, cw.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, w.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	t.Cleanup(func() { rdr.Close() })
	return rdr
}

func TestGeometryColumnGeoStatistics(t *testing.T) {
	for _, dict := range []bool{false, true} {
		props := parquet.NewWriterProperties(parquet.WithStats(true), parquet.WithDictionaryDefault(dict))
		rdr := writeGeoFile(t, props, func(cw *file.ByteArrayColumnChunkWriter) {
			// batch 1: dense with one null; batch 2: spaced
			_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(-122.4, 37.8), geoPoint(-73.9, 40.7)}, []int16{1, 1, 0}, nil)
			require.NoError(t, err)
			_, err = cw.WriteBatchSpacedWithError([]parquet.ByteArray{geoPoint(5, 60), nil}, []int16{1, 0}, nil, []byte{0b01}, 0)
			require.NoError(t, err)
		})

		col, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
		require.NoError(t, err)
		gs := col.GeoStatistics()
		require.NotNil(t, gs, "dict=%v", dict)
		assert.True(t, gs.HasBoundingBox())
		assert.Equal(t, -122.4, gs.XMin())
		assert.Equal(t, 5.0, gs.XMax())
		assert.Equal(t, 37.8, gs.YMin())
		assert.Equal(t, 60.0, gs.YMax())
		assert.False(t, gs.HasZ())
		assert.False(t, gs.HasM())
		assert.Equal(t, []int32{1}, gs.GeometryTypes())
	}
}

func TestGeometryColumnArrowOffsetsGeoStatistics(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithStats(true), parquet.WithDictionaryDefault(false))
	a, b := geoPoint(1, 2), geoPoint(3, 4)
	data := append(append([]byte{}, a...), b...)
	rdr := writeGeoFile(t, props, func(cw *file.ByteArrayColumnChunkWriter) {
		_, err := cw.WriteBatchArrow(data, []int32{0, 21, 42}, []int16{1, 1}, nil)
		require.NoError(t, err)
		_, err = cw.WriteBatchArrow64(data, []int64{0, 21, 42}, []int16{1, 1}, nil)
		require.NoError(t, err)
	})
	col, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)
	gs := col.GeoStatistics()
	require.NotNil(t, gs)
	assert.Equal(t, []float64{1, 3, 2, 4}, []float64{gs.XMin(), gs.XMax(), gs.YMin(), gs.YMax()})
}

func TestGeometryColumnInvalidWKBOmitsGeoStatistics(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithStats(true))
	rdr := writeGeoFile(t, props, func(cw *file.ByteArrayColumnChunkWriter) {
		_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2), []byte("not wkb")}, []int16{1, 1}, nil)
		require.NoError(t, err)
	})
	col, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)
	assert.Nil(t, col.GeoStatistics())
}

func TestGeometryColumnStatsDisabled(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithStats(false))
	rdr := writeGeoFile(t, props, func(cw *file.ByteArrayColumnChunkWriter) {
		_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2)}, []int16{1}, nil)
		require.NoError(t, err)
	})
	col, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)
	assert.Nil(t, col.GeoStatistics())
}

func TestGeoStatisticsReferenceFiles(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	dir = path.Join(dir, "geospatial")
	assert.DirExists(t, dir)

	geomColumn := func(t *testing.T, rdr *file.Reader) int {
		t.Helper()
		idx := rdr.MetaData().Schema.ColumnIndexByName("geometry")
		require.GreaterOrEqual(t, idx, 0)
		return idx
	}

	t.Run("GEOMETRY statistics are returned", func(t *testing.T) {
		rdr, err := file.OpenParquetFile(path.Join(dir, "crs-default.parquet"), false)
		require.NoError(t, err)
		defer rdr.Close()

		col, err := rdr.MetaData().RowGroup(0).ColumnChunk(geomColumn(t, rdr))
		require.NoError(t, err)
		gs := col.GeoStatistics()
		require.NotNil(t, gs)
		assert.True(t, gs.HasBoundingBox())
	})

	t.Run("GEOGRAPHY statistics are not returned", func(t *testing.T) {
		// This file has GEOGRAPHY statistics in every row group, some with
		// bounding boxes that wrap across the antimeridian.
		rdr, err := file.OpenParquetFile(path.Join(dir, "geography-lines.parquet"), false)
		require.NoError(t, err)
		defer rdr.Close()

		idx := geomColumn(t, rdr)
		require.IsType(t, schema.GeographyLogicalType{}, rdr.MetaData().Schema.Column(idx).LogicalType())
		for rg := 0; rg < rdr.NumRowGroups(); rg++ {
			col, err := rdr.MetaData().RowGroup(rg).ColumnChunk(idx)
			require.NoError(t, err)
			assert.Nil(t, col.GeoStatistics(), "row group %d", rg)
		}
	})
}
