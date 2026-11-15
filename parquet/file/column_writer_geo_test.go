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
	"fmt"
	"math"
	"os"
	"path"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/metadata"
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
	buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Optional, props, write)
	rdr, err := file.NewParquetReader(bytes.NewReader(buf))
	require.NoError(t, err)
	t.Cleanup(func() { rdr.Close() })
	return rdr
}

// writeGeoColumn writes a file with a single row group holding one byte array
// column of the given logical type, and returns the file's bytes.
func writeGeoColumn(t *testing.T, lt schema.LogicalType, rep parquet.Repetition, props *parquet.WriterProperties, write func(*file.ByteArrayColumnChunkWriter)) []byte {
	t.Helper()
	geom, err := schema.NewPrimitiveNodeLogical("geom", rep, lt, parquet.Types.ByteArray, -1, -1)
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
	return buf.Bytes()
}

// footerColumnMeta decodes the file footer and returns the thrift metadata of
// the first column chunk, so tests can check exactly which fields are set.
func footerColumnMeta(t *testing.T, buf []byte) *format.ColumnMetaData {
	t.Helper()
	require.Greater(t, len(buf), 8)
	footerLen := int(binary.LittleEndian.Uint32(buf[len(buf)-8:]))
	var fmd format.FileMetaData
	_, err := thrift.DeserializeThrift(&fmd, buf[len(buf)-8-footerLen:len(buf)-8])
	require.NoError(t, err)
	require.Len(t, fmd.RowGroups, 1)
	require.Len(t, fmd.RowGroups[0].Columns, 1)
	return fmd.RowGroups[0].Columns[0].MetaData
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

	t.Run("NaN coordinates are skipped like the reference writer", func(t *testing.T) {
		// One geometry in this file is an all-NaN linestring. The reference
		// writer ignores it and still records a bounding box for the others,
		// recomputing from the values must give the same statistics.
		rdr, err := file.OpenParquetFile(path.Join(dir, "geospatial-with-nan.parquet"), false)
		require.NoError(t, err)
		defer rdr.Close()

		idx := geomColumn(t, rdr)
		col, err := rdr.MetaData().RowGroup(0).ColumnChunk(idx)
		require.NoError(t, err)
		want := col.GeoStatistics()
		require.NotNil(t, want)
		assert.Equal(t, []float64{10, 130, 20, 140}, []float64{want.XMin(), want.XMax(), want.YMin(), want.YMax()})
		assert.Equal(t, []int32{3001, 3002}, want.GeometryTypes())

		cr, err := rdr.RowGroup(0).Column(idx)
		require.NoError(t, err)
		n := rdr.NumRows()
		values := make([]parquet.ByteArray, n)
		defLevels := make([]int16, n)
		_, read, err := cr.(*file.ByteArrayColumnChunkReader).ReadBatch(n, values, defLevels, nil)
		require.NoError(t, err)

		got := metadata.NewGeoStatistics()
		got.Update(values[:read])
		require.True(t, got.Valid())
		assert.Equal(t, want.GeometryTypes(), got.GeometryTypes())
		assert.Equal(t,
			[]float64{want.XMin(), want.XMax(), want.YMin(), want.YMax(), want.ZMin(), want.ZMax(), want.MMin(), want.MMax()},
			[]float64{got.XMin(), got.XMax(), got.YMin(), got.YMax(), got.ZMin(), got.ZMax(), got.MMin(), got.MMax()})
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

func TestGeoColumnNullCountStatistics(t *testing.T) {
	assertNullCountOnly := func(t *testing.T, md *format.ColumnMetaData, want int64) {
		t.Helper()
		require.True(t, md.IsSetStatistics(), "statistics block should be written")
		st := md.Statistics
		require.True(t, st.IsSetNullCount())
		assert.Equal(t, want, st.GetNullCount())
		assert.False(t, st.IsSetMin())
		assert.False(t, st.IsSetMax())
		assert.False(t, st.IsSetMinValue())
		assert.False(t, st.IsSetMaxValue())
		assert.False(t, st.IsSetDistinctCount())
	}

	for _, dict := range []bool{false, true} {
		for _, v := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			props := parquet.NewWriterProperties(parquet.WithStats(true),
				parquet.WithDictionaryDefault(dict), parquet.WithDataPageVersion(v))
			suffix := fmt.Sprintf(" (dict=%v, page v%d)", dict, v+1)

			t.Run("GEOMETRY with nulls"+suffix, func(t *testing.T) {
				buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(-122.4, 37.8), geoPoint(-73.9, 40.7)}, []int16{1, 0, 1, 0}, nil)
					require.NoError(t, err)
					_, err = cw.WriteBatchSpacedWithError([]parquet.ByteArray{geoPoint(5, 60), nil}, []int16{1, 0}, nil, []byte{0b01}, 0)
					require.NoError(t, err)
					_, err = cw.WriteBatchArrow(geoPoint(1, 2), []int32{0, 21, 21}, []int16{1, 0}, nil)
					require.NoError(t, err)
				})
				md := footerColumnMeta(t, buf)
				assertNullCountOnly(t, md, 4)

				// the geospatial statistics are unchanged
				gs := md.GetGeospatialStatistics()
				require.NotNil(t, gs)
				require.NotNil(t, gs.Bbox)
				assert.Equal(t, []float64{-122.4, 5, 2, 60}, []float64{gs.Bbox.Xmin, gs.Bbox.Xmax, gs.Bbox.Ymin, gs.Bbox.Ymax})
				assert.Equal(t, []int32{1}, gs.GeospatialTypes)
			})

			t.Run("GEOMETRY without nulls"+suffix, func(t *testing.T) {
				buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2), geoPoint(3, 4)}, []int16{1, 1}, nil)
					require.NoError(t, err)
				})
				md := footerColumnMeta(t, buf)
				assertNullCountOnly(t, md, 0)
				assert.True(t, md.IsSetGeospatialStatistics())
			})

			t.Run("required GEOMETRY"+suffix, func(t *testing.T) {
				buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Required, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2), geoPoint(3, 4)}, nil, nil)
					require.NoError(t, err)
				})
				assertNullCountOnly(t, footerColumnMeta(t, buf), 0)
			})

			t.Run("GEOMETRY all null"+suffix, func(t *testing.T) {
				buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch(nil, []int16{0, 0, 0}, nil)
					require.NoError(t, err)
				})
				md := footerColumnMeta(t, buf)
				assertNullCountOnly(t, md, 3)
				assert.False(t, md.IsSetGeospatialStatistics())
			})

			t.Run("GEOMETRY with invalid WKB"+suffix, func(t *testing.T) {
				// the bounding box is dropped, but the null count is still valid
				buf := writeGeoColumn(t, schema.GeometryLogicalType{}, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch([]parquet.ByteArray{[]byte("not wkb")}, []int16{1, 0}, nil)
					require.NoError(t, err)
				})
				md := footerColumnMeta(t, buf)
				assertNullCountOnly(t, md, 1)
				assert.False(t, md.IsSetGeospatialStatistics())
			})

			t.Run("GEOGRAPHY"+suffix, func(t *testing.T) {
				buf := writeGeoColumn(t, schema.GeographyLogicalType{}, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
					_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2)}, []int16{0, 1, 0}, nil)
					require.NoError(t, err)
				})
				md := footerColumnMeta(t, buf)
				assertNullCountOnly(t, md, 2)
				assert.False(t, md.IsSetGeospatialStatistics())
			})
		}
	}

	t.Run("statistics disabled", func(t *testing.T) {
		for _, lt := range []schema.LogicalType{schema.GeometryLogicalType{}, schema.GeographyLogicalType{}} {
			props := parquet.NewWriterProperties(parquet.WithStats(false))
			buf := writeGeoColumn(t, lt, parquet.Repetitions.Optional, props, func(cw *file.ByteArrayColumnChunkWriter) {
				_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2)}, []int16{1, 0}, nil)
				require.NoError(t, err)
			})
			md := footerColumnMeta(t, buf)
			assert.False(t, md.IsSetStatistics(), "%s", lt)
			assert.False(t, md.IsSetGeospatialStatistics(), "%s", lt)
		}
	})

	t.Run("reader round trip", func(t *testing.T) {
		props := parquet.NewWriterProperties(parquet.WithStats(true))
		rdr := writeGeoFile(t, props, func(cw *file.ByteArrayColumnChunkWriter) {
			_, err := cw.WriteBatch([]parquet.ByteArray{geoPoint(1, 2)}, []int16{1, 0}, nil)
			require.NoError(t, err)
		})
		col, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
		require.NoError(t, err)
		require.NotNil(t, col.GeoStatistics())

		// the sort order is undefined, so the reader still reports no
		// typed statistics and never trusts a min/max for the column
		ok, err := col.StatsSet()
		require.NoError(t, err)
		assert.False(t, ok)
		stats, err := col.Statistics()
		require.NoError(t, err)
		assert.Nil(t, stats)
	})
}
