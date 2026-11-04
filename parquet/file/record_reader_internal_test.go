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

package file

import (
	"bytes"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestDelimitRecordsRejectsNonzeroInitialRepetitionLevel(t *testing.T) {
	descr := schema.NewColumn(schema.NewInt32Node("values", parquet.Repetitions.Repeated, -1), 1, 1)
	rr := newRecordReader(descr, LevelInfo{DefLevel: 1}, memory.DefaultAllocator, nil).(*recordReader)
	defer rr.Release()

	rr.repLevels.ResizeNoShrink(arrow.Int16SizeBytes)
	rr.defLevels.ResizeNoShrink(arrow.Int16SizeBytes)
	rr.RepLevels()[0] = 1
	rr.DefLevels()[0] = 1
	rr.levelsWritten = 1
	rr.atRecStart = true

	records, values, err := rr.delimitRecords(1)
	require.Error(t, err)
	require.Zero(t, records)
	require.Zero(t, values)
	require.Zero(t, rr.levelsPos)
}

type valuePathRecorder struct {
	recordReaderImpl
	paths []string
}

func (p *valuePathRecorder) ReadValuesDense(toRead int64) error {
	p.paths = append(p.paths, "dense")
	return p.recordReaderImpl.ReadValuesDense(toRead)
}

func (p *valuePathRecorder) ReadValuesSpaced(valuesWithNulls, nullCount int64) error {
	p.paths = append(p.paths, "spaced")
	return p.recordReaderImpl.ReadValuesSpaced(valuesWithNulls, nullCount)
}

func writeInt32Column(t *testing.T, rep parquet.Repetition, values []int32, defLevels []int16, opts ...parquet.WriterProperty) []byte {
	t.Helper()
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("values", rep, -1),
	}, -1))
	var buf bytes.Buffer
	w := NewParquetWriter(&buf, root, WithWriterProps(parquet.NewWriterProperties(opts...)))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*Int32ColumnChunkWriter).WriteBatch(values, defLevels, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func newInt32RecordReader(t *testing.T, data []byte) *recordReader {
	t.Helper()
	pf, err := NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { pf.Close() })
	descr := pf.MetaData().Schema.Column(0)
	rr := newRecordReader(descr, LevelInfo{DefLevel: descr.MaxDefinitionLevel()}, memory.DefaultAllocator, nil).(*recordReader)
	t.Cleanup(rr.Release)
	pr, err := pf.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	rr.SetPageReader(pr)
	return rr
}

func TestReadRecordsDenseForBatchesWithoutNulls(t *testing.T) {
	// Pages of four levels each: with nulls, without nulls, with nulls.
	defLevels := []int16{1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1}
	values := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	data := writeInt32Column(t, parquet.Repetitions.Optional, values, defLevels,
		parquet.WithBatchSize(4), parquet.WithDataPageSize(1))
	rr := newInt32RecordReader(t, data)
	rec := &valuePathRecorder{recordReaderImpl: rr.recordReaderImpl}
	rr.recordReaderImpl = rec

	n, err := rr.ReadRecords(int64(len(defLevels)))
	require.NoError(t, err)
	require.EqualValues(t, len(defLevels), n)
	require.Equal(t, []string{"spaced", "dense", "spaced"}, rec.paths)

	got := arrow.Int32Traits.CastFromBytes(rr.Values())
	valid := rr.GetValidBits()
	next := values
	for i, def := range defLevels {
		require.Equalf(t, def == 1, bitutil.BitIsSet(valid, i), "validity of row %d", i)
		if def == 1 {
			require.Equalf(t, next[0], got[i], "value of row %d", i)
			next = next[1:]
		}
	}
}

func TestReadRecordsRejectsShortPage(t *testing.T) {
	values := []int32{10, 20, 30, 40}
	tests := []struct {
		name      string
		rep       parquet.Repetition
		defLevels []int16
	}{
		{"optional", parquet.Repetitions.Optional, []int16{1, 1, 1, 1}},
		{"required", parquet.Repetitions.Required, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := writeInt32Column(t, tt.rep, values, tt.defLevels,
				parquet.WithDictionaryDefault(false), parquet.WithEncoding(parquet.Encodings.DeltaBinaryPacked))
			// DELTA_BINARY_PACKED header: block size 128, 4 miniblocks, 4 values, first value 10.
			header := []byte{0x80, 0x01, 0x04, 0x04, 0x14}
			require.Equal(t, 1, bytes.Count(data, header))
			data[bytes.Index(data, header)+3] = 2

			_, err := newInt32RecordReader(t, data).ReadRecords(int64(len(values)))
			require.ErrorContains(t, err, "number of values / definition levels read did not match")
		})
	}
}

func TestReadRecordsRejectsShortDictionaryPageWithNulls(t *testing.T) {
	defLevels := []int16{1, 1, 1, 1, 1, 1, 1, 1, 1, 0}
	values := slices.Repeat([]parquet.ByteArray{parquet.ByteArray("x")}, 9)
	root := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewByteArrayNode("values", parquet.Repetitions.Optional, -1),
	}, -1))
	var buf bytes.Buffer
	w := NewParquetWriter(&buf, root, WithWriterProps(parquet.NewWriterProperties(parquet.WithDictionaryCostFallbackFor("values", false))))
	rgw := w.AppendRowGroup()
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*ByteArrayColumnChunkWriter).WriteBatch(values, defLevels, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())
	data := buf.Bytes()

	// Dictionary indices: bit width 1, then one RLE run of nine zeros, shortened to eight.
	indices := []byte{0x01, 0x12, 0x00}
	require.Equal(t, 1, bytes.Count(data, indices))
	data[bytes.Index(data, indices)+1] = 0x10

	pf, err := NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer pf.Close()
	descr := pf.MetaData().Schema.Column(0)
	rr := newByteArrayDictRecordReader(descr, LevelInfo{DefLevel: 1, NullSlotUsage: 1},
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary},
		memory.DefaultAllocator, nil).(*binaryRecordReader)
	defer rr.Release()
	pr, err := pf.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	rr.SetPageReader(pr)

	_, err = rr.ReadRecords(int64(len(defLevels)))
	require.ErrorContains(t, err, "number of values / definition levels read did not match")
}
