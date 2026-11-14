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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/internal/encoding"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/testutils"
	"github.com/apache/arrow-go/v18/parquet/internal/thrift"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func initValues(values reflect.Value) {
	if values.Kind() != reflect.Slice {
		panic("must init values with slice")
	}

	r := rand.New(rand.NewSource(0))
	typ := values.Type().Elem()
	switch {
	case typ.Kind() == reflect.Bool:
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int31n(2) == 1))
		}
	case typ.Bits() <= 32:
		max := int64(math.MaxInt32)
		min := int64(math.MinInt32)
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int63n(max-min+1) + min).Convert(reflect.TypeOf(int32(0))))
		}
	case typ.Bits() <= 64:
		max := int64(math.MaxInt64)
		min := int64(math.MinInt64)
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int63n(max-min+1) + min))
		}
	}
}

func initDictValues(values reflect.Value, numDicts int) {
	repeatFactor := values.Len() / numDicts
	initValues(values)
	// add some repeated values
	for j := 1; j < repeatFactor; j++ {
		for i := 0; i < numDicts; i++ {
			values.Index(numDicts*j + i).Set(values.Index(i))
		}
	}
	// computed only dict_per_page * repeat_factor - 1 values < num_values compute remaining
	for i := numDicts * repeatFactor; i < values.Len(); i++ {
		values.Index(i).Set(values.Index(i - numDicts*repeatFactor))
	}
}

func makePages(version parquet.DataPageVersion, d *schema.Column, npages, lvlsPerPage int, typ reflect.Type, enc parquet.Encoding) ([]file.Page, int, reflect.Value, []int16, []int16) {
	nlevels := lvlsPerPage * npages
	nvalues := 0

	maxDef := d.MaxDefinitionLevel()
	maxRep := d.MaxRepetitionLevel()

	var (
		defLevels []int16
		repLevels []int16
	)

	valuesPerPage := make([]int, npages)
	if maxDef > 0 {
		defLevels = make([]int16, nlevels)
		testutils.FillRandomInt16(0, 0, maxDef, defLevels)
		for idx := range valuesPerPage {
			numPerPage := 0
			for i := 0; i < lvlsPerPage; i++ {
				if defLevels[i+idx*lvlsPerPage] == maxDef {
					numPerPage++
					nvalues++
				}
			}
			valuesPerPage[idx] = numPerPage
		}
	} else {
		nvalues = nlevels
		valuesPerPage[0] = lvlsPerPage
		for i := 1; i < len(valuesPerPage); i *= 2 {
			copy(valuesPerPage[i:], valuesPerPage[:i])
		}
	}

	if maxRep > 0 {
		repLevels = make([]int16, nlevels)
		testutils.FillRandomInt16(0, 0, maxRep, repLevels)
	}

	values := reflect.MakeSlice(reflect.SliceOf(typ), nvalues, nvalues)
	switch enc {
	case parquet.Encodings.Plain:
		initValues(values)
		return testutils.PaginatePlain(version, d, values, defLevels, repLevels, maxDef, maxRep, lvlsPerPage, valuesPerPage, parquet.Encodings.Plain), nvalues, values, defLevels, repLevels
	case parquet.Encodings.PlainDict, parquet.Encodings.RLEDict:
		initDictValues(values, lvlsPerPage)
		return testutils.PaginateDict(version, d, values, defLevels, repLevels, maxDef, maxRep, lvlsPerPage, valuesPerPage, parquet.Encodings.RLEDict), nvalues, values, defLevels, repLevels
	}
	panic("invalid encoding type for make pages")
}

//lint:ignore U1000 compareVectorWithDefLevels
func compareVectorWithDefLevels(left, right reflect.Value, defLevels []int16, maxDef, maxRep int16) assert.Comparison {
	return func() bool {
		if left.Kind() != reflect.Slice || right.Kind() != reflect.Slice {
			return false
		}

		if left.Type().Elem() != right.Type().Elem() {
			return false
		}

		iLeft, iRight := 0, 0
		for _, def := range defLevels {
			if def == maxDef {
				if !reflect.DeepEqual(left.Index(iLeft).Interface(), right.Index(iRight).Interface()) {
					return false
				}
				iLeft++
				iRight++
			} else if def == (maxDef - 1) {
				// null entry on the lowest nested level
				iRight++
			} else if def < (maxDef - 1) {
				// null entry on higher nesting level, only supported for non-repeating data
				if maxRep == 0 {
					iRight++
				}
			}
		}
		return true
	}
}

var mem = memory.DefaultAllocator

type PrimitiveReaderSuite struct {
	suite.Suite

	dataPageVersion parquet.DataPageVersion
	pager           file.PageReader
	reader          file.ColumnChunkReader
	pages           []file.Page
	values          reflect.Value
	defLevels       []int16
	repLevels       []int16
	nlevels         int
	nvalues         int
	maxDefLvl       int16
	maxRepLvl       int16

	bufferPool sync.Pool
}

func (p *PrimitiveReaderSuite) SetupTest() {
	p.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := memory.NewResizableBuffer(mem)
			runtime.SetFinalizer(buf, func(obj *memory.Buffer) {
				obj.Release()
			})
			return buf
		},
	}
}

func (p *PrimitiveReaderSuite) TearDownTest() {
	p.clear()
	p.bufferPool = sync.Pool{}
}

func (p *PrimitiveReaderSuite) initReader(d *schema.Column) {
	m := new(testutils.MockPageReader)
	m.Test(p.T())
	m.TestData().Set("pages", p.pages)
	m.On("Err").Return((error)(nil))
	p.pager = m
	p.reader = file.NewColumnReader(d, m, mem, &p.bufferPool)
}

func (p *PrimitiveReaderSuite) checkResults(typ reflect.Type) {
	vresult := reflect.MakeSlice(reflect.SliceOf(typ), p.nvalues, p.nvalues)
	dresult := make([]int16, p.nlevels)
	rresult := make([]int16, p.nlevels)

	var (
		read        int64 = 0
		totalRead         = 0
		batchActual       = 0
		batchSize   int32 = 8
		batch             = 0
	)

	p.Require().NotNil(p.reader)

	// this will cover both cases:
	// 1) batch size < page size (multiple ReadBatch from a single page)
	// 2) batch size > page size (BatchRead limits to single page)
	for {
		switch rdr := p.reader.(type) {
		case *file.Int32ColumnChunkReader:
			intVals := make([]int32, batchSize)
			read, batch, _ = rdr.ReadBatch(int64(batchSize), intVals, dresult[batchActual:], rresult[batchActual:])
			for i := 0; i < batch; i++ {
				vresult.Index(totalRead + i).Set(reflect.ValueOf(intVals[i]))
			}

		case *file.BooleanColumnChunkReader:
			boolVals := make([]bool, batchSize)
			read, batch, _ = rdr.ReadBatch(int64(batchSize), boolVals, dresult[batchActual:], rresult[batchActual:])
			for i := 0; i < batch; i++ {
				vresult.Index(totalRead + i).Set(reflect.ValueOf(boolVals[i]))
			}
		default:
			p.Fail("column reader not implemented")
		}

		totalRead += batch
		batchActual += int(read)
		batchSize = int32(utils.Min(1<<24, utils.Max(int(batchSize*2), 4096)))
		if read <= 0 {
			break
		}
	}

	p.Equal(p.nlevels, batchActual)
	p.Equal(p.nvalues, totalRead)
	p.Equal(p.values.Interface(), vresult.Interface())
	if p.maxDefLvl > 0 {
		p.Equal(p.defLevels, dresult)
	}
	if p.maxRepLvl > 0 {
		p.Equal(p.repLevels, rresult)
	}

	// catch improper writes at EOS
	switch rdr := p.reader.(type) {
	case *file.Int32ColumnChunkReader:
		intVals := make([]int32, batchSize)
		read, batchActual, _ = rdr.ReadBatch(5, intVals, nil, nil)
	case *file.BooleanColumnChunkReader:
		boolVals := make([]bool, batchSize)
		read, batchActual, _ = rdr.ReadBatch(5, boolVals, nil, nil)
	default:
		p.Fail("column reader not implemented")
	}

	p.Zero(batchActual)
	p.Zero(read)
}

func (p *PrimitiveReaderSuite) clear() {
	p.values = reflect.ValueOf(nil)
	p.defLevels = nil
	p.repLevels = nil
	p.pages = nil
	p.pager = nil
	p.reader = nil
}

func (p *PrimitiveReaderSuite) testPlain(npages, levels int, d *schema.Column, typ reflect.Type) {
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levels, typ, parquet.Encodings.Plain)
	p.nlevels = npages * levels
	p.initReader(d)
	p.checkResults(typ)
	p.clear()
}

func (p *PrimitiveReaderSuite) testDict(npages, levels int, d *schema.Column, typ reflect.Type) {
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levels, typ, parquet.Encodings.RLEDict)
	p.nlevels = npages * levels
	p.initReader(d)
	p.checkResults(typ)
	p.clear()
}

func (p *PrimitiveReaderSuite) TestBoolFlatRequired() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(true))
}

func (p *PrimitiveReaderSuite) TestBoolFlatOptional() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("b", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(true))
}

func (p *PrimitiveReaderSuite) TestBoolFlatOptionalSkip() {
	const (
		levelsPerPage int = 1000
		npages        int = 5
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("a", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(true), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]bool, levelsPerPage/2)
	dresult := make([]int16, levelsPerPage/2)
	rresult := make([]int16, levelsPerPage/2)

	rdr := p.reader.(*file.BooleanColumnChunkReader)

	values := p.values.Interface().([]bool)
	rIdx := int64(0)

	p.Run("skip_size > page_size", func() {
		// skip first 2 pages
		skipped, _ := rdr.Skip(int64(2 * levelsPerPage))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:]
			}
			rIdx++
		}
		p.Equal(int64(2*levelsPerPage), skipped)

		// Read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})

	p.Run("skip_size == page_size", func() {
		// skip one page worth of values across page 2 and 3
		skipped, _ := rdr.Skip(int64(levelsPerPage))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:]
			}
			rIdx++
		}
		p.Equal(int64(levelsPerPage), skipped)

		// read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})

	p.Run("skip_size < page_size", func() {
		// skip limited to a single page
		// skip half a page
		skipped, _ := rdr.Skip(int64(levelsPerPage / 2))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:] // move test values forward
			}
			rIdx++
		}
		p.Equal(int64(0.5*float32(levelsPerPage)), skipped)

		// Read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})
}

func (p *PrimitiveReaderSuite) TestInt32FlatRequired() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func (p *PrimitiveReaderSuite) TestInt32FlatOptional() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("b", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func (p *PrimitiveReaderSuite) TestInt32FlatRepeated() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 2
	typ := schema.NewInt32Node("c", parquet.Repetitions.Repeated, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func TestSkipEmptyRepeatedRows(t *testing.T) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("values", parquet.Repetitions.Repeated, -1),
	}, -1)))

	var out bytes.Buffer
	w := file.NewParquetWriter(&out, sc.Root(), file.WithWriterProps(
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))))
	rg := w.AppendRowGroup()
	cw, err := rg.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(
		[]int32{42}, []int16{0, 0, 1}, []int16{0, 0, 0})
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, w.Close())

	r, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	require.NoError(t, err)
	defer r.Close()

	col, err := r.RowGroup(0).Column(0)
	require.NoError(t, err)
	typed := col.(*file.Int32ColumnChunkReader)
	skipped, err := typed.Skip(2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, skipped)

	values := make([]int32, 1)
	defs := make([]int16, 1)
	reps := make([]int16, 1)
	levelsRead, valuesRead, err := typed.ReadBatch(1, values, defs, reps)
	require.NoError(t, err)
	assert.EqualValues(t, 1, levelsRead)
	assert.Equal(t, 1, valuesRead)
	assert.Equal(t, []int32{42}, values)
	assert.Equal(t, []int16{1}, defs)
	assert.Equal(t, []int16{0}, reps)
}

func TestSkipStopsAtDecodeError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	t.Run("fails on the first value", func(t *testing.T) {
		// the length prefix runs past the data, so only decoding fails
		body := memory.NewBufferBytes([]byte{3, 0, 0, 0, 'a'})
		page := file.NewDataPageV1(body, 20, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))
		defer page.Release()

		pages := new(testutils.MockPageReader)
		pages.Test(t)
		pages.TestData().Set("pages", []file.Page{page})
		pages.On("Err").Return((error)(nil))
		r := file.NewColumnReader(descr, pages, mem, nil).(*file.ByteArrayColumnChunkReader)
		defer r.Close()

		n, err := r.Skip(10)
		require.Error(t, err)
		assert.EqualValues(t, 0, n)
	})

	t.Run("valid value before the bad one", func(t *testing.T) {
		first := memory.NewBufferBytes([]byte{1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b'})
		firstPage := file.NewDataPageV1(first, 2, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.RLE, int32(first.Len()))
		defer firstPage.Release()

		// the length prefix runs past the data, so only decoding fails
		second := memory.NewBufferBytes([]byte{1, 0, 0, 0, 'a', 3, 0, 0, 0})
		secondPage := file.NewDataPageV1(second, 3, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.RLE, int32(second.Len()))
		defer secondPage.Release()

		pages := new(testutils.MockPageReader)
		pages.Test(t)
		pages.TestData().Set("pages", []file.Page{firstPage, secondPage})
		pages.On("Err").Return((error)(nil))
		r := file.NewColumnReader(descr, pages, mem, nil).(*file.ByteArrayColumnChunkReader)
		defer r.Close()

		n, err := r.Skip(4)
		require.Error(t, err)
		assert.EqualValues(t, 2, n)
	})
}

func TestSkipPastEndOfColumn(t *testing.T) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("values", parquet.Repetitions.Required, -1),
	}, -1)))

	var out bytes.Buffer
	w := file.NewParquetWriter(&out, sc.Root(), file.WithWriterProps(
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))))
	rg := w.AppendRowGroup()
	cw, err := rg.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{1, 2, 3, 4, 5}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, w.Close())

	r, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	require.NoError(t, err)
	defer r.Close()
	col, err := r.RowGroup(0).Column(0)
	require.NoError(t, err)
	icr := col.(*file.Int32ColumnChunkReader)

	skipped, err := icr.Skip(15)
	require.NoError(t, err)
	assert.EqualValues(t, 5, skipped)

	skipped, err = icr.Skip(1)
	require.NoError(t, err)
	assert.Zero(t, skipped)
}

func TestSkipAndSeekRepeatedAcrossPages(t *testing.T) {
	const numRows = 200

	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("values", parquet.Repetitions.Repeated, -1),
	}, -1)))

	var (
		values    []int32
		defs      []int16
		reps      []int16
		rowStarts []int
	)
	for row := 0; row < numRows; row++ {
		rowStarts = append(rowStarts, len(reps))
		for i := 0; i < row%4+1; i++ {
			values = append(values, int32(row*10+i))
			defs = append(defs, 1)
			if i == 0 {
				reps = append(reps, 0)
			} else {
				reps = append(reps, 1)
			}
		}
	}

	for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		t.Run(fmt.Sprintf("DataPageVersion=%v", version+1), func(t *testing.T) {
			var out bytes.Buffer
			w := file.NewParquetWriter(&out, sc.Root(), file.WithWriterProps(parquet.NewWriterProperties(
				parquet.WithDataPageVersion(version),
				parquet.WithDictionaryDefault(false),
				parquet.WithDataPageSize(64),
				parquet.WithBatchSize(16),
				parquet.WithPageIndexEnabled(true))))
			rg := w.AppendRowGroup()
			cw, err := rg.NextColumn()
			require.NoError(t, err)
			_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(values, defs, reps)
			require.NoError(t, err)
			require.NoError(t, cw.Close())
			require.NoError(t, rg.Close())
			require.NoError(t, w.Close())
			data := out.Bytes()

			pf, err := file.NewParquetReader(bytes.NewReader(data))
			require.NoError(t, err)
			rgIdx, err := pf.GetPageIndexReader().RowGroup(0)
			require.NoError(t, err)
			oidx, err := rgIdx.GetOffsetIndex(0)
			require.NoError(t, err)
			locs := oidx.GetPageLocations()
			require.NoError(t, pf.Close())
			require.Greater(t, len(locs), 2, "test data must span several data pages")
			require.GreaterOrEqual(t, locs[1].FirstRowIndex, int64(4), "first page must hold several rows")
			require.GreaterOrEqual(t, locs[2].FirstRowIndex-locs[1].FirstRowIndex, int64(3), "second page must hold several rows")
			secondPageRow := int(locs[1].FirstRowIndex)

			newReader := func(t *testing.T) *file.Int32ColumnChunkReader {
				r, err := file.NewParquetReader(bytes.NewReader(data))
				require.NoError(t, err)
				t.Cleanup(func() { r.Close() })
				col, err := r.RowGroup(0).Column(0)
				require.NoError(t, err)
				t.Cleanup(func() { col.Close() })
				return col.(*file.Int32ColumnChunkReader)
			}

			checkLevels := func(t *testing.T, col *file.Int32ColumnChunkReader, from, to int) {
				n := to - from
				gotVals := make([]int32, n)
				gotDefs := make([]int16, n)
				gotReps := make([]int16, n)
				levels, nvals, err := col.ReadBatch(int64(n), gotVals, gotDefs, gotReps)
				require.NoError(t, err)
				require.EqualValues(t, n, levels)
				assert.Equal(t, reps[from:to], gotReps[:levels])
				assert.Equal(t, defs[from:to], gotDefs[:levels])
				assert.Equal(t, values[from:to], gotVals[:nvals])
			}

			skipRows := func(t *testing.T, col *file.Int32ColumnChunkReader, n int) {
				skipped, err := col.Skip(int64(n))
				require.NoError(t, err)
				require.EqualValues(t, n, skipped)
			}

			t.Run("seek within a page", func(t *testing.T) {
				col := newReader(t)
				require.NoError(t, col.SeekToRow(1))
				checkLevels(t, col, rowStarts[1], rowStarts[2])

				require.NoError(t, col.SeekToRow(2))
				checkLevels(t, col, rowStarts[2], len(reps))
			})

			t.Run("seek to the start of the next page", func(t *testing.T) {
				col := newReader(t)
				require.NoError(t, col.SeekToRow(1))
				checkLevels(t, col, rowStarts[1], rowStarts[2])

				require.NoError(t, col.SeekToRow(int64(secondPageRow)))
				checkLevels(t, col, rowStarts[secondPageRow], rowStarts[secondPageRow+3])
			})

			t.Run("skip after skip", func(t *testing.T) {
				col := newReader(t)
				skipRows(t, col, 1)
				skipRows(t, col, 1)
				checkLevels(t, col, rowStarts[2], len(reps))
			})

			t.Run("skip after read", func(t *testing.T) {
				col := newReader(t)
				checkLevels(t, col, 0, rowStarts[1])
				skipRows(t, col, 1)
				checkLevels(t, col, rowStarts[2], len(reps))
			})

			t.Run("skip across a page boundary", func(t *testing.T) {
				col := newReader(t)
				require.NoError(t, col.SeekToRow(1))
				skipRows(t, col, secondPageRow)
				checkLevels(t, col, rowStarts[secondPageRow+1], rowStarts[secondPageRow+2])
			})

			t.Run("skip past the end", func(t *testing.T) {
				const remaining = 3
				col := newReader(t)
				require.NoError(t, col.SeekToRow(numRows-remaining))
				skipped, err := col.Skip(numRows)
				require.NoError(t, err)
				require.EqualValues(t, remaining, skipped)
				assert.False(t, col.HasNext())
			})

			t.Run("skip after seek", func(t *testing.T) {
				col := newReader(t)
				require.NoError(t, col.SeekToRow(1))
				skipRows(t, col, 1)
				checkLevels(t, col, rowStarts[2], len(reps))
			})
		})
	}
}

func (p *PrimitiveReaderSuite) TestReadBatchMultiPage() {
	const (
		levelsPerPage int = 100
		npages        int = 3
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(int32(0)), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]int32, levelsPerPage*npages)
	dresult := make([]int16, levelsPerPage*npages)
	rresult := make([]int16, levelsPerPage*npages)

	rdr := p.reader.(*file.Int32ColumnChunkReader)
	total, read, err := rdr.ReadBatch(int64(levelsPerPage*npages), vresult, dresult, rresult)
	p.NoError(err)
	p.EqualValues(levelsPerPage*npages, total)
	p.EqualValues(levelsPerPage*npages, read)
}

func (p *PrimitiveReaderSuite) TestInt32FlatRequiredSkip() {
	const (
		levelsPerPage int = 100
		npages        int = 5
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(int32(0)), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]int32, levelsPerPage/2)
	dresult := make([]int16, levelsPerPage/2)
	rresult := make([]int16, levelsPerPage/2)

	rdr := p.reader.(*file.Int32ColumnChunkReader)

	p.Run("skip_size > page_size", func() {
		// Skip first 2 pages
		skipped, _ := rdr.Skip(int64(2 * levelsPerPage))
		p.Equal(int64(2*levelsPerPage), skipped)

		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(2*levelsPerPage, int(2.5*float64(levelsPerPage))).Interface().([]int32)
		p.Equal(subVals, vresult)
	})

	p.Run("skip_size == page_size", func() {
		// skip across two pages
		skipped, _ := rdr.Skip(int64(levelsPerPage))
		p.Equal(int64(levelsPerPage), skipped)
		// read half a page
		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(int(3.5*float64(levelsPerPage)), 4*levelsPerPage).Interface().([]int32)
		p.Equal(subVals, vresult)
	})

	p.Run("skip_size < page_size", func() {
		// skip limited to a single page
		// Skip half a page
		skipped, _ := rdr.Skip(int64(levelsPerPage / 2))
		p.Equal(int64(0.5*float32(levelsPerPage)), skipped)
		// Read half a page
		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(int(4.5*float64(levelsPerPage)), p.values.Len()).Interface().([]int32)
		p.Equal(subVals, vresult)
	})
}

func (p *PrimitiveReaderSuite) TestRepetitionLvlBytesWithMaxRepZero() {
	const batchSize = 4
	p.maxDefLvl = 1
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Optional, -1)
	descr := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	// Bytes here came from the example parquet file in ARROW-17453's int32
	// column which was delta bit-packed. The key part is the first three
	// bytes: the page header reports 1 byte for repetition levels even
	// though the max rep level is 0. If that byte isn't skipped then
	// we get def levels of [1, 1, 0, 0] instead of the correct [1, 1, 1, 0].
	pageData := [...]byte{
		0x3, 0x3, 0x7, 0x80, 0x1, 0x4, 0x3,
		0x18, 0x1, 0x2, 0x0, 0x0, 0x0, 0xc,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	}

	p.pages = append(p.pages, file.NewDataPageV2(memory.NewBufferBytes(pageData[:]), batchSize, 1, batchSize,
		parquet.Encodings.DeltaBinaryPacked, 2, 1, int32(len(pageData)), false))

	p.initReader(descr)
	p.NotPanics(func() { p.reader.HasNext() })

	var (
		values  [4]int32
		defLvls [4]int16
	)
	i32Rdr := p.reader.(*file.Int32ColumnChunkReader)
	total, read, err := i32Rdr.ReadBatch(batchSize, values[:], defLvls[:], nil)
	p.NoError(err)
	p.EqualValues(batchSize, total)
	p.EqualValues(3, read)
	p.Equal([]int16{1, 1, 1, 0}, defLvls[:])
	p.Equal([]int32{12, 11, 13, 0}, values[:])
}

func (p *PrimitiveReaderSuite) TestDictionaryEncodedPages() {
	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	dummy := memory.NewResizableBuffer(mem)

	p.Run("Dict: Plain, Data: RLEDict", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.Plain)
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.RLEDict, dummy, nil, nil, 0, 0, 0)

		p.pages = append(p.pages, dictPage, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.reader.HasNext() })
		p.NoError(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Dict: Plain Dictionary, Data: Plain Dictionary", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.PlainDict)
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.PlainDict, dummy, nil, nil, 0, 0, 0)
		p.pages = append(p.pages, dictPage, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.reader.HasNext() })
		p.NoError(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Panic if dict page not first", func() {
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.RLEDict, dummy, nil, nil, 0, 0, 0)
		p.pages = append(p.pages, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Only RLE is supported", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.DeltaByteArray)
		p.pages = append(p.pages, dictPage)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Cannot have more than one dict", func() {
		dictPage1 := file.NewDictionaryPage(dummy, 0, parquet.Encodings.PlainDict)
		dictPage2 := file.NewDictionaryPage(dummy, 0, parquet.Encodings.Plain)
		p.pages = append(p.pages, dictPage1, dictPage2)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	// The decoder is chosen before the page body is read, so the body is empty.
	unsupported := []struct {
		enc   parquet.Encoding
		want  string
		types []parquet.Type
	}{
		{parquet.Encodings.RLE, "RLE encoding only supports BOOLEAN",
			[]parquet.Type{parquet.Types.Int32}},
		{parquet.Encodings.DeltaBinaryPacked, "DELTA_BINARY_PACKED encoding only supports INT32 and INT64",
			[]parquet.Type{parquet.Types.Boolean, parquet.Types.Int96, parquet.Types.Float, parquet.Types.Double,
				parquet.Types.ByteArray, parquet.Types.FixedLenByteArray}},
		{parquet.Encodings.DeltaLengthByteArray, "DELTA_LENGTH_BYTE_ARRAY encoding only supports BYTE_ARRAY",
			[]parquet.Type{parquet.Types.Boolean, parquet.Types.Int32, parquet.Types.Int64, parquet.Types.Int96,
				parquet.Types.Float, parquet.Types.Double, parquet.Types.FixedLenByteArray}},
		{parquet.Encodings.DeltaByteArray, "DELTA_BYTE_ARRAY encoding only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY",
			[]parquet.Type{parquet.Types.Boolean, parquet.Types.Int32, parquet.Types.Int64, parquet.Types.Int96,
				parquet.Types.Float, parquet.Types.Double}},
		{parquet.Encodings.ByteStreamSplit, "BYTE_STREAM_SPLIT encoding only supports INT32, INT64, FLOAT, DOUBLE and FIXED_LEN_BYTE_ARRAY",
			[]parquet.Type{parquet.Types.Boolean, parquet.Types.Int96, parquet.Types.ByteArray}},
	}
	for _, tt := range unsupported {
		for _, typ := range tt.types {
			p.Run(fmt.Sprintf("Unsupported encoding %s for %s", tt.enc, typ), func() {
				typeLen := int32(-1)
				if typ == parquet.Types.FixedLenByteArray {
					typeLen = 4
				}
				node, err := schema.NewPrimitiveNode("a", parquet.Repetitions.Required, typ, -1, typeLen)
				p.Require().NoError(err)
				descr := schema.NewColumn(node, p.maxDefLvl, p.maxRepLvl)
				dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, tt.enc, dummy, nil, nil, 0, 0, 0)
				p.pages = append(p.pages, dataPage)
				p.initReader(descr)
				p.NotPanics(func() { p.False(p.reader.HasNext()) })
				p.ErrorContains(p.reader.Err(), fmt.Sprintf("%s, got %s", tt.want, typ))
				p.pages = p.pages[:0]
			})
		}
	}

	p.pages = p.pages[:2]
}

func (p *PrimitiveReaderSuite) TestSeekToRowRequired() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	for _, enc := range []parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.RLEDict} {
		p.maxDefLvl, p.maxRepLvl = 0, 0
		typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
		d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)

		p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion,
			d, npages, levelsPerPage, reflect.TypeOf(int32(0)), enc)
		p.nlevels = npages * levelsPerPage
		p.initReader(d)

		p.pager.(*testutils.MockPageReader).TestData().Set("row_map", func(idx int64) int {
			return (int(idx) / levelsPerPage) + 1
		})

		// check seek back to beginning
		p.checkResults(reflect.TypeOf(int32(0)))
		p.Require().NoError(p.reader.SeekToRow(0))
		p.checkResults(reflect.TypeOf(int32(0)))

		p.Require().NoError(p.reader.SeekToRow(550))
		p.nvalues -= 550
		p.nlevels -= 550
		p.values = p.values.Slice(550, p.values.Len())
		p.checkResults(reflect.TypeOf(int32(0)))
		p.clear()
	}
}

func (p *PrimitiveReaderSuite) TestSeekToRowOptional() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	for _, enc := range []parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.RLEDict} {
		p.maxDefLvl, p.maxRepLvl = 4, 0
		typ := schema.NewInt32Node("a", parquet.Repetitions.Optional, -1)
		d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)

		p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion,
			d, npages, levelsPerPage, reflect.TypeOf(int32(0)), enc)
		p.nlevels = npages * levelsPerPage
		p.initReader(d)

		p.pager.(*testutils.MockPageReader).TestData().Set("row_map", func(idx int64) int {
			return (int(idx) / levelsPerPage) + 1
		})

		// check seek back to beginning
		p.checkResults(reflect.TypeOf(int32(0)))
		p.Require().NoError(p.reader.SeekToRow(0))
		p.checkResults(reflect.TypeOf(int32(0)))

		p.Require().NoError(p.reader.SeekToRow(550))
		realValuesSkipped := 0
		for i := 0; i < 550; i++ {
			if p.defLevels[i] == p.maxDefLvl {
				realValuesSkipped++
			}
		}

		p.nvalues -= realValuesSkipped
		p.values = p.values.Slice(realValuesSkipped, p.values.Len())
		p.nlevels -= 550
		p.defLevels = p.defLevels[550:]
		p.checkResults(reflect.TypeOf(int32(0)))
		p.clear()
	}
}

func TestPrimitiveReader(t *testing.T) {
	t.Parallel()
	t.Run("datapage v1", func(t *testing.T) {
		suite.Run(t, new(PrimitiveReaderSuite))
	})
	t.Run("datapage v2", func(t *testing.T) {
		suite.Run(t, &PrimitiveReaderSuite{dataPageVersion: parquet.DataPageV2})
	})
}

func TestFullSeekRow(t *testing.T) {
	mem := memory.DefaultAllocator

	for _, dataPageVersion := range []parquet.DataPageVersion{parquet.DataPageV2, parquet.DataPageV1} {
		t.Run(fmt.Sprintf("DataPageVersion=%v", dataPageVersion+1), func(t *testing.T) {
			props := parquet.NewWriterProperties(parquet.WithAllocator(mem),
				parquet.WithDataPageVersion(dataPageVersion), parquet.WithDataPageSize(1),
				parquet.WithPageIndexEnabled(true))

			sc := arrow.NewSchema([]arrow.Field{
				{Name: "c0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "c1", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "c2", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
			}, nil)

			tbl, err := array.TableFromJSON(mem, sc, []string{`[
				{"c0": 1,    "c1": "a",  "c2": [1]},
				{"c0": 2,    "c1": "b",  "c2": [1, 2]},
				{"c0": 3,    "c1": "c",  "c2": [null]},
				{"c0": null, "c1": "d",  "c2": []},
				{"c0": 5,    "c1": null, "c2": [3, 3, 3]},
				{"c0": 6,    "c1": "f",  "c2": null}
			]`})
			require.NoError(t, err)
			defer tbl.Release()

			schema := tbl.Schema()
			arrWriterProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))

			var buf bytes.Buffer
			wr, err := pqarrow.NewFileWriter(schema, &buf, props, arrWriterProps)
			require.NoError(t, err)

			require.NoError(t, wr.WriteTable(tbl, tbl.NumRows()))
			require.NoError(t, wr.Close())

			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()

			rgr := rdr.RowGroup(0)
			col, err := rgr.Column(0)
			require.NoError(t, err)

			icr := col.(*file.Int64ColumnChunkReader)
			require.NoError(t, icr.SeekToRow(3))

			vals := make([]int64, 5)
			defLvls := make([]int16, 5)
			repLvls := make([]int16, 5)

			total, read, err := icr.ReadBatch(5, vals, defLvls, repLvls)
			require.NoError(t, err)

			assert.EqualValues(t, 3, total)
			assert.EqualValues(t, 2, read)

			assert.Equal(t, []int64{5, 6}, vals[:read])
			assert.Equal(t, []int16{0, 1, 1}, defLvls[:total])
			assert.Equal(t, []int16{0, 0, 0}, repLvls[:total])

			col2, err := rgr.Column(2)
			require.NoError(t, err)

			icr = col2.(*file.Int64ColumnChunkReader)
			require.NoError(t, icr.SeekToRow(3))

			total, read, err = icr.ReadBatch(5, vals, defLvls, repLvls)
			require.NoError(t, err)

			// 5 definition levels are read for the last 3 rows
			// because of the repetitions
			assert.EqualValues(t, 5, total)
			// only 3 physical values though
			assert.EqualValues(t, 3, read)

			assert.Equal(t, []int64{3, 3, 3}, vals[:read])
			assert.Equal(t, []int16{1, 3, 3, 3, 0}, defLvls[:total])
			assert.Equal(t, []int16{0, 0, 1, 1, 0}, repLvls[:total])
		})
	}
}

type seekBatchReader[T any] interface {
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}

// readRows reads n flat values, returning nil entries for nulls.
func readRows[T any](t *testing.T, r seekBatchReader[T], n int, maxDef int16) []*T {
	t.Helper()
	vals := make([]T, n)
	defs := make([]int16, n)
	got, nvals, err := r.ReadBatch(int64(n), vals, defs, nil)
	require.NoError(t, err)
	require.EqualValues(t, n, got)

	out := make([]*T, n)
	next := 0
	for i := range out {
		if maxDef == 0 || defs[i] == maxDef {
			out[i] = &vals[next]
			next++
		}
	}
	require.Equal(t, nvals, next)
	return out
}

// Both files hold dictionary encoded columns whose chunks have no
// dictionary_page_offset, so the dictionary page sits at data_page_offset.
func TestSeekToRowDictionaryAtDataPageOffset(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	require.DirExists(t, dir)

	t.Run("offset index", func(t *testing.T) {
		fr, err := file.OpenParquetFile(filepath.Join(dir, "alltypes_tiny_pages.parquet"), false)
		require.NoError(t, err)
		defer fr.Close()
		rg := fr.RowGroup(0)

		const col, nrows = 2, 100
		maxDef := fr.MetaData().Schema.Column(col).MaxDefinitionLevel()
		open := func() *file.Int32ColumnChunkReader {
			cr, err := rg.Column(col)
			require.NoError(t, err)
			return cr.(*file.Int32ColumnChunkReader)
		}
		want := readRows[int32](t, open(), nrows, maxDef)

		// Reading past the seek target crosses data pages, which only works if the
		// dictionary load left the page stream where it was.
		cr := open()
		require.NoError(t, cr.SeekToRow(0))
		require.Equal(t, want, readRows[int32](t, cr, nrows, maxDef))
	})

	t.Run("no offset index", func(t *testing.T) {
		fr, err := file.OpenParquetFile(filepath.Join(dir, "datapage_v2.snappy.parquet"), false)
		require.NoError(t, err)
		defer fr.Close()
		rg := fr.RowGroup(0)

		const col, nrows = 2, 5
		maxDef := fr.MetaData().Schema.Column(col).MaxDefinitionLevel()
		open := func() *file.Float64ColumnChunkReader {
			cr, err := rg.Column(col)
			require.NoError(t, err)
			return cr.(*file.Float64ColumnChunkReader)
		}
		want := readRows[float64](t, open(), nrows, maxDef)

		t.Run("fresh reader", func(t *testing.T) {
			cr := open()
			require.NoError(t, cr.SeekToRow(4))
			require.Equal(t, want[4:], readRows[float64](t, cr, 1, maxDef))
		})

		t.Run("reseek after read", func(t *testing.T) {
			cr := open()
			readRows[float64](t, cr, nrows, maxDef)
			require.NoError(t, cr.SeekToRow(0))
			require.Equal(t, want, readRows[float64](t, cr, nrows, maxDef))
		})
	})
}

func sharedOffsetFile(t *testing.T, values []int32, pageIndex bool) []byte {
	t.Helper()
	sc := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("x", parquet.Repetitions.Required, -1),
	}, -1))
	var out bytes.Buffer
	w, err := file.NewParquetWriterWithError(&out, sc, file.WithWriterProps(parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryPageSizeLimit(1<<20),
		parquet.WithDictionaryCostFallbackFor("x", false),
		parquet.WithDataPageSize(256),
		parquet.WithBatchSize(50),
		parquet.WithPageIndexEnabled(pageIndex))))
	require.NoError(t, err)
	rg := w.AppendRowGroup()
	cw, err := rg.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, w.Close())

	data := out.Bytes()
	footerStart := len(data) - 8 - int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	md := format.NewFileMetaData()
	_, err = thrift.DeserializeThrift(md, data[footerStart:len(data)-8])
	require.NoError(t, err)
	chunk := md.RowGroups[0].Columns[0].MetaData
	require.True(t, chunk.IsSetDictionaryPageOffset())
	require.Greater(t, chunk.DataPageOffset, chunk.GetDictionaryPageOffset())
	chunk.DataPageOffset = chunk.GetDictionaryPageOffset()

	var footer bytes.Buffer
	_, err = thrift.NewThriftSerializer().Serialize(md, &footer, nil)
	require.NoError(t, err)
	var footerLen [4]byte
	binary.LittleEndian.PutUint32(footerLen[:], uint32(footer.Len()))
	return append(append(append(bytes.Clone(data[:footerStart]), footer.Bytes()...), footerLen[:]...), "PAR1"...)
}

// Unreleased parquet-mr builds between PARQUET-1850 and PARQUET-1977 wrote
// chunks whose data_page_offset equals dictionary_page_offset, so the
// dictionary page sits at both offsets and a seek has to read it from there.
func TestSeekToRowDictionaryPageOffsetEqualsDataPageOffset(t *testing.T) {
	const nrows = 2000
	values := make([]int32, nrows)
	for i := range values {
		values[i] = int32((i * 37) % 1000)
	}

	for _, pageIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("offset index %v", pageIndex), func(t *testing.T) {
			rdr, err := file.NewParquetReader(bytes.NewReader(sharedOffsetFile(t, values, pageIndex)))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rdr.Close()) })
			open := func() *file.Int32ColumnChunkReader {
				cr, err := rdr.RowGroup(0).Column(0)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, cr.Close()) })
				return cr.(*file.Int32ColumnChunkReader)
			}
			read := func(cr *file.Int32ColumnChunkReader, n int) []int32 {
				got := make([]int32, n)
				read, _, err := cr.ReadBatch(int64(n), got, nil, nil)
				require.NoError(t, err)
				require.EqualValues(t, n, read)
				return got
			}

			require.Equal(t, values, read(open(), nrows))

			const row = 150
			cr := open()
			require.NoError(t, cr.SeekToRow(row))
			require.Equal(t, values[row:row+250], read(cr, 250))

			if pageIndex {
				return
			}
			t.Run("seek to row 0 after a read", func(t *testing.T) {
				cr := open()
				read(cr, 10)
				require.NoError(t, cr.SeekToRow(0))
				require.Equal(t, values[:100], read(cr, 100))
			})
		})
	}
}

// The dictionary page offset of each column chunk in this file is past its
// data page offset of 0.
func TestGetDictionaryPageOffsetPastDataPageOffset(t *testing.T) {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		dir = "../../parquet-testing/data"
		t.Log("PARQUET_TEST_DATA not set, using ../../parquet-testing/data")
	}
	require.DirExists(t, dir)

	fr, err := file.OpenParquetFile(filepath.Join(dir, "column_chunk_key_value_metadata.parquet"), false)
	require.NoError(t, err)
	defer fr.Close()

	for col := range fr.MetaData().Schema.NumColumns() {
		pr, err := fr.RowGroup(0).GetColumnPageReader(col)
		require.NoError(t, err)
		_, err = pr.GetDictionaryPage()
		require.Error(t, err)
		require.NoError(t, pr.Close())
	}
}

const (
	encSeekRowGroups = 2
	encSeekRows      = 1500
	encSeekKey       = "0123456789012345"
)

func encryptedDictionaryFile(t *testing.T, pageIndex bool) []byte {
	t.Helper()
	sc := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("x", parquet.Repetitions.Required, -1),
	}, -1))

	var out bytes.Buffer
	w, err := file.NewParquetWriterWithError(&out, sc, file.WithWriterProps(parquet.NewWriterProperties(
		parquet.WithEncryptionProperties(parquet.NewFileEncryptionProperties(encSeekKey)),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryCostFallbackFor("x", false),
		parquet.WithDictionaryPageSizeLimit(1<<20),
		parquet.WithDataPageSize(400),
		parquet.WithBatchSize(16),
		parquet.WithPageIndexEnabled(pageIndex))))
	require.NoError(t, err)
	for rg := range encSeekRowGroups {
		rgw, err := w.AppendRowGroupChecked()
		require.NoError(t, err)
		cw, err := rgw.NextColumn()
		require.NoError(t, err)
		vals := make([]int64, encSeekRows)
		for i := range vals {
			vals[i] = int64((i*31 + rg) % 97)
		}
		_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		require.NoError(t, rgw.Close())
	}
	require.NoError(t, w.Close())
	return out.Bytes()
}

func openEncryptedFile(t *testing.T, data []byte) *file.Reader {
	t.Helper()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.FileDecryptProps = parquet.NewFileDecryptionProperties(parquet.WithFooterKey(encSeekKey))
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdr.Close()) })
	return rdr
}

// dataPageRows returns the number of values in each data page of column 0, and
// whether the chunk starts with a dictionary page.
func dataPageRows(t *testing.T, rg *file.RowGroupReader) (rows []int64, dictionary bool) {
	t.Helper()
	pr, err := rg.GetColumnPageReader(0)
	require.NoError(t, err)
	defer pr.Close()
	for pr.Next() {
		switch pg := pr.Page().(type) {
		case *file.DictionaryPage:
			dictionary = true
		case file.DataPage:
			rows = append(rows, int64(pg.NumValues()))
		}
	}
	require.NoError(t, pr.Err())
	return rows, dictionary
}

func TestSeekToRowEncryptedDictionary(t *testing.T) {
	const row = 700
	for _, pageIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("offset index %v", pageIndex), func(t *testing.T) {
			rdr := openEncryptedFile(t, encryptedDictionaryFile(t, pageIndex))
			rg := rdr.RowGroup(encSeekRowGroups - 1)

			chunk, err := rdr.MetaData().RowGroup(encSeekRowGroups - 1).ColumnChunk(0)
			require.NoError(t, err)
			require.Equal(t, pageIndex, chunk.GetOffsetIndexLocation() != nil)
			rows, dictionary := dataPageRows(t, rg)
			require.True(t, dictionary)
			require.Greater(t, len(rows), 4)
			require.Less(t, rows[0], int64(row))

			open := func() *file.Int64ColumnChunkReader {
				cr, err := rg.Column(0)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, cr.Close()) })
				return cr.(*file.Int64ColumnChunkReader)
			}
			want := readRows[int64](t, open(), encSeekRows, 0)

			cr := open()
			require.NoError(t, cr.SeekToRow(row))
			require.Equal(t, want[row:row+300], readRows[int64](t, cr, 300, 0))
		})
	}
}

func TestGetDictionaryPageOnEncryptedChunk(t *testing.T) {
	rdr := openEncryptedFile(t, encryptedDictionaryFile(t, false))
	rg := rdr.RowGroup(encSeekRowGroups - 1)
	wantRows, _ := dataPageRows(t, rg)

	scan := func(t *testing.T, pr file.PageReader) {
		var rows []int64
		for pr.Next() {
			rows = append(rows, int64(pr.Page().(file.DataPage).NumValues()))
		}
		require.NoError(t, pr.Err())
		require.Equal(t, wantRows, rows)
	}

	t.Run("before the first page", func(t *testing.T) {
		pr, err := rg.GetColumnPageReader(0)
		require.NoError(t, err)
		defer pr.Close()

		dict, err := pr.GetDictionaryPage()
		require.NoError(t, err)
		require.NotNil(t, dict)

		require.True(t, pr.Next(), "reading after GetDictionaryPage: %v", pr.Err())
		require.IsType(t, &file.DictionaryPage{}, pr.Page())
		scan(t, pr)
	})

	t.Run("after the dictionary page was read", func(t *testing.T) {
		pr, err := rg.GetColumnPageReader(0)
		require.NoError(t, err)
		defer pr.Close()

		require.True(t, pr.Next(), pr.Err())
		want := pr.Page().(*file.DictionaryPage).NumValues()

		dict, err := pr.GetDictionaryPage()
		require.NoError(t, err)
		require.NotNil(t, dict)
		require.Equal(t, want, dict.NumValues())
		scan(t, pr)
	})
}

// Reject ALP before non-floating-point decoder traits can panic.
func TestALPOnIntegerColumnIsRejected(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.Int32, -1, -1)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	body := memory.NewResizableBuffer(mem)
	body.Resize(64)
	clear(body.Bytes())
	page := file.NewDataPageV1(body, 4, parquet.Encodings.ALP,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))
	defer page.Release()

	pages := new(testutils.MockPageReader)
	pages.Test(t)
	pages.TestData().Set("pages", []file.Page{page})
	pages.On("Err").Return((error)(nil))
	r := file.NewColumnReader(descr, pages, mem, nil)
	defer r.Close()
	total, read, err := r.(*file.Int32ColumnChunkReader).ReadBatch(4, make([]int32, 4), nil, nil)
	require.ErrorContains(t, err, "ALP encoding only supports FLOAT and DOUBLE")
	require.Zero(t, total)
	require.Zero(t, read)
}

// A mismatched page count must return an error instead of a zero-length read,
// which the column reader would retry indefinitely.
func TestALPPageClaimingTooManyValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	node, err := schema.NewPrimitiveNode("value", parquet.Repetitions.Required, parquet.Types.Double, -1, -1)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	enc := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.ALP, false, descr, mem)
	defer enc.Release()
	enc.(encoding.Float64Encoder).Put([]float64{1.5, 2.5})
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	body := memory.NewBufferBytes(buf.Bytes())
	page := file.NewDataPageV1(body, 8, parquet.Encodings.ALP,
		parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))
	defer page.Release()

	pages := new(testutils.MockPageReader)
	pages.Test(t)
	pages.TestData().Set("pages", []file.Page{page})
	pages.On("Err").Return((error)(nil))
	r := file.NewColumnReader(descr, pages, mem, nil)
	defer r.Close()
	values := make([]float64, 8)
	_, read, err := r.(*file.Float64ColumnChunkReader).ReadBatch(8, values, nil, nil)
	require.ErrorContains(t, err, "ALP page holds 2 values")
	require.Equal(t, 2, read, "the values the page does hold still come back")
	require.Equal(t, []float64{1.5, 2.5}, values[:read])
}

func newCorruptPageFile(t *testing.T, rep parquet.Repetition) (*file.Reader, memory.Allocator) {
	t.Helper()
	sc := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("x", rep, -1),
	}, -1))
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Gzip),
		parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(1), parquet.WithPageIndexEnabled(true))

	// each WriteBatch call ends in its own page
	var out bytes.Buffer
	w, err := file.NewParquetWriterWithError(&out, sc, file.WithWriterProps(props))
	require.NoError(t, err)
	rg := w.AppendRowGroup()
	cw, err := rg.NextColumn()
	require.NoError(t, err)
	for page := range int32(3) {
		vals := make([]int32, 10)
		var defs []int16
		if rep == parquet.Repetitions.Optional {
			defs = make([]int16, 10)
		}
		for i := range vals {
			vals[i] = page*10 + int32(i)
			if defs != nil {
				defs[i] = 1
			}
		}
		_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, defs, nil)
		require.NoError(t, err)
	}
	require.NoError(t, cw.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, w.Close())
	data := out.Bytes()

	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	rgIdx, err := rdr.GetPageIndexReader().RowGroup(0)
	require.NoError(t, err)
	oidx, err := rgIdx.GetOffsetIndex(0)
	require.NoError(t, err)
	locs := oidx.GetPageLocations()
	require.Len(t, locs, 3)
	require.NoError(t, rdr.Close())

	// the last eight bytes of a gzip stream are its CRC-32 and length
	data[locs[1].Offset+int64(locs[1].CompressedPageSize)-8] ^= 0xff

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	rdr, err = file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	t.Cleanup(func() { rdr.Close() })
	return rdr, mem
}

func newCorruptPageReader(t *testing.T) *file.Int32ColumnChunkReader {
	t.Helper()
	rdr, _ := newCorruptPageFile(t, parquet.Repetitions.Required)
	col, err := rdr.RowGroup(0).Column(0)
	require.NoError(t, err)
	t.Cleanup(func() { col.Close() })
	return col.(*file.Int32ColumnChunkReader)
}

func TestReadBatchStopsAtCorruptPage(t *testing.T) {
	icr := newCorruptPageReader(t)

	vals := make([]int32, 30)
	total, n, err := icr.ReadBatch(int64(len(vals)), vals, nil, nil)
	require.ErrorContains(t, err, "invalid checksum")
	assert.EqualValues(t, 10, total)
	assert.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, vals[:n])

	total, _, err2 := icr.ReadBatch(int64(len(vals)), vals, nil, nil)
	assert.Equal(t, err, err2)
	assert.Zero(t, total)
	assert.False(t, icr.HasNext())
}

func TestSkipStopsAtCorruptPage(t *testing.T) {
	icr := newCorruptPageReader(t)

	n, err := icr.Skip(25)
	require.ErrorContains(t, err, "invalid checksum")
	assert.EqualValues(t, 10, n)
}

func TestSeekToRowAfterCorruptPage(t *testing.T) {
	icr := newCorruptPageReader(t)

	vals := make([]int32, 30)
	_, _, err := icr.ReadBatch(int64(len(vals)), vals, nil, nil)
	require.ErrorContains(t, err, "invalid checksum")

	require.NoError(t, icr.SeekToRow(25))
	_, n, err := icr.ReadBatch(5, vals, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []int32{25, 26, 27, 28, 29}, vals[:n])
}

func TestSeekToRowToCorruptPageStopsReader(t *testing.T) {
	icr := newCorruptPageReader(t)

	vals := make([]int32, 30)
	_, n, err := icr.ReadBatch(5, vals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1, 2, 3, 4}, vals[:n])

	err = icr.SeekToRow(15)
	require.ErrorContains(t, err, "invalid checksum")
	assert.False(t, icr.HasNext())

	_, n, err2 := icr.ReadBatch(5, vals, nil, nil)
	assert.Equal(t, err, err2)
	assert.Zero(t, n)

	require.NoError(t, icr.SeekToRow(25))
	_, n, err = icr.ReadBatch(5, vals, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []int32{25, 26, 27, 28, 29}, vals[:n])
}

func TestSeekToRowOutOfRangeKeepsReader(t *testing.T) {
	for _, row := range []int64{1000, -1} {
		t.Run(fmt.Sprintf("row %d", row), func(t *testing.T) {
			icr := newCorruptPageReader(t)

			vals := make([]int32, 30)
			_, n, err := icr.ReadBatch(5, vals, nil, nil)
			require.NoError(t, err)
			require.Equal(t, []int32{0, 1, 2, 3, 4}, vals[:n])

			require.Error(t, icr.SeekToRow(row))
			assert.True(t, icr.HasNext())

			_, n, err = icr.ReadBatch(3, vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, []int32{5, 6, 7}, vals[:n])
		})
	}

	t.Run("a stopped reader stays stopped", func(t *testing.T) {
		icr := newCorruptPageReader(t)

		vals := make([]int32, 30)
		_, _, err := icr.ReadBatch(int64(len(vals)), vals, nil, nil)
		require.ErrorContains(t, err, "invalid checksum")

		require.ErrorContains(t, icr.SeekToRow(1000), "cannot seek column reader to row index 1000")
		assert.False(t, icr.HasNext())

		total, _, err2 := icr.ReadBatch(5, vals, nil, nil)
		assert.Equal(t, err, err2)
		assert.Zero(t, total)
	})
}

// patchedDictionaryFile writes two row groups of 100 dictionary encoded rows, or
// plain rows when dict is false, and damages the footer entry of the second chunk.
// With a dictionary, the chunk's data page offset is moved onto the dictionary page
// and the chunk is cut after it. Without one, the chunk is cut to nothing.
func patchedDictionaryFile(t *testing.T, dict bool) []byte {
	t.Helper()
	sc := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("x", parquet.Repetitions.Required, -1),
	}, -1))
	var out bytes.Buffer
	w, err := file.NewParquetWriterWithError(&out, sc, file.WithWriterProps(
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(dict))))
	require.NoError(t, err)
	for range 2 {
		rg := w.AppendRowGroup()
		cw, err := rg.NextColumn()
		require.NoError(t, err)
		vals := make([]int32, 100)
		for i := range vals {
			vals[i] = int32(i % 7)
		}
		_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		require.NoError(t, rg.Close())
	}
	require.NoError(t, w.Close())
	data := out.Bytes()

	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	footerStart := len(data) - 8 - footerLen
	md := format.NewFileMetaData()
	_, err = thrift.DeserializeThrift(md, data[footerStart:len(data)-8])
	require.NoError(t, err)
	meta := md.RowGroups[1].Columns[0].MetaData
	cut := func(size int64) { meta.TotalCompressedSize, meta.TotalUncompressedSize = size, size }
	if !dict {
		cut(0)
	} else {
		dictOffset, dataOffset := meta.GetDictionaryPageOffset(), meta.DataPageOffset
		meta.DataPageOffset = dictOffset
		cut(dataOffset - dictOffset)
	}
	var footer bytes.Buffer
	_, err = thrift.NewThriftSerializer().Serialize(md, &footer, nil)
	require.NoError(t, err)
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(footer.Len()))
	return append(append(append(bytes.Clone(data[:footerStart]), footer.Bytes()...), length[:]...), "PAR1"...)
}

func TestSeekToRowWithoutDataPage(t *testing.T) {
	tests := []struct {
		name   string
		dict   bool
		row    int64
		errMsg string
	}{
		{"chunk ends after the dictionary page", true, 0, "seek found no data page for row index 0"},
		{"chunk ends after the dictionary page at a later row", true, 5, "seek found no data page for row index 5"},
		{"chunk without a dictionary is empty", false, 5, "seek found no data page for row index 5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			rdr, err := file.NewParquetReader(bytes.NewReader(patchedDictionaryFile(t, tt.dict)),
				file.WithReadProps(parquet.NewReaderProperties(mem)))
			require.NoError(t, err)
			defer rdr.Close()
			col, err := rdr.RowGroup(1).Column(0)
			require.NoError(t, err)
			defer col.Close()
			icr := col.(*file.Int32ColumnChunkReader)

			err = icr.SeekToRow(tt.row)
			require.ErrorContains(t, err, tt.errMsg)
			require.NotErrorIs(t, err, file.ErrSeekRowOutOfRange)
			assert.False(t, icr.HasNext())

			total, _, err2 := icr.ReadBatch(5, make([]int32, 5), nil, nil)
			assert.Equal(t, err, err2)
			assert.Zero(t, total)
		})
	}
}

// A page reader can end right after a dictionary page, with or without an error.
func TestSeekToRowAfterDictionaryPageEnds(t *testing.T) {
	pageErr := errors.New("boom reading next page")
	tests := []struct {
		name string
		err  error
	}{
		{"page reader ends", nil},
		{"page reader fails", pageErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descr := schema.NewColumn(schema.NewInt32Node("x", parquet.Repetitions.Required, -1), 0, 0)
			dict, _ := testutils.MakeDictPage(descr, []int32{1, 2, 3}, []int{3}, parquet.Encodings.Plain)
			m := new(testutils.MockPageReader)
			m.Test(t)
			m.TestData().Set("pages", []file.Page{dict})
			m.TestData().Set("row_map", func(int64) int { return 0 })
			m.On("Err").Return(tt.err)
			icr := file.NewColumnReader(descr, m, memory.DefaultAllocator, nil).(*file.Int32ColumnChunkReader)

			err := icr.SeekToRow(3)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.ErrorContains(t, err, "seek found no data page for row index 3")
				require.NotErrorIs(t, err, file.ErrSeekRowOutOfRange)
			}
			assert.False(t, icr.HasNext())

			total, _, err2 := icr.ReadBatch(5, make([]int32, 5), nil, nil)
			assert.Equal(t, err, err2)
			assert.Zero(t, total)
		})
	}
}

// A seek into a damaged chunk must not leave the record reader on the previous row group.
func TestRecordReaderSeekToRowWithoutDataPage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	rdr, err := file.NewParquetReader(bytes.NewReader(patchedDictionaryFile(t, true)),
		file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer rdr.Close()
	reader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 5}, mem)
	require.NoError(t, err)
	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()

	err = rr.SeekToRow(105)
	require.ErrorContains(t, err, "seek found no data page for row index 5")
	require.NotErrorIs(t, err, file.ErrSeekRowOutOfRange)
	assert.False(t, rr.Next())
}

type closeErrorPageReader struct {
	file.PageReader
}

func (r closeErrorPageReader) Close() error {
	r.PageReader.Close()
	return errors.New("close failed")
}

func TestRecordReaderStopsAfterPageReaderFailure(t *testing.T) {
	tests := []struct {
		name string
		fail func(rr file.RecordReaderWithError) error
	}{
		{"failed seek", func(rr file.RecordReaderWithError) error { return rr.SeekToRow(15) }},
		{"failed page reader replacement", func(rr file.RecordReaderWithError) error { return rr.SetPageReaderWithError(nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdr, mem := newCorruptPageFile(t, parquet.Repetitions.Optional)
			pages, err := rdr.RowGroup(0).GetColumnPageReader(0)
			require.NoError(t, err)
			rr := file.NewRecordReader(rdr.MetaData().Schema.Column(0),
				file.LevelInfo{NullSlotUsage: 1, DefLevel: 1}, arrow.PrimitiveTypes.Int32, mem, rdr.BufferPool()).(file.RecordReaderWithError)
			defer rr.Release()
			rr.SetPageReader(closeErrorPageReader{pages})
			require.NoError(t, rr.Reserve(10))

			n, err := rr.ReadRecords(5)
			require.NoError(t, err)
			require.EqualValues(t, 5, n)

			failure := tt.fail(rr)
			require.Error(t, failure)

			n, err = rr.ReadRecords(5)
			assert.Zero(t, n)
			assert.Equal(t, failure, err)
		})
	}
}

// The record reader buffers a whole page of levels, so an out of range seek must
// not drop the ones it has not consumed yet.
func TestRecordReaderSeekToRowOutOfRangeKeepsLevels(t *testing.T) {
	rdr, mem := newCorruptPageFile(t, parquet.Repetitions.Optional)
	pages, err := rdr.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	rr := file.NewRecordReader(rdr.MetaData().Schema.Column(0),
		file.LevelInfo{NullSlotUsage: 1, DefLevel: 1}, arrow.PrimitiveTypes.Int32, mem, rdr.BufferPool()).(file.RecordReaderWithError)
	defer rr.Release()
	rr.SetPageReader(pages)
	require.NoError(t, rr.Reserve(10))

	n, err := rr.ReadRecords(5)
	require.NoError(t, err)
	require.EqualValues(t, 5, n)

	require.Error(t, rr.SeekToRow(1000))

	n, err = rr.ReadRecords(5)
	require.NoError(t, err)
	assert.EqualValues(t, 5, n)
	assert.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, arrow.Int32Traits.CastFromBytes(rr.Values())[:10])
}

// rleLevels is a V1 definition level block holding n defined levels.
func rleLevels(n int) []byte {
	run := []byte{byte(n << 1), 1}
	return append([]byte{byte(len(run)), 0, 0, 0}, run...)
}

// truncatedByteArray is a PLAIN value whose length prefix runs past the data, so
// SetData accepts it and only decoding fails.
var truncatedByteArray = []byte{3, 0, 0, 0, 'a'}

func TestReadBatchStopsAtDecodeError(t *testing.T) {
	tests := []struct {
		name string
		rep  parquet.Repetition
		body []byte
	}{
		{"required value", parquet.Repetitions.Required, truncatedByteArray},
		{"optional value", parquet.Repetitions.Optional, append(rleLevels(20), truncatedByteArray...)},
		{"truncated levels", parquet.Repetitions.Optional, rleLevels(5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			node, err := schema.NewPrimitiveNode("value", tt.rep, parquet.Types.ByteArray, -1, -1)
			require.NoError(t, err)
			maxDef := int16(0)
			if tt.rep == parquet.Repetitions.Optional {
				maxDef = 1
			}
			descr := schema.NewColumn(node, maxDef, 0)

			body := memory.NewBufferBytes(tt.body)
			page := file.NewDataPageV1(body, 20, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.RLE, int32(body.Len()))
			defer page.Release()

			pages := new(testutils.MockPageReader)
			pages.Test(t)
			pages.TestData().Set("pages", []file.Page{page})
			pages.On("Err").Return((error)(nil))
			r := file.NewColumnReader(descr, pages, mem, nil).(*file.ByteArrayColumnChunkReader)
			defer r.Close()

			vals := make([]parquet.ByteArray, 10)
			defs := make([]int16, 10)
			_, _, err = r.ReadBatch(10, vals, defs, nil)
			require.Error(t, err)
			assert.Equal(t, err, r.Err())
			assert.False(t, r.HasNext())

			total, _, err2 := r.ReadBatch(10, vals, defs, nil)
			assert.Equal(t, err, err2)
			assert.Zero(t, total)
		})
	}
}

func BenchmarkReadInt32Column(b *testing.B) {
	// generate parquet with RLE-dictionary encoded int32 column
	tempdir := b.TempDir()
	filepath := filepath.Join(tempdir, "rle-dict-int32.parquet")

	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(128*1024*1024), // 128MB
		parquet.WithBatchSize(128*1024*1024),
		parquet.WithMaxRowGroupLength(100_000),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithVersion(parquet.V2_LATEST),
	)
	outFile, err := os.Create(filepath)
	require.NoError(b, err)

	sc, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("col", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(b, err)

	writer := file.NewParquetWriter(outFile, sc, file.WithWriterProps(props))

	// 10 row groups of 100 000 rows = 1 000 000 rows in total
	value := int32(1)
	for range 10 {
		rgWriter := writer.AppendBufferedRowGroup()
		cwr, _ := rgWriter.Column(0)
		cw := cwr.(*file.Int32ColumnChunkWriter)
		valuesIn := make([]int32, 0, 100_000)
		repeats := 1
		for len(valuesIn) < 100_000 {
			repeatedValue := make([]int32, repeats)
			for i := range repeatedValue {
				repeatedValue[i] = value
			}
			if len(valuesIn)+len(repeatedValue) > 100_000 {
				repeatedValue = repeatedValue[:100_000-len(valuesIn)]
			}
			valuesIn = append(valuesIn, repeatedValue[:]...)
			// repeat values from 1 to 50 times
			repeats = (repeats % 50) + 1
			value++
		}
		cw.WriteBatch(valuesIn, nil, nil)
		rgWriter.Close()
	}
	err = writer.Close()
	require.NoError(b, err)

	reader, err := file.OpenParquetFile(filepath, false)
	require.NoError(b, err)
	defer reader.Close()

	numValues := reader.NumRows()
	values := make([]int32, numValues)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startIndex := 0
		for rg := 0; rg < reader.NumRowGroups(); rg++ {
			rgReader := reader.RowGroup(rg)
			colReader, err := rgReader.Column(0)
			require.NoError(b, err)

			cr, ok := colReader.(*file.Int32ColumnChunkReader)
			require.True(b, ok)

			b.StartTimer()
			_, valuesRead, err := cr.ReadBatch(rgReader.NumRows(), values, nil, nil)
			b.StopTimer()
			require.NoError(b, err)

			startIndex += valuesRead
			require.Equal(b, rgReader.NumRows(), int64(valuesRead))
		}
		require.Equal(b, numValues, int64(startIndex))
	}
}
