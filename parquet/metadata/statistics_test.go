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
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE(zeroshade): tests will be added and updated after merging the "file" package
// since the tests that I wrote relied on the file writer/reader for ease of use.

func newFloat16Node(name string, rep parquet.Repetition, fieldID int32) *schema.PrimitiveNode {
	return schema.MustPrimitive(schema.NewPrimitiveNodeLogical(name, rep, schema.Float16LogicalType{}, parquet.Types.FixedLenByteArray, 2, fieldID))
}

func TestCheckNaNs(t *testing.T) {
	const (
		numvals = 8
		min     = -4.0
		max     = 3.0
	)
	var (
		nan                              = math.NaN()
		f16Min parquet.FixedLenByteArray = float16.New(float32(min)).ToLEBytes()
		f16Max parquet.FixedLenByteArray = float16.New(float32(max)).ToLEBytes()
	)

	allNans := []float64{nan, nan, nan, nan, nan, nan, nan, nan}
	allNansf32 := make([]float32, numvals)
	allNansf16 := make([]parquet.FixedLenByteArray, numvals)
	for idx, v := range allNans {
		allNansf32[idx] = float32(v)
		allNansf16[idx] = float16.New(float32(v)).ToLEBytes()
	}

	someNans := []float64{nan, max, -3.0, -1.0, nan, 2.0, min, nan}
	someNansf32 := make([]float32, numvals)
	someNansf16 := make([]parquet.FixedLenByteArray, numvals)
	for idx, v := range someNans {
		someNansf32[idx] = float32(v)
		someNansf16[idx] = float16.New(float32(v)).ToLEBytes()
	}

	validBitmap := []byte{0x7F}       // 0b01111111
	validBitmapNoNaNs := []byte{0x6E} // 0b01101110

	assertUnsetMinMax := func(stats metadata.TypedStatistics, values interface{}, bitmap []byte) {
		if bitmap == nil {
			switch s := stats.(type) {
			case *metadata.Float32Statistics:
				s.Update(values.([]float32), 0)
			case *metadata.Float64Statistics:
				s.Update(values.([]float64), 0)
			case *metadata.Float16Statistics:
				s.Update(values.([]parquet.FixedLenByteArray), 0)
			}
			assert.False(t, stats.HasMinMax())
		} else {
			nvalues := reflect.ValueOf(values).Len()
			nullCount := bitutil.CountSetBits(bitmap, 0, nvalues)
			switch s := stats.(type) {
			case *metadata.Float32Statistics:
				s.UpdateSpaced(values.([]float32), bitmap, 0, int64(nullCount))
			case *metadata.Float64Statistics:
				s.UpdateSpaced(values.([]float64), bitmap, 0, int64(nullCount))
			case *metadata.Float16Statistics:
				s.UpdateSpaced(values.([]parquet.FixedLenByteArray), bitmap, 0, int64(nullCount))
			}
			assert.False(t, stats.HasMinMax())
		}
	}

	assertMinMaxAre := func(stats metadata.TypedStatistics, values interface{}, expectedMin, expectedMax interface{}) {
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.Update(values.([]float32), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float64Statistics:
			s.Update(values.([]float64), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float16Statistics:
			s.Update(values.([]parquet.FixedLenByteArray), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		}
	}

	assertMinMaxAreSpaced := func(stats metadata.TypedStatistics, values interface{}, bitmap []byte, expectedMin, expectedMax interface{}) {
		nvalues := reflect.ValueOf(values).Len()
		nullCount := bitutil.CountSetBits(bitmap, 0, nvalues)
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.UpdateSpaced(values.([]float32), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float64Statistics:
			s.UpdateSpaced(values.([]float64), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float16Statistics:
			s.UpdateSpaced(values.([]parquet.FixedLenByteArray), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		}
	}

	f32Col := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	f64Col := schema.NewColumn(schema.NewFloat64Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	f16Col := schema.NewColumn(newFloat16Node("f", parquet.Repetitions.Required, -1), 1, 1)
	// test values
	someNanStats := metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 := metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	someNanStatsf16 := metadata.NewStatistics(f16Col, memory.DefaultAllocator)
	// ingesting only nans should not yield a min or max
	assertUnsetMinMax(someNanStats, allNans, nil)
	assertUnsetMinMax(someNanStatsf32, allNansf32, nil)
	assertUnsetMinMax(someNanStatsf16, allNansf16, nil)
	// ingesting a mix should yield a valid min/max
	assertMinMaxAre(someNanStats, someNans, min, max)
	assertMinMaxAre(someNanStatsf32, someNansf32, float32(min), float32(max))
	assertMinMaxAre(someNanStatsf16, someNansf16, f16Min, f16Max)
	// ingesting only nans after a valid min/max should have no effect
	assertMinMaxAre(someNanStats, allNans, min, max)
	assertMinMaxAre(someNanStatsf32, allNansf32, float32(min), float32(max))
	assertMinMaxAre(someNanStatsf16, allNansf16, f16Min, f16Max)

	someNanStats = metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 = metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	someNanStatsf16 = metadata.NewStatistics(f16Col, memory.DefaultAllocator)
	assertUnsetMinMax(someNanStats, allNans, validBitmap)
	assertUnsetMinMax(someNanStatsf32, allNansf32, validBitmap)
	assertUnsetMinMax(someNanStatsf16, allNansf16, validBitmap)
	// nans should not pollute min/max when excluded via null bitmap
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmapNoNaNs, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmapNoNaNs, float32(min), float32(max))
	assertMinMaxAreSpaced(someNanStatsf16, someNansf16, validBitmapNoNaNs, f16Min, f16Max)
	// ingesting nans with a null bitmap should not change the result
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmap, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmap, float32(min), float32(max))
	assertMinMaxAreSpaced(someNanStatsf16, someNansf16, validBitmap, f16Min, f16Max)
}

func TestCheckNegativeZeroStats(t *testing.T) {
	assertMinMaxZeroesSign := func(stats metadata.TypedStatistics, values interface{}) {
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.Update(values.([]float32), 0)
			assert.True(t, s.HasMinMax())
			var zero float32
			assert.Equal(t, zero, s.Min())
			assert.True(t, math.Signbit(float64(s.Min())))
			assert.Equal(t, zero, s.Max())
			assert.False(t, math.Signbit(float64(s.Max())))
		case *metadata.Float64Statistics:
			s.Update(values.([]float64), 0)
			assert.True(t, s.HasMinMax())
			var zero float64
			assert.Equal(t, zero, s.Min())
			assert.True(t, math.Signbit(s.Min()))
			assert.Equal(t, zero, s.Max())
			assert.False(t, math.Signbit(s.Max()))
		case *metadata.Float16Statistics:
			s.Update(values.([]parquet.FixedLenByteArray), 0)
			assert.True(t, s.HasMinMax())
			var zero float64
			min := float64(float16.FromLEBytes(s.Min()).Float32())
			max := float64(float16.FromLEBytes(s.Max()).Float32())
			assert.Equal(t, zero, min)
			assert.True(t, math.Signbit(min))
			assert.Equal(t, zero, max)
			assert.False(t, math.Signbit(max))
		}
	}

	fcol := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	dcol := schema.NewColumn(schema.NewFloat64Node("d", parquet.Repetitions.Optional, -1), 1, 1)
	hcol := schema.NewColumn(newFloat16Node("h", parquet.Repetitions.Optional, -1), 1, 1)

	var f32zero float32
	var f64zero float64
	var f16PosZero parquet.FixedLenByteArray = float16.New(+f32zero).ToLEBytes()
	var f16NegZero parquet.FixedLenByteArray = float16.New(-f32zero).ToLEBytes()

	assert.False(t, float16.FromLEBytes(f16PosZero).Signbit())
	assert.True(t, float16.FromLEBytes(f16NegZero).Signbit())
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16NegZero, f16PosZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, -f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16PosZero, f16NegZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, -f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16NegZero, f16NegZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16PosZero, f16PosZero})
	}
}

func TestBooleanStatisticsEncoding(t *testing.T) {
	n := schema.NewBooleanNode("boolean", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(n, 0, 0)
	s := metadata.NewStatistics(descr, nil)
	bs := s.(*metadata.BooleanStatistics)
	bs.SetMinMax(false, true)
	maxEnc := bs.EncodeMax()
	minEnc := bs.EncodeMin()
	assert.Equal(t, []byte{1}, maxEnc)
	assert.Equal(t, []byte{0}, minEnc)
}

func TestUnsignedDefaultStats(t *testing.T) {
	// testing issue github.com/apache/arrow-go/issues/209
	// stats for unsigned columns should not have invalid min values
	{
		n, err := schema.NewPrimitiveNodeLogical("uint16", parquet.Repetitions.Optional,
			schema.NewIntLogicalType(16, false), parquet.Types.Int32, 4, -1)
		require.NoError(t, err)
		descr := schema.NewColumn(n, 1, 0)
		s := metadata.NewStatistics(descr, nil).(*metadata.Int32Statistics)
		s.UpdateSpaced([]int32{0}, []byte{1}, 0, 0)

		minv, maxv := s.Min(), s.Max()
		assert.Zero(t, minv)
		assert.Zero(t, maxv)
	}
	{
		n, err := schema.NewPrimitiveNodeLogical("uint64", parquet.Repetitions.Optional,
			schema.NewIntLogicalType(64, false), parquet.Types.Int64, 4, -1)
		require.NoError(t, err)
		descr := schema.NewColumn(n, 1, 0)
		s := metadata.NewStatistics(descr, nil).(*metadata.Int64Statistics)
		s.UpdateSpaced([]int64{0}, []byte{1}, 0, 0)

		minv, maxv := s.Min(), s.Max()
		assert.Zero(t, minv)
		assert.Zero(t, maxv)
	}
}

func TestBooleanStatisticsUpdateFromBitmap(t *testing.T) {
	mem := memory.DefaultAllocator
	node, err := schema.NewPrimitiveNode("test", parquet.Repetitions.Required, parquet.Types.Boolean, 0, 0)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	t.Run("all true bits", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0xFF, 0xFF} // all bits set
		numValues := int64(16)
		
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, true, stats.Min(), "min should be true when all bits are true")
		assert.Equal(t, true, stats.Max(), "max should be true when all bits are true")
		assert.Equal(t, numValues, stats.NumValues())
		assert.Equal(t, int64(0), stats.NullCount())
	})

	t.Run("all false bits", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0x00, 0x00} // all bits clear
		numValues := int64(16)
		
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, false, stats.Min(), "min should be false when all bits are false")
		assert.Equal(t, false, stats.Max(), "max should be false when all bits are false")
		assert.Equal(t, numValues, stats.NumValues())
		assert.Equal(t, int64(0), stats.NullCount())
	})

	t.Run("mixed true and false", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0b10101010} // alternating bits
		numValues := int64(8)
		
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, false, stats.Min(), "min should be false with mixed bits")
		assert.Equal(t, true, stats.Max(), "max should be true with mixed bits")
		assert.Equal(t, numValues, stats.NumValues())
	})

	t.Run("unaligned offset", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		// Bitmap: [0,0,0,1,1,0,1,0, 1,1,...]
		bitmap := make([]byte, 2)
		bitutil.SetBit(bitmap, 3)
		bitutil.SetBit(bitmap, 4)
		bitutil.SetBit(bitmap, 6)
		bitutil.SetBit(bitmap, 8)
		bitutil.SetBit(bitmap, 9)
		
		// Read 5 bits starting from offset 3: [1,1,0,1,0] -> has both true and false
		offset := int64(3)
		numValues := int64(5)
		
		stats.UpdateFromBitmap(bitmap, offset, numValues, 0)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, false, stats.Min(), "should find false bit")
		assert.Equal(t, true, stats.Max(), "should find true bit")
		assert.Equal(t, numValues, stats.NumValues())
	})

	t.Run("with nulls", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0xFF}
		numValues := int64(8)
		numNulls := int64(3)
		
		stats.UpdateFromBitmap(bitmap, 0, numValues, numNulls)
		
		assert.Equal(t, numValues, stats.NumValues())
		assert.Equal(t, numNulls, stats.NullCount())
	})

	t.Run("multiple updates", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		
		// First update: all true
		bitmap1 := []byte{0xFF}
		stats.UpdateFromBitmap(bitmap1, 0, 8, 0)
		assert.Equal(t, true, stats.Min())
		assert.Equal(t, true, stats.Max())
		
		// Second update: has false - should update min
		bitmap2 := []byte{0x00}
		stats.UpdateFromBitmap(bitmap2, 0, 8, 0)
		assert.Equal(t, false, stats.Min(), "min should update to false")
		assert.Equal(t, true, stats.Max(), "max should remain true")
		assert.Equal(t, int64(16), stats.NumValues())
	})

	t.Run("early exit optimization", func(t *testing.T) {
		// Create a large bitmap with both true and false in first few bits
		stats := metadata.NewBooleanStatistics(descr, mem)
		numValues := int64(10000)
		bitmap := make([]byte, bitutil.BytesForBits(numValues))
		
		// Set first bit to true, second to false (early exit should trigger)
		bitutil.SetBit(bitmap, 0) // true
		// bit 1 is false
		
		stats.UpdateFromBitmap(bitmap, 0, numValues, 0)
		
		assert.Equal(t, false, stats.Min())
		assert.Equal(t, true, stats.Max())
	})

	t.Run("consistency with Update method", func(t *testing.T) {
		// Verify UpdateFromBitmap produces same results as Update with []bool
		testCases := []struct {
			name   string
			bitmap []byte
			offset int64
			count  int64
		}{
			{"all_true", []byte{0xFF}, 0, 8},
			{"all_false", []byte{0x00}, 0, 8},
			{"mixed", []byte{0b10110010}, 0, 8},
			{"unaligned", []byte{0xFF, 0x00}, 3, 10},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				stats1 := metadata.NewBooleanStatistics(descr, mem)
				stats2 := metadata.NewBooleanStatistics(descr, mem)
				
				// Update using bitmap
				stats1.UpdateFromBitmap(tc.bitmap, tc.offset, tc.count, 0)
				
				// Update using []bool
				bools := make([]bool, tc.count)
				for i := int64(0); i < tc.count; i++ {
					bools[i] = bitutil.BitIsSet(tc.bitmap, int(tc.offset+i))
				}
				stats2.Update(bools, 0)
				
				// Both should produce same results
				assert.Equal(t, stats2.Min(), stats1.Min(), "min mismatch")
				assert.Equal(t, stats2.Max(), stats1.Max(), "max mismatch")
				assert.Equal(t, stats2.NumValues(), stats1.NumValues(), "numValues mismatch")
			})
		}
	})
}

func TestBooleanStatisticsUpdateFromBitmapSpaced(t *testing.T) {
	mem := memory.DefaultAllocator
	node, err := schema.NewPrimitiveNode("test", parquet.Repetitions.Optional, parquet.Types.Boolean, 0, 0)
	require.NoError(t, err)
	descr := schema.NewColumn(node, 0, 0)

	t.Run("all valid bits", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0b10101010} // data: alternating
		validBits := []byte{0xFF}     // all valid
		numValues := int64(8)
		
		stats.UpdateFromBitmapSpaced(bitmap, 0, numValues, validBits, 0, 0)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, false, stats.Min())
		assert.Equal(t, true, stats.Max())
		assert.Equal(t, numValues, stats.NumValues())
	})

	t.Run("some null values", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		// Data bitmap: [1,1,1,1,0,0,0,0]
		bitmap := []byte{0b00001111}
		// Valid bits: [1,0,1,0,1,0,1,0] - only positions 0,2,4,6 are valid
		validBits := []byte{0b01010101}
		numValues := int64(8)
		numNulls := int64(4)
		
		stats.UpdateFromBitmapSpaced(bitmap, 0, numValues, validBits, 0, numNulls)
		
		assert.True(t, stats.HasMinMax())
		// Valid positions are 0,2,4,6 with data bits: 1,1,0,0
		assert.Equal(t, false, stats.Min(), "should find false at position 4 or 6")
		assert.Equal(t, true, stats.Max(), "should find true at position 0 or 2")
		assert.Equal(t, int64(4), stats.NumValues(), "should count only valid values")
		assert.Equal(t, numNulls, stats.NullCount())
	})

	t.Run("all null values", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		bitmap := []byte{0xFF}
		validBits := []byte{0x00} // all null
		numValues := int64(8)
		
		stats.UpdateFromBitmapSpaced(bitmap, 0, numValues, validBits, 0, numValues)
		
		assert.False(t, stats.HasMinMax(), "should not have min/max with all nulls")
		assert.Equal(t, int64(0), stats.NumValues())
		assert.Equal(t, numValues, stats.NullCount())
	})

	t.Run("unaligned offsets", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		// Create larger bitmaps with offset
		bitmap := make([]byte, 3)
		validBits := make([]byte, 3)
		
		// Set some data bits starting from offset 5
		bitutil.SetBit(bitmap, 5)  // true
		bitutil.SetBit(bitmap, 6)  // true
		// bit 7 = false
		bitutil.SetBit(bitmap, 8)  // true
		// bit 9 = false
		
		// Set valid bits (skip position 6)
		bitutil.SetBit(validBits, 5)
		// bit 6 is null
		bitutil.SetBit(validBits, 7)
		bitutil.SetBit(validBits, 8)
		bitutil.SetBit(validBits, 9)
		
		stats.UpdateFromBitmapSpaced(bitmap, 5, 5, validBits, 5, 1)
		
		assert.True(t, stats.HasMinMax())
		// Valid positions: 5(true), 7(false), 8(true), 9(false)
		assert.Equal(t, false, stats.Min())
		assert.Equal(t, true, stats.Max())
		assert.Equal(t, int64(4), stats.NumValues())
	})

	t.Run("only true values valid", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		// Data: [1,1,1,1,0,0,0,0]
		bitmap := []byte{0b00001111}
		// Valid: only first 4 bits (all true in data)
		validBits := []byte{0b00001111}
		
		stats.UpdateFromBitmapSpaced(bitmap, 0, 8, validBits, 0, 4)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, true, stats.Min(), "only true values are valid")
		assert.Equal(t, true, stats.Max())
	})

	t.Run("only false values valid", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		// Data: [1,1,1,1,0,0,0,0]
		bitmap := []byte{0b00001111}
		// Valid: only last 4 bits (all false in data)
		validBits := []byte{0b11110000}
		
		stats.UpdateFromBitmapSpaced(bitmap, 0, 8, validBits, 0, 4)
		
		assert.True(t, stats.HasMinMax())
		assert.Equal(t, false, stats.Min(), "only false values are valid")
		assert.Equal(t, false, stats.Max())
	})

	t.Run("consistency with UpdateSpaced", func(t *testing.T) {
		// Verify UpdateFromBitmapSpaced produces same results as UpdateSpaced with []bool
		bitmap := []byte{0b10110010}
		validBits := []byte{0b11010110}
		numValues := int64(8)
		numNulls := int64(3)
		
		stats1 := metadata.NewBooleanStatistics(descr, mem)
		stats2 := metadata.NewBooleanStatistics(descr, mem)
		
		// Update using bitmap
		stats1.UpdateFromBitmapSpaced(bitmap, 0, numValues, validBits, 0, numNulls)
		
		// Update using []bool
		bools := make([]bool, numValues)
		for i := int64(0); i < numValues; i++ {
			bools[i] = bitutil.BitIsSet(bitmap, int(i))
		}
		stats2.UpdateSpaced(bools, validBits, 0, numNulls)
		
		// Both should produce same results
		assert.Equal(t, stats2.HasMinMax(), stats1.HasMinMax())
		if stats1.HasMinMax() {
			assert.Equal(t, stats2.Min(), stats1.Min(), "min mismatch")
			assert.Equal(t, stats2.Max(), stats1.Max(), "max mismatch")
		}
		assert.Equal(t, stats2.NumValues(), stats1.NumValues(), "numValues mismatch")
		assert.Equal(t, stats2.NullCount(), stats1.NullCount(), "nullCount mismatch")
	})

	t.Run("multiple updates accumulate correctly", func(t *testing.T) {
		stats := metadata.NewBooleanStatistics(descr, mem)
		
		// First update: only true values
		bitmap1 := []byte{0xFF}
		validBits1 := []byte{0x0F} // first 4 valid
		stats.UpdateFromBitmapSpaced(bitmap1, 0, 8, validBits1, 0, 4)
		assert.Equal(t, true, stats.Min())
		assert.Equal(t, true, stats.Max())
		
		// Second update: has false values
		bitmap2 := []byte{0x00}
		validBits2 := []byte{0xF0} // last 4 valid
		stats.UpdateFromBitmapSpaced(bitmap2, 0, 8, validBits2, 0, 4)
		
		assert.Equal(t, false, stats.Min(), "should update to include false")
		assert.Equal(t, true, stats.Max())
		assert.Equal(t, int64(8), stats.NumValues())
		assert.Equal(t, int64(8), stats.NullCount())
	})
}
