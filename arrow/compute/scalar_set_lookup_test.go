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

package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type ScalarSetLookupSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func (ss *ScalarSetLookupSuite) SetupTest() {
	ss.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	ss.ctx = compute.WithAllocator(context.TODO(), ss.mem)
}

func (ss *ScalarSetLookupSuite) getArr(dt arrow.DataType, str string) arrow.Array {
	arr, _, err := array.FromJSON(ss.mem, dt, strings.NewReader(str), array.WithUseNumber())
	ss.Require().NoError(err)
	return arr
}

func (ss *ScalarSetLookupSuite) checkIsIn(input, valueSet arrow.Array, expectedJSON string, matching compute.NullMatchingBehavior) {
	expected := ss.getArr(arrow.FixedWidthTypes.Boolean, expectedJSON)
	defer expected.Release()

	result, err := compute.IsIn(ss.ctx, compute.SetOptions{
		ValueSet:     compute.NewDatumWithoutOwning(valueSet),
		NullBehavior: matching,
	}, compute.NewDatumWithoutOwning(input))
	ss.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(ss.T(), compute.NewDatumWithoutOwning(expected), result, nil, nil)
}

func (ss *ScalarSetLookupSuite) checkIsInFromJSON(typ arrow.DataType, input, valueSet, expected string, matching compute.NullMatchingBehavior) {
	inputArr := ss.getArr(typ, input)
	defer inputArr.Release()

	valueSetArr := ss.getArr(typ, valueSet)
	defer valueSetArr.Release()

	ss.checkIsIn(inputArr, valueSetArr, expected, matching)
}

func (ss *ScalarSetLookupSuite) checkIsInDictionary(typ, idxType arrow.DataType, inputDict, inputIndex, valueSet, expected string, matching compute.NullMatchingBehavior) {
	dictType := &arrow.DictionaryType{IndexType: idxType, ValueType: typ}
	indices := ss.getArr(idxType, inputIndex)
	defer indices.Release()
	dict := ss.getArr(typ, inputDict)
	defer dict.Release()

	input := array.NewDictionaryArray(dictType, indices, dict)
	defer input.Release()

	valueSetArr := ss.getArr(typ, valueSet)
	defer valueSetArr.Release()

	ss.checkIsIn(input, valueSetArr, expected, matching)
}

func (ss *ScalarSetLookupSuite) checkIsInChunked(input, value, expected *arrow.Chunked, matching compute.NullMatchingBehavior) {
	result, err := compute.IsIn(ss.ctx, compute.SetOptions{
		ValueSet:     compute.NewDatumWithoutOwning(value),
		NullBehavior: matching,
	}, compute.NewDatumWithoutOwning(input))
	ss.Require().NoError(err)
	defer result.Release()

	ss.Len(result.(*compute.ChunkedDatum).Chunks(), 1)
	assertDatumsEqual(ss.T(), compute.NewDatumWithoutOwning(expected), result, nil, nil)
}

func (ss *ScalarSetLookupSuite) TestIsInPrimitive() {
	type testCase struct {
		expected string
		matching compute.NullMatchingBehavior
	}

	tests := []struct {
		name     string
		input    string
		valueset string
		cases    []testCase
	}{
		{"no nulls", `[0, 1, 2, 3, 2]`, `[2, 1]`, []testCase{
			{`[false, true, true, false, true]`, compute.NullMatchingMatch},
		}},
		{"nulls in left", `[null, 1, 2, 3, 2]`, `[2, 1]`, []testCase{
			{`[false, true, true, false, true]`, compute.NullMatchingMatch},
			{`[false, true, true, false, true]`, compute.NullMatchingSkip},
			{`[null, true, true, false, true]`, compute.NullMatchingEmitNull},
			{`[null, true, true, false, true]`, compute.NullMatchingInconclusive},
		}},
		{"nulls in right", `[0, 1, 2, 3, 2]`, `[2, null, 1]`, []testCase{
			{`[false, true, true, false, true]`, compute.NullMatchingMatch},
			{`[false, true, true, false, true]`, compute.NullMatchingSkip},
			{`[false, true, true, false, true]`, compute.NullMatchingEmitNull},
			{`[null, true, true, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"nulls in both", `[null, 1, 2, 3, 2]`, `[2, null, 1]`, []testCase{
			{`[true, true, true, false, true]`, compute.NullMatchingMatch},
			{`[false, true, true, false, true]`, compute.NullMatchingSkip},
			{`[null, true, true, false, true]`, compute.NullMatchingEmitNull},
			{`[null, true, true, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"duplicates in right", `[null, 1, 2, 3, 2]`, `[null, 2, 2, null, 1, 1]`, []testCase{
			{`[true, true, true, false, true]`, compute.NullMatchingMatch},
			{`[false, true, true, false, true]`, compute.NullMatchingSkip},
			{`[null, true, true, false, true]`, compute.NullMatchingEmitNull},
			{`[null, true, true, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"empty arrays", `[]`, `[]`, []testCase{
			{`[]`, compute.NullMatchingMatch},
		}},
	}

	typList := append([]arrow.DataType{}, numericTypes...)
	typList = append(typList, arrow.FixedWidthTypes.Time32s,
		arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64us,
		arrow.FixedWidthTypes.Time64ns, arrow.FixedWidthTypes.Timestamp_us,
		arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Duration_s,
		arrow.FixedWidthTypes.Duration_ms, arrow.FixedWidthTypes.Duration_us,
		arrow.FixedWidthTypes.Duration_ns)

	for _, typ := range typList {
		ss.Run(typ.String(), func() {
			for _, tt := range tests {
				ss.Run(tt.name, func() {
					for _, tc := range tt.cases {
						ss.checkIsInFromJSON(typ,
							tt.input, tt.valueset, tc.expected, tc.matching)
					}
				})
			}
		})
	}
}

func (ss *ScalarSetLookupSuite) TestDurationCasts() {
	vals := ss.getArr(arrow.FixedWidthTypes.Duration_s, `[0, 1, 2]`)
	defer vals.Release()

	valueset := ss.getArr(arrow.FixedWidthTypes.Duration_ms, `[1, 2, 2000]`)
	defer valueset.Release()

	ss.checkIsIn(vals, valueset, `[false, false, true]`, compute.NullMatchingMatch)
}

func (ss *ScalarSetLookupSuite) TestIsInBinary() {
	type testCase struct {
		expected string
		matching compute.NullMatchingBehavior
	}

	tests := []struct {
		name     string
		input    string
		valueset string
		cases    []testCase
	}{
		{"nulls on left", `["YWFh", "", "Y2M=", null, ""]`, `["YWFh", ""]`, []testCase{
			{`[true, true, false, false, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, false, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"nulls on right", `["YWFh", "", "Y2M=", null, ""]`, `["YWFh", "", null]`, []testCase{
			{`[true, true, false, true, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, null, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"duplicates in right array", `["YWFh", "", "Y2M=", null, ""]`, `[null, "YWFh", "YWFh", "", "", null]`, []testCase{
			{`[true, true, false, true, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, null, null, true]`, compute.NullMatchingInconclusive},
		}},
	}

	for _, typ := range baseBinaryTypes {
		ss.Run(typ.String(), func() {
			for _, tt := range tests {
				ss.Run(tt.name, func() {
					for _, tc := range tt.cases {
						ss.checkIsInFromJSON(typ,
							tt.input, tt.valueset, tc.expected, tc.matching)
					}
				})
			}
		})
	}
}

func (ss *ScalarSetLookupSuite) TestIsInFixedSizeBinary() {
	type testCase struct {
		expected string
		matching compute.NullMatchingBehavior
	}

	tests := []struct {
		name     string
		input    string
		valueset string
		cases    []testCase
	}{
		{"nulls on left", `["YWFh", "YmJi", "Y2Nj", null, "YmJi"]`, `["YWFh", "YmJi"]`, []testCase{
			{`[true, true, false, false, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, false, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"nulls on right", `["YWFh", "YmJi", "Y2Nj", null, "YmJi"]`, `["YWFh", "YmJi", null]`, []testCase{
			{`[true, true, false, true, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, null, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"duplicates in right array", `["YWFh", "YmJi", "Y2Nj", null, "YmJi"]`, `["YWFh", null, "YWFh", "YmJi", "YmJi", null]`, []testCase{
			{`[true, true, false, true, true]`, compute.NullMatchingMatch},
			{`[true, true, false, false, true]`, compute.NullMatchingSkip},
			{`[true, true, false, null, true]`, compute.NullMatchingEmitNull},
			{`[true, true, null, null, true]`, compute.NullMatchingInconclusive},
		}},
	}

	typ := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	for _, tt := range tests {
		ss.Run(tt.name, func() {
			for _, tc := range tt.cases {
				ss.checkIsInFromJSON(typ,
					tt.input, tt.valueset, tc.expected, tc.matching)
			}
		})
	}
}

func (ss *ScalarSetLookupSuite) TestIsInDecimal() {
	type testCase struct {
		expected string
		matching compute.NullMatchingBehavior
	}

	tests := []struct {
		name     string
		input    string
		valueset string
		cases    []testCase
	}{
		{"nulls on left", `["12.3", "45.6", "78.9", null, "12.3"]`, `["12.3", "78.9"]`, []testCase{
			{`[true, false, true, false, true]`, compute.NullMatchingMatch},
			{`[true, false, true, false, true]`, compute.NullMatchingSkip},
			{`[true, false, true, null, true]`, compute.NullMatchingEmitNull},
			{`[true, false, true, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"nulls on right", `["12.3", "45.6", "78.9", null, "12.3"]`, `["12.3", "78.9", null]`, []testCase{
			{`[true, false, true, true, true]`, compute.NullMatchingMatch},
			{`[true, false, true, false, true]`, compute.NullMatchingSkip},
			{`[true, false, true, null, true]`, compute.NullMatchingEmitNull},
			{`[true, null, true, null, true]`, compute.NullMatchingInconclusive},
		}},
		{"duplicates in right array", `["12.3", "45.6", "78.9", null, "12.3"]`, `[null, "12.3", "12.3", "78.9", "78.9", null]`, []testCase{
			{`[true, false, true, true, true]`, compute.NullMatchingMatch},
			{`[true, false, true, false, true]`, compute.NullMatchingSkip},
			{`[true, false, true, null, true]`, compute.NullMatchingEmitNull},
			{`[true, null, true, null, true]`, compute.NullMatchingInconclusive},
		}},
	}

	decTypes := []arrow.DataType{
		&arrow.Decimal32Type{Precision: 3, Scale: 1},
		&arrow.Decimal64Type{Precision: 3, Scale: 1},
		&arrow.Decimal128Type{Precision: 3, Scale: 1},
		&arrow.Decimal256Type{Precision: 3, Scale: 1},
	}

	for _, typ := range decTypes {
		ss.Run(typ.String(), func() {
			for _, tt := range tests {
				ss.Run(tt.name, func() {
					for _, tc := range tt.cases {
						ss.checkIsInFromJSON(typ,
							tt.input, tt.valueset, tc.expected, tc.matching)
					}
				})
			}

			// don't yet have Decimal32 or Decimal64 implemented for casting
			if typ.ID() == arrow.DECIMAL128 || typ.ID() == arrow.DECIMAL256 {
				// test cast
				in := ss.getArr(&arrow.Decimal128Type{Precision: 4, Scale: 2}, `["12.30", "45.60", "78.90"]`)
				defer in.Release()
				values := ss.getArr(typ, `["12.3", "78.9"]`)
				defer values.Release()

				ss.checkIsIn(in, values, `[true, false true]`, compute.NullMatchingMatch)
			}
		})
	}
}

func (ss *ScalarSetLookupSuite) TestIsInDictionary() {
	tests := []struct {
		typ       arrow.DataType
		inputDict string
		inputIdx  string
		valueSet  string
		expected  string
		matching  compute.NullMatchingBehavior
	}{
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 2, null, 0]`,
			valueSet:  `["A", "B", "C"]`,
			expected:  `[true, true, false, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.PrimitiveTypes.Float32,
			inputDict: `[4.1, -1.0, 42, 9.8]`,
			inputIdx:  `[1, 2, null, 0]`,
			valueSet:  `[4.1, 42, -1.0]`,
			expected:  `[true, true, false, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[true, false, true, true, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[true, false, true, true, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A"]`,
			expected:  `[false, false, false, true, false]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[true, false, false, true, true]`,
			matching:  compute.NullMatchingSkip,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[false, false, false, true, false]`,
			matching:  compute.NullMatchingSkip,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A"]`,
			expected:  `[false, false, false, true, false]`,
			matching:  compute.NullMatchingSkip,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[true, false, null, true, true]`,
			matching:  compute.NullMatchingEmitNull,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[null, false, null, true, null]`,
			matching:  compute.NullMatchingEmitNull,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A"]`,
			expected:  `[null, false, null, true, null]`,
			matching:  compute.NullMatchingEmitNull,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[true, null, null, true, true]`,
			matching:  compute.NullMatchingInconclusive,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A", null]`,
			expected:  `[null, null, null, true, null]`,
			matching:  compute.NullMatchingInconclusive,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", null, "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "B", "A"]`,
			expected:  `[null, false, null, true, null]`,
			matching:  compute.NullMatchingInconclusive,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 2, null, 0]`,
			valueSet:  `["A", "A", "B", "A", "B", "C"]`,
			expected:  `[true, true, false, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "C", "B", "A", null, null, "B"]`,
			expected:  `[true, false, true, true, true]`,
			matching:  compute.NullMatchingMatch,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "C", "B", "A", null, null, "B"]`,
			expected:  `[true, false, false, true, true]`,
			matching:  compute.NullMatchingSkip,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "C", "B", "A", null, null, "B"]`,
			expected:  `[true, false, null, true, true]`,
			matching:  compute.NullMatchingEmitNull,
		},
		{
			typ:       arrow.BinaryTypes.String,
			inputDict: `["A", "B", "C", "D"]`,
			inputIdx:  `[1, 3, null, 0, 1]`,
			valueSet:  `["C", "C", "B", "A", null, null, "B"]`,
			expected:  `[true, null, null, true, true]`,
			matching:  compute.NullMatchingInconclusive,
		},
	}

	for _, ty := range dictIndexTypes {
		ss.Run("idx="+ty.String(), func() {
			for _, test := range tests {
				ss.Run(test.typ.String(), func() {
					ss.checkIsInDictionary(test.typ, ty,
						test.inputDict, test.inputIdx, test.valueSet,
						test.expected, test.matching)
				})
			}
		})
	}
}

func (ss *ScalarSetLookupSuite) TestIsInChunked() {
	input, err := array.ChunkedFromJSON(ss.mem, arrow.BinaryTypes.String,
		[]string{`["abc", "def", "", "abc", "jkl"]`, `["def", null, "abc", "zzz"]`})
	ss.Require().NoError(err)
	defer input.Release()

	valueSet, err := array.ChunkedFromJSON(ss.mem, arrow.BinaryTypes.String,
		[]string{`["", "def"]`, `["abc"]`})
	ss.Require().NoError(err)
	defer valueSet.Release()

	expected, err := array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[true, true, true, true, false]`, `[true, false, true, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingMatch)
	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingSkip)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[true, true, true, true, false]`, `[true, null, true, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingEmitNull)
	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingInconclusive)

	valueSet, err = array.ChunkedFromJSON(ss.mem, arrow.BinaryTypes.String,
		[]string{`["", "def"]`, `[null]`})
	ss.Require().NoError(err)
	defer valueSet.Release()

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, true, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingMatch)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, false, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingSkip)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, null, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingEmitNull)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[null, true, true, null, null]`, `[true, null, null, null]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingInconclusive)

	valueSet, err = array.ChunkedFromJSON(ss.mem, arrow.BinaryTypes.String,
		[]string{`["", null, "", "def"]`, `["def", null]`})
	ss.Require().NoError(err)
	defer valueSet.Release()

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, true, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingMatch)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, false, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingSkip)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[false, true, true, false, false]`, `[true, null, false, false]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingEmitNull)

	expected, err = array.ChunkedFromJSON(ss.mem, arrow.FixedWidthTypes.Boolean,
		[]string{`[null, true, true, null, null]`, `[true, null, null, null]`})
	ss.Require().NoError(err)
	defer expected.Release()

	ss.checkIsInChunked(input, valueSet, expected, compute.NullMatchingInconclusive)
}

func (ss *ScalarSetLookupSuite) TearDownTest() {
	ss.mem.AssertSize(ss.T(), 0)
}

func TestScalarSetLookup(t *testing.T) {
	suite.Run(t, new(ScalarSetLookupSuite))
}
