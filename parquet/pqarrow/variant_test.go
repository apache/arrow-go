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

package pqarrow_test

import (
	"context"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/endian"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/suite"
)

type ShreddedVariantTestSuite struct {
	suite.Suite

	dirPrefix string
	outDir    string
	cases     []Case

	errorCases    []Case
	singleVariant []Case
	multiVariant  []Case
}

func (s *ShreddedVariantTestSuite) SetupSuite() {
	dir := os.Getenv("PARQUET_TEST_DATA")
	if dir == "" {
		s.T().Skip("PARQUET_TEST_DATA environment variable not set")
	}

	s.outDir = filepath.Join(dir, "..", "go_variant")
	s.dirPrefix = filepath.Join(dir, "..", "shredded_variant")
	cases, err := os.Open(filepath.Join(s.dirPrefix, "cases.json"))

	s.Require().NoError(err, "Failed to open cases.json")
	defer cases.Close()

	s.Require().NoError(json.NewDecoder(cases).Decode(&s.cases))

	// Copy cases.json to output directory
	s.copyCasesJSON()

	s.errorCases = slices.DeleteFunc(slices.Clone(s.cases), func(c Case) bool {
		return c.ErrorMessage == ""
	})

	s.singleVariant = slices.DeleteFunc(slices.Clone(s.cases), func(c Case) bool {
		return c.ErrorMessage != "" || c.VariantFile == "" || len(c.VariantFiles) > 0
	})

	s.multiVariant = slices.DeleteFunc(slices.Clone(s.cases), func(c Case) bool {
		return c.ErrorMessage != "" || c.VariantFile != "" || len(c.VariantFiles) == 0
	})
}

type Case struct {
	Number       int       `json:"case_number"`
	Title        string    `json:"test"`
	ParquetFile  string    `json:"parquet_file"`
	VariantFile  string    `json:"variant_file"`
	VariantFiles []*string `json:"variant_files"`
	VariantData  string    `json:"variant"`
	Variants     string    `json:"variants"`
	ErrorMessage string    `json:"error_message"`
}

func readUnsigned(b []byte) (result uint32) {
	v := (*[4]byte)(unsafe.Pointer(&result))
	copy(v[:], b)
	return endian.FromLE(result)
}

func (s *ShreddedVariantTestSuite) readVariant(filename string) variant.Value {
	data, err := os.ReadFile(filepath.Join(s.dirPrefix, filename))

	s.Require().NoError(err, "Failed to read variant file: %s", filename)

	hdr := data[0]
	offsetSize := int(1 + ((hdr & 0b11000000) >> 6))
	dictSize := int(readUnsigned(data[1 : 1+offsetSize]))
	offsetListOffset := 1 + offsetSize
	dataOffset := offsetListOffset + ((1 + dictSize) * offsetSize)

	idx := offsetListOffset + (offsetSize * dictSize)
	endOffset := dataOffset + int(readUnsigned(data[idx:idx+offsetSize]))
	val, err := variant.New(data[:endOffset], data[endOffset:])
	s.Require().NoError(err, "Failed to create variant from data: %s", filename)

	s.writeVariant(val, filepath.Join(s.outDir, filename))
	return val
}

func (s *ShreddedVariantTestSuite) readParquet(filename string) arrow.Table {
	file, err := os.Open(filepath.Join(s.dirPrefix, filename))
	s.Require().NoError(err, "Failed to open Parquet file: %s", filename)
	defer file.Close()

	tbl, err := pqarrow.ReadTable(context.Background(), file, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	s.Require().NoError(err, "Failed to read Parquet file: %s", filename)

	s.writeParquet(tbl, filepath.Join(s.outDir, filename))
	return tbl
}

func (s *ShreddedVariantTestSuite) writeVariant(v variant.Value, filename string) {
	// Helper to serialize a variant.Value to disk in the same layout expected by
	// readVariant (metadata immediately followed by value bytes).
	// The file will be created relative to the suite data directory unless an
	// absolute path is supplied.

	path := filename
	if !filepath.IsAbs(filename) {
		path = filepath.Join(s.dirPrefix, filename)
	}

	// Ensure the destination directory exists.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.Require().NoError(err, "failed to create directory for variant file")
	}

	f, err := os.Create(path)
	s.Require().NoError(err, "failed to create variant file: %s", path)
	defer f.Close()

	// Write metadata followed by value bytes.
	if _, err = f.Write(v.Metadata().Bytes()); err != nil {
		s.Require().NoError(err, "failed to write metadata to variant file: %s", path)
	}
	if _, err = f.Write(v.Bytes()); err != nil {
		s.Require().NoError(err, "failed to write value bytes to variant file: %s", path)
	}
}

func (s *ShreddedVariantTestSuite) writeParquet(tbl arrow.Table, filename string) {
	// Helper to persist an Arrow table containing variant columns to a Parquet file.
	path := filename
	if !filepath.IsAbs(filename) {
		path = filepath.Join(s.dirPrefix, filename)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.Require().NoError(err, "failed to create directory for parquet file")
	}

	f, err := os.Create(path)
	s.Require().NoError(err, "failed to create parquet file: %s", path)
	defer f.Close()

	fw, err := pqarrow.NewFileWriter(tbl.Schema(), f, nil, pqarrow.DefaultWriterProps())
	s.Require().NoError(err, "failed to create parquet writer")
	defer fw.Close()

	// Write the entire table as a single row group.
	rowGroupSize := int64(tbl.NumRows())
	if rowGroupSize == 0 {
		rowGroupSize = 1 // still produce a single row group for empty tables
	}

	s.Require().NoError(fw.WriteTable(tbl, rowGroupSize), "failed to write table to parquet file")
}

func (s *ShreddedVariantTestSuite) copyCasesJSON() {
	// Copy cases.json from source directory to output directory
	srcPath := filepath.Join(s.dirPrefix, "cases.json")
	destPath := filepath.Join(s.outDir, "cases.json")

	// Copy file directly
	data, err := os.ReadFile(srcPath)
	s.Require().NoError(err, "failed to read source cases.json")

	err = os.WriteFile(destPath, data, 0o644)
	s.Require().NoError(err, "failed to write destination cases.json")
}

func zip[T, U any](a iter.Seq[T], b iter.Seq[U]) iter.Seq2[T, U] {
	return func(yield func(T, U) bool) {
		nexta, stopa := iter.Pull(a)
		nextb, stopb := iter.Pull(b)
		defer stopa()
		defer stopb()

		for {
			a, ok := nexta()
			if !ok {
				return
			}
			b, ok := nextb()
			if !ok {
				return
			}
			if !yield(a, b) {
				return
			}
		}
	}
}

func (s *ShreddedVariantTestSuite) assertVariantEqual(expected, actual variant.Value) {
	switch expected.BasicType() {
	case variant.BasicObject:
		exp := expected.Value().(variant.ObjectValue)
		act := actual.Value().(variant.ObjectValue)

		s.Equal(exp.NumElements(), act.NumElements(), "Expected %d elements in object, got %d", exp.NumElements(), act.NumElements())
		for i := range exp.NumElements() {
			expectedField, err := exp.FieldAt(i)
			s.Require().NoError(err, "Failed to get expected field at index %d", i)
			actualField, err := act.FieldAt(i)
			s.Require().NoError(err, "Failed to get actual field at index %d", i)

			s.Equal(expectedField.Key, actualField.Key, "Expected field key %s, got %s", expectedField.Key, actualField.Key)
			s.assertVariantEqual(expectedField.Value, actualField.Value)
		}
	case variant.BasicArray:
		exp := expected.Value().(variant.ArrayValue)
		act := actual.Value().(variant.ArrayValue)

		s.Equal(exp.Len(), act.Len(), "Expected array length %d, got %d", exp.Len(), act.Len())
		for e, a := range zip(exp.Values(), act.Values()) {
			s.assertVariantEqual(e, a)
		}
	default:
		switch expected.Type() {
		case variant.Decimal4, variant.Decimal8, variant.Decimal16:
			e, err := json.Marshal(expected.Value())
			s.Require().NoError(err, "Failed to marshal expected value")
			a, err := json.Marshal(actual.Value())
			s.Require().NoError(err, "Failed to marshal actual value")
			s.JSONEq(string(e), string(a), "Expected variant value %s, got %s", e, a)
		default:
			s.EqualValues(expected.Value(), actual.Value(), "Expected variant value %v, got %v", expected.Value(), actual.Value())
		}
	}
}

func (s *ShreddedVariantTestSuite) TestSingleVariantCases() {
	for _, c := range s.singleVariant {
		s.Run(c.Title, func() {
			s.Run(fmt.Sprint(c.Number), func() {
				switch c.Number {
				case 125:
					s.T().Skip("Skipping case 125 due to inconsistent definition of behavior")
				case 41:
					s.T().Skip("Skipping case 41 due to missing value column")
				case 43:
					s.T().Skip("Skipping case 43 due to unknown definition of behavior")
				case 84:
					s.T().Skip("Skipping case 84 due to incorrect optional fields")
				case 88:
					s.T().Skip("Skipping case 88 due to missing value column")
				case 131:
					s.T().Skip("Skipping case 131 due to missing value column")
				case 132:
					s.T().Skip("Skipping case 132 due to missing value column")
				case 138:
					s.T().Skip("Skipping case 138 due to missing value column")
				}

				expected := s.readVariant(c.VariantFile)
				tbl := s.readParquet(c.ParquetFile)

				defer tbl.Release()

				col := tbl.Column(1).Data().Chunk(0)
				s.Require().IsType(&extensions.VariantArray{}, col)

				variantArray := col.(*extensions.VariantArray)
				s.Require().Equal(1, variantArray.Len(), "Expected single variant value")

				val, err := variantArray.Value(0)
				s.Require().NoError(err, "Failed to get variant value from array")
				s.assertVariantEqual(expected, val)
			})
		})
	}
}

func (s *ShreddedVariantTestSuite) TestMultiVariantCases() {
	for _, c := range s.multiVariant {
		s.Run(c.Title, func() {
			s.Run(fmt.Sprint(c.Number), func() {
				tbl := s.readParquet(c.ParquetFile)
				defer tbl.Release()

				s.Require().EqualValues(len(c.VariantFiles), tbl.NumRows(), "Expected number of rows to match number of variant files")
				col := tbl.Column(1).Data().Chunk(0)
				s.Require().IsType(&extensions.VariantArray{}, col)

				variantArray := col.(*extensions.VariantArray)
				for i, variantFile := range c.VariantFiles {
					if variantFile == nil {
						s.True(variantArray.IsNull(i), "Expected null value at index %d", i)
						continue
					}

					expected := s.readVariant(*variantFile)
					actual, err := variantArray.Value(i)
					s.Require().NoError(err, "Failed to get variant value at index %d", i)
					s.assertVariantEqual(expected, actual)
				}
			})
		})
	}
}

func (s *ShreddedVariantTestSuite) TestErrorCases() {
	for _, c := range s.errorCases {
		s.Run(c.Title, func() {
			s.Run(fmt.Sprint(c.Number), func() {
				switch c.Number {
				case 127:
					s.T().Skip("Skipping case 127: test says uint32 should error, we just upcast to int64")
				case 137:
					s.T().Skip("Skipping case 137: test says flba(4) should error, we just treat it as a binary variant")
				}

				tbl := s.readParquet(c.ParquetFile)
				defer tbl.Release()

				col := tbl.Column(1).Data().Chunk(0)
				s.Require().IsType(&extensions.VariantArray{}, col)

				variantArray := col.(*extensions.VariantArray)
				_, err := variantArray.Value(0)
				s.Error(err, "Expected error for case %d: %s", c.Number, c.ErrorMessage)
			})
		})
	}
}

func TestShreddedVariantExamples(t *testing.T) {
	suite.Run(t, new(ShreddedVariantTestSuite))
}
