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

package pqarrow

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// RowRange is a half-open interval of rows, including Start and excluding End.
type RowRange struct {
	Start, End int64
}

// GetRecordReaderWithRowRanges reads only the requested row ranges from the
// selected row groups and columns. Row offsets are relative to the requested
// row groups in their supplied order. Ranges must be non-empty, sorted by
// start, and non-overlapping.
//
// As with GetRecordReader, nil colIndices or rowGroups selects all columns or
// row groups, respectively. Nil ranges selects all rows and an empty, non-nil
// slice selects no rows. Batches may be shorter than the configured batch size
// at range boundaries. For projections containing repeated columns, gaps are
// read sequentially to preserve row alignment across page boundaries.
func (fr *FileReader) GetRecordReaderWithRowRanges(ctx context.Context, colIndices, rowGroups []int, ranges []RowRange) (RecordReader, error) {
	return fr.getRecordReader(ctx, colIndices, rowGroups, ranges)
}

type rowRangeSelection struct {
	ranges             []RowRange
	index              int
	nextRow            int64
	hasRepeatedColumns bool
}

func newRowRangeSelection(ranges []RowRange, numRows int64) (*rowRangeSelection, error) {
	if ranges == nil {
		return nil, nil
	}
	var merged []RowRange
	for i, rows := range ranges {
		if rows.Start < 0 || rows.End <= rows.Start || rows.End > numRows {
			return nil, fmt.Errorf("%w: row range %d [%d, %d) exceeds %d rows or is empty", arrow.ErrInvalid, i, rows.Start, rows.End, numRows)
		}
		if i > 0 {
			previous := &merged[len(merged)-1]
			if rows.Start < previous.End {
				return nil, fmt.Errorf("%w: row range %d overlaps or precedes a previous range", arrow.ErrInvalid, i)
			}
			if rows.Start == previous.End {
				previous.End = rows.End
				continue
			}
		}
		merged = append(merged, rows)
	}
	if len(merged) == 1 && merged[0] == (RowRange{0, numRows}) {
		return nil, nil
	}
	return &rowRangeSelection{ranges: merged}, nil
}

func (r *recordReader) seekToSelectedRow(row int64) error {
	if row < 0 {
		return fmt.Errorf("%w: negative row index %d", arrow.ErrIndex, row)
	}
	remaining := row
	for i, rows := range r.selection.ranges {
		length := rows.End - rows.Start
		if remaining < length {
			inputRow := rows.Start + remaining
			if err := r.seekToRangeRow(inputRow); err != nil {
				r.err = err
				return err
			}
			r.selection.index = i
			r.selection.nextRow = inputRow
			r.err = nil
			return nil
		}
		remaining -= length
	}
	return fmt.Errorf("%w: row index %d exceeds %d selected rows", arrow.ErrIndex, row, row-remaining)
}

func (r *recordReader) seekToRangeRow(row int64) error {
	if !r.selection.hasRepeatedColumns {
		return r.seekToInputRow(row)
	}
	if err := r.seekToInputRow(0); err != nil {
		return err
	}
	return r.readThroughGap(row)
}

// Arbitrary seeks into repeated Data Page V1 columns without offset indexes
// can lose row alignment (GH-1522). Read through gaps in repeated projections.
func (r *recordReader) readThroughGap(rows int64) error {
	for rows > 0 {
		if !r.readBatch(min(r.batchSize, rows)) {
			if r.err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return r.err
		}
		rows -= r.cur.NumRows()
		r.cur.Release()
		r.cur = nil
	}
	return nil
}

func (r *recordReader) nextSelectedBatch() bool {
	if r.err != nil {
		return false
	}
	s := r.selection
	if s.index == len(s.ranges) {
		r.err = io.EOF
		return false
	}
	rows := s.ranges[s.index]
	if s.nextRow < rows.Start {
		var err error
		if s.hasRepeatedColumns {
			err = r.readThroughGap(rows.Start - s.nextRow)
		} else {
			err = r.seekToInputRow(rows.Start)
		}
		if err != nil {
			r.err = err
			return false
		}
		s.nextRow = rows.Start
	}
	if !r.readBatch(min(r.batchSize, rows.End-s.nextRow)) {
		if r.err == io.EOF {
			r.err = io.ErrUnexpectedEOF
		}
		return false
	}
	s.nextRow += r.cur.NumRows()
	if s.nextRow == rows.End {
		s.index++
	}
	return true
}
