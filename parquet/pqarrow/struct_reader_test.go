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
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

type structReaderTestChild struct {
	name      string
	events    *[]string
	seekRow   int64
	batchSize int64
	seekErr   error
	loadErr   error
}

func (r *structReaderTestChild) LoadBatch(nrecords int64) error {
	*r.events = append(*r.events, r.name)
	r.batchSize = nrecords
	return r.loadErr
}

func (r *structReaderTestChild) BuildArray(int64) (*arrow.Chunked, error) { return nil, nil }

func (r *structReaderTestChild) GetDefLevels() ([]int16, error) { return nil, nil }

func (r *structReaderTestChild) GetRepLevels() ([]int16, error) { return nil, nil }

func (r *structReaderTestChild) Field() *arrow.Field {
	return &arrow.Field{Name: r.name, Type: arrow.PrimitiveTypes.Int32}
}

func (r *structReaderTestChild) SeekToRow(row int64) error {
	*r.events = append(*r.events, r.name)
	r.seekRow = row
	return r.seekErr
}

func (r *structReaderTestChild) IsOrHasRepeatedChild() bool { return false }

func (r *structReaderTestChild) Retain() {}

func (r *structReaderTestChild) Release() {}

func TestStructReaderSerialOperationsVisitEveryChild(t *testing.T) {
	seekErr := errors.New("seek failed")
	loadErr := errors.New("load failed")

	tests := []struct {
		name        string
		call        func(*structReader) error
		expectedErr error
		check       func(*testing.T, []*structReaderTestChild)
	}{
		{
			name:        "seek to row",
			call:        func(reader *structReader) error { return reader.SeekToRow(42) },
			expectedErr: seekErr,
			check: func(t *testing.T, children []*structReaderTestChild) {
				for _, child := range children {
					require.Equal(t, int64(42), child.seekRow)
				}
			},
		},
		{
			name:        "load batch",
			call:        func(reader *structReader) error { return reader.LoadBatch(128) },
			expectedErr: loadErr,
			check: func(t *testing.T, children []*structReaderTestChild) {
				for _, child := range children {
					require.Equal(t, int64(128), child.batchSize)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make([]string, 0, 3)
			children := []*structReaderTestChild{
				{name: "first", events: &events},
				{name: "second", events: &events, seekErr: seekErr, loadErr: loadErr},
				{name: "third", events: &events, seekErr: errors.New("later seek failed"), loadErr: errors.New("later load failed")},
			}

			readers := make([]*ColumnReader, len(children))
			for i, child := range children {
				readers[i] = &ColumnReader{colReaderImpl: child}
			}

			reader := &structReader{children: readers}
			err := tt.call(reader)

			require.ErrorIs(t, err, tt.expectedErr, "the first child error should be returned")
			require.Equal(t, []string{"first", "second", "third"}, events)
			tt.check(t, children)
		})
	}
}

var _ colReaderImpl = (*structReaderTestChild)(nil)
