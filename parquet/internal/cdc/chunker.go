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

// Package cdc implements content-defined chunking, which splits Parquet data
// pages at boundaries derived from the data itself, so identical content yields
// identical pages even when it is written at different offsets.
package cdc

import (
	"fmt"
	"math/bits"

	"github.com/apache/arrow-go/v18/parquet"
)

// Chunker splits a column's data into content-defined chunks.
type Chunker struct {
	minSize int64
	maxSize int64
	mask    uint64
	tables  *[numGearTables][256]uint64
}

// NewChunker validates opts and returns a Chunker that uses them.
func NewChunker(opts parquet.CDCOptions) (*Chunker, error) {
	mask, err := calculateMask(opts.MinChunkSize, opts.MaxChunkSize, opts.NormLevel)
	if err != nil {
		return nil, err
	}
	return &Chunker{
		minSize: opts.MinChunkSize,
		maxSize: opts.MaxChunkSize,
		mask:    mask,
		tables:  gearTables(),
	}, nil
}

// calculateMask derives the boundary mask from the chunk size bounds, aiming
// for an average chunk size of (minSize+maxSize)/2 after skipping minSize bytes.
func calculateMask(minSize, maxSize int64, normLevel int) (uint64, error) {
	if minSize < 0 {
		return 0, fmt.Errorf("cdc: min chunk size must be non-negative, got %d", minSize)
	}
	if maxSize <= minSize {
		return 0, fmt.Errorf("cdc: max chunk size must be greater than min chunk size, got min %d and max %d", minSize, maxSize)
	}
	// Written as minSize + half the span so that large bounds cannot overflow.
	avgSize := minSize + (maxSize-minSize)/2
	target := uint64((avgSize - minSize) / numGearTables)
	maskBits := max(bits.Len64(target)-1, 0)
	effectiveBits := maskBits - normLevel
	if effectiveBits < 1 || effectiveBits > 63 {
		return 0, fmt.Errorf("cdc: the number of bits in the CDC mask must be between 1 and 63, got %d", effectiveBits)
	}
	return ^uint64(0) << (64 - effectiveBits), nil
}
