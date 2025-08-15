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

package utils

import (
	"github.com/apache/arrow-go/v18/internal/bitutils"
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
	"golang.org/x/xerrors"
)

func getspaced[T parquet.ColumnTypes | uint64](r *RleDecoder, dc DictionaryConverter, vals []T, batchSize, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	if nullCount == batchSize {
		dc.FillZero(vals[:batchSize])
		return batchSize, nil
	}

	read := 0
	remain := batchSize - nullCount

	const bufferSize = 1024
	var indexbuffer [bufferSize]IndexType

	// assume no bits to start
	bitReader := bitutils.NewBitRunReader(validBits, validBitsOffset, int64(batchSize))
	validRun := bitReader.NextRun()
	for read < batchSize {
		if validRun.Len == 0 {
			validRun = bitReader.NextRun()
		}

		if !validRun.Set {
			dc.FillZero(vals[:int(validRun.Len)])
			vals = vals[int(validRun.Len):]
			read += int(validRun.Len)
			validRun.Len = 0
			continue
		}

		if r.repCount == 0 && r.litCount == 0 {
			if !r.Next() {
				return read, nil
			}
		}

		var batch int
		switch {
		case r.repCount > 0:
			batch, remain, validRun = r.consumeRepeatCounts(read, batchSize, remain, validRun, bitReader)
			current := IndexType(r.curVal)
			if !dc.IsValid(current) {
				return read, nil
			}
			dc.Fill(vals[:batch], current)
		case r.litCount > 0:
			var (
				litread int
				skipped int
				err     error
			)
			litread, skipped, validRun, err = consumeLiterals(r, dc, vals, remain, indexbuffer[:], validRun, bitReader)
			if err != nil {
				return read, err
			}
			batch = litread + skipped
			remain -= litread
		}

		vals = vals[batch:]
		read += batch
	}
	return read, nil
}

func consumeLiterals[T parquet.ColumnTypes | uint64](r *RleDecoder, dc DictionaryConverter, vals []T, remain int, buf []IndexType, run bitutils.BitRun, bitRdr bitutils.BitRunReader) (int, int, bitutils.BitRun, error) {
	batch := utils.Min(utils.Min(remain, int(r.litCount)), len(buf))
	buf = buf[:batch]

	n, _ := r.r.GetBatchIndex(uint(r.bitWidth), buf)
	if n != batch {
		return 0, 0, run, xerrors.New("was not able to retrieve correct number of indexes")
	}

	if !dc.IsValid(buf...) {
		return 0, 0, run, xerrors.New("invalid index values found for dictionary converter")
	}

	var (
		read    int
		skipped int
	)
	for read < batch {
		if run.Set {
			updateSize := utils.Min(batch-read, int(run.Len))
			if err := dc.Copy(vals, buf[read:read+updateSize]); err != nil {
				return 0, 0, run, err
			}
			read += updateSize
			vals = vals[updateSize:]
			run.Len -= int64(updateSize)
		} else {
			dc.FillZero(vals[:int(run.Len)])
			vals = vals[int(run.Len):]
			skipped += int(run.Len)
			run.Len = 0
		}
		if run.Len == 0 {
			run = bitRdr.NextRun()
		}
	}
	r.litCount -= int32(batch)
	return read, skipped, run, nil
}
