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

package encoding

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/internal/bitutils"
	"github.com/apache/arrow-go/v18/parquet"
)

type PlainEncoder[T fixedLenTypes] struct {
	encoder

	bitSetReader bitutils.SetBitRunReader
}

func (enc *PlainEncoder[T]) Put(in []T) {
	writeLE(&enc.encoder, in)
}

func (enc *PlainEncoder[T]) PutSpaced(in []T, validBits []byte, validBitsOffset int64) {
	nbytes := requiredBytes[T](len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = bitutils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

func (PlainEncoder[T]) Type() parquet.Type {
	return parquet.GetColumnType[T]()
}

type PlainDecoder[T fixedLenTypes] struct {
	decoder

	bitSetReader bitutils.SetBitRunReader
}

func (PlainDecoder[T]) Type() parquet.Type {
	return parquet.GetColumnType[T]()
}

func (dec *PlainDecoder[T]) Discard(n int) (int, error) {
	n = min(n, dec.nvals)
	nbytes := requiredBytes[T](n)
	if nbytes > len(dec.data) || nbytes > math.MaxInt32 {
		return 0, fmt.Errorf("parquet: eof exception discard plain %s, nvals: %d, nbytes: %d, datalen: %d",
			dec.Type(), n, nbytes, len(dec.data))
	}

	dec.data = dec.data[nbytes:]
	dec.nvals -= n
	return n, nil
}

func (dec *PlainDecoder[T]) Decode(out []T) (int, error) {
	max := min(len(out), dec.nvals)
	nbytes := requiredBytes[T](max)
	if nbytes > len(dec.data) || nbytes > math.MaxInt32 {
		return 0, fmt.Errorf("parquet: eof exception decode plain %s, nvals: %d, nbytes: %d, datalen: %d",
			dec.Type(), max, nbytes, len(dec.data))
	}

	copyFrom(out[:max], dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

func (dec *PlainDecoder[T]) DecodeSpaced(out []T, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	if nullCount == 0 {
		if err := dec.decodeDense(out, len(out)); err != nil {
			return 0, err
		}
		return len(out), nil
	}

	toread := len(out) - nullCount
	if toread == 0 {
		return len(out), nil
	}

	if dec.bitSetReader == nil {
		dec.bitSetReader = bitutils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(len(out)))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(len(out)))
	}

	const maxDirectDecodeRuns = 8
	var validRuns [maxDirectDecodeRuns]bitutils.SetBitRun
	runCount := 0
	decodedPos := int64(0)
	needsExpansion := false
	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		if runCount == len(validRuns) {
			return dec.decodeSpacedDense(out, nullCount, validBits, validBitsOffset)
		}
		validRuns[runCount] = run
		runCount++
		densePos := int64(toread) - decodedPos - run.Length
		if run.Pos != densePos {
			needsExpansion = true
		}
		decodedPos += run.Length
		if decodedPos >= int64(toread) {
			break
		}
	}

	if decodedPos != int64(toread) {
		return dec.decodeSpacedDense(out, nullCount, validBits, validBitsOffset)
	}
	if !needsExpansion {
		if err := dec.decodeDense(out, toread); err != nil {
			return 0, err
		}
		return len(out), nil
	}

	for i := runCount - 1; i >= 0; i-- {
		run := validRuns[i]
		values, err := dec.Decode(out[int(run.Pos):int(run.Pos+run.Length)])
		if err != nil {
			return 0, err
		}
		if values != int(run.Length) {
			return 0, errors.New("parquet: number of values / definition levels read did not match")
		}
	}
	return len(out), nil
}

func (dec *PlainDecoder[T]) decodeSpacedDense(out []T, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	if err := dec.decodeDense(out, toread); err != nil {
		return 0, err
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = bitutils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}

func (dec *PlainDecoder[T]) decodeDense(out []T, toread int) error {
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return err
	}
	if values != toread {
		return errors.New("parquet: number of values / definition levels read did not match")
	}
	return nil
}

type (
	PlainInt32Encoder   = PlainEncoder[int32]
	PlainInt32Decoder   = PlainDecoder[int32]
	PlainInt64Encoder   = PlainEncoder[int64]
	PlainInt64Decoder   = PlainDecoder[int64]
	PlainFloat32Encoder = PlainEncoder[float32]
	PlainFloat32Decoder = PlainDecoder[float32]
	PlainFloat64Encoder = PlainEncoder[float64]
	PlainFloat64Decoder = PlainDecoder[float64]
	PlainInt96Encoder   = PlainEncoder[parquet.Int96]
	PlainInt96Decoder   = PlainDecoder[parquet.Int96]
)
