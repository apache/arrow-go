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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/utils"
	"github.com/apache/arrow-go/v18/parquet"
)

// DeltaByteArrayEncoder is an encoder for writing bytearrays which are delta encoded
// this is also known as incremental encoding or front compression. For each element
// in a sequence of strings, we store the prefix length of the previous entry plus the suffix
// see https://en.wikipedia.org/wiki/Incremental_encoding for a longer description.
//
// This is stored as a sequence of delta-encoded prefix lengths followed by the suffixes
// encoded as delta length byte arrays.
type DeltaByteArrayEncoder struct {
	encoder

	prefixEncoder *DeltaBitPackInt32Encoder
	suffixEncoder *DeltaLengthByteArrayEncoder

	lastVal parquet.ByteArray
}

const deltaByteArrayBatchSize = 256

func (enc *DeltaByteArrayEncoder) EstimatedDataEncodedSize() int64 {
	prefixEstimatedSize := int64(0)
	if enc.prefixEncoder != nil {
		prefixEstimatedSize = enc.prefixEncoder.EstimatedDataEncodedSize()
	}
	suffixEstimatedSize := int64(0)
	if enc.suffixEncoder != nil {
		suffixEstimatedSize = enc.suffixEncoder.EstimatedDataEncodedSize()
	}
	return prefixEstimatedSize + suffixEstimatedSize
}

func (enc *DeltaByteArrayEncoder) initEncoders() {
	enc.prefixEncoder = &DeltaBitPackInt32Encoder{
		encoder: newEncoderBase(enc.encoding, nil, enc.mem),
	}
	enc.suffixEncoder = &DeltaLengthByteArrayEncoder{
		newEncoderBase(enc.encoding, nil, enc.mem),
		&DeltaBitPackInt32Encoder{
			encoder: newEncoderBase(enc.encoding, nil, enc.mem),
		},
	}
}

// Type returns the underlying physical type this operates on, in this case ByteArrays only
func (DeltaByteArrayEncoder) Type() parquet.Type { return parquet.Types.ByteArray }

// Put writes a slice of ByteArrays to the encoder
func (enc *DeltaByteArrayEncoder) Put(in []parquet.ByteArray) {
	if len(in) == 0 {
		return
	}

	if enc.prefixEncoder == nil { // initialize our encoders if we haven't yet
		enc.initEncoders()
	}

	lastVal := enc.lastVal
	var prefixLengths [deltaByteArrayBatchSize]int32
	var suffixes [deltaByteArrayBatchSize]parquet.ByteArray
	for i := 0; i < len(in); i += deltaByteArrayBatchSize {
		batchSize := min(deltaByteArrayBatchSize, len(in)-i)
		for j := 0; j < batchSize; j++ {
			val := in[i+j]
			commonPrefixLength := 0
			maximumCommonPrefixLength := min(lastVal.Len(), val.Len())
			for commonPrefixLength < maximumCommonPrefixLength {
				if lastVal[commonPrefixLength] != val[commonPrefixLength] {
					break
				}
				commonPrefixLength++
			}

			lastVal = val
			prefixLengths[j] = int32(commonPrefixLength)
			suffixes[j] = val[commonPrefixLength:]
		}
		enc.suffixEncoder.Put(suffixes[:batchSize])
		enc.prefixEncoder.Put(prefixLengths[:batchSize])
	}

	// do the memcpy after the loops to keep a copy of the lastVal
	// we do a copy here so that we only copy and keep a reference
	// to the suffix, and aren't forcing the *entire* value to stay
	// in memory while we have this reference to just the suffix.
	enc.lastVal = append([]byte{}, lastVal...)
}

// PutSpaced is like Put, but assumes the data is already spaced for nulls and uses the bitmap provided and offset
// to compress the data before writing it without the null slots.
func (enc *DeltaByteArrayEncoder) PutSpaced(in []parquet.ByteArray, validBits []byte, validBitsOffset int64) {
	if validBits != nil {
		data := make([]parquet.ByteArray, len(in))
		nvalid := spacedCompress(in, data, validBits, validBitsOffset)
		enc.Put(data[:nvalid])
	} else {
		enc.Put(in)
	}
}

// Flush flushes any remaining data out and returns the finished encoded buffer.
// or returns nil and any error encountered during flushing.
func (enc *DeltaByteArrayEncoder) FlushValues() (Buffer, error) {
	if enc.prefixEncoder == nil {
		enc.initEncoders()
	}
	prefixBuf, err := enc.prefixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer prefixBuf.Release()

	suffixBuf, err := enc.suffixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer suffixBuf.Release()

	ret := bufferPool.Get().(*memory.Buffer)
	ret.ResizeNoShrink(prefixBuf.Len() + suffixBuf.Len())
	copy(ret.Bytes(), prefixBuf.Bytes())
	copy(ret.Bytes()[prefixBuf.Len():], suffixBuf.Bytes())
	return poolBuffer{ret}, nil
}

// DeltaByteArrayDecoder is a decoder for a column of data encoded using incremental or prefix encoding.
type DeltaByteArrayDecoder struct {
	*DeltaLengthByteArrayDecoder

	prefixLengths []int32
	lastVal       parquet.ByteArray
}

// Type returns the underlying physical type this decoder operates on, in this case ByteArrays only
func (DeltaByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.ByteArray
}

func (d *DeltaByteArrayDecoder) Allocator() memory.Allocator { return d.mem }

// SetData expects the passed in data to be the prefix lengths, followed by the
// blocks of suffix data in order to initialize the decoder.
func (d *DeltaByteArrayDecoder) SetData(nvalues int, data []byte) error {
	d.lastVal = nil
	prefixLenDec := DeltaBitPackInt32Decoder{
		decoder: newDecoderBase(d.encoding, d.descr),
		mem:     d.mem,
	}

	if err := prefixLenDec.SetData(nvalues, data); err != nil {
		return err
	}
	if nvalues < 0 || prefixLenDec.totalValues > uint64(nvalues) {
		return fmt.Errorf("parquet: delta prefix count %d exceeds value count %d", prefixLenDec.totalValues, nvalues)
	}

	d.prefixLengths = make([]int32, prefixLenDec.ValuesLeft())
	// decode all the prefix lengths first so we know how many bytes it took to get the
	// prefix lengths for nvalues
	decoded, err := prefixLenDec.Decode(d.prefixLengths)
	if err != nil {
		return err
	}
	if decoded != len(d.prefixLengths) {
		return errors.New("parquet: not enough delta byte array prefix lengths")
	}

	// now that we know how many bytes we needed for the prefix lengths, the rest are the
	// delta length byte array encoding.
	offset := prefixLenDec.bytesRead()
	if offset < 0 || offset > int64(len(data)) {
		return errors.New("parquet: invalid delta byte array suffix offset")
	}
	if err := d.DeltaLengthByteArrayDecoder.SetData(nvalues, data[offset:]); err != nil {
		return err
	}
	if len(d.prefixLengths) != d.nvals {
		return errors.New("parquet: delta prefix and suffix length counts do not match")
	}
	return nil
}

func (d *DeltaByteArrayDecoder) Discard(n int) (int, error) {
	n = min(n, d.nvals)
	if n == 0 {
		return 0, nil
	}

	remaining := n
	tmp := make([]parquet.ByteArray, 1)
	if d.lastVal == nil {
		if len(d.prefixLengths) == 0 || d.prefixLengths[0] != 0 {
			return 0, errors.New("parquet: first delta byte array prefix length must be zero")
		}
		if _, err := d.DeltaLengthByteArrayDecoder.Decode(tmp); err != nil {
			return 0, err
		}
		d.lastVal = tmp[0]
		d.prefixLengths = d.prefixLengths[1:]
		remaining--
	}

	var prefixLen int32
	for remaining > 0 {
		if len(d.prefixLengths) == 0 {
			return n - remaining, errors.New("parquet: not enough delta byte array prefix lengths")
		}
		prefixLen, d.prefixLengths = d.prefixLengths[0], d.prefixLengths[1:]
		if prefixLen < 0 || int(prefixLen) > len(d.lastVal) {
			return n - remaining, fmt.Errorf("parquet: invalid delta byte array prefix length %d", prefixLen)
		}
		prefix := d.lastVal[:prefixLen:prefixLen]

		if _, err := d.DeltaLengthByteArrayDecoder.Decode(tmp); err != nil {
			return n - remaining, err
		}

		if len(tmp[0]) == 0 {
			d.lastVal = prefix
		} else {
			d.lastVal = make([]byte, int(prefixLen)+len(tmp[0]))
			copy(d.lastVal, prefix)
			copy(d.lastVal[prefixLen:], tmp[0])
		}
		remaining--
	}

	return n, nil
}

// Decode decodes byte arrays into the slice provided and returns the number of values actually decoded
func (d *DeltaByteArrayDecoder) Decode(out []parquet.ByteArray) (int, error) {
	max := utils.Min(len(out), d.nvals)
	if max == 0 {
		return 0, nil
	}
	out = out[:max]

	var err error
	if d.lastVal == nil {
		if len(d.prefixLengths) == 0 || d.prefixLengths[0] != 0 {
			return 0, errors.New("parquet: first delta byte array prefix length must be zero")
		}
		_, err = d.DeltaLengthByteArrayDecoder.Decode(out[:1])
		if err != nil {
			return 0, err
		}
		d.lastVal = out[0]
		out = out[1:]
		d.prefixLengths = d.prefixLengths[1:]
	}

	var prefixLen int32
	suffixHolder := make([]parquet.ByteArray, 1)
	for len(out) > 0 {
		if len(d.prefixLengths) == 0 {
			return 0, errors.New("parquet: not enough delta byte array prefix lengths")
		}
		prefixLen, d.prefixLengths = d.prefixLengths[0], d.prefixLengths[1:]
		if prefixLen < 0 || int(prefixLen) > len(d.lastVal) {
			return 0, fmt.Errorf("parquet: invalid delta byte array prefix length %d", prefixLen)
		}

		prefix := d.lastVal[:prefixLen:prefixLen]
		_, err = d.DeltaLengthByteArrayDecoder.Decode(suffixHolder)
		if err != nil {
			return 0, err
		}

		if len(suffixHolder[0]) == 0 {
			d.lastVal = prefix
		} else {
			d.lastVal = make([]byte, int(prefixLen)+len(suffixHolder[0]))
			copy(d.lastVal, prefix)
			copy(d.lastVal[prefixLen:], suffixHolder[0])
		}
		out[0], out = d.lastVal, out[1:]
	}
	return max, nil
}

// DecodeSpaced is like decode, but the result is spaced out based on the bitmap provided.
func (d *DeltaByteArrayDecoder) DecodeSpaced(out []parquet.ByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := d.Decode(out[:toread])
	if err != nil {
		return values, err
	}
	if values != toread {
		return values, errors.New("parquet: number of values / definition levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}
