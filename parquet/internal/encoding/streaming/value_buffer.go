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

// Package streaming provides the incremental value source used to read large
// Parquet data pages without materializing the whole uncompressed page
// (EnablePageStreaming). See apache/arrow-go#865.
package streaming

import "io"

// ValueBuffer is the incremental byte source a streaming decoder reads from, over
// one data page's value stream. A decoder indexes Bytes() directly, calls Advance
// as it consumes, and Fill only when short; the page Closes it when done.
type ValueBuffer interface {
	// Bytes returns the currently available bytes, valid until the next Fill.
	Bytes() []byte
	// Advance marks the first n bytes of the current window as consumed.
	Advance(n int)
	// Fill ensures at least need contiguous bytes are available (growing to fit an
	// oversized value), returning the window or io.ErrUnexpectedEOF if fewer remain.
	Fill(need int) ([]byte, error)
	io.Closer
}

// Decoder is implemented by the PLAIN decoders that can read incrementally from a
// ValueBuffer via SetSource, alongside the []byte-based SetData.
type Decoder interface {
	SetSource(nvals int, src ValueBuffer)
}

const defaultStreamBufferSize = 1 << 20

type streamBuffer struct {
	r       io.Reader
	onClose func() error
	buf     []byte
	off, n  int
}

// NewStreamBuffer returns a streaming ValueBuffer over r. onClose, if non-nil, runs
// on Close (the caller uses it to drain + close the underlying stream).
func NewStreamBuffer(r io.Reader, onClose func() error) ValueBuffer {
	return &streamBuffer{r: r, onClose: onClose, buf: make([]byte, defaultStreamBufferSize)}
}

func (s *streamBuffer) Bytes() []byte { return s.buf[s.off:s.n] }

func (s *streamBuffer) Advance(n int) { s.off += n }

func (s *streamBuffer) Close() error {
	if s.onClose != nil {
		return s.onClose()
	}
	return nil
}

func (s *streamBuffer) Fill(need int) ([]byte, error) {
	if s.n-s.off >= need {
		return s.buf[s.off:s.n], nil
	}

	// Slide the unconsumed tail to the front.
	if s.off > 0 {
		s.n = copy(s.buf, s.buf[s.off:s.n])
		s.off = 0
	}

	// Grow the backing storage if it cannot hold a single value of size need.
	if len(s.buf) < need {
		grown := make([]byte, need)
		copy(grown, s.buf[:s.n])
		s.buf = grown
	}

	// Fill the whole buffer, not just need: batches reads into ~buffer-sized
	// chunks regardless of the underlying reader's chunk size. Bounded because
	// the value stream is a finite page region.
	for s.n < len(s.buf) {
		m, err := s.r.Read(s.buf[s.n:])
		s.n += m
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	if s.n-s.off < need {
		return nil, io.ErrUnexpectedEOF
	}
	return s.buf[s.off:s.n], nil
}
