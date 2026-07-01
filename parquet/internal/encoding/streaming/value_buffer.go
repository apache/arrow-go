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

// ValueBuffer is the incremental byte source a streaming decoder reads over one
// data page's value stream.
type ValueBuffer interface {
	// Advance marks the first n bytes of the current window as consumed.
	Advance(n int)
	// Fill ensures at least need contiguous bytes, returning a window valid only
	// until the next call (io.ErrUnexpectedEOF if fewer remain). Use when skipping.
	Fill(need int) ([]byte, error)
	// FillOwned is like Fill but the returned bytes stay valid after later calls,
	// so a decoder can alias them instead of copying.
	FillOwned(need int) ([]byte, error)
	io.Closer
}

// Decoder is implemented by the PLAIN decoders that read from a ValueBuffer via
// SetSource, alongside the []byte-based SetData.
type Decoder interface {
	SetSource(nvals int, src ValueBuffer)
}

const defaultStreamBufferSize = 1 << 20

type streamBuffer struct {
	r       io.Reader
	onClose func() error
	buf     []byte
	off, n  int
	// shared: buf was handed out via FillOwned and may back live aliases, so it
	// must not be slid/overwritten.
	shared bool
}

// NewStreamBuffer returns a ValueBuffer over r; onClose, if non-nil, runs on Close.
func NewStreamBuffer(r io.Reader, onClose func() error) ValueBuffer {
	return &streamBuffer{r: r, onClose: onClose, buf: make([]byte, defaultStreamBufferSize)}
}

func (s *streamBuffer) Advance(n int) { s.off += n }

func (s *streamBuffer) Close() error {
	if s.onClose != nil {
		return s.onClose()
	}
	return nil
}

func (s *streamBuffer) Fill(need int) ([]byte, error)      { return s.fill(need, false) }
func (s *streamBuffer) FillOwned(need int) ([]byte, error) { return s.fill(need, true) }

func (s *streamBuffer) fill(need int, owned bool) ([]byte, error) {
	if s.n-s.off >= need {
		s.shared = s.shared || owned
		return s.buf[s.off:s.n], nil
	}

	switch {
	case s.shared || len(s.buf) < need:
		// buf is shared (live aliases) or too small: move the tail to a fresh one.
		size := len(s.buf)
		if size < need {
			size = need
		}
		fresh := make([]byte, size)
		s.n = copy(fresh, s.buf[s.off:s.n])
		s.off, s.buf = 0, fresh
	case s.off > 0:
		s.n = copy(s.buf, s.buf[s.off:s.n]) // reuse: slide to the front
		s.off = 0
	}

	// Fill the whole buffer, batching reads regardless of the reader's chunking;
	// bounded because the value stream is a finite page region.
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
	s.shared = owned
	return s.buf[s.off:s.n], nil
}
