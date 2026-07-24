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
	"bytes"
	"errors"
	"io"
	"testing"
)

type readerReturningDataAndError struct {
	*bytes.Reader
	err      error
	returned bool
}

func (r *readerReturningDataAndError) Read(p []byte) (int, error) {
	if r.returned {
		return 0, r.err
	}
	r.returned = true
	n, _ := r.Reader.Read(p)
	return n, r.err
}

func TestBufferedReaderResetClearsPendingError(t *testing.T) {
	r := NewBufferedReader(&readerReturningDataAndError{
		Reader: bytes.NewReader([]byte("a")),
		err:    io.ErrUnexpectedEOF,
	}, 2)

	buf := make([]byte, 1)
	if n, err := r.Read(buf); n != 1 || err != nil || string(buf) != "a" {
		t.Fatalf("initial read = (%d, %v, %q), want (1, nil, %q)", n, err, buf, "a")
	}

	r.Reset(bytes.NewReader([]byte("b")))
	if n, err := r.Read(buf); n != 1 || err != nil || string(buf) != "b" {
		t.Fatalf("read after reset = (%d, %v, %q), want (1, nil, %q)", n, err, buf, "b")
	}
}

func TestBufferedReaderPeekReturnsAvailableBytesOnError(t *testing.T) {
	r := NewBufferedReader(bytes.NewReader([]byte("a")), 2)

	got, err := r.Peek(2)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Peek error = %v, want EOF", err)
	}
	if string(got) != "a" {
		t.Fatalf("Peek = %q, want %q", got, "a")
	}
}

func TestByteReaderSeekErrorPreservesPosition(t *testing.T) {
	r := NewByteReader([]byte("abc"))
	buf := make([]byte, 1)
	if n, err := r.Read(buf); n != 1 || err != nil {
		t.Fatalf("Read = (%d, %v), want (1, nil)", n, err)
	}

	if _, err := r.Seek(0, -1); err == nil {
		t.Fatal("Seek with invalid whence returned nil error")
	}
	if got, err := r.Peek(1); err != nil || string(got) != "b" {
		t.Fatalf("Peek after failed seek = (%q, %v), want (%q, nil)", got, err, "b")
	}
}

func TestByteReaderDiscardExactRemainingBytes(t *testing.T) {
	r := NewByteReader([]byte("abc"))
	if n, err := r.Discard(3); n != 3 || err != nil {
		t.Fatalf("Discard = (%d, %v), want (3, nil)", n, err)
	}
}
