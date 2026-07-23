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

package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"
	"testing/iotest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

type rejectingAllocator struct{}

func (rejectingAllocator) Allocate(int) []byte {
	panic("unexpected allocation")
}

func (rejectingAllocator) Reallocate(int, []byte) []byte {
	panic("unexpected allocation")
}

func (rejectingAllocator) Free([]byte) {}

func messageReaderInput(t *testing.T, bodyLen int64) *bytes.Buffer {
	t.Helper()

	b := flatbuffers.NewBuilder(0)
	metadata := writeMessageFB(b, memory.DefaultAllocator, flatbuf.MessageHeaderNONE, 0, bodyLen, arrow.Metadata{})
	defer metadata.Release()

	var input bytes.Buffer
	require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(kIPCContToken)))
	require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(metadata.Len())))
	_, err := input.Write(metadata.Bytes())
	require.NoError(t, err)
	return &input
}

func TestMessageReaderRejectsInvalidMetadataLengths(t *testing.T) {
	for _, length := range []uint32{1, 3, 0x7fffffff, 0x80000000, 0xfffffffe} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			var input bytes.Buffer
			require.NoError(t, binary.Write(&input, binary.LittleEndian, length))

			r := NewMessageReader(&input)
			defer r.Release()
			_, err := r.Message()
			require.ErrorContains(t, err, "message metadata length")
		})
	}
}

func TestMessageReaderRejectsInvalidBodyLengths(t *testing.T) {
	type testCase struct {
		name    string
		length  int64
		message string
	}
	tests := []testCase{
		{name: "negative", length: -1, message: "invalid message body length"},
		{name: "over default limit", length: 1 << 30, message: "message body length 1073741824 exceeds limit 268435456"},
	}
	if strconv.IntSize == 32 {
		tests = append(tests, testCase{
			name: "over max int", length: int64(math.MaxInt32) + 1, message: "invalid message body length",
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewMessageReader(messageReaderInput(t, tt.length), WithAllocator(rejectingAllocator{}))
			defer r.Release()

			_, err := r.Message()
			require.ErrorContains(t, err, tt.message)
		})
	}
}

func TestMessageReaderRejectsTruncatedMessages(t *testing.T) {
	continuationToken := make([]byte, 4)
	binary.LittleEndian.PutUint32(continuationToken, uint32(kIPCContToken))

	metadataLength := append([]byte(nil), continuationToken...)
	metadataLength = binary.LittleEndian.AppendUint32(metadataLength, 4)

	tests := []struct {
		name  string
		input io.Reader
	}{
		{name: "message length", input: bytes.NewBuffer(continuationToken)},
		{
			name: "wrapped EOF after continuation token",
			input: io.MultiReader(
				bytes.NewReader(continuationToken),
				iotest.ErrReader(fmt.Errorf("wrapped: %w", io.EOF)),
			),
		},
		{name: "message metadata", input: bytes.NewBuffer(metadataLength)},
		{name: "message body", input: messageReaderInput(t, 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewMessageReader(tt.input)
			defer r.Release()

			_, err := r.Message()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.NotErrorIs(t, err, io.EOF)
		})
	}
}

func TestMessageReaderAllowsEndOfStreamAtMessageBoundary(t *testing.T) {
	r := NewMessageReader(bytes.NewReader(nil))
	defer r.Release()

	_, err := r.Message()
	require.ErrorIs(t, err, io.EOF)
}

func TestReaderReportsTruncatedMessage(t *testing.T) {
	input := writeRecordsIntoBuffer(t, 0)
	input.Truncate(input.Len() - 4)

	r, err := NewReader(input)
	require.NoError(t, err)
	defer r.Release()

	require.False(t, r.Next())
	require.ErrorIs(t, r.Err(), io.ErrUnexpectedEOF)
}

func TestBodySizeLimitConfig(t *testing.T) {
	require.Equal(t, defaultMaxBodySize, newConfig().maxBodySize)
	require.Zero(t, newConfig(WithBodySizeLimit(0)).maxBodySize)
	require.Equal(t, int64(1024), newConfig(WithBodySizeLimit(1024)).maxBodySize)
}

func TestValidateFileBlock(t *testing.T) {
	tests := []struct {
		name               string
		offset, body, size int64
		meta               int32
	}{
		{"negative offset", -1, 0, 16, 8},
		{"short metadata", 0, 0, 16, 3},
		{"negative body", 0, -1, 16, 8},
		{"past end", 8, 9, 24, 8},
		{"overflow", 1, math.MaxInt64, math.MaxInt64, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, validateFileBlock(tt.offset, tt.meta, tt.body, tt.size, 0, 0))
		})
	}
	require.NoError(t, validateFileBlock(8, 8, 8, 24, 0, 0))
}

func TestMessageReaderBodyInAllocator(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	const numRecords = 3
	buf := writeRecordsIntoBuffer(t, numRecords)
	r := NewMessageReader(buf, WithAllocator(mem))
	defer r.Release()

	msgs := make([]*Message, 0)
	for {
		m, err := r.Message()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		m.Retain()
		msgs = append(msgs, m)
	}
	if len(msgs) != numRecords+1 {
		t.Fatalf("expected %d messages but got %d", numRecords+1, len(msgs))
	}

	if mem.CurrentAlloc() <= 0 {
		t.Fatal("message bodies should have been allocated")
	}

	for _, m := range msgs {
		m.Release()
	}
}

func writeRecordsIntoBuffer(t *testing.T, numRecords int) *bytes.Buffer {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	s, recs := getTestRecords(mem, numRecords)
	buf := new(bytes.Buffer)
	w := NewWriter(buf, WithAllocator(mem), WithSchema(s))
	for _, rec := range recs {
		err := w.Write(rec)
		rec.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf
}

func getTestRecords(mem memory.Allocator, numRecords int) (*arrow.Schema, []arrow.RecordBatch) {
	meta := arrow.NewMetadata([]string{}, []string{})
	s := arrow.NewSchema([]arrow.Field{
		{Name: "test-col", Type: arrow.PrimitiveTypes.Int64},
	}, &meta)

	builder := array.NewRecordBuilder(mem, s)
	defer builder.Release()

	recs := make([]arrow.RecordBatch, numRecords)
	for i := 0; i < len(recs); i++ {
		col := builder.Field(0).(*array.Int64Builder)
		for i := 0; i < 10; i++ {
			col.Append(int64(i))
		}
		recs[i] = builder.NewRecordBatch()
	}

	return s, recs
}
