// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
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
	"math"
	"testing"
)

func TestRleEncoderMaximumRepeatedRun(t *testing.T) {
	buf := make([]byte, 16)
	enc := NewRleEncoder(NewWriterAtBuffer(buf), 1)
	enc.curVal = 1
	enc.repCount = math.MaxInt32

	if n := enc.Flush(); n != 6 {
		t.Fatalf("encoded %d bytes, want 6", n)
	}
	want := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1}
	for i, value := range want {
		if buf[i] != value {
			t.Fatalf("byte %d = %#x, want %#x", i, buf[i], value)
		}
	}
}

func TestRleEncoderBatchSplitsMaximumRepeatedRun(t *testing.T) {
	buf := make([]byte, 16)
	enc := NewRleEncoder(NewWriterAtBuffer(buf), 1)
	enc.curVal = 1
	enc.repCount = math.MaxInt32 - 4

	n, err := enc.PutBatchLevels([]int16{1, 1, 1, 1, 1, 1})
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("encoded %d values, want 6", n)
	}
	if n := enc.Flush(); n != 8 {
		t.Fatalf("encoded %d bytes, want 8", n)
	}

	want := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f, 1, 4, 1}
	for i, value := range want {
		if buf[i] != value {
			t.Fatalf("byte %d = %#x, want %#x", i, buf[i], value)
		}
	}
}

func TestRleEncoderBatchReportsValuesBeforeFlushError(t *testing.T) {
	enc := NewRleEncoder(NewWriterAtBuffer(nil), 1)
	values := []int16{0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	n, err := enc.PutBatchLevels(values)
	if err == nil {
		t.Fatal("expected a write error")
	}
	if n != 8 {
		t.Fatalf("encoded %d values, want 8", n)
	}
}
