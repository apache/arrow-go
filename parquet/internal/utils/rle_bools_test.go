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

package utils_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/utils"
)

func TestRleDecoderGetBatchBools(t *testing.T) {
	// 13 false, 19 true, 16 literal bits, 12 true, 8 literal bits.
	data := []byte{26, 0, 38, 1, 5, 0xa5, 0x3c, 24, 1, 3, 0xaa}
	const count = 68

	reference := utils.NewRleDecoder(bytes.NewReader(data), 1)
	want := make([]uint64, count)
	n, err := reference.GetBatch(want)
	if err != nil || n != count {
		t.Fatalf("reference decode: n=%d, err=%v", n, err)
	}

	for _, chunk := range []int{1, 2, 3, 7, 8, 9, 31, 64, count, count + 3} {
		t.Run(fmt.Sprintf("chunk_%d", chunk), func(t *testing.T) {
			dec := utils.NewRleDecoder(bytes.NewReader(data), 1)
			got := make([]bool, count)
			for i := range got {
				got[i] = true
			}
			for pos := 0; pos < count; {
				end := min(pos+chunk, count)
				n, err := dec.GetBatchBools(got[pos:end])
				if err != nil || n != end-pos {
					t.Fatalf("decode at %d: n=%d, err=%v", pos, n, err)
				}
				pos = end
			}
			for i, value := range got {
				if value != (want[i] != 0) {
					t.Fatalf("value %d: got %t, want %t", i, value, want[i] != 0)
				}
			}
			n, err := dec.GetBatchBools(make([]bool, 1))
			if n != 0 || err != nil {
				t.Fatalf("exhausted decoder: n=%d, err=%v", n, err)
			}
			dec.Reset(bytes.NewReader(data), 1)
			n, err = dec.GetBatchBools(make([]bool, count+3))
			if err != nil || n != count {
				t.Fatalf("reset decoder: n=%d, err=%v", n, err)
			}
		})
	}

	t.Run("mixed_with_discard", func(t *testing.T) {
		dec := utils.NewRleDecoder(bytes.NewReader(data), 1)
		first := make([]bool, 17)
		n, err := dec.GetBatchBools(first)
		if err != nil || n != len(first) {
			t.Fatalf("first batch: n=%d, err=%v", n, err)
		}
		if skipped := dec.Discard(13); skipped != 13 {
			t.Fatalf("discarded %d, want 13", skipped)
		}
		rest := make([]bool, count-30)
		n, err = dec.GetBatchBools(rest)
		if err != nil || n != len(rest) {
			t.Fatalf("last batch: n=%d, err=%v", n, err)
		}
		for i, value := range first {
			if value != (want[i] != 0) {
				t.Fatalf("before discard, value %d", i)
			}
		}
		for i, value := range rest {
			if value != (want[i+30] != 0) {
				t.Fatalf("after discard, value %d", i+30)
			}
		}
	})
}

func TestRleDecoderGetBatchBoolsRejectsNonBooleanWidth(t *testing.T) {
	dec := utils.NewRleDecoder(bytes.NewReader([]byte{16, 2}), 2)
	n, err := dec.GetBatchBools(make([]bool, 8))
	if n != 0 || err == nil {
		t.Fatalf("invalid width: n=%d, err=%v", n, err)
	}
}
