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

//go:build arm64
// +build arm64

package bitutil_test

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/stretchr/testify/assert"
)

func TestBitmapOpsPartialOverlap(t *testing.T) {
	const nbytes = 256

	rng := rand.New(rand.NewSource(18))
	leftInput := make([]byte, nbytes)
	rightInput := make([]byte, nbytes)
	_, _ = rng.Read(leftInput)
	_, _ = rng.Read(rightInput)

	for _, op := range []struct {
		name string
		fn   noAllocFn
		want func(byte, byte) byte
	}{
		{name: "and", fn: bitutil.BitmapAnd, want: func(left, right byte) byte { return left & right }},
		{name: "or", fn: bitutil.BitmapOr, want: func(left, right byte) byte { return left | right }},
		{name: "and-not", fn: bitutil.BitmapAndNot, want: func(left, right byte) byte { return left &^ right }},
		{name: "xor", fn: bitutil.BitmapXor, want: func(left, right byte) byte { return left ^ right }},
		{name: "xnor", fn: bitutil.BitmapXnor, want: func(left, right byte) byte { return ^(left ^ right) }},
	} {
		for _, input := range []string{"left", "right"} {
			t.Run(op.name+"/"+input, func(t *testing.T) {
				backing := make([]byte, nbytes+1)
				_, _ = rng.Read(backing)
				wantBacking := append([]byte(nil), backing...)
				left, right := append([]byte(nil), leftInput...), append([]byte(nil), rightInput...)
				wantLeft, wantRight := left, right
				if input == "left" {
					left = backing[:nbytes]
					wantLeft = wantBacking[:nbytes]
				} else {
					right = backing[:nbytes]
					wantRight = wantBacking[:nbytes]
				}

				wantOut := wantBacking[1:]
				wantOut[0] = op.want(wantLeft[0], wantRight[0])
				wordEnd := 1 + (len(wantOut)-2)/8*8
				for i := 1; i < wordEnd; i += 8 {
					end := i + 8
					var expected [8]byte
					for j := i; j < end; j++ {
						expected[j-i] = op.want(wantLeft[j], wantRight[j])
					}
					copy(wantOut[i:end], expected[:end-i])
				}
				for i := wordEnd; i < len(wantOut)-1; i++ {
					wantOut[i] = op.want(wantLeft[i], wantRight[i])
				}
				wantOut[len(wantOut)-1] = op.want(wantLeft[len(wantOut)-1], wantRight[len(wantOut)-1])

				op.fn(left, right, 0, 0, backing[1:], 0, nbytes*8)
				assert.Equal(t, wantBacking, backing)
			})
		}
	}
}
