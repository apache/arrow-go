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
	"crypto/rand"
	"testing"
)

// BenchmarkUnpack32 drives BitReader.GetBatch through the packed-decode
// fast path. Running it with -cpuprofile exercises asynchronous signal
// unwinding inside the assembly kernels (GH-983).
func BenchmarkUnpack32(b *testing.B) {
	const batchSize = 512
	input := make([]byte, batchSize*4)
	if _, err := rand.Read(input); err != nil {
		b.Fatal(err)
	}
	output := make([]uint64, batchSize)
	reader := NewBitReader(bytes.NewReader(input))
	for b.Loop() {
		reader.Reset(bytes.NewReader(input))
		reader.GetBatch(32, output)
	}
}
