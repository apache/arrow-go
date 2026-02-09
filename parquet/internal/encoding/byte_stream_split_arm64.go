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

//go:build !noasm

package encoding

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/parquet/internal/debug"
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.ARM64.HasASIMD {
		decodeByteStreamSplitBatchWidth4InByteOrder = decodeByteStreamSplitBatchWidth4NEON
		decodeByteStreamSplitBatchWidth8InByteOrder = decodeByteStreamSplitBatchWidth8NEON
	}
}

//go:noescape
func _decodeByteStreamSplitWidth4NEON(data, out unsafe.Pointer, nValues, stride int)

//go:noescape
func _decodeByteStreamSplitWidth8NEON(data, out unsafe.Pointer, nValues, stride int)

func decodeByteStreamSplitBatchWidth4NEON(data []byte, nValues, stride int, out []byte) {
	if nValues == 0 {
		return
	}
	const width = 4
	debug.Assert(len(out) >= nValues*width, fmt.Sprintf("not enough space in output buffer for decoding, out: %d bytes, data: %d bytes", len(out), len(data)))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
	_decodeByteStreamSplitWidth4NEON(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
}

func decodeByteStreamSplitBatchWidth8NEON(data []byte, nValues, stride int, out []byte) {
	if nValues == 0 {
		return
	}
	const width = 8
	debug.Assert(len(out) >= nValues*width, fmt.Sprintf("not enough space in output buffer for decoding, out: %d bytes, data: %d bytes", len(out), len(data)))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
	_decodeByteStreamSplitWidth8NEON(unsafe.Pointer(&data[0]), unsafe.Pointer(&out[0]), nValues, stride)
}
