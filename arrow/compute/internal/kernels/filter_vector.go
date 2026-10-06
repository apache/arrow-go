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

//go:build go1.18

package kernels

func filterVectorInput(filterData []byte, filterOffset, length int64) ([]byte, bool) {
	if length < 64 || length%8 != 0 || filterOffset%8 != 0 {
		return nil, false
	}

	numBytes := length / 8
	filterByteOffset := filterOffset / 8
	if filterByteOffset < 0 || numBytes > int64(len(filterData)) ||
		filterByteOffset > int64(len(filterData))-numBytes {
		return nil, false
	}
	filterBytes := filterData[filterByteOffset : filterByteOffset+numBytes]

	const (
		sampleBytes = 64
		minMixed    = 4
	)
	mixedBytes := 0
	for i := 0; i < len(filterBytes) && i < sampleBytes; i++ {
		mask := filterBytes[i]
		if mask != 0 && mask != 0xff {
			mixedBytes++
			if mixedBytes == minMixed {
				return filterBytes, true
			}
		}
	}
	return nil, false
}
